//! Equality access paths whose coverage follows from required conjuncts.
//! Legacy indexes omit missing/null fields, so every component must have an
//! equality constraint. Decimal equality is logical rather than byte equality
//! ("07" equals "7"); those fields and keys deliberately keep the scan path.

use alloc::{string::String, vec::Vec};
use flower_sdk::{Result, Value, schema::Schema};

use crate::{
    Plan, invalid,
    parse::{Compare, Expr, Part, Predicate},
};

/// A local index from the deployed collection catalog.
#[derive(Clone, Debug)]
pub struct Index {
    /// Index name, resolved by the host within the source collection.
    pub name: String,
    /// Every indexed wire field, in stored component order.
    pub fields: Vec<String>,
}

/// A plan's parameter-bound read request. SQL still applies the entire
/// predicate after policy filtering, followed by logical sorting and LIMIT.
#[derive(Clone, Debug, PartialEq)]
pub enum Access<'a> {
    /// Explicitly permitted bounded full collection scan.
    CollectionScan,
    /// At most one staged-or-committed row, including absent-key observation.
    PrimaryKey(&'a Value),
    /// A complete equality bucket. It may contain arbitrarily many rows.
    IndexEquality {
        index: &'a str,
        components: Vec<&'a Value>,
    },
    /// A required equality compares with NULL and cannot be TRUE.
    Empty,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Path {
    Scan,
    Key(Expr),
    Index {
        name: String,
        fields: Vec<String>,
        values: Vec<Expr>,
    },
}

fn equality<'a>(predicate: &'a Predicate, field: &str) -> Option<&'a Expr> {
    fn side<'a>(left: &Expr, right: &'a Expr, field: &str) -> Option<&'a Expr> {
        if matches!(left, Expr::Path(parts) if matches!(parts.as_slice(), [Part::Field(name)] if name == field))
            && matches!(right, Expr::Param(_) | Expr::Literal(_))
        {
            Some(right)
        } else {
            None
        }
    }
    match predicate {
        Predicate::And(a, b) => equality(a, field).or_else(|| equality(b, field)),
        Predicate::Compare(a, Compare::Eq, b) => side(a, b, field).or_else(|| side(b, a, field)),
        _ => None, // Equalities inside OR/NOT are not required conjuncts.
    }
}

fn exact_bytes(schema: Schema, tuple_key: bool) -> bool {
    match schema {
        Schema::Nullable { inner } => exact_bytes(*inner, tuple_key),
        Schema::String { .. }
        | Schema::Enum { .. }
        | Schema::Boolean
        | Schema::Int { .. }
        | Schema::Number { .. }
        | Schema::Null => true,
        Schema::Tuple { elements } if tuple_key => elements.iter().all(|s| exact_bytes(*s, true)),
        _ => false,
    }
}

fn value<'a>(expr: &'a Expr, params: &'a Value) -> Result<&'a Value> {
    match expr {
        Expr::Literal(value) => Ok(value),
        Expr::Param(name) => params
            .as_object()
            .and_then(|p| p.get(name))
            .ok_or_else(|| invalid("missing bound access parameter")),
        _ => Err(invalid("access selector must be a literal or parameter")),
    }
}

impl Plan {
    /// Choose a policy-equivalent access path from the deployed catalog.
    /// `allow_scan = false` rejects deployment when no safe path exists.
    /// Choosing indexes by name makes the result independent of catalog order.
    pub fn with_indexes(mut self, indexes: &[Index], allow_scan: bool) -> Result<Self> {
        self.access = Path::Scan;
        if let Some(predicate) = &self.ast.predicate {
            if exact_bytes(self.key, true)
                && let Some(expr) = equality(predicate, "_key")
            {
                self.access = Path::Key(expr.clone());
            } else if let Schema::Object { fields } = self.value {
                let mut selected: Option<(&Index, Vec<Expr>)> = None;
                for index in indexes {
                    if index.fields.is_empty() || index.fields.len() > 64 {
                        continue;
                    }
                    let mut values = Vec::new();
                    for name in &index.fields {
                        let Some(field) = fields.iter().find(|f| f.name == name) else {
                            break;
                        };
                        if !exact_bytes(field.schema, false) {
                            break;
                        }
                        let Some(expr) = equality(predicate, name) else {
                            break;
                        };
                        values.push(expr.clone());
                    }
                    if values.len() == index.fields.len()
                        && selected
                            .as_ref()
                            .is_none_or(|(best, _)| index.name < best.name)
                    {
                        selected = Some((index, values));
                    }
                }
                if let Some((index, values)) = selected {
                    self.access = Path::Index {
                        name: index.name.clone(),
                        fields: index.fields.clone(),
                        values,
                    };
                }
            }
        }
        if !allow_scan && self.access == Path::Scan {
            return Err(invalid(
                "SQL requires an index: no exact primary-key or complete index equality path is proven",
            ));
        }
        Ok(self)
    }

    /// Bind the chosen access selectors. No SQL text is accepted at execution.
    pub fn access<'a>(&'a self, params: &'a Value) -> Result<Access<'a>> {
        self.validate_params(params)?;
        match &self.access {
            Path::Scan => Ok(Access::CollectionScan),
            Path::Key(expr) => match value(expr, params)? {
                Value::Null => Ok(Access::Empty),
                key => Ok(Access::PrimaryKey(key)),
            },
            Path::Index { name, values, .. } => {
                let mut components = Vec::new();
                for expr in values {
                    let v = value(expr, params)?;
                    if *v == Value::Null {
                        return Ok(Access::Empty);
                    }
                    components.push(v);
                }
                Ok(Access::IndexEquality {
                    index: name,
                    components,
                })
            }
        }
    }
}
