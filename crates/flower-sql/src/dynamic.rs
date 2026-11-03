//! Request-owned binding metadata. Reuses deployed schema interiors without
//! leaking parameter names, projection names or schemas per request.

use crate::{
    Parameters, Plan, compatible, inner, invalid,
    parse::{Expr, Predicate},
};
use alloc::{
    collections::{BTreeMap, BTreeSet},
    format,
    string::String,
};
use flower_sdk::{Result, SqlBudget, Value, error::codes, fail, schema::Schema};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Parameter {
    pub schema: Schema,
    contextual: bool,
}

fn inferred(value: &Value) -> Schema {
    match value {
        Value::Null => Schema::Null,
        Value::Bool(_) => Schema::Boolean,
        Value::Number(_) => Schema::Number {
            min: None,
            max: None,
        },
        Value::String(_) => Schema::String {
            min_length: None,
            max_length: None,
            pattern: None,
        },
        Value::Array(_) | Value::Object(_) => Schema::Json,
    }
}

fn logical(schema: Schema) -> Schema {
    match inner(schema) {
        Schema::String { .. } | Schema::Enum { .. } => Schema::String {
            min_length: None,
            max_length: None,
            pattern: None,
        },
        Schema::Int { .. } | Schema::Number { .. } => Schema::Number {
            min: None,
            max: None,
        },
        schema => schema,
    }
}

impl Plan {
    /// Bind request-time SQL to one deployed collection. Parameters are inferred
    /// from their values and field-comparison context (including exact decimals).
    /// Output objects follow the projection, with missing/redacted fields omitted.
    pub fn bind_dynamic(
        statement: &str,
        key: Schema,
        value: Schema,
        params: &Value,
        budget: SqlBudget,
        redactable: bool,
    ) -> Result<Self> {
        params.validate()?;
        let mut types = BTreeMap::new();
        match params {
            Value::Null => {}
            Value::Object(values) if values.len() <= 64 => {
                for (name, value) in values {
                    types.insert(
                        name.clone(),
                        Parameter {
                            schema: inferred(value),
                            contextual: false,
                        },
                    );
                }
            }
            _ => {
                return Err(fail(
                    codes::INVALID_ARGUMENT,
                    "SQL parameters require an object with at most 64 members (or null for no parameters)",
                ));
            }
        }
        let plan = Self::build(
            statement,
            key,
            value,
            Parameters::Inferred(types),
            Schema::Json,
            budget,
            redactable,
        )?;
        plan.validate_params(params)?;
        Ok(plan)
    }

    pub(super) fn infer_parameters(&mut self) -> Result<()> {
        let mut used = BTreeSet::new();
        for (_, expr) in &self.ast.projection {
            observe(expr, &mut used);
        }
        for (expr, _) in &self.ast.order {
            observe(expr, &mut used);
        }
        observe(&self.ast.limit, &mut used);
        if let Some(predicate) = self.ast.predicate.clone() {
            self.infer_predicate(&predicate, &mut used)?;
        }
        let Parameters::Inferred(params) = &self.params else {
            return Ok(());
        };
        if used.len() != params.len() || used.iter().any(|name| !params.contains_key(name)) {
            return Err(fail(
                codes::INVALID_ARGUMENT,
                "SQL parameters must match exactly the names used by the statement",
            ));
        }
        Ok(())
    }

    fn infer_predicate(
        &mut self,
        predicate: &Predicate,
        used: &mut BTreeSet<String>,
    ) -> Result<()> {
        match predicate {
            Predicate::And(a, b) | Predicate::Or(a, b) => {
                self.infer_predicate(a, used)?;
                self.infer_predicate(b, used)
            }
            Predicate::Not(p) => self.infer_predicate(p, used),
            Predicate::IsNull(expr, _) => {
                observe(expr, used);
                Ok(())
            }
            Predicate::Compare(a, _, b) => {
                observe(a, used);
                observe(b, used);
                self.infer_side(a, b)?;
                self.infer_side(b, a)
            }
        }
    }

    fn infer_side(&mut self, a: &Expr, b: &Expr) -> Result<()> {
        let (Expr::Param(name), Expr::Path(_)) = (a, b) else {
            return Ok(());
        };
        let expected = logical(self.expr_schema(b)?.0);
        if expected == Schema::Null {
            return Ok(());
        }
        let Parameters::Inferred(params) = &mut self.params else {
            return Ok(());
        };
        let param = params.get_mut(name).ok_or_else(|| {
            fail(
                codes::INVALID_ARGUMENT,
                format!("missing SQL parameter `:{name}`"),
            )
        })?;
        if param.contextual && !compatible(param.schema, expected) {
            return Err(invalid(format!(
                "parameter `:{name}` has incompatible field contexts"
            )));
        }
        param.schema = expected;
        param.contextual = true;
        Ok(())
    }

    pub(super) fn validate_dynamic_params(&self, values: &Value) -> Result<()> {
        values.validate()?;
        let Parameters::Inferred(params) = &self.params else {
            return Ok(());
        };
        let values = match values {
            Value::Null if params.is_empty() => return Ok(()),
            Value::Object(values) if values.len() == params.len() => values,
            _ => {
                return Err(fail(
                    codes::INVALID_ARGUMENT,
                    "SQL parameter names do not match the bound statement",
                ));
            }
        };
        for (name, param) in params {
            let value = values.get(name).ok_or_else(|| {
                fail(
                    codes::INVALID_ARGUMENT,
                    format!("missing SQL parameter `:{name}`"),
                )
            })?;
            if *value != Value::Null {
                param
                    .schema
                    .parse(value)
                    .map_err(|e| e.invalid_argument(&format!("SQL parameter `:{name}`")))?;
            }
        }
        Ok(())
    }
}

fn observe(expr: &Expr, used: &mut BTreeSet<String>) {
    if let Expr::Param(name) = expr {
        used.insert(name.clone());
    }
}
