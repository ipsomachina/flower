//! A deliberately small, read-only SQL dialect. Parsing and binding happen at
//! deployment. Evaluation sees only policy-filtered rows supplied by the host.
//! All scans, expression work and retained bytes have explicit ceilings.

#![no_std]
extern crate alloc;

mod access;
mod dynamic;
mod parse;
pub use access::{Access, Index};

use alloc::{
    collections::BTreeMap,
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::cmp::Ordering;
use flower_sdk::{Failure, Result, SqlBudget, Value, fail, schema::Schema};
use parse::{Ast, Expr, Predicate};

pub use flower_sdk::error::codes::{SQL_BUDGET, SQL_INVALID};

fn invalid(message: impl Into<String>) -> Failure {
    fail(SQL_INVALID, message)
}
fn exhausted(what: &str) -> Failure {
    let mut failure = fail(SQL_BUDGET, format!("SQL {what} budget exceeded"));
    failure.details = Some(Value::Object(BTreeMap::from([(
        String::from("budget"),
        Value::from(what),
    )])));
    failure
}

/// A deployment-bound plan. Access paths select candidates; filtering, logical
/// ordering and policy semantics remain independent of storage order.
#[derive(Clone, Debug, PartialEq)]
pub struct Plan {
    ast: Ast,
    params: Parameters,
    row: Schema,
    key: Schema,
    value: Schema,
    budget: SqlBudget,
    access: access::Path,
}

#[derive(Clone, Debug, PartialEq)]
enum Parameters {
    Declared(Schema),
    Inferred(BTreeMap<String, dynamic::Parameter>),
}

/// Validate execution ceilings before parsing a request or declaring an endpoint.
pub fn validate_budget(budget: SqlBudget) -> Result<()> {
    if budget.candidates == 0
        || budget.candidates > 10_000
        || budget.bytes == 0
        || budget.bytes > 64 * 1024 * 1024
        || budget.work == 0
        || budget.work > 10_000_000
    {
        return Err(invalid(
            "SQL budgets must be positive; maxima: 10000 candidates, 64MiB, 10000000 work",
        ));
    }
    Ok(())
}

/// Logical bytes charged for binding request-time SQL and its parameters.
/// Inputs must have passed Value's ordinary depth/validity checks.
pub fn binding_work(statement: &str, params: &Value) -> u64 {
    statement
        .len()
        .saturating_mul(2)
        .saturating_add(payload_size(params)) as u64
}

/// The local collection named by a syntactically valid statement.
pub fn collection(statement: &str) -> Result<String> {
    Ok(parse::parse(statement)?.collection)
}

fn inner(schema: Schema) -> Schema {
    match schema {
        Schema::Nullable { inner: value } => inner(*value),
        _ => schema,
    }
}

fn numeric(schema: Schema) -> bool {
    matches!(inner(schema), Schema::Number { .. } | Schema::Int { .. })
}

fn text(schema: Schema) -> bool {
    matches!(inner(schema), Schema::String { .. } | Schema::Enum { .. })
}

fn compatible(a: Schema, b: Schema) -> bool {
    let (a, b) = (inner(a), inner(b));
    a == Schema::Null
        || b == Schema::Null
        || a == b
        || (numeric(a) && numeric(b))
        || (text(a) && text(b))
}

fn ordered(schema: Schema) -> bool {
    match inner(schema) {
        Schema::Null
        | Schema::Boolean
        | Schema::Int { .. }
        | Schema::Number { .. }
        | Schema::String { .. }
        | Schema::Enum { .. }
        | Schema::DecimalI64
        | Schema::DecimalU64 => true,
        Schema::Tuple { elements } => elements.iter().all(|s| ordered(*s)),
        _ => false,
    }
}

// Bytes plus value nodes: string and tuple comparisons pay for their input,
// rather than treating a megabyte-long common prefix as one unit of work.
fn weight(value: &Value) -> u32 {
    match value {
        Value::String(s) => (s.len() as u32).saturating_add(1),
        Value::Array(items) => items.iter().fold(1u32, |n, v| n.saturating_add(weight(v))),
        Value::Object(map) => map.iter().fold(1u32, |n, (k, v)| {
            n.saturating_add(k.len() as u32).saturating_add(weight(v))
        }),
        _ => 1,
    }
}

// The tagged binary payload size, computed before cloning projected values.
// Framing follows Value's stable ABI: tags, u32 counts/lengths and f64 payloads.
fn payload_size(value: &Value) -> usize {
    match value {
        Value::Null | Value::Bool(_) => 1,
        Value::Number(_) => 9,
        Value::String(s) => 5usize.saturating_add(s.len()),
        Value::Array(items) => items
            .iter()
            .fold(5usize, |n, v| n.saturating_add(payload_size(v))),
        Value::Object(map) => map.iter().fold(5usize, |n, (k, v)| {
            n.saturating_add(4)
                .saturating_add(k.len())
                .saturating_add(payload_size(v))
        }),
    }
}

impl Plan {
    /// Parse and bind to an explicit catalog and parameter/result contracts.
    /// `redactable` makes value projections optional, because the collection's
    /// field policy may omit even a schema-required stored member.
    pub fn bind(
        statement: &str,
        key: Schema,
        value: Schema,
        params: Schema,
        row: Schema,
        budget: SqlBudget,
        redactable: bool,
    ) -> Result<Self> {
        Self::build(
            statement,
            key,
            value,
            Parameters::Declared(params),
            row,
            budget,
            redactable,
        )
    }

    fn build(
        statement: &str,
        key: Schema,
        value: Schema,
        params: Parameters,
        row: Schema,
        budget: SqlBudget,
        redactable: bool,
    ) -> Result<Self> {
        validate_budget(budget)?;
        if matches!(params, Parameters::Declared(schema) if !matches!(schema,Schema::Object { .. } | Schema::Null))
        {
            return Err(invalid(
                "SQL parameters require a closed object (or unit for no parameters)",
            ));
        }
        let mut ast = parse::parse(statement)?;
        if let Schema::Object { fields } = value
            && fields.iter().any(|f| f.name == "_key")
        {
            return Err(invalid("SQL reserves `_key` for the collection key"));
        }
        if ast.star {
            let Schema::Object { fields } = value else {
                return Err(invalid("SELECT * requires an object collection"));
            };
            if fields.len() > 64 {
                return Err(invalid("SELECT * exceeds 64 projected columns"));
            }
            ast.projection = fields
                .iter()
                .map(|f| {
                    (
                        f.name.to_string(),
                        Expr::Path(vec![parse::Part::Field(f.name.to_string())]),
                    )
                })
                .collect();
        }
        let mut plan = Self {
            ast,
            key,
            value,
            params,
            row,
            budget,
            access: access::Path::Scan,
        };
        if matches!(plan.params, Parameters::Inferred(_)) {
            plan.infer_parameters()?;
            for (_, expr) in &plan.ast.projection {
                plan.expr_schema(expr)?;
            }
        } else {
            let Schema::Object { fields } = row else {
                return Err(invalid("SQL rows require an explicit object contract"));
            };
            if fields.len() != plan.ast.projection.len() {
                return Err(invalid(
                    "SQL projection and row contract must name the same columns",
                ));
            }
            for (name, expr) in &plan.ast.projection {
                let (schema, absent, nullable_parent) = plan.expr_schema(expr)?;
                let field = fields.iter().find(|f| f.name == name).ok_or_else(|| {
                    invalid(format!("unknown projected column `{name}` in row contract"))
                })?;
                let may_redact = redactable && expr.reads_value();
                let fits = match expr {
                    Expr::Literal(v) => field.schema.is(v),
                    _ => field.schema.accepts(&schema),
                };
                if (!field.optional && (absent || may_redact))
                    || !fits
                    || (nullable_parent && !field.schema.accepts(&Schema::Null))
                {
                    return Err(invalid(format!(
                        "column `{name}` does not fit its row contract; optional/redacted fields need Field<T> or Omittable<T>"
                    )));
                }
            }
        }
        if let Some(predicate) = &plan.ast.predicate {
            plan.bind_predicate(predicate)?;
        }
        for (expr, _) in &plan.ast.order {
            if !ordered(plan.expr_schema(expr)?.0) {
                return Err(invalid(
                    "ORDER BY requires a logically ordered scalar or tuple",
                ));
            }
        }
        if !ordered(key) {
            return Err(invalid(
                "SQL requires a logically ordered key for deterministic ties",
            ));
        }
        let limit_schema = plan.expr_schema(&plan.ast.limit)?.0;
        if !numeric(limit_schema)
            || !matches!(
                plan.ast.limit,
                Expr::Param(_) | Expr::Literal(Value::Number(_))
            )
        {
            return Err(invalid(
                "LIMIT requires an integer literal or named numeric parameter",
            ));
        }
        if matches!(plan.ast.limit, Expr::Literal(_)) {
            plan.limit(&Value::Null)?;
        }
        Ok(plan)
    }

    /// The collection scanned by this plan.
    pub fn collection(&self) -> &str {
        &self.ast.collection
    }
    /// Configured ceilings, never claims about the collection's cardinality.
    pub fn budget(&self) -> SqlBudget {
        self.budget
    }

    /// Machine-readable EXPLAIN. No rows or caller-sensitive sort values appear.
    pub fn explain(&self) -> Value {
        let mut result = BTreeMap::new();
        result.insert("version".into(), Value::Number(2.0));
        let (name, dependencies, bound, probes) = match &self.access {
            access::Path::Scan => ("bounded_collection_scan", "whole_collection", false, 0),
            access::Path::Key(_) => ("primary_key_lookup", "record", true, 1),
            access::Path::Index { name, fields, .. } => {
                result.insert("index".into(), Value::String(name.clone()));
                result.insert(
                    "equality_fields".into(),
                    Value::Array(fields.iter().cloned().map(Value::String).collect()),
                );
                (
                    "index_equality",
                    "index_bucket_and_records",
                    false,
                    self.budget.candidates,
                )
            }
        };
        result.insert("access".into(), Value::String(name.into()));
        result.insert("max_record_probes".into(), Value::Number(probes.into()));
        result.insert(
            "candidate_bound".into(),
            if bound {
                Value::Number(1.0)
            } else {
                Value::Null
            },
        );
        result.insert(
            "collection".into(),
            Value::String(self.ast.collection.clone()),
        );
        result.insert("input_bound_proven".into(), Value::Bool(bound));
        result.insert(
            "max_candidates".into(),
            Value::Number(self.budget.candidates.into()),
        );
        result.insert(
            "overflow_probe".into(),
            Value::Number(if bound { 0.0 } else { 1.0 }),
        );
        result.insert(
            "max_index_seeks".into(),
            Value::Number(if matches!(self.access, access::Path::Index { .. }) {
                1.0
            } else {
                0.0
            }),
        );
        result.insert("max_bytes".into(), Value::Number(self.budget.bytes.into()));
        result.insert("max_work".into(), Value::Number(self.budget.work.into()));
        result.insert(
            "sort".into(),
            Value::String(
                if self.ast.order.is_empty() {
                    "logical_key"
                } else {
                    "logical_values_then_key"
                }
                .into(),
            ),
        );
        result.insert("dependencies".into(), Value::String(dependencies.into()));
        result.insert("pagination".into(), Value::Bool(false));
        Value::Object(result)
    }

    /// Validate named parameters before the host reads any rows.
    pub fn validate_params(&self, params: &Value) -> Result<()> {
        match &self.params {
            Parameters::Declared(schema) => schema
                .parse(params)
                .map_err(|e| e.invalid_argument("SQL parameters"))?,
            Parameters::Inferred(_) => self.validate_dynamic_params(params)?,
        }
        self.limit(params)?;
        self.input_bytes_limit(params)?;
        Ok(())
    }

    /// Bytes left for input candidates after accounting for bound parameters.
    pub fn input_bytes_limit(&self, params: &Value) -> Result<usize> {
        (self.budget.bytes as usize)
            .checked_sub(payload_size(params))
            .ok_or_else(|| exhausted("bytes"))
    }

    fn limit(&self, params: &Value) -> Result<usize> {
        let value = match &self.ast.limit {
            Expr::Literal(value) => Some(value),
            Expr::Param(name) => match params {
                Value::Object(map) => map.get(name),
                _ => None,
            },
            _ => None,
        };
        match value {
            Some(Value::Number(n)) if *n >= 1.0 && *n <= 10_000.0 && *n % 1.0 == 0.0 => {
                Ok(*n as usize)
            }
            _ => Err(invalid("LIMIT must be an integer between 1 and 10000")),
        }
    }

    fn expr_schema(&self, expr: &Expr) -> Result<(Schema, bool, bool)> {
        match expr {
            Expr::Literal(v) => Ok((
                match v {
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
                    _ => return Err(invalid("unsupported literal")),
                },
                false,
                false,
            )),
            Expr::Param(name) => {
                if let Parameters::Inferred(params) = &self.params {
                    let param = params
                        .get(name)
                        .ok_or_else(|| invalid(format!("unknown parameter `:{name}`")))?;
                    return Ok((param.schema, false, true)); // SQL parameters can carry NULL.
                }
                let Parameters::Declared(Schema::Object { fields }) = self.params else {
                    return Err(invalid("query has no named parameters"));
                };
                let f = fields
                    .iter()
                    .find(|f| f.name == name)
                    .ok_or_else(|| invalid(format!("unknown parameter `:{name}`")))?;
                if f.optional {
                    return Err(invalid(
                        "SQL parameters must be present; use nullable for NULL",
                    ));
                }
                Ok((f.schema, false, false))
            }
            Expr::Path(path) => {
                let mut schema = self.value;
                let mut absent = false;
                let mut nullable_parent = false;
                for (i, part) in path.iter().enumerate() {
                    if i == 0 && *part == parse::Part::Field("_key".into()) {
                        schema = self.key;
                        continue;
                    }
                    nullable_parent |= matches!(schema, Schema::Nullable { .. });
                    schema = match (inner(schema), part) {
                        (Schema::Object { fields }, parse::Part::Field(name)) => {
                            let f = fields
                                .iter()
                                .find(|f| f.name == name)
                                .ok_or_else(|| invalid(format!("unknown field `{name}`")))?;
                            absent |= f.optional;
                            f.schema
                        }
                        (Schema::Tuple { elements }, parse::Part::Index(index)) => *elements
                            .get(*index)
                            .ok_or_else(|| invalid("tuple index out of bounds"))?,
                        _ => return Err(invalid("field path does not match collection schema")),
                    };
                }
                Ok((schema, absent, nullable_parent))
            }
        }
    }

    fn bind_predicate(&self, pred: &Predicate) -> Result<()> {
        match pred {
            Predicate::And(a, b) | Predicate::Or(a, b) => {
                self.bind_predicate(a)?;
                self.bind_predicate(b)
            }
            Predicate::Not(p) => self.bind_predicate(p),
            Predicate::IsNull(expr, _) => self.expr_schema(expr).map(|_| ()),
            Predicate::Compare(a, op, b) => {
                let a = self.expr_schema(a)?.0;
                let b = self.expr_schema(b)?.0;
                if !compatible(a, b) {
                    return Err(invalid(
                        "comparison operands have incompatible logical types",
                    ));
                }
                if !matches!(op, parse::Compare::Eq | parse::Compare::Ne)
                    && (!ordered(a) || !ordered(b))
                {
                    return Err(invalid("comparison operands are not logically ordered"));
                }
                Ok(())
            }
        }
    }

    /// Start evaluating policy-visible rows. `input_bytes` accounts for the
    /// host's committed/staged scan buffers, including rows hidden by policy.
    pub fn evaluator<'a>(&'a self, params: &'a Value, input_bytes: usize) -> Result<Evaluator<'a>> {
        self.validate_params(params)?;
        let bytes = input_bytes.saturating_add(payload_size(params));
        if bytes > self.budget.bytes as usize {
            return Err(exhausted("bytes"));
        }
        let input_work = input_bytes.min(u32::MAX as usize) as u32;
        if input_work > self.budget.work {
            return Err(exhausted("input work"));
        }
        Ok(Evaluator {
            plan: self,
            params,
            rows: Vec::new(),
            work: input_work,
            work_limit: self.budget.work,
            bytes,
            candidates: 0,
        })
    }
}

/// Incremental evaluator. No unredacted values may be supplied by the host.
pub struct Evaluator<'a> {
    plan: &'a Plan,
    params: &'a Value,
    rows: Vec<Output>,
    work: u32,
    work_limit: u32,
    bytes: usize,
    candidates: u32,
}

struct Output {
    key: Value,
    sort: Vec<Value>,
    row: Value,
}

#[derive(Clone, Copy, PartialEq)]
enum Truth {
    True,
    False,
    Unknown,
}

impl Truth {
    fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }
    fn and(self, b: Self) -> Self {
        match (self, b) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::True, Self::True) => Self::True,
            _ => Self::Unknown,
        }
    }
    fn or(self, b: Self) -> Self {
        match (self, b) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::False, Self::False) => Self::False,
            _ => Self::Unknown,
        }
    }
}

fn eval<'a>(
    expr: &'a Expr,
    key: &'a Value,
    row: &'a Value,
    params: &'a Value,
) -> Option<&'a Value> {
    match expr {
        Expr::Literal(v) => Some(v),
        Expr::Param(name) => match params {
            Value::Object(map) => map.get(name),
            _ => None,
        },
        Expr::Path(parts) => {
            let mut value = row;
            for (i, part) in parts.iter().enumerate() {
                if i == 0 && *part == parse::Part::Field("_key".into()) {
                    value = key;
                    continue;
                }
                if *value == Value::Null {
                    return Some(value);
                }
                value = match (value, part) {
                    (Value::Object(map), parse::Part::Field(name)) => map.get(name)?,
                    (Value::Array(items), parse::Part::Index(i)) => items.get(*i)?,
                    _ => return None,
                };
            }
            Some(value)
        }
    }
}

/// Logical comparison, independent of legacy canonical-JSON index bytes.
/// NULL sorts first; decimal strings compare exactly across their full range.
pub fn compare(a: &Value, b: &Value, schema: Schema) -> Result<Ordering> {
    match (a, b) {
        (Value::Null, Value::Null) => return Ok(Ordering::Equal),
        (Value::Null, _) => return Ok(Ordering::Less),
        (_, Value::Null) => return Ok(Ordering::Greater),
        _ => {}
    }
    match (a, b, inner(schema)) {
        (Value::String(a), Value::String(b), Schema::DecimalI64) => Ok(a
            .parse::<i64>()
            .map_err(|_| invalid("invalid signed decimal"))?
            .cmp(
                &b.parse::<i64>()
                    .map_err(|_| invalid("invalid signed decimal"))?,
            )),
        (Value::String(a), Value::String(b), Schema::DecimalU64) => Ok(a
            .parse::<u64>()
            .map_err(|_| invalid("invalid unsigned decimal"))?
            .cmp(
                &b.parse::<u64>()
                    .map_err(|_| invalid("invalid unsigned decimal"))?,
            )),
        (Value::String(a), Value::String(b), _) => Ok(a.cmp(b)),
        (Value::Number(a), Value::Number(b), _) => a
            .partial_cmp(b)
            .ok_or_else(|| invalid("nonfinite SQL value")),
        (Value::Bool(a), Value::Bool(b), _) => Ok(a.cmp(b)),
        (Value::Array(a), Value::Array(b), Schema::Tuple { elements }) => {
            for ((a, b), schema) in a.iter().zip(b).zip(elements) {
                let order = compare(a, b, *schema)?;
                if order != Ordering::Equal {
                    return Ok(order);
                }
            }
            Ok(a.len().cmp(&b.len()))
        }
        _ if a == b => Ok(Ordering::Equal),
        _ => Err(invalid("values cannot be compared with this logical type")),
    }
}

impl Evaluator<'_> {
    fn spend(&mut self, work: u32) -> Result<()> {
        self.work = self.work.saturating_add(work);
        if self.work > self.work_limit {
            return Err(exhausted("work"));
        }
        Ok(())
    }

    fn predicate(&mut self, pred: &Predicate, key: &Value, row: &Value) -> Result<Truth> {
        self.spend(1)?;
        Ok(match pred {
            Predicate::And(a, b) => {
                let a = self.predicate(a, key, row)?;
                a.and(self.predicate(b, key, row)?)
            }
            Predicate::Or(a, b) => {
                let a = self.predicate(a, key, row)?;
                a.or(self.predicate(b, key, row)?)
            }
            Predicate::Not(p) => self.predicate(p, key, row)?.not(),
            Predicate::IsNull(expr, not) => match eval(expr, key, row, self.params) {
                None => Truth::Unknown,
                Some(v) => {
                    if (*v == Value::Null) != *not {
                        Truth::True
                    } else {
                        Truth::False
                    }
                }
            },
            Predicate::Compare(a, op, b) => {
                match (
                    eval(a, key, row, self.params),
                    eval(b, key, row, self.params),
                ) {
                    (Some(a_value), Some(b_value))
                        if *a_value != Value::Null && *b_value != Value::Null =>
                    {
                        let cost = weight(a_value).saturating_add(weight(b_value));
                        self.spend(cost)?;
                        let schema = self.plan.expr_schema(a)?.0;
                        let schema = if schema == Schema::Null {
                            self.plan.expr_schema(b)?.0
                        } else {
                            schema
                        };
                        let order = compare(a_value, b_value, schema);
                        let yes = match op {
                            parse::Compare::Eq => {
                                if ordered(schema) {
                                    order? == Ordering::Equal
                                } else {
                                    a_value == b_value
                                }
                            }
                            parse::Compare::Ne => {
                                if ordered(schema) {
                                    order? != Ordering::Equal
                                } else {
                                    a_value != b_value
                                }
                            }
                            parse::Compare::Lt => order? == Ordering::Less,
                            parse::Compare::Le => order? != Ordering::Greater,
                            parse::Compare::Gt => order? == Ordering::Greater,
                            parse::Compare::Ge => order? != Ordering::Less,
                        };
                        if yes { Truth::True } else { Truth::False }
                    }
                    _ => Truth::Unknown,
                }
            }
        })
    }

    /// Supply one visible row, after the host applies row and field policy.
    pub fn push(&mut self, key: Value, row: Value) -> Result<()> {
        key.validate()?;
        row.validate()?;
        self.candidates = self.candidates.saturating_add(1);
        if self.candidates > self.plan.budget.candidates {
            return Err(exhausted("candidates"));
        }
        self.spend(1)?;
        if let Some(predicate) = &self.plan.ast.predicate
            && self.predicate(predicate, &key, &row)? != Truth::True
        {
            return Ok(());
        }
        self.spend((self.plan.ast.projection.len() + self.plan.ast.order.len()) as u32)?;
        self.spend(weight(&key).saturating_add(weight(&row)))?;
        let mut size = payload_size(&key).saturating_add(138); // two containers plus per-row allowance
        for (name, expr) in &self.plan.ast.projection {
            if let Some(value) = eval(expr, &key, &row, self.params) {
                size = size
                    .saturating_add(4)
                    .saturating_add(name.len())
                    .saturating_add(payload_size(value));
            }
        }
        for (expr, _) in &self.plan.ast.order {
            size = size.saturating_add(eval(expr, &key, &row, self.params).map_or(1, payload_size));
        }
        let bytes = self.bytes.saturating_add(size);
        if bytes > self.plan.budget.bytes as usize {
            return Err(exhausted("bytes"));
        }
        self.spend(size.min(u32::MAX as usize) as u32)?;
        self.bytes = bytes;
        let mut projected = BTreeMap::new();
        for (name, expr) in &self.plan.ast.projection {
            if let Some(v) = eval(expr, &key, &row, self.params) {
                projected.insert(name.clone(), v.clone());
            }
        }
        let sort: Vec<Value> = self
            .plan
            .ast
            .order
            .iter()
            .map(|(expr, _)| {
                eval(expr, &key, &row, self.params)
                    .cloned()
                    .unwrap_or(Value::Null)
            })
            .collect();
        let row = Value::Object(projected);
        self.plan.row.parse(&row).map_err(Failure::from)?;
        self.rows.push(Output { key, row, sort });
        Ok(())
    }

    /// Sort deterministically, then apply the total SQL limit.
    pub fn finish(&mut self) -> Result<Vec<Value>> {
        let schemas = self
            .plan
            .ast
            .order
            .iter()
            .map(|(expr, _)| self.plan.expr_schema(expr).map(|s| s.0))
            .collect::<Result<Vec<_>>>()?;
        // Fallible heapsort stops at the first exhausted comparison. A standard
        // sort_by cannot propagate budget errors without an invalid comparator.
        let len = self.rows.len();
        for root in (0..len / 2).rev() {
            self.sift(root, len, &schemas)?;
        }
        for end in (1..len).rev() {
            self.rows.swap(0, end);
            self.sift(0, end, &schemas)?;
        }
        let limit = self.plan.limit(self.params)?;
        Ok(core::mem::take(&mut self.rows)
            .into_iter()
            .take(limit)
            .map(|r| r.row)
            .collect())
    }

    /// Expression and comparison work charged so far, including failed work.
    pub fn work(&self) -> u32 {
        self.work
    }

    /// Restrict evaluation to the invocation's remaining shared fuel.
    pub fn limit_work(&mut self, available: u64) {
        self.work_limit = self
            .plan
            .budget
            .work
            .min(available.min(u32::MAX.into()) as u32);
    }

    fn sift(&mut self, mut root: usize, end: usize, schemas: &[Schema]) -> Result<()> {
        loop {
            let mut child = root * 2 + 1;
            if child >= end {
                return Ok(());
            }
            if child + 1 < end && self.order(child, child + 1, schemas)? == Ordering::Less {
                child += 1;
            }
            if self.order(root, child, schemas)? != Ordering::Less {
                return Ok(());
            }
            self.rows.swap(root, child);
            root = child;
        }
    }

    fn order(&mut self, a: usize, b: usize, schemas: &[Schema]) -> Result<Ordering> {
        let cost = self.rows[a].sort.iter().chain(&self.rows[b].sort).fold(
            weight(&self.rows[a].key).saturating_add(weight(&self.rows[b].key)),
            |n, v| n.saturating_add(weight(v)),
        );
        self.spend(cost)?;
        let (a, b) = (&self.rows[a], &self.rows[b]);
        for (((a, b), schema), (_, desc)) in a
            .sort
            .iter()
            .zip(&b.sort)
            .zip(schemas)
            .zip(&self.plan.ast.order)
        {
            let order = compare(a, b, *schema)?;
            if order != Ordering::Equal {
                return Ok(if *desc { order.reverse() } else { order });
            }
        }
        let order = compare(&a.key, &b.key, self.plan.key)?;
        if order == Ordering::Equal && a.key != b.key {
            // Legacy decimal strings can include leading zeros. Distinct
            // stored keys with equal logical values still need a total order.
            return Ok(a.key.canonical_json()?.cmp(&b.key.canonical_json()?));
        }
        Ok(order)
    }
}
