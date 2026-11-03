//! Named, deployment-checked SQL plans. List each plan in the application's
//! components, then call it from an ordinary query, mutation or derived value.
//! SQL never creates an HTTP alias or changes the invocation's authority.

use alloc::{string::String, vec, vec::Vec};
use core::marker::PhantomData;

use crate::schema::Schema;
use crate::{Component, FlowerType, HostOp, ReadCtx, Result, Value, fail};

/// Maximum bytes in one request-time SQL statement.
pub const MAX_SQL_BYTES: usize = 16_384;

/// Stable fields of an explain response. The original object retains extensions.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlExplain {
    pub version: u32,
    pub collection: String,
    pub access: String,
    pub input_bound_proven: bool,
    pub max_candidates: u32,
    pub max_bytes: u32,
    pub max_work: u32,
    pub raw: Value,
}
impl TryFrom<Value> for SqlExplain {
    type Error = crate::Failure;
    fn try_from(raw: Value) -> Result<Self> {
        raw.validate()?;
        let Value::Object(fields) = &raw else {
            return Err(fail(
                crate::error::codes::TYPE_MISMATCH,
                "SQL explain response must be an object",
            ));
        };
        let field = |name: &str| {
            fields
                .get(name)
                .cloned()
                .ok_or_else(|| fail(crate::error::codes::MISSING_FIELD, name))
        };
        Ok(Self {
            version: u32::decode(field("version")?)?,
            collection: String::decode(field("collection")?)?,
            access: String::decode(field("access")?)?,
            input_bound_proven: bool::decode(field("input_bound_proven")?)?,
            max_candidates: u32::decode(field("max_candidates")?)?,
            max_bytes: u32::decode(field("max_bytes")?)?,
            max_work: u32::decode(field("max_work")?)?,
            raw,
        })
    }
}

/// Explicit execution ceilings. Exhaustion fails the whole operation;
/// `LIMIT` never makes a scan's input bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SqlBudget {
    /// Maximum committed/staged rows (scan) or index entries (equality bucket),
    /// including hidden rows. A point lookup observes at most one overlay row.
    pub candidates: u32,
    /// Maximum input plus projected/sort bytes retained by the evaluator.
    pub bytes: u32,
    /// Maximum input-byte, expression and comparison work, sharing host fuel.
    pub work: u32,
}

impl SqlBudget {
    /// Set resource ceilings independently of the permitted access paths.
    pub const fn new(candidates: u32, bytes: u32, work: u32) -> Self {
        Self {
            candidates,
            bytes,
            work,
        }
    }
    /// Opt in to a bounded full scan. There is deliberately no default budget.
    pub const fn scan(candidates: u32, bytes: u32, work: u32) -> Self {
        Self {
            candidates,
            bytes,
            work,
        }
    }
}

/// Manifest declaration used by [`SqlQuery`].
#[derive(Clone, Copy, Debug)]
pub struct SqlDef {
    /// Whether deployment may choose a bounded collection scan as a fallback.
    pub allow_scan: bool,
    /// Module-local plan name.
    pub name: &'static str,
    /// One read-only statement, bound by the host at deployment.
    pub statement: &'static str,
    /// Named parameter object.
    pub params: Schema,
    /// One projected row's contract.
    pub row: Schema,
    /// Explicit resource ceilings.
    pub budget: SqlBudget,
}

/// Typed handle and component for a named plan. Parameters and rows are explicit
/// Rust types; no macro relies on discovering declarations in another module.
/// Use `Omittable<T>` for projected fields a policy can redact when returning
/// rows from a typed callback; `typed::Field<T>` deliberately rejects encoding
/// a redacted value.
pub struct SqlQuery<P, R> {
    definition: SqlDef,
    marker: PhantomData<fn(P) -> R>,
}

impl<P: FlowerType, R: FlowerType> SqlQuery<P, R> {
    pub fn explain_typed(&self, ctx: &mut impl ReadCtx) -> Result<SqlExplain> {
        self.explain(ctx)?.try_into()
    }
    /// Declare a plan. Its collection must be local to this application.
    pub const fn new(name: &'static str, statement: &'static str, budget: SqlBudget) -> Self {
        Self {
            definition: SqlDef {
                allow_scan: true,
                name,
                statement,
                params: P::SCHEMA,
                row: R::SCHEMA,
                budget,
            },
            marker: PhantomData,
        }
    }

    /// Reject deployment unless a primary-key lookup or a complete index
    /// equality bucket can be proven. Candidate/byte/work ceilings still apply.
    pub const fn require_index(mut self) -> Self {
        self.definition.allow_scan = false;
        self
    }

    /// Execute at the invocation's pinned snapshot, including staged writes.
    /// Returns the complete limited result; this version has no continuation cursor.
    pub fn run(&self, ctx: &mut impl ReadCtx, params: &P) -> Result<Vec<R>> {
        let value = ctx.host_op(
            HostOp::Sql,
            vec![
                Value::String(String::from(self.definition.name)),
                params.encode()?,
                Value::Bool(false),
            ]
            .into(),
        )?;
        let Value::Array(rows) = value else {
            return Err(fail(
                crate::error::codes::TYPE_MISMATCH,
                "SQL response is not rows",
            ));
        };
        rows.into_iter().map(R::decode).collect()
    }

    /// Inspect the deployed plan and its configured ceilings, without scanning.
    pub fn explain(&self, ctx: &mut impl ReadCtx) -> Result<Value> {
        ctx.host_op(
            HostOp::Sql,
            Value::Array(vec![
                Value::String(String::from(self.definition.name)),
                Value::Null,
                Value::Bool(true),
            ]),
        )
    }
}

impl<P: FlowerType, R: FlowerType> Component for SqlQuery<P, R> {
    fn sql(&self) -> Option<&SqlDef> {
        Some(&self.definition)
    }
}

/// A request-time SQL capability, declared once in App. A module query chooses
/// who may call it; the host fixes its source collections and resource ceilings.
/// Statements use the same read-only dialect as named plans.
#[derive(Clone, Copy, Debug)]
pub struct SqlEndpoint {
    /// Module-local capability name, not an HTTP alias.
    pub name: &'static str,
    /// Explicit allowlist of local collection names; no cross-module sources.
    pub collections: &'static [&'static str],
    /// Host-enforced candidate, byte and work ceilings, including binding work.
    pub budget: SqlBudget,
    /// Whether the planner may fall back to a bounded collection scan.
    pub allow_scan: bool,
}

impl SqlEndpoint {
    pub fn explain_typed(
        &self,
        ctx: &mut impl ReadCtx,
        statement: &str,
        params: &impl FlowerType,
    ) -> Result<SqlExplain> {
        self.explain(ctx, statement, params)?.try_into()
    }
    /// Declare an endpoint. Register it in App as well as its wrapping query.
    pub const fn new(
        name: &'static str,
        collections: &'static [&'static str],
        budget: SqlBudget,
    ) -> Self {
        Self {
            name,
            collections,
            budget,
            allow_scan: true,
        }
    }

    /// Reject requests without a proven primary-key or full index equality path.
    pub const fn require_index(mut self) -> Self {
        self.allow_scan = false;
        self
    }

    fn invoke(
        &self,
        ctx: &mut impl ReadCtx,
        statement: &str,
        params: &Value,
        explain: bool,
    ) -> Result<Value> {
        if statement.len() > MAX_SQL_BYTES {
            return Err(fail(
                crate::error::codes::SQL_INVALID,
                "SQL statement exceeds 16KiB",
            ));
        }
        params.validate()?;
        ctx.host_op(
            HostOp::SqlDynamic,
            Value::Array(vec![
                Value::from(self.name),
                Value::from(statement),
                params.clone(),
                Value::Bool(explain),
            ]),
        )
    }

    /// Bind and execute request-time SQL, returning projected JSON objects.
    /// Parameters are a closed named object (or null when there are none).
    /// Row and field policies apply with the enclosing invocation's authority.
    pub fn run(
        &self,
        ctx: &mut impl ReadCtx,
        statement: &str,
        params: &impl FlowerType,
    ) -> Result<Vec<Value>> {
        Vec::<Value>::decode(self.invoke(ctx, statement, &params.encode()?, false)?)
    }

    /// Decode projected rows into a caller-selected contract, when useful.
    pub fn run_as<R: FlowerType>(
        &self,
        ctx: &mut impl ReadCtx,
        statement: &str,
        params: &impl FlowerType,
    ) -> Result<Vec<R>> {
        self.run(ctx, statement, params)?
            .into_iter()
            .map(R::decode)
            .collect()
    }

    /// Bind the supplied statement/parameters and explain it without reading rows.
    pub fn explain(
        &self,
        ctx: &mut impl ReadCtx,
        statement: &str,
        params: &impl FlowerType,
    ) -> Result<Value> {
        self.invoke(ctx, statement, &params.encode()?, true)
    }
}

impl Component for SqlEndpoint {
    fn sql_endpoint(&self) -> Option<&Self> {
        Some(self)
    }
}
