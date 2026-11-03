//! Multi-database transactions: one mutation per participant
//! database, committed atomically through one consensus entry. Step
//! failures abort before replicating and surface as business
//! failures; stale participant revisions retry inside the server.

use std::collections::BTreeMap;

use crate::client::{FlowerClient, RequestOptions};
use crate::error::FlowerError;
use crate::work::post;

/// One transaction step: a mutation against one database.
pub use flower_protocol::request::TransactionStep;

/// Transaction call options: idempotency plus base options. There is
/// no expected revision: each participant guards automatically.
#[derive(Clone, Debug, Default)]
pub struct TransactionOptions {
    /// Stable request identity; a UUID v4 when absent.
    pub request_id: Option<String>,
    /// Base call options.
    pub base: RequestOptions,
}

impl TransactionOptions {
    /// Default options.
    pub fn new() -> Self {
        TransactionOptions::default()
    }
}

/// A committed transaction: per-step values plus ordering counters.
#[derive(Clone, Debug)]
pub struct TransactionResult {
    /// Per-step values in step order.
    pub values: Vec<serde_json::Value>,
    /// Per-participant `(database, revision)` pairs.
    pub revisions: BTreeMap<String, u64>,
    /// Applied log position (0 when served without replicating); for a
    /// transaction across groups, the first step's group's.
    pub position: u64,
    /// For a transaction across groups, where each participant group
    /// committed its part, in its own log. Empty otherwise.
    pub positions: BTreeMap<String, u64>,
    /// Whether the server answered from a stored receipt or a
    /// coordinator record: this request ID committed earlier, and the
    /// answer is that outcome, not a second run.
    pub duplicate: bool,
    pub request_id: String,
}

/// Transaction response on the wire.
type TransactionResponse = flower_protocol::CallEnvelope;

impl FlowerClient {
    /// Run a multi-database transaction: one mutation per step, each
    /// step against a different database, committed atomically.
    pub async fn transaction(
        &self,
        steps: &[TransactionStep],
        options: &TransactionOptions,
    ) -> Result<TransactionResult, FlowerError> {
        crate::options::require_command_options(&options.base)?;
        if steps.is_empty()
            || steps
                .iter()
                .map(|s| &s.db)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != steps.len()
        {
            return Err(FlowerError::Decode("transactions require one mutation per distinct database; compose same-database work in a guest mutation".into()));
        }
        let request_id = options
            .request_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let body = serde_json::to_value(flower_protocol::request::TransactionRequest {
            steps: steps.to_vec(),
            idempotency_key: Some(request_id.clone()),
            principal: None,
            timestamp_ms: None,
            credentials: None,
        })
        .map_err(|e| FlowerError::Decode(e.to_string()))?;
        let response: TransactionResponse =
            post(self, "/v1/transaction", &body, &options.base).await?;
        response.validate(true).map_err(FlowerError::Decode)?;
        let values = match response.value {
            Some(serde_json::Value::Array(values)) if values.len() == steps.len() => values,
            _ => {
                return Err(FlowerError::Decode(
                    "transaction result count does not match its steps".into(),
                ));
            }
        };
        let revisions = response.revisions.ok_or_else(|| {
            FlowerError::Decode("transaction has no participant revisions".into())
        })?;
        if revisions.len() != steps.len() || steps.iter().any(|s| !revisions.contains_key(&s.db)) {
            return Err(FlowerError::Decode(
                "transaction participant revisions do not match its steps".into(),
            ));
        }
        Ok(TransactionResult {
            values,
            revisions,
            position: response.position.unwrap_or(0),
            positions: response.positions.unwrap_or_default(),
            duplicate: response.duplicate,
            request_id,
        })
    }
}

/// Persist this typed batch before sending it; execution always reuses its identity.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound = "", deny_unknown_fields)]
pub struct TypedTransaction<R = (), Errors = ()> {
    version: u32,
    steps: Vec<TransactionStep>,
    contracts: Vec<crate::CommandContract>,
    #[serde(skip)]
    argument_checks: Vec<ArgumentCheck>,
    request_id: String,
    #[serde(skip)]
    results: TransactionTypes<R, Errors>,
}
type TransactionTypes<R, E> = std::marker::PhantomData<fn() -> (R, E)>;
type ArgumentCheck = fn(flower_sdk::Value) -> Result<(), FlowerError>;
/// The result and error tuples after appending a typed mutation.
pub type AppendedTransaction<R, Errors, T, E> =
    TypedTransaction<<R as Append<T>>::Output, <Errors as Append<E>>::Output>;
impl Default for TypedTransaction<()> {
    fn default() -> Self {
        Self::new()
    }
}
impl TypedTransaction<()> {
    pub fn new() -> Self {
        Self {
            version: 2,
            steps: Vec::new(),
            contracts: Vec::new(),
            argument_checks: Vec::new(),
            request_id: uuid::Uuid::new_v4().to_string(),
            results: std::marker::PhantomData,
        }
    }
}
impl<R, Errors> TypedTransaction<R, Errors> {
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn step<
        A: flower_sdk::FlowerType,
        T: flower_sdk::FlowerType,
        E: flower_sdk::BusinessError,
    >(
        mut self,
        db: &str,
        module: &str,
        op: &flower_sdk::MutationOp<A, T, E>,
        args: &A,
    ) -> Result<AppendedTransaction<R, Errors, T, E>, FlowerError>
    where
        R: Append<T>,
        <R as Append<T>>::Output: TransactionValues,
        Errors: Append<E>,
        <Errors as Append<E>>::Output: TransactionErrors,
    {
        if db.is_empty() || module.is_empty() || self.steps.iter().any(|s| s.db == db) {
            return Err(FlowerError::Decode(
                "each transaction step needs a different named database".into(),
            ));
        }
        self.steps.push(TransactionStep {
            db: db.into(),
            module: module.into(),
            method: op.name.into(),
            args: crate::typed::value_json(
                &args.encode().map_err(|e| FlowerError::Decode(e.message))?,
            )?,
        });
        self.contracts
            .push(crate::CommandContract::of::<A, T, E>()?);
        self.argument_checks.push(|value| {
            A::SCHEMA
                .parse(&value)
                .map_err(|e| FlowerError::Decode(format!("{}: {}", e.path, e.message)))?;
            A::decode(value)
                .map(|_| ())
                .map_err(|e| FlowerError::Decode(e.message))
        });
        Ok(TypedTransaction {
            version: self.version,
            steps: self.steps,
            contracts: self.contracts,
            argument_checks: self.argument_checks,
            request_id: self.request_id,
            results: std::marker::PhantomData,
        })
    }
}
#[derive(Debug)]
pub struct TypedTransactionResult<R> {
    pub values: R,
    pub commit: TransactionResult,
}

#[derive(Debug)]
pub struct TransactionError<E = std::convert::Infallible> {
    pub request_id: String,
    pub step: Option<usize>,
    pub database: Option<String>,
    pub execution: flower_protocol::Execution,
    pub source: Box<FlowerError>,
    /// The failed step's declared error, with its zero-based step encoded in the variant.
    pub declared: Option<Box<E>>,
}
impl<E> TransactionError<E> {
    pub fn problem(&self) -> Option<&flower_protocol::Problem> {
        self.source.problem()
    }
    pub fn execution(&self) -> flower_protocol::Execution {
        self.execution
    }
    pub fn declared(&self) -> Option<&E> {
        self.declared.as_deref()
    }
}
impl<E: std::fmt::Debug> std::fmt::Display for TransactionError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "transaction {} step {:?}: {}",
            self.request_id, self.step, self.source
        )
    }
}
impl<E: std::fmt::Debug + 'static> std::error::Error for TransactionError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}
impl<R: TransactionValues, Errors: TransactionErrors> TypedTransaction<R, Errors> {
    /// Restore against a current transaction template, preserving the stored identity and arguments.
    pub fn restore(&self, bytes: &[u8]) -> Result<Self, FlowerError> {
        let mut stored: Self =
            serde_json::from_slice(bytes).map_err(|e| FlowerError::Decode(e.to_string()))?;
        if stored.version != 2
            || stored.request_id.is_empty()
            || stored.contracts != self.contracts
            || stored.steps.len() != self.steps.len()
            || stored
                .steps
                .iter()
                .zip(&self.steps)
                .any(|(a, b)| a.db != b.db || a.module != b.module || a.method != b.method)
        {
            return Err(FlowerError::Unsupported("stored transaction scope or contract changed; reconcile or explicitly migrate the original transaction".into()));
        }
        if self.argument_checks.len() != stored.steps.len() {
            return Err(FlowerError::Unsupported(
                "restore requires a template built with current typed steps".into(),
            ));
        }
        for (step, check) in stored.steps.iter().zip(&self.argument_checks) {
            check(crate::typed::json_value(&step.args)?)?;
        }
        stored.argument_checks = self.argument_checks.clone();
        Ok(stored)
    }
    pub async fn execute(
        &self,
        client: &FlowerClient,
        options: &crate::CommandOptions,
    ) -> Result<TypedTransactionResult<R>, TransactionError<Errors::Declared>> {
        let mut execution = flower_protocol::Execution::NotStarted;
        let result = async {
            if self.version != 2
                || self.request_id.is_empty()
                || self.steps.len() != R::COUNT
                || Errors::COUNT.is_some_and(|count| count != self.steps.len())
                || self.contracts.len() != self.steps.len()
            {
                return Err(FlowerError::Decode(
                    "invalid or unsupported typed transaction".into(),
                ));
            }
            let results = R::fingerprints()?;
            let errors = Errors::fingerprints()?;
            if self
                .contracts
                .iter()
                .zip(results)
                .any(|(c, r)| c.result != r)
                || errors.is_some_and(|errors| {
                    self.contracts
                        .iter()
                        .zip(errors)
                        .any(|(c, e)| c.errors != e)
                })
            {
                return Err(FlowerError::Unsupported(
                    "stored transaction result or error contract changed".into(),
                ));
            }
            let commit = client
                .transaction(
                    &self.steps,
                    &TransactionOptions {
                        request_id: Some(self.request_id.clone()),
                        base: options.0.clone(),
                    },
                )
                .await
                .inspect_err(|error| execution = error.execution())?;
            execution = flower_protocol::Execution::Decided;
            let values = R::decode(commit.values.clone())?;
            Ok(TypedTransactionResult { values, commit })
        }
        .await;
        result.map_err(|source| {
            let details = match &source {
                FlowerError::Business { details, .. } => details.as_ref(),
                _ => source
                    .problem()
                    .and_then(|problem| problem.details.as_ref()),
            };
            let step = details
                .and_then(|d| d["transaction"]["step"].as_u64())
                .and_then(|s| usize::try_from(s).ok());
            let declared = step.and_then(|index| {
                let expected = self.steps.get(index)?;
                let context = &details?["transaction"];
                if context["database"] != expected.db
                    || context["module"] != expected.module
                    || context["method"] != expected.method
                {
                    return None;
                }
                let mut failure = crate::typed::call_failure(&source)?;
                failure.details = details?
                    .get("cause")
                    .map(crate::typed::json_value)
                    .transpose()
                    .ok()?;
                Errors::decode(index, &failure)
            });
            TransactionError {
                request_id: self.request_id.clone(),
                step,
                database: details
                    .and_then(|d| d["transaction"]["database"].as_str())
                    .map(str::to_owned),
                execution,
                source: Box::new(source),
                declared: declared.map(Box::new),
            }
        })
    }
}

/// A heterogeneous transaction's declared failure. Indices follow `TransactionError::step`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StepError<
    A = std::convert::Infallible,
    B = std::convert::Infallible,
    C = std::convert::Infallible,
    D = std::convert::Infallible,
    E = std::convert::Infallible,
    F = std::convert::Infallible,
    G = std::convert::Infallible,
    H = std::convert::Infallible,
> {
    Step0(A),
    Step1(B),
    Step2(C),
    Step3(D),
    Step4(E),
    Step5(F),
    Step6(G),
    Step7(H),
}

pub trait TransactionErrors {
    type Declared;
    const COUNT: Option<usize>;
    fn decode(step: usize, failure: &flower_sdk::Failure) -> Option<Self::Declared>;
    fn fingerprints() -> Result<Option<Vec<String>>, FlowerError>;
}
// Explicitly using the legacy one-parameter type keeps dynamic errors after deserialization.
impl TransactionErrors for () {
    fn fingerprints() -> Result<Option<Vec<String>>, FlowerError> {
        Ok(None)
    }
    type Declared = std::convert::Infallible;
    const COUNT: Option<usize> = None;
    fn decode(_: usize, _: &flower_sdk::Failure) -> Option<Self::Declared> {
        None
    }
}
pub trait Append<T> {
    type Output;
}
pub trait TransactionValues: Sized {
    const COUNT: usize;
    fn decode(values: Vec<serde_json::Value>) -> Result<Self, FlowerError>;
    fn fingerprints() -> Result<Vec<String>, FlowerError>;
}
impl<T> Append<T> for () {
    type Output = (T,);
}
impl TransactionValues for () {
    fn fingerprints() -> Result<Vec<String>, FlowerError> {
        Ok(Vec::new())
    }
    const COUNT: usize = 0;
    fn decode(values: Vec<serde_json::Value>) -> Result<Self, FlowerError> {
        if values.is_empty() {
            Ok(())
        } else {
            Err(FlowerError::Decode("unexpected transaction results".into()))
        }
    }
}
macro_rules! tuple_results {
    ($($name:ident:$index:literal:$variant:ident),+) => {
        impl<$($name:flower_sdk::FlowerType),+> TransactionValues for ($($name,)+) {
            fn fingerprints()->Result<Vec<String>,FlowerError> { Ok(vec![$(crate::contract::fingerprint($name::SCHEMA)?),+]) }
            const COUNT:usize=[$(stringify!($name)),+].len();
            fn decode(values:Vec<serde_json::Value>)->Result<Self,FlowerError> {
                if values.len()!=Self::COUNT {return Err(FlowerError::Decode("transaction result count mismatch".into()));}
                let mut values=values.into_iter();
                Ok(($($name::decode(crate::typed::json_value(&values.next().expect("checked cardinality"))?).map_err(|e|FlowerError::Decode(e.message))?,)+))
            }
        }
        impl<T,$($name),+> Append<T> for ($($name,)+) {type Output=($($name,)+T,);}
        impl<$($name: flower_sdk::BusinessError),+> TransactionErrors for ($($name,)+) {
            fn fingerprints()->Result<Option<Vec<String>>,FlowerError> { Ok(Some(vec![$(crate::contract::fingerprint($name::SCHEMA)?),+])) }
            type Declared = StepError<$($name),+>;
            const COUNT: Option<usize> = Some([$(stringify!($name)),+].len());
            fn decode(step:usize, failure:&flower_sdk::Failure)->Option<Self::Declared> {
                match step { $($index => $name::from_failure(failure).ok().map(StepError::$variant),)+ _=>None }
            }
        }
    };
}
tuple_results!(A:0:Step0);
tuple_results!(A:0:Step0, B:1:Step1);
tuple_results!(A:0:Step0, B:1:Step1, C:2:Step2);
tuple_results!(A:0:Step0, B:1:Step1, C:2:Step2, D:3:Step3);
tuple_results!(A:0:Step0, B:1:Step1, C:2:Step2, D:3:Step3, E:4:Step4);
tuple_results!(A:0:Step0, B:1:Step1, C:2:Step2, D:3:Step3, E:4:Step4, F:5:Step5);
tuple_results!(A:0:Step0, B:1:Step1, C:2:Step2, D:3:Step3, E:4:Step4, F:5:Step5, G:6:Step6);
tuple_results!(A:0:Step0, B:1:Step1, C:2:Step2, D:3:Step3, E:4:Step4, F:5:Step5, G:6:Step6, H:7:Step7);
