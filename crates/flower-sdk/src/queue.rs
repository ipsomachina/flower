//! Guest queues: durable jobs in scopes, claimed by priority and group turn
//! under fenced leases, with named jobs, kept results, a waiter line and
//! declared contracts.
//!
//! Mutations enqueue payloads into a queue's scope (`""` is the default
//! scope) at a priority, optionally in a group and under a name unique
//! within the scope; enqueues stage into the mutation, so a job exists
//! exactly when its mutation commits — never on rollback. Native workers
//! over HTTP and guest mutations alike claim the claimable jobs of one
//! scope (due pending jobs and claims whose lease lapsed): the lowest
//! priority first, within a priority each group's next job before any
//! group's second, then the job that waited longest. Each claim carries
//! a secret token its reports must present and a *fencing token*, larger
//! than every fencing token issued before it in that queue and scope, so
//! a downstream system can refuse a write from a holder whose lease
//! lapsed and was taken over. Claims are bound to the database's history
//! incarnation: after a restore to an earlier history, their holders can
//! no longer renew or report. A claim that lapses redelivers, or dies on
//! its last attempt; queues are at-least-once.
//!
//! A module uses only the queues it declares among its components (a
//! [`QueueDef`], or a [`TypedQueue`]): a queue's jobs are its declaring
//! modules' work, and removing the last of them with `purge` deletes
//! the queue. Any other queue name fails `queue_undeclared`.
//!
//! ```ignore
//! static MAIL: QueueDef = QueueDef::new("mail").payload(MAIL_SCHEMA).result(SENT_SCHEMA);
//! let options = EnqueueOptions::default().scope("tenant-7").named("invoice-42").group("acme");
//! Queue::enqueue(ctx, "mail", payload, &options)?;
//! let claim = ClaimOptions::new("relay-1").scope("tenant-7").limit(10).wait_ms(30_000);
//! for claim in Queue::claim(ctx, "mail", &claim)? {
//!     Queue::complete(ctx, "mail", &claim.lease(), Value::Bool(true))?;
//! }
//! ```
//!
//! Scopes separate work logically; they do not authenticate anyone.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use core::marker::PhantomData;

use crate::abi::HostOp;
use crate::app::Component;
use crate::ctx::{MutationCtx, ReadCtx};
use crate::error::{Result, codes, fail};
use crate::schema::Schema;
use crate::typed::FlowerType;
use crate::value::Value;

/// Longest queue name, in bytes.
pub const MAX_QUEUE_NAME: usize = 128;
/// Longest scope, job name, group or owner, in bytes.
pub const MAX_NAME: usize = 1024;
/// Largest priority magnitude: the safe-integer range.
pub const MAX_PRIORITY: i64 = (1 << 53) - 1;
/// Longest enqueue delay: 30 days in milliseconds.
pub const MAX_DELAY_MS: u64 = 30 * 24 * 60 * 60 * 1000;
/// Most attempts before a job dies.
pub const MAX_ATTEMPTS: u32 = 100;
/// Longest claim lease: one hour in milliseconds.
pub const MAX_LEASE_MS: u64 = 60 * 60 * 1000;
/// Longest place in the waiter line: one hour in milliseconds.
pub const MAX_WAIT_MS: u64 = 60 * 60 * 1000;
/// Most jobs one claim takes.
pub const MAX_CLAIMS: usize = 100;
/// Most leases one renewal names.
pub const MAX_RENEWALS: usize = 1024;
/// How long the first owner in line has new work to itself, by default.
pub const DEFAULT_TURN_MS: u64 = 1_000;

/// A queue declaration: the contracts its payloads and kept results
/// meet, and how long the first owner in its lines has new work to
/// itself. List it among the app's components to use the queue; the host then
/// checks every enqueue and every kept result, from guests and native
/// workers alike.
pub struct QueueDef {
    /// The queue's name.
    pub name: &'static str,
    /// Contract every enqueued payload meets.
    pub payload: Schema,
    /// Contract every kept result meets.
    pub result: Schema,
    /// The line's turn, in milliseconds.
    pub turn_ms: u64,
}

impl QueueDef {
    /// Declare a queue whose payloads and results are any JSON, with a
    /// one-second turn.
    pub const fn new(name: &'static str) -> Self {
        QueueDef {
            name,
            payload: Schema::Json,
            result: Schema::Json,
            turn_ms: DEFAULT_TURN_MS,
        }
    }

    /// Every enqueued payload must meet `schema`.
    pub const fn payload(mut self, schema: Schema) -> Self {
        self.payload = schema;
        self
    }

    /// Every kept result must meet `schema`.
    pub const fn result(mut self, schema: Schema) -> Self {
        self.result = schema;
        self
    }

    /// How long the first owner in a line has new work to itself.
    pub const fn turn_ms(mut self, turn_ms: u64) -> Self {
        self.turn_ms = turn_ms;
        self
    }
}

impl Component for QueueDef {
    fn queue(&self) -> Option<&QueueDef> {
        Some(self)
    }
}

/// Enqueue options.
#[derive(Clone, Debug)]
pub struct EnqueueOptions {
    /// Delay before the job becomes claimable, in milliseconds.
    pub delay_ms: u64,
    /// Attempts before the job dies. Defaults to 5.
    pub max_attempts: u32,
    /// The scope the job belongs to (`""`, the default scope).
    pub scope: String,
    /// Claim priority: lower values claim first (default 0).
    pub priority: i64,
    /// Become claimable at this absolute time instead (epoch
    /// milliseconds, at most 30 days ahead; a past time is due now).
    /// Use either this or `delay_ms`.
    pub at_ms: Option<u64>,
    /// The group whose turns the job takes: within a priority, each
    /// ready group's next job goes before any group's second.
    pub group: Option<String>,
    /// A name unique within the scope; a random ID without one.
    pub name: Option<String>,
    /// With a name: replace a completed or dead job of that name instead
    /// of failing `JOB_EXISTS` (a pending or claimed one still fails).
    pub replace: bool,
}

impl Default for EnqueueOptions {
    fn default() -> Self {
        EnqueueOptions {
            delay_ms: 0,
            max_attempts: 5,
            scope: String::new(),
            priority: 0,
            at_ms: None,
            group: None,
            name: None,
            replace: false,
        }
    }
}

impl EnqueueOptions {
    /// Enqueue into this scope.
    pub fn scope(mut self, scope: &str) -> Self {
        self.scope = String::from(scope);
        self
    }

    /// Claim at this priority: lower values go first.
    pub fn priority(mut self, priority: i64) -> Self {
        self.priority = priority;
        self
    }

    /// Become claimable after this delay.
    pub fn delay_ms(mut self, delay_ms: u64) -> Self {
        self.delay_ms = delay_ms;
        self
    }

    /// Become claimable at this absolute time (epoch milliseconds).
    pub fn at_ms(mut self, at_ms: u64) -> Self {
        self.at_ms = Some(at_ms);
        self
    }

    /// Die after this many attempts.
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Take turns with this group's other jobs.
    pub fn group(mut self, group: &str) -> Self {
        self.group = Some(String::from(group));
        self
    }

    /// Name the job, unique within its scope.
    pub fn named(mut self, name: &str) -> Self {
        self.name = Some(String::from(name));
        self
    }

    /// Replace a completed or dead job of the same name.
    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }
}

/// Claim options.
#[derive(Clone, Debug)]
pub struct ClaimOptions {
    /// The claimer, recorded on each lease.
    pub owner: String,
    /// The scope to claim from (`""`, the default scope).
    pub scope: String,
    /// Most jobs to claim (default 1, at most [`MAX_CLAIMS`]).
    pub limit: usize,
    /// Lease length (default 30 s).
    pub lease_ms: u64,
    /// Wait in the scope's line this long when the claim comes back
    /// short: the first owner in line has new work to itself for the
    /// queue's turn (see [`Queue::ready`]). Claim again before the place
    /// runs out to keep it.
    pub wait_ms: Option<u64>,
}

impl ClaimOptions {
    /// Claim one job of the default scope for `owner` under a 30 s
    /// lease.
    pub fn new(owner: &str) -> Self {
        ClaimOptions {
            owner: String::from(owner),
            scope: String::new(),
            limit: 1,
            lease_ms: 30_000,
            wait_ms: None,
        }
    }

    /// Claim from this scope.
    pub fn scope(mut self, scope: &str) -> Self {
        self.scope = String::from(scope);
        self
    }

    /// Claim up to this many jobs.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Lease each claim this long.
    pub fn lease_ms(mut self, lease_ms: u64) -> Self {
        self.lease_ms = lease_ms;
        self
    }

    /// Wait in line this long when the claim comes back short.
    pub fn wait_ms(mut self, wait_ms: u64) -> Self {
        self.wait_ms = Some(wait_ms);
        self
    }
}

/// How a job is named: its ID, or its scope and name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Job<'a> {
    /// The job ID.
    Id(&'a str),
    /// A named job.
    Named {
        /// Its scope.
        scope: &'a str,
        /// Its name.
        name: &'a str,
    },
}

impl Job<'_> {
    fn value(&self) -> Value {
        match self {
            Job::Id(id) => Value::String(String::from(*id)),
            Job::Named { scope, name } => {
                let mut map = BTreeMap::new();
                map.insert(String::from("scope"), Value::String(String::from(*scope)));
                map.insert(String::from("name"), Value::String(String::from(*name)));
                Value::Object(map)
            }
        }
    }
}

/// A claimed job.
#[derive(Clone, Debug, PartialEq)]
pub struct Claim<P = Value> {
    /// Job ID.
    pub id: String,
    /// The job's name, if it has one.
    pub name: Option<String>,
    /// The enqueued payload.
    pub payload: P,
    /// The secret claim token reports present.
    pub token: String,
    /// The fencing token: larger than every one issued before it in
    /// this queue and scope.
    pub fence: u64,
    /// The claimer.
    pub owner: Option<String>,
    /// The job's scope.
    pub scope: String,
    /// The job's priority.
    pub priority: i64,
    /// The job's group, if any.
    pub group: Option<String>,
    /// Claims so far, this one included.
    pub attempts: u32,
    /// When the lease lapses unless renewed.
    pub expires_ms: u64,
    /// When the job became claimable: its due time, or the end of the
    /// lease that lapsed; never before it was enqueued.
    pub available_ms: u64,
    /// When this claim was taken.
    pub claimed_ms: u64,
}

impl<P> Claim<P> {
    /// The identity renewals and reports name.
    pub fn lease(&self) -> Lease {
        Lease {
            id: self.id.clone(),
            token: self.token.clone(),
        }
    }
}

/// What renewals and reports identify a claim by.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    /// Job ID.
    pub id: String,
    /// The claim's secret token.
    pub token: String,
}

/// One stored job as guests read it.
#[derive(Clone, Debug, PartialEq)]
pub struct Item {
    /// Job ID.
    pub id: String,
    /// The job's name, if any.
    pub name: Option<String>,
    /// The enqueued payload.
    pub payload: Value,
    /// `pending`, `claimed`, `dead` or `completed`.
    pub state: String,
    /// The job's scope.
    pub scope: String,
    /// The job's priority.
    pub priority: i64,
    /// The job's group, if any.
    pub group: Option<String>,
    /// Its place among its priority's turns.
    pub turn: u64,
    /// Claims so far.
    pub attempts: u32,
    /// Attempts before death.
    pub max_attempts: u32,
    /// The fencing token of its latest claim, 0 before any.
    pub fence: u64,
    /// The current claim's owner.
    pub owner: Option<String>,
    /// The result a completion kept.
    pub result: Option<Value>,
    /// Enqueue time.
    pub enqueued_ms: u64,
    /// Due time.
    pub due_ms: u64,
    /// Lease expiry while claimed.
    pub claimed_until_ms: u64,
    /// When the job last changed state.
    pub updated_ms: u64,
}

/// Counts and readiness for one scope, or for every scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stats {
    /// Due-or-waiting jobs.
    pub pending: u64,
    /// Jobs under claim.
    pub claimed: u64,
    /// Exhausted or killed jobs.
    pub dead: u64,
    /// Completed jobs kept with their results.
    pub completed: u64,
    /// Whether a claim on the scope (by the owner asked about) would
    /// take a job now.
    pub ready: bool,
    /// When this next changes: a delayed job falls due, a lease lapses
    /// or the line's turn passes. A query reading stats is re-run then.
    pub next_ms: Option<u64>,
}

/// Durable queue operations.
pub struct Queue;

impl Queue {
    /// Enqueue a payload, returning the job ID. Queue names take ASCII
    /// letters, digits, dots, dashes and underscores; the module must
    /// declare the queue, as every operation here needs. The payload
    /// must meet the queue's declared contracts ([`QueueDef`]).
    pub fn enqueue(
        ctx: &mut MutationCtx<'_, '_>,
        queue: &str,
        payload: Value,
        options: &EnqueueOptions,
    ) -> Result<String> {
        validate_queue(queue)?;
        validate_scope(&options.scope)?;
        let delay_ms = match options.at_ms {
            None => options.delay_ms,
            Some(_) if options.delay_ms != 0 => {
                return Err(fail(
                    codes::INVALID_VALUE,
                    "use delay_ms or at_ms, not both",
                ));
            }
            Some(at_ms) => at_ms.saturating_sub(ctx.ctx().clock()? as u64),
        };
        if delay_ms > MAX_DELAY_MS {
            return Err(fail(codes::INVALID_VALUE, "enqueue delay is too long"));
        }
        if options.max_attempts == 0 || options.max_attempts > MAX_ATTEMPTS {
            return Err(fail(codes::INVALID_VALUE, "max attempts out of range"));
        }
        if !(-MAX_PRIORITY..=MAX_PRIORITY).contains(&options.priority) {
            return Err(fail(codes::INVALID_VALUE, "queue priority out of range"));
        }
        let optional = |value: &Option<String>| value.clone().map_or(Value::Null, Value::String);
        let args = Value::Array(Vec::from([
            Value::String(String::from(queue)),
            payload,
            Value::Number(delay_ms as f64),
            Value::Number(options.max_attempts as f64),
            Value::String(options.scope.clone()),
            Value::Number(options.priority as f64),
            optional(&options.group),
            optional(&options.name),
            Value::Bool(options.replace),
        ]));
        match ctx.ctx().call(HostOp::Enqueue, args)? {
            Value::String(id) => Ok(id),
            _ => Err(fail(codes::TYPE_MISMATCH, "enqueue returns a job ID")),
        }
    }

    /// Claim up to `options.limit` claimable jobs of one scope: lowest
    /// priority first, then group turns, then the longest waiting. Each
    /// claim takes an attempt and the scope's next fencing token. With
    /// `wait_ms`, a claim that comes back short keeps the owner's place
    /// in the scope's line.
    pub fn claim(
        ctx: &mut MutationCtx<'_, '_>,
        queue: &str,
        options: &ClaimOptions,
    ) -> Result<Vec<Claim>> {
        validate_queue(queue)?;
        validate_scope(&options.scope)?;
        if options.limit == 0 || options.limit > MAX_CLAIMS {
            return Err(fail(codes::INVALID_VALUE, "claim limit out of range"));
        }
        if options.lease_ms == 0 || options.lease_ms > MAX_LEASE_MS {
            return Err(fail(codes::INVALID_VALUE, "lease out of range"));
        }
        let args = Value::Array(Vec::from([
            Value::String(String::from(queue)),
            Value::String(options.scope.clone()),
            Value::String(options.owner.clone()),
            Value::Number(options.limit as f64),
            Value::Number(options.lease_ms as f64),
            options
                .wait_ms
                .map_or(Value::Null, |wait| Value::Number(wait as f64)),
        ]));
        match ctx.ctx().call(HostOp::QueueClaim, args)? {
            Value::Array(claims) => claims.into_iter().map(decode_claim).collect(),
            _ => Err(fail(codes::TYPE_MISMATCH, "claim returns claims")),
        }
    }

    /// Claim like [`Queue::claim`], admitting each job through `admit` (run
    /// as the calling mutation, which may read and write): a job it refuses
    /// is deleted and the next one taken, until `limit` jobs are admitted or
    /// none is left. This is admission and discard, not a filter that leaves
    /// refused jobs for other workers. With `wait_ms`, coming back short
    /// keeps the owner's place in line.
    pub fn claim_admitting(
        ctx: &mut MutationCtx<'_, '_>,
        queue: &str,
        options: &ClaimOptions,
        mut admit: impl FnMut(&mut MutationCtx<'_, '_>, &Claim) -> Result<bool>,
    ) -> Result<Vec<Claim>> {
        let one = ClaimOptions {
            limit: 1,
            wait_ms: None,
            ..options.clone()
        };
        let mut admitted = Vec::new();
        while admitted.len() < options.limit {
            let Some(claim) = Self::claim(ctx, queue, &one)?.into_iter().next() else {
                break;
            };
            if admit(ctx, &claim)? {
                admitted.push(claim);
            } else {
                Self::cancel(ctx, queue, Job::Id(&claim.id))?;
            }
        }
        if let Some(wait_ms) = options.wait_ms {
            let room = (options.limit - admitted.len()) as u64;
            Self::wait(ctx, queue, &options.scope, &options.owner, room, wait_ms)?;
        }
        Ok(admitted)
    }

    /// Take or keep `owner`'s place in a scope's line, asking for `room`
    /// more jobs, for `wait_ms`; a `room` or `wait_ms` of 0 leaves it.
    pub fn wait(
        ctx: &mut MutationCtx<'_, '_>,
        queue: &str,
        scope: &str,
        owner: &str,
        room: u64,
        wait_ms: u64,
    ) -> Result<()> {
        validate_queue(queue)?;
        validate_scope(scope)?;
        if wait_ms > MAX_WAIT_MS {
            return Err(fail(codes::INVALID_VALUE, "line wait is too long"));
        }
        let args = Value::Array(Vec::from([
            Value::String(String::from(queue)),
            Value::String(String::from(scope)),
            Value::String(String::from(owner)),
            Value::Number(room as f64),
            Value::Number(wait_ms as f64),
        ]));
        ctx.ctx().call(HostOp::QueueWait, args)?;
        Ok(())
    }

    /// Extend leases by `lease_ms` from now: one new expiry per lease,
    /// or `None` for a lease no longer held (reported, lapsed, claimed
    /// again, or taken in another history). Fencing tokens stay the same.
    pub fn renew(
        ctx: &mut MutationCtx<'_, '_>,
        queue: &str,
        leases: &[Lease],
        lease_ms: u64,
    ) -> Result<Vec<Option<u64>>> {
        validate_queue(queue)?;
        if leases.len() > MAX_RENEWALS {
            return Err(fail(
                codes::INVALID_VALUE,
                "renewals per call are at most 1024",
            ));
        }
        let leases = leases
            .iter()
            .map(|lease| {
                Value::Array(Vec::from([
                    Value::String(lease.id.clone()),
                    Value::String(lease.token.clone()),
                ]))
            })
            .collect();
        let args = Value::Array(Vec::from([
            Value::String(String::from(queue)),
            Value::Array(leases),
            Value::Number(lease_ms as f64),
        ]));
        match ctx.ctx().call(HostOp::QueueRenew, args)? {
            Value::Array(expiries) => expiries
                .iter()
                .map(|expiry| match expiry {
                    Value::Null => Ok(None),
                    value => whole(value, "expiry").map(Some),
                })
                .collect(),
            _ => Err(fail(codes::TYPE_MISMATCH, "renew returns expiries")),
        }
    }

    /// Complete a claim and keep the job with `result`, which must meet
    /// the queue's declared result contracts. A lease no longer held
    /// fails.
    pub fn complete(
        ctx: &mut MutationCtx<'_, '_>,
        queue: &str,
        lease: &Lease,
        result: Value,
    ) -> Result<()> {
        report(ctx, queue, lease, "complete", 0, result)
    }

    /// Finish a claim and delete the job.
    pub fn finish(ctx: &mut MutationCtx<'_, '_>, queue: &str, lease: &Lease) -> Result<()> {
        report(ctx, queue, lease, "finish", 0, Value::Null)
    }

    /// Fail a claim: pending again after `delay_ms`, keeping its turn,
    /// or dead once its attempts are spent.
    pub fn fail(
        ctx: &mut MutationCtx<'_, '_>,
        queue: &str,
        lease: &Lease,
        delay_ms: u64,
    ) -> Result<()> {
        report(ctx, queue, lease, "fail", delay_ms, Value::Null)
    }

    /// Move a claim to the dead letters now, whatever attempts remain.
    pub fn kill(ctx: &mut MutationCtx<'_, '_>, queue: &str, lease: &Lease) -> Result<()> {
        report(ctx, queue, lease, "kill", 0, Value::Null)
    }

    /// Hand a claim back, pending again after `delay_ms`, keeping its
    /// turn and attempt count.
    pub fn release(
        ctx: &mut MutationCtx<'_, '_>,
        queue: &str,
        lease: &Lease,
        delay_ms: u64,
    ) -> Result<()> {
        report(ctx, queue, lease, "release", delay_ms, Value::Null)
    }

    /// Delete a job in any state, returning whether one existed.
    pub fn cancel(ctx: &mut MutationCtx<'_, '_>, queue: &str, job: Job<'_>) -> Result<bool> {
        validate_queue(queue)?;
        let args = Value::Array(Vec::from([Value::String(String::from(queue)), job.value()]));
        match ctx.ctx().call(HostOp::QueueCancel, args)? {
            Value::Bool(removed) => Ok(removed),
            _ => Err(fail(codes::TYPE_MISMATCH, "cancel returns whether removed")),
        }
    }

    /// Requeue a dead job with a fresh attempt budget and a new turn.
    pub fn retry(
        ctx: &mut MutationCtx<'_, '_>,
        queue: &str,
        job: Job<'_>,
        delay_ms: u64,
    ) -> Result<()> {
        validate_queue(queue)?;
        let args = Value::Array(Vec::from([
            Value::String(String::from(queue)),
            job.value(),
            Value::Number(delay_ms as f64),
        ]));
        ctx.ctx().call(HostOp::QueueRetry, args)?;
        Ok(())
    }

    /// Delete dead and completed jobs enqueued before `older_than_ms`,
    /// in every scope, returning how many.
    pub fn purge(ctx: &mut MutationCtx<'_, '_>, queue: &str, older_than_ms: u64) -> Result<u64> {
        validate_queue(queue)?;
        let args = Value::Array(Vec::from([
            Value::String(String::from(queue)),
            Value::Number(older_than_ms as f64),
        ]));
        whole(&ctx.ctx().call(HostOp::QueuePurge, args)?, "purge count")
    }

    /// Read one job in any state, in queries and mutations alike.
    pub fn get(ctx: &mut impl ReadCtx, queue: &str, job: Job<'_>) -> Result<Option<Item>> {
        validate_queue(queue)?;
        let args = Value::Array(Vec::from([Value::String(String::from(queue)), job.value()]));
        match ctx.host_op(HostOp::QueueGet, args)? {
            Value::Null => Ok(None),
            value => decode_item(value).map(Some),
        }
    }

    /// Count one scope's jobs (every scope's for `None`) and say whether
    /// a claim would take one now, in queries and mutations alike. A
    /// query reading this re-runs when the next delayed job falls due or
    /// lease lapses.
    pub fn stats(ctx: &mut impl ReadCtx, queue: &str, scope: Option<&str>) -> Result<Stats> {
        stats(ctx, queue, scope, None)
    }

    /// Whether `owner` should claim from a scope now: there is work and,
    /// when others wait in the scope's line, it is `owner`'s turn (the
    /// first in line has new work to itself for the queue's turn, so one
    /// process wakes per job). A query reading this re-runs when that
    /// changes.
    pub fn ready(ctx: &mut impl ReadCtx, queue: &str, scope: &str, owner: &str) -> Result<bool> {
        Ok(Self::stats_for(ctx, queue, scope, owner)?.ready)
    }

    /// [`Queue::stats`] for one scope as `owner` sees it: `ready` and
    /// `next_ms` follow [`Queue::ready`].
    pub fn stats_for(
        ctx: &mut impl ReadCtx,
        queue: &str,
        scope: &str,
        owner: &str,
    ) -> Result<Stats> {
        stats(ctx, queue, Some(scope), Some(owner))
    }
}

/// Counts and readiness, for one owner when given.
fn stats(
    ctx: &mut impl ReadCtx,
    queue: &str,
    scope: Option<&str>,
    owner: Option<&str>,
) -> Result<Stats> {
    validate_queue(queue)?;
    if let Some(scope) = scope {
        validate_scope(scope)?;
    }
    let text =
        |text: Option<&str>| text.map_or(Value::Null, |text| Value::String(String::from(text)));
    let args = Value::Array(Vec::from([
        Value::String(String::from(queue)),
        text(scope),
        text(owner),
    ]));
    let Value::Object(map) = ctx.host_op(HostOp::QueueStats, args)? else {
        return Err(fail(codes::TYPE_MISMATCH, "stats is an object"));
    };
    Ok(Stats {
        pending: whole(field(&map, "pending")?, "pending")?,
        claimed: whole(field(&map, "claimed")?, "claimed")?,
        dead: whole(field(&map, "dead")?, "dead")?,
        completed: whole(field(&map, "completed")?, "completed")?,
        ready: matches!(field(&map, "ready")?, Value::Bool(true)),
        next_ms: match field(&map, "next_ms")? {
            Value::Null => None,
            value => Some(whole(value, "next_ms")?),
        },
    })
}

/// A queue whose payloads are one [`FlowerType`] and whose kept results
/// another: one declaration that is the component (its contracts come from
/// `P` and `R`) and the handle. Enqueues encode payloads, claims decode them,
/// completions encode results.
///
/// ```ignore
/// static MAIL: TypedQueue<Mail, Sent> = TypedQueue::new("mail");
/// static APP: App = App::new(&[&MAIL, ...]);
/// MAIL.enqueue(ctx, &mail, &EnqueueOptions::default().scope("tenant-7"))?;
/// for claim in MAIL.claim(ctx, &ClaimOptions::new("relay-1").scope("tenant-7"))? {
///     let sent = send(&claim.payload)?;
///     MAIL.complete(ctx, &claim.lease(), &sent)?;
/// }
/// ```
pub struct TypedQueue<P, R = Value> {
    /// The queue's name.
    pub name: &'static str,
    /// The declaration the host checks: `P` and `R`'s contracts.
    pub def: QueueDef,
    _types: PhantomData<fn() -> (P, R)>,
}

impl<P: FlowerType, R: FlowerType> Component for TypedQueue<P, R> {
    fn queue(&self) -> Option<&QueueDef> {
        Some(&self.def)
    }
}

impl<P: FlowerType, R: FlowerType> TypedQueue<P, R> {
    /// Declare the queue `name` over `P` payloads and `R` results.
    pub const fn new(name: &'static str) -> Self {
        TypedQueue {
            name,
            def: QueueDef::new(name).payload(P::SCHEMA).result(R::SCHEMA),
            _types: PhantomData,
        }
    }

    /// How long the first owner in a line has new work to itself.
    pub const fn turn_ms(mut self, turn_ms: u64) -> Self {
        self.def = self.def.turn_ms(turn_ms);
        self
    }

    /// The job one of this queue's items names, by ID or by name in a
    /// scope, as [`Queue::get`] reads it.
    pub fn get(&self, ctx: &mut impl ReadCtx, job: Job<'_>) -> Result<Option<Item>> {
        Queue::get(ctx, self.name, job)
    }

    /// Enqueue a payload, returning the job ID.
    pub fn enqueue(
        &self,
        ctx: &mut MutationCtx<'_, '_>,
        payload: &P,
        options: &EnqueueOptions,
    ) -> Result<String> {
        Queue::enqueue(ctx, self.name, payload.encode()?, options)
    }

    /// Claim jobs, decoding their payloads; a payload that does not
    /// decode fails the claim (and with it the mutation).
    pub fn claim(
        &self,
        ctx: &mut MutationCtx<'_, '_>,
        options: &ClaimOptions,
    ) -> Result<Vec<Claim<P>>> {
        Queue::claim(ctx, self.name, options)?
            .into_iter()
            .map(|claim| {
                Ok(Claim {
                    payload: P::decode(claim.payload)?,
                    id: claim.id,
                    name: claim.name,
                    token: claim.token,
                    fence: claim.fence,
                    owner: claim.owner,
                    scope: claim.scope,
                    priority: claim.priority,
                    group: claim.group,
                    attempts: claim.attempts,
                    expires_ms: claim.expires_ms,
                    available_ms: claim.available_ms,
                    claimed_ms: claim.claimed_ms,
                })
            })
            .collect()
    }

    /// See [`Queue::renew`].
    pub fn renew(
        &self,
        ctx: &mut MutationCtx<'_, '_>,
        leases: &[Lease],
        lease_ms: u64,
    ) -> Result<Vec<Option<u64>>> {
        Queue::renew(ctx, self.name, leases, lease_ms)
    }

    /// Complete a claim, keeping the job with its typed result.
    pub fn complete(&self, ctx: &mut MutationCtx<'_, '_>, lease: &Lease, result: &R) -> Result<()> {
        Queue::complete(ctx, self.name, lease, result.encode()?)
    }

    /// The result a completed job kept, decoded.
    pub fn result(&self, ctx: &mut impl ReadCtx, job: Job<'_>) -> Result<Option<R>> {
        match Queue::get(ctx, self.name, job)?.and_then(|item| item.result) {
            Some(result) => R::decode(result).map(Some),
            None => Ok(None),
        }
    }

    /// See [`Queue::finish`].
    pub fn finish(&self, ctx: &mut MutationCtx<'_, '_>, lease: &Lease) -> Result<()> {
        Queue::finish(ctx, self.name, lease)
    }

    /// See [`Queue::fail`].
    pub fn fail(&self, ctx: &mut MutationCtx<'_, '_>, lease: &Lease, delay_ms: u64) -> Result<()> {
        Queue::fail(ctx, self.name, lease, delay_ms)
    }

    /// See [`Queue::kill`].
    pub fn kill(&self, ctx: &mut MutationCtx<'_, '_>, lease: &Lease) -> Result<()> {
        Queue::kill(ctx, self.name, lease)
    }

    /// See [`Queue::release`].
    pub fn release(
        &self,
        ctx: &mut MutationCtx<'_, '_>,
        lease: &Lease,
        delay_ms: u64,
    ) -> Result<()> {
        Queue::release(ctx, self.name, lease, delay_ms)
    }

    /// See [`Queue::cancel`].
    pub fn cancel(&self, ctx: &mut MutationCtx<'_, '_>, job: Job<'_>) -> Result<bool> {
        Queue::cancel(ctx, self.name, job)
    }

    /// See [`Queue::stats`].
    pub fn stats(&self, ctx: &mut impl ReadCtx, scope: Option<&str>) -> Result<Stats> {
        Queue::stats(ctx, self.name, scope)
    }

    /// See [`Queue::ready`].
    pub fn ready(&self, ctx: &mut impl ReadCtx, scope: &str, owner: &str) -> Result<bool> {
        Queue::ready(ctx, self.name, scope, owner)
    }
}

/// Send one report on a claim.
fn report(
    ctx: &mut MutationCtx<'_, '_>,
    queue: &str,
    lease: &Lease,
    action: &str,
    delay_ms: u64,
    result: Value,
) -> Result<()> {
    validate_queue(queue)?;
    if delay_ms > MAX_DELAY_MS {
        return Err(fail(codes::INVALID_VALUE, "report delay is too long"));
    }
    let args = Value::Array(Vec::from([
        Value::String(String::from(queue)),
        Value::String(lease.id.clone()),
        Value::String(lease.token.clone()),
        Value::String(String::from(action)),
        Value::Number(delay_ms as f64),
        result,
    ]));
    ctx.ctx().call(HostOp::QueueReport, args)?;
    Ok(())
}

/// Validate a queue name on both sides of the ABI.
pub fn validate_queue(queue: &str) -> Result<()> {
    if queue.is_empty() || queue.len() > MAX_QUEUE_NAME {
        return Err(fail(codes::INVALID_VALUE, "queue name has a bad length"));
    }
    if !queue
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(fail(codes::INVALID_VALUE, "queue name has bad characters"));
    }
    Ok(())
}

/// Validate a scope: any text of at most [`MAX_NAME`] bytes, empty for
/// the default scope.
pub fn validate_scope(scope: &str) -> Result<()> {
    if scope.len() > MAX_NAME {
        return Err(fail(codes::INVALID_VALUE, "queue scope is too long"));
    }
    Ok(())
}

fn field<'a>(map: &'a BTreeMap<String, Value>, name: &str) -> Result<&'a Value> {
    map.get(name)
        .ok_or_else(|| fail(codes::TYPE_MISMATCH, "queue answer misses a field"))
}

fn text(map: &BTreeMap<String, Value>, name: &str) -> Result<String> {
    match field(map, name)? {
        Value::String(text) => Ok(text.clone()),
        _ => Err(fail(codes::TYPE_MISMATCH, "queue field is text")),
    }
}

fn optional_text(map: &BTreeMap<String, Value>, name: &str) -> Result<Option<String>> {
    match map.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.clone())),
        Some(_) => Err(fail(codes::TYPE_MISMATCH, "queue field is text")),
    }
}

fn whole(value: &Value, what: &str) -> Result<u64> {
    match value {
        Value::Number(number) if number.is_finite() && number % 1.0 == 0.0 && *number >= 0.0 => {
            Ok(*number as u64)
        }
        _ => Err(fail(
            codes::TYPE_MISMATCH,
            alloc::format!("queue {what} is a whole number"),
        )),
    }
}

fn integer(value: &Value) -> Result<i64> {
    match value {
        Value::Number(number) if number.is_finite() && number % 1.0 == 0.0 => Ok(*number as i64),
        _ => Err(fail(codes::TYPE_MISMATCH, "queue priority is an integer")),
    }
}

fn count(map: &BTreeMap<String, Value>, name: &str) -> Result<u32> {
    u32::try_from(whole(field(map, name)?, name)?)
        .map_err(|_| fail(codes::TYPE_MISMATCH, "queue counter exceeds u32"))
}

/// Decode a claim from the host.
fn decode_claim(value: Value) -> Result<Claim> {
    let Value::Object(mut map) = value else {
        return Err(fail(codes::TYPE_MISMATCH, "a claim is an object"));
    };
    let payload = map.remove("payload").unwrap_or(Value::Null);
    Ok(Claim {
        id: text(&map, "id")?,
        name: optional_text(&map, "name")?,
        payload,
        token: text(&map, "token")?,
        fence: whole(field(&map, "fence")?, "fence")?,
        owner: optional_text(&map, "owner")?,
        scope: text(&map, "scope")?,
        priority: integer(field(&map, "priority")?)?,
        group: optional_text(&map, "group")?,
        attempts: count(&map, "attempts")?,
        expires_ms: whole(field(&map, "expires_ms")?, "expiry")?,
        available_ms: whole(field(&map, "available_ms")?, "available time")?,
        claimed_ms: whole(field(&map, "claimed_ms")?, "claim time")?,
    })
}

/// Decode a job from the host.
fn decode_item(value: Value) -> Result<Item> {
    let Value::Object(mut map) = value else {
        return Err(fail(codes::TYPE_MISMATCH, "a job is an object"));
    };
    let payload = map.remove("payload").unwrap_or(Value::Null);
    let state = text(&map, "state")?;
    let result = match map.remove("result") {
        Some(result) if state == "completed" => Some(result),
        _ => None,
    };
    Ok(Item {
        id: text(&map, "id")?,
        name: optional_text(&map, "name")?,
        payload,
        state,
        scope: text(&map, "scope")?,
        priority: integer(field(&map, "priority")?)?,
        group: optional_text(&map, "group")?,
        turn: whole(field(&map, "turn")?, "turn")?,
        attempts: count(&map, "attempts")?,
        max_attempts: count(&map, "max_attempts")?,
        fence: whole(field(&map, "fence")?, "fence")?,
        owner: optional_text(&map, "owner")?,
        result,
        enqueued_ms: whole(field(&map, "enqueued_ms")?, "enqueue time")?,
        due_ms: whole(field(&map, "due_ms")?, "due time")?,
        claimed_until_ms: whole(field(&map, "claimed_until_ms")?, "lease expiry")?,
        updated_ms: whole(field(&map, "updated_ms")?, "update time")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_names_and_scopes_validate() {
        assert!(validate_queue("email.send-1_x").is_ok());
        assert!(validate_queue("").is_err());
        assert!(validate_queue("has space").is_err());
        assert!(validate_queue("has/slash").is_err());
        assert!(validate_queue(&"q".repeat(129)).is_err());
        assert!(validate_scope("").is_ok());
        assert!(validate_scope("tenant:7 / eu").is_ok());
        assert!(validate_scope(&"s".repeat(MAX_NAME + 1)).is_err());
    }

    #[test]
    fn claims_and_items_decode_from_host_answers() {
        let mut claim = BTreeMap::new();
        for (name, value) in [
            ("id", Value::String("ab".into())),
            ("name", Value::String("job-1".into())),
            ("payload", Value::Number(7.0)),
            ("token", Value::String("t".into())),
            ("fence", Value::Number(3.0)),
            ("owner", Value::String("w".into())),
            ("scope", Value::String("s".into())),
            ("priority", Value::Number(-2.0)),
            ("group", Value::Null),
            ("attempts", Value::Number(1.0)),
            ("expires_ms", Value::Number(10.0)),
            ("available_ms", Value::Number(4.0)),
            ("claimed_ms", Value::Number(5.0)),
        ] {
            claim.insert(String::from(name), value);
        }
        let decoded = decode_claim(Value::Object(claim.clone())).unwrap();
        assert_eq!(decoded.fence, 3);
        assert_eq!(decoded.priority, -2);
        assert_eq!(decoded.name.as_deref(), Some("job-1"));
        assert_eq!(decoded.group, None);
        assert_eq!(
            decoded.lease(),
            Lease {
                id: "ab".into(),
                token: "t".into()
            }
        );
        claim.insert(String::from("fence"), Value::Number(-1.0));
        assert!(decode_claim(Value::Object(claim)).is_err());
        assert!(decode_item(Value::Null).is_err());
    }

    #[test]
    fn declarations_carry_their_contracts() {
        static JOBS: QueueDef = QueueDef::new("jobs")
            .payload(crate::schema::v::string().build())
            .turn_ms(250);
        assert_eq!(JOBS.queue().map(|queue| queue.name), Some("jobs"));
        assert_eq!(JOBS.turn_ms, 250);
        assert!(JOBS.payload.is(&Value::String("x".into())));
        assert!(!JOBS.payload.is(&Value::Number(1.0)));
        assert!(JOBS.result.is(&Value::Number(1.0)));
    }
}
