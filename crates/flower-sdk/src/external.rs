//! External values: results computed outside Flower by a worker (an
//! API call, a model, anything nondeterministic) and published back,
//! guarded by the input they were computed from. The reconcile worker
//! (`flower_worker::reconcile`) drives them.
//!
//! An external value declares a read-only *input* derived from its
//! arguments (null when there is nothing to compute). Its *key* is the
//! canonical JSON of `[args, input]`: work carries the key it was
//! computed for, and a publication is accepted only while that key is
//! still the current one, so results computed from inputs that changed
//! meanwhile are rejected, never stored. The first result published for
//! a key wins; later ones for the same key are accepted without
//! overwriting it. There is no exactly-once side effect: any worker may
//! compute the same key.
//!
//! An external value that *tracks* a source collection is a pool: each
//! source row's key is an argument set, every change to a row marks it
//! stale (or, when the row is gone, removes its result), and workers
//! drain the stale work in one of two ways:
//!
//! - `next` lists stale work, oldest marker first, optionally only one
//!   hash *shard* of the argument sets (`[index, count]`), so several
//!   unleased workers can split a pool between them;
//! - a *leased pool* hands each argument set to one worker at a time:
//!   `claim` leases due markers to an owner, `renew` extends the leases
//!   it holds, `release` hands one back (after a delay, to back off),
//!   and `ready` and `stats` say whether anything is claimable now and
//!   when that next changes. A lease spreads work; it does not fence
//!   publication — any worker holding the current key may publish, and
//!   an input change drops the lease (and any release delay) with the
//!   work it was for.
//!
//! Declare one with [`external!`](crate::external!) and list it in the
//! [`App`]: it brings its collections, definitions and trigger, and with
//! the `http` option its worker operations' HTTP aliases.
//!
//! ```ignore
//! fn words_input(ctx: &mut Ctx<'_, '_>, id: Value) -> Result<Value> { ... }
//! flower_sdk::external!(pub static WORDS = "words", input = words_input, tracks = "docs");
//! static APP: App = App::new(&[&DOCS, &WORDS]);
//! ```
//!
//! [`External::typed`] gives the same operations over typed arguments,
//! inputs and results.
//!
//! [`App`]: crate::App

use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;
use core::marker::PhantomData;

use crate::app::{Change, Collection, Component, Derived, IndexDef, Method, Trigger};
use crate::ctx::{CollectionRef, Ctx, MutationCtx, RangeBounds, ReadCtx};
use crate::error::{Result, codes, fail};
use crate::schema::Schema;
use crate::typed::FlowerType;
use crate::value::Value;

/// Most work items one `next` or `claim` lists.
pub const MAX_NEXT: u32 = 256;
/// Most leases one `renew` names.
pub const MAX_RENEWALS: usize = 1024;
/// Default lease length: 30 s.
pub const DEFAULT_LEASE_MS: u64 = 30_000;
/// Default longest lease: 300 s.
pub const MAX_LEASE_MS: u64 = 300_000;
/// Longest lease owner, in bytes.
pub const MAX_OWNER: usize = 128;
/// Markers one page of the `since` index reads.
const PAGE: u32 = 64;

/// A pool's stale markers are ordered by `since`: when the marker was
/// set, or when its lease or release delay ends.
pub static STALE_INDEXES: [IndexDef; 1] = [IndexDef::new("since", &["since"])];

/// What `next` lists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Next {
    /// Most work items (default 16, at most [`MAX_NEXT`]).
    pub limit: u32,
    /// Only argument sets in hash shard `index` of `count`.
    pub shard: Option<(u32, u32)>,
}

impl Default for Next {
    fn default() -> Self {
        Next {
            limit: 16,
            shard: None,
        }
    }
}

/// What `claim` leases.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClaimOptions {
    /// Most due markers to inspect, and so most claims (default 16, at
    /// most [`MAX_NEXT`]).
    pub limit: u32,
    /// Lease length (default the value's default lease).
    pub lease_ms: Option<u64>,
}

impl Default for ClaimOptions {
    fn default() -> Self {
        ClaimOptions {
            limit: 16,
            lease_ms: None,
        }
    }
}

/// An external value's descriptors. Build it with
/// [`external!`](crate::external!), which wires its callbacks to it.
pub struct External {
    /// The external value's name; its definitions are `<name>.get`,
    /// `<name>.pending`, `<name>.publish`, `<name>.next`,
    /// `<name>.claim`, `<name>.renew`, `<name>.release`, `<name>.ready`,
    /// `<name>.stats` and `<name>.input`.
    pub name: &'static str,
    /// Published results, keyed by arguments: `{key, value}`.
    pub results: Collection,
    /// Pools: stale markers, keyed by arguments, indexed by `since`.
    pub stale: Collection,
    /// The read-only input, a derived value over the arguments.
    pub input: Derived,
    /// Query: `{status: "ready", value}`, `{status: "pending"}`, or null
    /// when there is nothing to compute.
    pub get: Method,
    /// Query: the work for one argument set, `{args, key, input}`, or
    /// null when its result is current.
    pub pending: Method,
    /// Mutation over `{args, key, value}`: whether the result was
    /// accepted (false: the input changed meanwhile).
    pub publish: Method,
    /// Query over `{limit, shard}` (pools): stale work, oldest first.
    pub next: Method,
    /// Mutation over `{owner, limit, leaseMs}` (pools): lease due work.
    pub claim: Method,
    /// Mutation over `{leases, leaseMs}` (pools): extend leases.
    pub renew: Method,
    /// Mutation over a lease plus `delayMs` (pools): hand one back.
    pub release: Method,
    /// Query (pools): whether a claim would find work now.
    pub ready: Method,
    /// Query (pools): `{ready, oldestReadyAt, nextAvailableAt}`.
    pub stats: Method,
    /// Pools: the trigger marking changed source rows stale.
    pub track: Trigger,
    /// The internal mutation the engine runs for the trigger, over
    /// `{collection, key, before, after}`.
    pub on_change: Method,
    /// Contract every published value must meet.
    pub result: Schema,
    /// Whether it tracks a source collection.
    pub pool: bool,
    /// Lease length when a claim names none.
    pub default_lease_ms: u64,
    /// Longest lease a claim or renewal may take.
    pub max_lease_ms: u64,
}

impl Component for External {
    /// Its results collection, input, get, pending, publish, next, the
    /// trigger's mutation, claim, renew, release, ready and stats; a
    /// pool adds its stale markers and the trigger. The pool operations
    /// refuse outside pools.
    fn components(&'static self, visit: &mut dyn FnMut(&'static dyn Component)) {
        visit(&self.results);
        if self.pool {
            visit(&self.stale);
            visit(&self.track);
        }
        visit(&self.input);
        visit(&self.get);
        visit(&self.pending);
        visit(&self.publish);
        visit(&self.next);
        visit(&self.on_change);
        visit(&self.claim);
        visit(&self.renew);
        visit(&self.release);
        visit(&self.ready);
        visit(&self.stats);
    }
}

impl External {
    /// The same external value with typed arguments, inputs and
    /// results.
    pub const fn typed<A, I, R>(&'static self) -> TypedExternal<A, I, R> {
        TypedExternal {
            raw: self,
            _types: PhantomData,
        }
    }

    /// The input wanted now and its key, or `None` when the input is
    /// null.
    fn wanted(&self, ctx: &mut impl ReadCtx, args: &Value) -> Result<Option<(String, Value)>> {
        let input = ctx.get_derived("", self.input.name, args)?;
        if input == Value::Null {
            return Ok(None);
        }
        let key = Value::Array(vec![args.clone(), input.clone()]).canonical_json()?;
        Ok(Some((key, input)))
    }

    /// The key a stored result was published for.
    fn stored(&self, ctx: &mut impl ReadCtx, row: &Value) -> Result<Option<(String, Value)>> {
        let Some(Value::Object(mut stored)) =
            ctx.get(CollectionRef::local(self.results.name), row)?
        else {
            return Ok(None);
        };
        match (stored.remove("key"), stored.remove("value")) {
            (Some(Value::String(key)), Some(value)) => Ok(Some((key, value))),
            _ => Ok(None),
        }
    }

    fn stale(&self) -> CollectionRef<'static> {
        CollectionRef::local(self.stale.name)
    }

    /// Refuse pool operations outside pools.
    fn require_pool(&self) -> Result<()> {
        if self.pool {
            Ok(())
        } else {
            Err(fail(
                codes::INVALID_INVOCATION,
                "this external value tracks no collection",
            ))
        }
    }

    /// The lease a claim or renewal takes.
    fn lease_length(&self, lease_ms: Option<u64>) -> Result<u64> {
        let lease_ms = lease_ms.unwrap_or(self.default_lease_ms);
        if lease_ms == 0 {
            return Err(fail(codes::INVALID_VALUE, "leases last at least 1 ms"));
        }
        if lease_ms > self.max_lease_ms {
            return Err(fail(
                "LEASE_TOO_LONG",
                alloc::format!("leases last at most {} ms", self.max_lease_ms),
            ));
        }
        Ok(lease_ms)
    }

    /// The current state for an argument set: ready with its value,
    /// pending, or null when there is nothing to compute.
    pub fn state(&self, ctx: &mut impl ReadCtx, args: &Value) -> Result<Value> {
        let Some((key, _)) = self.wanted(ctx, args)? else {
            return Ok(Value::Null);
        };
        let row = args.clone();
        let mut state = BTreeMap::new();
        match self.stored(ctx, &row)? {
            Some((stored, value)) if stored == key => {
                state.insert("status".to_string(), Value::String("ready".to_string()));
                state.insert("value".to_string(), value);
            }
            _ => {
                state.insert("status".to_string(), Value::String("pending".to_string()));
            }
        }
        Ok(Value::Object(state))
    }

    /// The work for one argument set, `{args, key, input}`, or null when
    /// its result is current or there is nothing to compute.
    pub fn work(&self, ctx: &mut impl ReadCtx, args: &Value) -> Result<Value> {
        let Some((key, input)) = self.wanted(ctx, args)? else {
            return Ok(Value::Null);
        };
        let row = args.clone();
        if self
            .stored(ctx, &row)?
            .is_some_and(|(stored, _)| stored == key)
        {
            return Ok(Value::Null);
        }
        let mut work = BTreeMap::new();
        work.insert("args".to_string(), args.clone());
        work.insert("key".to_string(), Value::String(key));
        work.insert("input".to_string(), input);
        Ok(Value::Object(work))
    }

    /// Publish a result computed for `key`: stored when `key` is still
    /// current (unless a result for it is already stored, which wins),
    /// rejected with `false` when the input changed meanwhile. The value
    /// is checked against the result contract first. In a pool the
    /// marker goes with an accepted result, lease and all.
    pub fn publish(
        &self,
        ctx: &mut Ctx<'_, '_>,
        args: &Value,
        key: &str,
        value: Value,
    ) -> Result<bool> {
        value.validate()?;
        if !self.result.is(&value) {
            return Err(fail(
                codes::INVALID_VALUE,
                "the published value does not meet the result contract",
            ));
        }
        let Some((wanted, _)) = self.wanted(ctx, args)? else {
            return Ok(false);
        };
        if wanted != key {
            return Ok(false);
        }
        let row = args.clone();
        if self
            .stored(ctx, &row)?
            .is_none_or(|(stored, _)| stored != key)
        {
            let mut stored = BTreeMap::new();
            stored.insert("key".to_string(), Value::String(wanted));
            stored.insert("value".to_string(), value);
            ctx.set(
                CollectionRef::local(self.results.name),
                &row,
                Value::Object(stored),
            )?;
        }
        if self.pool {
            ctx.delete(self.stale(), &row)?;
        }
        Ok(true)
    }

    /// One page of markers in `since` order, optionally only those due
    /// by `until`.
    fn markers(
        &self,
        ctx: &mut impl ReadCtx,
        until: Option<f64>,
        limit: u32,
        after: Option<String>,
    ) -> Result<crate::ctx::RawPage> {
        let mut bounds = RangeBounds::limited(limit);
        bounds.lte = until.map(Value::Number);
        bounds.after = after;
        ctx.range(self.stale(), "since", bounds)
    }

    /// Pools: up to `limit` work items for stale argument sets, oldest
    /// marker first (leased and delayed markers included, as their
    /// lease or delay end orders them), only those in `shard` when
    /// given. Markers whose work is already done are skipped.
    pub fn next(&self, ctx: &mut impl ReadCtx, next: Next) -> Result<Value> {
        self.require_pool()?;
        let limit = next.limit.clamp(1, MAX_NEXT) as usize;
        if let Some((index, count)) = next.shard
            && (count == 0 || index >= count)
        {
            return Err(fail(
                codes::INVALID_VALUE,
                "shard is [index, count] with index < count",
            ));
        }
        let mut work = Vec::new();
        let mut after = None;
        loop {
            let page = self.markers(ctx, None, PAGE, after)?;
            for row in page.rows {
                if let Some((index, count)) = next.shard
                    && shard_of(&row.key.canonical_json()?, count) != index
                {
                    continue;
                }
                let item = self.work(ctx, &row.key)?;
                if item != Value::Null {
                    work.push(item);
                    if work.len() == limit {
                        return Ok(Value::Array(work));
                    }
                }
            }
            match page.cursor {
                Some(cursor) => after = Some(cursor),
                None => return Ok(Value::Array(work)),
            }
        }
    }

    /// Pools: lease due markers to `owner`, inspecting at most
    /// `options.limit` of them in `since` order and dropping those whose
    /// work is already done. Each claim is `{args, key, input, owner,
    /// attempt, expiresAt}`; attempts count claims of the same key, and
    /// start again at 1 for a new key.
    pub fn claim(
        &self,
        ctx: &mut Ctx<'_, '_>,
        owner: &str,
        options: ClaimOptions,
    ) -> Result<Value> {
        self.require_pool()?;
        validate_owner(owner)?;
        let limit = options.limit.clamp(1, MAX_NEXT);
        let lease_ms = self.lease_length(options.lease_ms)?;
        let now = ctx.clock()?;
        let expires_at = now + lease_ms as f64;
        let page = self.markers(ctx, Some(now), limit, None)?;
        let mut claims = Vec::new();
        for row in page.rows {
            let found = self.work(ctx, &row.key)?;
            let marker = args_row(&row.key)?;
            let Value::Object(mut claim) = found else {
                ctx.delete(self.stale(), &marker)?;
                continue;
            };
            let key = claim.get("key").cloned().unwrap_or(Value::Null);
            let attempt = next_attempt(&row.value, &key);
            let mut lease = BTreeMap::new();
            lease.insert("since".to_string(), Value::Number(expires_at));
            lease.insert("key".to_string(), key);
            lease.insert("attempt".to_string(), Value::Number(attempt as f64));
            lease.insert("owner".to_string(), Value::String(owner.to_string()));
            ctx.set(self.stale(), &marker, Value::Object(lease))?;
            claim.insert("owner".to_string(), Value::String(owner.to_string()));
            claim.insert("attempt".to_string(), Value::Number(attempt as f64));
            claim.insert("expiresAt".to_string(), Value::Number(expires_at));
            claims.push(Value::Object(claim));
        }
        Ok(Value::Array(claims))
    }

    /// The marker a lease still holds, with its row key.
    fn held(
        &self,
        ctx: &mut Ctx<'_, '_>,
        lease: &Value,
        now: f64,
    ) -> Result<Option<(Value, BTreeMap<String, Value>)>> {
        let (args, key, owner, attempt) = lease_parts(lease)?;
        let row = args_row(args)?;
        let Some(Value::Object(marker)) = ctx.get(self.stale(), &row)? else {
            return Ok(None);
        };
        let holds = marker.get("owner") == Some(&Value::String(owner.to_string()))
            && marker.get("key") == Some(&Value::String(key.to_string()))
            && marker.get("attempt") == Some(&Value::Number(attempt as f64))
            && matches!(marker.get("since"), Some(Value::Number(since)) if now < *since);
        Ok(holds.then_some((row, marker)))
    }

    /// Pools: extend leases by `lease_ms` from now: one new expiry per
    /// lease, or `None` for a lease no longer held (lapsed, released,
    /// published, or dropped by an input change).
    pub fn renew(
        &self,
        ctx: &mut Ctx<'_, '_>,
        leases: &[Value],
        lease_ms: Option<u64>,
    ) -> Result<Vec<Option<u64>>> {
        self.require_pool()?;
        if leases.len() > MAX_RENEWALS {
            return Err(fail(
                codes::INVALID_VALUE,
                "renewals per call are at most 1024",
            ));
        }
        let lease_ms = self.lease_length(lease_ms)?;
        let now = ctx.clock()?;
        let mut renewed = Vec::with_capacity(leases.len());
        for lease in leases {
            let Some((row, mut marker)) = self.held(ctx, lease, now)? else {
                renewed.push(None);
                continue;
            };
            let expires_at = now + lease_ms as f64;
            marker.insert("since".to_string(), Value::Number(expires_at));
            ctx.set(self.stale(), &row, Value::Object(marker))?;
            renewed.push(Some(expires_at as u64));
        }
        Ok(renewed)
    }

    /// Pools: hand a held lease back, claimable again after `delay_ms`
    /// (by anyone; the attempt count carries on). False when the lease
    /// is no longer held.
    pub fn release(&self, ctx: &mut Ctx<'_, '_>, lease: &Value, delay_ms: u64) -> Result<bool> {
        self.require_pool()?;
        let now = ctx.clock()?;
        let Some((row, mut marker)) = self.held(ctx, lease, now)? else {
            return Ok(false);
        };
        marker.remove("owner");
        marker.insert("since".to_string(), Value::Number(now + delay_ms as f64));
        ctx.set(self.stale(), &row, Value::Object(marker))?;
        Ok(true)
    }

    /// The oldest due marker's `since`, and the next one to fall due,
    /// declaring the latter as when a reading query changes.
    fn due(&self, ctx: &mut impl ReadCtx) -> Result<(Option<u64>, Option<u64>)> {
        let now = ctx.clock()?;
        let since = |row: &crate::ctx::RawRow| match &row.value {
            Value::Object(marker) => match marker.get("since") {
                Some(Value::Number(since)) => Some(*since),
                _ => None,
            },
            _ => None,
        };
        let oldest = self
            .markers(ctx, Some(now), 1, None)?
            .rows
            .first()
            .and_then(since);
        let mut bounds = RangeBounds::limited(1);
        bounds.gt = Some(Value::Number(now));
        let next = ctx
            .range(self.stale(), "since", bounds)?
            .rows
            .first()
            .and_then(since);
        ctx.changes_at(next)?;
        Ok((oldest.map(|at| at as u64), next.map(|at| at as u64)))
    }

    /// Pools: whether a claim would find a due marker now. A query
    /// reading it changes when the next lease or delay ends.
    pub fn ready(&self, ctx: &mut impl ReadCtx) -> Result<bool> {
        self.require_pool()?;
        Ok(self.due(ctx)?.0.is_some())
    }

    /// Pools: `{ready, oldestReadyAt, nextAvailableAt}`: whether a due
    /// marker exists, the oldest one's `since`, and when the next lease
    /// or delay ends.
    pub fn pool_stats(&self, ctx: &mut impl ReadCtx) -> Result<Value> {
        self.require_pool()?;
        let (oldest, next) = self.due(ctx)?;
        let time = |at: Option<u64>| at.map_or(Value::Null, |at| Value::Number(at as f64));
        let mut stats = BTreeMap::new();
        stats.insert("ready".to_string(), Value::Bool(oldest.is_some()));
        stats.insert("oldestReadyAt".to_string(), time(oldest));
        stats.insert("nextAvailableAt".to_string(), time(next));
        Ok(Value::Object(stats))
    }

    /// Pools: a source row changed. A row that is gone takes its result
    /// and marker with it; any other change marks its arguments stale
    /// unless its stored result is still current. A marker already
    /// there stays (its place, lease or release delay with it) unless it
    /// was for a key the change made obsolete.
    pub fn track(&self, ctx: &mut Ctx<'_, '_>, change: &Change) -> Result<()> {
        let row = change.key.clone();
        if change.after.is_none() {
            ctx.delete(CollectionRef::local(self.results.name), &row)?;
            return ctx.delete(self.stale(), &row);
        }
        let current = match self.wanted(ctx, &change.key)? {
            None => None,
            Some((key, _)) => {
                if self
                    .stored(ctx, &row)?
                    .is_some_and(|(stored, _)| stored == key)
                {
                    None
                } else {
                    Some(key)
                }
            }
        };
        let Some(key) = current else {
            return ctx.delete(self.stale(), &row);
        };
        let keep = match ctx.get(self.stale(), &row)? {
            Some(Value::Object(marker)) => match marker.get("key") {
                None => true,
                Some(leased) => *leased == Value::String(key),
            },
            _ => false,
        };
        if keep {
            return Ok(());
        }
        let mut marker = BTreeMap::new();
        marker.insert("since".to_string(), Value::Number(ctx.clock()?));
        ctx.set(self.stale(), &row, Value::Object(marker))
    }

    #[doc(hidden)]
    pub fn publish_call(&self, ctx: &mut Ctx<'_, '_>, args: Value) -> Result<Value> {
        let Value::Object(mut fields) = args else {
            return Err(fail(
                codes::TYPE_MISMATCH,
                "publish takes {args, key, value}",
            ));
        };
        let (Some(args), Some(Value::String(key)), Some(value)) = (
            fields.remove("args"),
            fields.remove("key"),
            fields.remove("value"),
        ) else {
            return Err(fail(
                codes::TYPE_MISMATCH,
                "publish takes {args, key, value}",
            ));
        };
        self.publish(ctx, &args, &key, value).map(Value::Bool)
    }

    #[doc(hidden)]
    pub fn change_call(&self, ctx: &mut Ctx<'_, '_>, args: Value) -> Result<Value> {
        let Value::Object(mut fields) = args else {
            return Err(fail(codes::TYPE_MISMATCH, "a change is an object"));
        };
        let image = |value: Option<Value>| value.filter(|value| *value != Value::Null);
        let change = Change {
            key: fields.remove("key").unwrap_or(Value::Null),
            before: image(fields.remove("before")),
            after: image(fields.remove("after")),
        };
        self.track(ctx, &change).map(|()| Value::Null)
    }

    #[doc(hidden)]
    pub fn next_call(&self, ctx: &mut Ctx<'_, '_>, args: Value) -> Result<Value> {
        let fields = options(&args, "next takes {limit, shard}")?;
        let shard = match fields.get("shard") {
            None | Some(Value::Null) => None,
            Some(Value::Array(pair)) if pair.len() == 2 => Some((
                whole(&pair[0], "shard index")? as u32,
                whole(&pair[1], "shard count")? as u32,
            )),
            Some(_) => {
                return Err(fail(codes::TYPE_MISMATCH, "shard is [index, count]"));
            }
        };
        let next = Next {
            limit: positive(fields.get("limit"), 16, "limit")?,
            shard,
        };
        self.next(ctx, next)
    }

    #[doc(hidden)]
    pub fn claim_call(&self, ctx: &mut Ctx<'_, '_>, args: Value) -> Result<Value> {
        let fields = options(&args, "claim takes {owner, limit, leaseMs}")?;
        let Some(Value::String(owner)) = fields.get("owner") else {
            return Err(fail(codes::TYPE_MISMATCH, "claim takes an owner"));
        };
        let options = ClaimOptions {
            limit: positive(fields.get("limit"), 16, "limit")?,
            lease_ms: optional_whole(fields.get("leaseMs"), "leaseMs")?,
        };
        self.claim(ctx, owner, options)
    }

    #[doc(hidden)]
    pub fn renew_call(&self, ctx: &mut Ctx<'_, '_>, args: Value) -> Result<Value> {
        let fields = options(&args, "renew takes {leases, leaseMs}")?;
        let Some(Value::Array(leases)) = fields.get("leases") else {
            return Err(fail(codes::TYPE_MISMATCH, "renew takes a list of leases"));
        };
        let lease_ms = optional_whole(fields.get("leaseMs"), "leaseMs")?;
        let renewed = self.renew(ctx, leases, lease_ms)?;
        Ok(Value::Array(
            renewed
                .into_iter()
                .map(|expiry| expiry.map_or(Value::Null, |at| Value::Number(at as f64)))
                .collect(),
        ))
    }

    #[doc(hidden)]
    pub fn release_call(&self, ctx: &mut Ctx<'_, '_>, args: Value) -> Result<Value> {
        let fields = options(&args, "release takes a lease and delayMs")?;
        let delay_ms = optional_whole(fields.get("delayMs"), "delayMs")?.unwrap_or(0);
        self.release(ctx, &args, delay_ms).map(Value::Bool)
    }
}

/// The row key of an argument set: the arguments themselves.
fn args_row(args: &Value) -> Result<Value> {
    args.validate()?;
    Ok(args.clone())
}

/// FNV-1a over the UTF-16 code units of an argument set's canonical
/// key, modulo `count`: v1's shard function, so v1 and v2 workers agree
/// on which shard owns which argument set.
pub fn shard_of(row: &str, count: u32) -> u32 {
    let hash = row.encode_utf16().fold(0x811c_9dc5_u32, |hash, unit| {
        (hash ^ u32::from(unit)).wrapping_mul(0x0100_0193)
    });
    hash % count.max(1)
}

/// A marker's next attempt for `key`: one more than its last for the
/// same key, else 1.
fn next_attempt(marker: &Value, key: &Value) -> u64 {
    match marker {
        Value::Object(marker) if marker.get("key") == Some(key) => match marker.get("attempt") {
            Some(Value::Number(attempt)) if *attempt >= 1.0 => *attempt as u64 + 1,
            _ => 1,
        },
        _ => 1,
    }
}

/// Validate a lease owner: 1-128 letters, digits, dots, dashes or
/// underscores.
pub fn validate_owner(owner: &str) -> Result<()> {
    if owner.is_empty()
        || owner.len() > MAX_OWNER
        || !owner
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(fail(
            codes::INVALID_VALUE,
            "lease owner takes 1-128 letters, digits, dots, dashes or underscores",
        ));
    }
    Ok(())
}

/// A lease's `{args, key, owner, attempt}`.
fn lease_parts(lease: &Value) -> Result<(&Value, &str, &str, u64)> {
    let malformed = || {
        fail(
            codes::TYPE_MISMATCH,
            "a lease is {args, key, owner, attempt}",
        )
    };
    let Value::Object(fields) = lease else {
        return Err(malformed());
    };
    let (Some(args), Some(Value::String(key)), Some(Value::String(owner)), Some(attempt)) = (
        fields.get("args"),
        fields.get("key"),
        fields.get("owner"),
        fields.get("attempt"),
    ) else {
        return Err(malformed());
    };
    validate_owner(owner)?;
    let attempt = whole(attempt, "attempt")?;
    if attempt == 0 {
        return Err(fail(codes::INVALID_VALUE, "attempts count from 1"));
    }
    Ok((args, key, owner, attempt))
}

/// The fields of an options object; null reads as none.
fn options(args: &Value, what: &str) -> Result<BTreeMap<String, Value>> {
    match args {
        Value::Object(fields) => Ok(fields.clone()),
        Value::Null => Ok(BTreeMap::new()),
        _ => Err(fail(codes::TYPE_MISMATCH, what)),
    }
}

fn whole(value: &Value, what: &str) -> Result<u64> {
    match value {
        Value::Number(number) if number.is_finite() && number % 1.0 == 0.0 && *number >= 0.0 => {
            Ok(*number as u64)
        }
        _ => Err(fail(
            codes::TYPE_MISMATCH,
            alloc::format!("{what} is a whole number"),
        )),
    }
}

fn optional_whole(value: Option<&Value>, what: &str) -> Result<Option<u64>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => whole(value, what).map(Some),
    }
}

fn positive(value: Option<&Value>, default: u32, what: &str) -> Result<u32> {
    match optional_whole(value, what)? {
        None => Ok(default),
        Some(0) => Err(fail(
            codes::TYPE_MISMATCH,
            alloc::format!("{what} is a positive number"),
        )),
        Some(value) => Ok(value.min(u64::from(u32::MAX)) as u32),
    }
}

/// An external value's state, typed.
#[derive(Clone, Debug, PartialEq)]
pub enum State<R> {
    /// Its current input has no accepted result yet.
    Pending,
    /// The result for its current input.
    Ready(R),
}

/// Work to compute, typed: the arguments, the key naming their current
/// input, and the input.
#[derive(Clone, Debug, PartialEq)]
pub struct Work<A, I> {
    /// The argument set.
    pub args: A,
    /// Canonical `[args, input]`: publish under it.
    pub key: String,
    /// The input to compute from.
    pub input: I,
}

/// A leased pool's lease, typed: what `renew` and `release` name.
#[derive(Clone, Debug, PartialEq)]
pub struct Lease<A> {
    /// The argument set.
    pub args: A,
    /// The key the lease is for.
    pub key: String,
    /// The lease's owner.
    pub owner: String,
    /// Claims of this key so far, this one included.
    pub attempt: u64,
}

impl<A: FlowerType> Lease<A> {
    /// The lease as the raw operations take it.
    pub fn encode(&self) -> Result<Value> {
        let mut lease = BTreeMap::new();
        lease.insert("args".to_string(), self.args.encode()?);
        lease.insert("key".to_string(), Value::String(self.key.clone()));
        lease.insert("owner".to_string(), Value::String(self.owner.clone()));
        lease.insert("attempt".to_string(), Value::Number(self.attempt as f64));
        let lease = Value::Object(lease);
        lease_parts(&lease)?;
        Ok(lease)
    }
}

/// A leased claim, typed.
#[derive(Clone, Debug, PartialEq)]
pub struct Claimed<A, I> {
    /// The lease.
    pub lease: Lease<A>,
    /// The input to compute from.
    pub input: I,
    /// When the lease ends unless renewed.
    pub expires_ms: u64,
}

/// A leased pool's readiness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolStats {
    /// Whether a claim would find a due marker now.
    pub ready: bool,
    /// The oldest due marker's `since`.
    pub oldest_ready_ms: Option<u64>,
    /// When the next lease or release delay ends.
    pub next_available_ms: Option<u64>,
}

/// The marker a typed handle carries: it owns no `A`, `I` or `R`.
type Types<A, I, R> = PhantomData<fn() -> (A, I, R)>;

/// An external value with typed arguments `A`, inputs `I` and results
/// `R` ([`External::typed`]). Reads take any context's raw [`Ctx`]
/// (`ctx.ctx()`); publishing and leasing take a [`MutationCtx`].
pub struct TypedExternal<A, I, R> {
    raw: &'static External,
    _types: Types<A, I, R>,
}

impl<A: FlowerType, I: FlowerType, R: FlowerType> TypedExternal<A, I, R> {
    /// The untyped external value.
    pub fn raw(&self) -> &'static External {
        self.raw
    }

    /// The state for `args`, or `None` when there is nothing to compute.
    pub fn state(&self, ctx: &mut impl ReadCtx, args: &A) -> Result<Option<State<R>>> {
        let Value::Object(mut state) = self.raw.state(ctx, &args.encode()?)? else {
            return Ok(None);
        };
        match state.remove("status") {
            Some(Value::String(status)) if status == "pending" => Ok(Some(State::Pending)),
            Some(Value::String(status)) if status == "ready" => Ok(Some(State::Ready(R::decode(
                state.remove("value").unwrap_or(Value::Null),
            )?))),
            _ => Err(fail(codes::INVALID_VALUE, "invalid external state")),
        }
    }

    /// The work for `args`, or `None` when its result is current.
    pub fn pending(&self, ctx: &mut impl ReadCtx, args: &A) -> Result<Option<Work<A, I>>> {
        match self.raw.work(ctx, &args.encode()?)? {
            Value::Null => Ok(None),
            work => decode_work(work).map(Some),
        }
    }

    /// Pools: stale work, typed (see [`External::next`]).
    pub fn next(&self, ctx: &mut impl ReadCtx, next: Next) -> Result<Vec<Work<A, I>>> {
        match self.raw.next(ctx, next)? {
            Value::Array(items) => items.into_iter().map(decode_work).collect(),
            _ => Err(fail(codes::TYPE_MISMATCH, "next lists work")),
        }
    }

    /// Publish a result for `key` (see [`External::publish`]).
    pub fn publish(
        &self,
        ctx: &mut MutationCtx<'_, '_>,
        args: &A,
        key: &str,
        value: &R,
    ) -> Result<bool> {
        self.raw
            .publish(ctx.ctx(), &args.encode()?, key, value.encode()?)
    }

    /// Pools: lease due work (see [`External::claim`]).
    pub fn claim(
        &self,
        ctx: &mut MutationCtx<'_, '_>,
        owner: &str,
        options: ClaimOptions,
    ) -> Result<Vec<Claimed<A, I>>> {
        let Value::Array(claims) = self.raw.claim(ctx.ctx(), owner, options)? else {
            return Err(fail(codes::TYPE_MISMATCH, "claim lists claims"));
        };
        claims
            .into_iter()
            .map(|claim| {
                let Value::Object(mut fields) = claim else {
                    return Err(fail(codes::TYPE_MISMATCH, "a claim is an object"));
                };
                let take = |fields: &mut BTreeMap<String, Value>, name: &str| {
                    fields.remove(name).unwrap_or(Value::Null)
                };
                let lease = Lease {
                    args: A::decode(take(&mut fields, "args"))?,
                    key: String::decode(take(&mut fields, "key"))?,
                    owner: String::decode(take(&mut fields, "owner"))?,
                    attempt: whole(&take(&mut fields, "attempt"), "attempt")?,
                };
                Ok(Claimed {
                    lease,
                    input: I::decode(take(&mut fields, "input"))?,
                    expires_ms: whole(&take(&mut fields, "expiresAt"), "expiresAt")?,
                })
            })
            .collect()
    }

    /// Pools: extend leases (see [`External::renew`]).
    pub fn renew(
        &self,
        ctx: &mut MutationCtx<'_, '_>,
        leases: &[Lease<A>],
        lease_ms: Option<u64>,
    ) -> Result<Vec<Option<u64>>> {
        let leases = leases
            .iter()
            .map(Lease::encode)
            .collect::<Result<Vec<_>>>()?;
        self.raw.renew(ctx.ctx(), &leases, lease_ms)
    }

    /// Pools: hand a lease back (see [`External::release`]).
    pub fn release(
        &self,
        ctx: &mut MutationCtx<'_, '_>,
        lease: &Lease<A>,
        delay_ms: u64,
    ) -> Result<bool> {
        self.raw.release(ctx.ctx(), &lease.encode()?, delay_ms)
    }

    /// Pools: whether a claim would find work now.
    pub fn ready(&self, ctx: &mut impl ReadCtx) -> Result<bool> {
        self.raw.ready(ctx)
    }

    /// Pools: readiness and timing.
    pub fn stats(&self, ctx: &mut Ctx<'_, '_>) -> Result<PoolStats> {
        self.raw.require_pool()?;
        let (oldest, next) = self.raw.due(ctx)?;
        Ok(PoolStats {
            ready: oldest.is_some(),
            oldest_ready_ms: oldest,
            next_available_ms: next,
        })
    }
}

/// Decode a raw work item.
fn decode_work<A: FlowerType, I: FlowerType>(work: Value) -> Result<Work<A, I>> {
    let Value::Object(mut fields) = work else {
        return Err(fail(codes::TYPE_MISMATCH, "work is an object"));
    };
    let mut take = |name: &str| fields.remove(name).unwrap_or(Value::Null);
    Ok(Work {
        args: A::decode(take("args"))?,
        key: String::decode(take("key"))?,
        input: I::decode(take("input"))?,
    })
}

/// Declare an [`External`] static whose definitions call it.
///
/// ```ignore
/// external!(pub static WORDS = "words", input = words_input);
/// external!(pub static TRACKED = "tracked", input = tracked_input, tracks = "docs");
/// external!(static SCORED = "scored", input = score_input, result = v::number());
/// external!(static LEASED = "leased", input = leased_input, tracks = "docs", lease = (10_000, 60_000));
/// ```
///
/// `input` is a raw derived callback over the arguments: it must only
/// read (the host runs it as a derived value), and returns null when
/// there is nothing to compute. `tracks` names a source collection,
/// making the value a pool over its row keys. `result` is the contract
/// published values must meet (any JSON by default). `lease` sets a
/// pool's default and longest lease in milliseconds (30 s and 300 s by
/// default). `http` lets workers and clients reach `get`, `pending`,
/// `publish`, `next`, `claim`, `renew`, `release`, `ready` and `stats`
/// over HTTP under their names; without it they answer engine calls
/// only. Who may call each is the authorization component's
/// [`methods`](crate::auth::Authorization::methods) to say.
///
/// ```ignore
/// external!(pub static WORDS = "words", input = words_input, http);
/// ```
#[macro_export]
macro_rules! external {
    (
        $(#[$meta:meta])*
        $vis:vis static $id:ident = $name:literal,
        input = $input:path
        $(, result = $result:expr)?
        $(, tracks = $source:literal)?
        $(, lease = ($default_lease:expr, $max_lease:expr))?
        , http
        $(,)?
    ) => {
        $crate::external!(
            @define true;
            $(#[$meta])* $vis static $id = $name, input = $input
            $(, result = $result)? $(, tracks = $source)?
            $(, lease = ($default_lease, $max_lease))?
        );
    };
    (
        $(#[$meta:meta])*
        $vis:vis static $id:ident = $name:literal,
        input = $input:path
        $(, result = $result:expr)?
        $(, tracks = $source:literal)?
        $(, lease = ($default_lease:expr, $max_lease:expr))?
        $(,)?
    ) => {
        $crate::external!(
            @define false;
            $(#[$meta])* $vis static $id = $name, input = $input
            $(, result = $result)? $(, tracks = $source)?
            $(, lease = ($default_lease, $max_lease))?
        );
    };
    (
        @define $http:literal;
        $(#[$meta:meta])*
        $vis:vis static $id:ident = $name:literal,
        input = $input:path
        $(, result = $result:expr)?
        $(, tracks = $source:literal)?
        $(, lease = ($default_lease:expr, $max_lease:expr))?
    ) => {
        $(#[$meta])*
        $vis static $id: $crate::external::External = $crate::external::External {
            name: $name,
            results: $crate::app::Collection::new(concat!($name, ".results")),
            stale: $crate::app::Collection::new(concat!($name, ".stale"))
                .indexes(&$crate::external::STALE_INDEXES),
            input: $crate::app::Derived::new(concat!($name, ".input"), $input),
            get: $crate::app::Method::query(concat!($name, ".get"), |ctx, args| {
                $id.state(ctx, &args)
            })
            .with_http($http),
            pending: $crate::app::Method::query(concat!($name, ".pending"), |ctx, args| {
                $id.work(ctx, &args)
            })
            .with_http($http),
            publish: $crate::app::Method::mutation(concat!($name, ".publish"), |ctx, args| {
                $id.publish_call(ctx, args)
            })
            .with_http($http),
            next: $crate::app::Method::query(concat!($name, ".next"), |ctx, args| {
                $id.next_call(ctx, args)
            })
            .with_http($http),
            claim: $crate::app::Method::mutation(concat!($name, ".claim"), |ctx, args| {
                $id.claim_call(ctx, args)
            })
            .with_http($http),
            renew: $crate::app::Method::mutation(concat!($name, ".renew"), |ctx, args| {
                $id.renew_call(ctx, args)
            })
            .with_http($http),
            release: $crate::app::Method::mutation(concat!($name, ".release"), |ctx, args| {
                $id.release_call(ctx, args)
            })
            .with_http($http),
            ready: $crate::app::Method::query(concat!($name, ".ready"), |ctx, _args| {
                $id.ready(ctx).map($crate::value::Value::Bool)
            })
            .with_http($http),
            stats: $crate::app::Method::query(concat!($name, ".stats"), |ctx, _args| {
                $id.pool_stats(ctx)
            })
            .with_http($http),
            track: $crate::app::Trigger::new(
                concat!($name, ".track"),
                $crate::external!(@source $($source)?),
                |ctx, change| $id.track(ctx, change),
            ),
            on_change: $crate::app::Method::mutation(concat!($name, ".track"), |ctx, args| {
                $id.change_call(ctx, args)
            }),
            result: $crate::external!(@result $($result)?),
            pool: $crate::external!(@pool $($source)?),
            default_lease_ms: $crate::external!(@default_lease $($default_lease)?),
            max_lease_ms: $crate::external!(@max_lease $($max_lease)?),
        };
    };
    (@source) => { "" };
    (@source $source:literal) => { $source };
    (@pool) => { false };
    (@pool $source:literal) => { true };
    (@result) => { $crate::schema::v::json() };
    (@result $result:expr) => { $result };
    (@default_lease) => { $crate::external::DEFAULT_LEASE_MS };
    (@default_lease $lease:expr) => { $lease };
    (@max_lease) => { $crate::external::MAX_LEASE_MS };
    (@max_lease $lease:expr) => { $lease };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::v;

    fn input(_ctx: &mut Ctx<'_, '_>, _args: Value) -> Result<Value> {
        Ok(Value::Null)
    }

    crate::external!(static PLAIN = "plain", input = input);
    crate::external!(
        static POOL = "pool",
        input = input,
        result = v::number().build(),
        tracks = "docs",
        lease = (5_000, 20_000),
        http,
    );

    #[test]
    fn the_macro_names_every_descriptor_after_the_value() {
        assert_eq!(PLAIN.results.name, "plain.results");
        assert_eq!(PLAIN.stale.name, "plain.stale");
        assert_eq!(PLAIN.input.name, "plain.input");
        assert_eq!(PLAIN.get.name, "plain.get");
        assert_eq!(PLAIN.pending.name, "plain.pending");
        assert_eq!(PLAIN.publish.name, "plain.publish");
        assert_eq!(PLAIN.next.name, "plain.next");
        assert!(!PLAIN.pool);
        assert_eq!(
            (PLAIN.default_lease_ms, PLAIN.max_lease_ms),
            (DEFAULT_LEASE_MS, MAX_LEASE_MS)
        );
        assert!(POOL.pool);
        assert_eq!((POOL.default_lease_ms, POOL.max_lease_ms), (5_000, 20_000));
        assert_eq!(POOL.track.source, "docs");
        assert_eq!(POOL.track.name, "pool.track");
        assert_eq!(POOL.stale.indexes.len(), 1);
        assert!(POOL.result.is(&Value::Number(1.0)));
        assert!(!POOL.result.is(&Value::String("one".into())));
        static APP: crate::App = crate::App::new(&[&POOL]);
        let manifest = crate::abi::manifest_value(&APP).unwrap();
        let names = |member: &str, field: &str| -> Vec<String> {
            manifest.as_object().unwrap()[member]
                .as_array()
                .unwrap()
                .iter()
                .map(|entry| match &entry.as_object().unwrap()[field] {
                    Value::String(name) => name.clone(),
                    _ => panic!("names are strings"),
                })
                .collect()
        };
        assert_eq!(
            names("http", "alias"),
            [
                "pool.get",
                "pool.pending",
                "pool.publish",
                "pool.next",
                "pool.claim",
                "pool.renew",
                "pool.release",
                "pool.ready",
                "pool.stats"
            ]
        );
        assert_eq!(names("definitions", "name").len(), 11);
        assert_eq!(names("collections", "name"), ["pool.results", "pool.stale"]);
        assert_eq!(names("triggers", "name"), ["pool.track"]);
        assert_eq!(POOL.on_change.name, "pool.track");
        // Without `http`, workers reach nothing over HTTP.
        static QUIET: crate::App = crate::App::new(&[&PLAIN]);
        let manifest = crate::abi::manifest_value(&QUIET).unwrap();
        assert_eq!(
            manifest.as_object().unwrap()["http"],
            Value::Array(Vec::new())
        );
    }

    #[test]
    fn leases_are_checked_before_any_host_call() {
        assert!(POOL.lease_length(None).is_ok_and(|lease| lease == 5_000));
        assert_eq!(
            POOL.lease_length(Some(20_001)).unwrap_err().code,
            "LEASE_TOO_LONG"
        );
        assert!(POOL.lease_length(Some(0)).is_err());
        let mut lease = BTreeMap::new();
        lease.insert("args".to_string(), Value::String("a".into()));
        lease.insert("key".to_string(), Value::String("k".into()));
        lease.insert("owner".to_string(), Value::String("w-1".into()));
        lease.insert("attempt".to_string(), Value::Number(2.0));
        assert!(lease_parts(&Value::Object(lease.clone())).is_ok());
        lease.insert("attempt".to_string(), Value::Number(0.0));
        assert!(lease_parts(&Value::Object(lease.clone())).is_err());
        lease.insert("attempt".to_string(), Value::Number(1.0));
        lease.insert("owner".to_string(), Value::String(String::new()));
        assert!(lease_parts(&Value::Object(lease)).is_err());
        let typed = Lease {
            args: String::from("a"),
            key: String::from("k"),
            owner: String::from("w"),
            attempt: 1,
        };
        assert!(typed.encode().is_ok());
    }

    #[test]
    fn attempts_count_per_key() {
        let mut marker = BTreeMap::new();
        marker.insert("key".to_string(), Value::String("k".into()));
        marker.insert("attempt".to_string(), Value::Number(2.0));
        let marker = Value::Object(marker);
        assert_eq!(next_attempt(&marker, &Value::String("k".into())), 3);
        assert_eq!(next_attempt(&marker, &Value::String("other".into())), 1);
        assert_eq!(next_attempt(&Value::Null, &Value::String("k".into())), 1);
    }

    #[test]
    fn shards_hash_utf16_like_v1() {
        // v1's golden: FNV-1a over UTF-16 units, so a supplementary-plane
        // character hashes as its two surrogates.
        let expected = [0x22u16, 0xd83c, 0xdf38, 0x22]
            .into_iter()
            .fold(0x811c_9dc5_u32, |hash, unit| {
                (hash ^ u32::from(unit)).wrapping_mul(0x0100_0193)
            })
            % 7;
        assert_eq!(shard_of("\"\u{1F338}\"", 7), expected);
        // Every argument set lands in exactly one shard.
        for row in ["\"a\"", "\"b\"", "[1,2]", "{\"id\":3}"] {
            let shard = shard_of(row, 4);
            assert!(shard < 4);
            assert_eq!(shard, shard_of(row, 4));
        }
    }
}
