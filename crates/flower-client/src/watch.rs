//! Watches: server-sent query snapshots with reconnecting
//! subscriptions.
//!
//! Pull-driven throughout: each `next` call drives the connection and
//! stall detection runs while polled. Dropping a watch releases its
//! connection. Reconnect begins with a fresh snapshot marked
//! `reset = true`; intermediate commits may be coalesced. This is not
//! an event-history cursor.
//!
//! Connections ask for deltas: after the first snapshot the server may
//! send a change as an RFC 6902 patch to the value it last sent, named
//! by that value's `id`. [`DeltaStream`] keeps the current value,
//! applies each patch and yields whole values, so every layer above
//! sees snapshots only. A patch that names another base, or does not
//! apply, is a gap: the stream reconnects at once for a fresh snapshot
//! and yields it as [`WatchEvent::Reset`]. After [`MAX_GAPS`] gaps in a
//! row it stops asking for deltas.

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::time::Duration;

use futures_util::StreamExt;
use serde::de::DeserializeOwned;

use crate::client::{Envelope, FlowerClient, QueryResult, status_error};
use crate::error::FlowerError;
use crate::retry::{Retry, backoff};
use crate::transport::{ByteStream, Credentials};

/// Bound on one buffered SSE event (field lines plus data) in bytes.
/// Larger events are discarded and surface as one decode error.
const MAX_EVENT_BYTES: usize = 16 * 1024 * 1024;
/// Consecutive gaps after which a stream stops asking for deltas.
pub const MAX_GAPS: u32 = 3;

/// Raw watch options: credential override and connect timeout.
#[derive(Clone, Debug, Default)]
pub struct WatchOptions {
    /// Credential override.
    pub credentials: Option<Credentials>,
    /// Connect timeout override.
    pub timeout: Option<Duration>,
    pub cancel: Option<crate::CancellationToken>,
}

/// Subscription options: reconnect backoff, reconnect bound, stall
/// timeout and monotonic suppression.
#[derive(Clone, Debug)]
pub struct SubscribeOptions {
    /// Raw watch options.
    pub watch: WatchOptions,
    /// Reconnect backoff shape. `Off` connects once.
    pub retry: Retry,
    /// Maximum consecutive reconnects without a delivered event;
    /// `None` reconnects forever. A connection that delivers an
    /// event restarts the budget and the backoff. Defaults to `None`.
    pub max_reconnects: Option<u32>,
    /// Reconnect when no bytes (events, heartbeats or comments)
    /// arrive within this long. Defaults to 45 s.
    pub stall_timeout: Duration,
    /// Suppress revisions older than the newest seen. Equal revisions
    /// still deliver. Defaults to false.
    pub monotonic: bool,
}

impl Default for SubscribeOptions {
    fn default() -> Self {
        SubscribeOptions {
            watch: WatchOptions::default(),
            retry: Retry::Default,
            max_reconnects: None,
            stall_timeout: Duration::from_secs(45),
            monotonic: false,
        }
    }
}

/// A raw watch event: a full snapshot, or a protocol reset carrying
/// the current value.
#[derive(Clone, Debug)]
pub enum WatchEvent {
    /// Full value at a revision.
    Snapshot {
        /// Database revision at evaluation.
        revision: u64,
        /// Evaluated value.
        value: serde_json::Value,
    },
    /// Resync marker with the current value.
    Reset {
        /// Database revision at evaluation.
        revision: u64,
        /// Evaluated value.
        value: serde_json::Value,
    },
}

/// A subscription update: decoded value at a revision. `reset` marks
/// the first event after (re)connecting.
#[derive(Clone, Debug)]
pub struct Update<T> {
    /// Database revision at evaluation.
    pub revision: u64,
    /// Decoded value.
    pub value: T,
    /// First event after (re)connecting.
    pub reset: bool,
    pub history: Option<flower_protocol::WatchHistory>,
    pub read_token: Option<flower_protocol::ReadToken>,
}

fn watch_token(
    database: &str,
    history: &flower_protocol::WatchHistory,
    revision: u64,
) -> flower_protocol::ReadToken {
    flower_protocol::ReadToken {
        version: flower_protocol::VERSION,
        database: database.into(),
        group: history.group.clone(),
        incarnation: history.incarnation,
        revision: flower_protocol::Revision(revision),
        position: flower_protocol::LogPosition(0),
    }
}

/// Whether a failure may clear on reconnect.
fn transient(error: &FlowerError) -> bool {
    error.is_transient()
}

/// The watch request: the query's call fields, asking for deltas when
/// `deltas`.
fn watch_body(
    db: &str,
    module: &str,
    name: &str,
    args: &serde_json::Value,
    deltas: bool,
) -> Result<Vec<u8>, FlowerError> {
    serde_json::to_vec(&flower_protocol::request::WatchRequest {
        call: flower_protocol::request::QueryRequest::new(db, module, name, args.clone()),
        deltas,
    })
    .map_err(|error| FlowerError::Decode(error.to_string()))
}

/// Map a non-200 connect status with its drained body. Overload
/// statuses keep their status code so reconnects treat them as
/// transient.
fn connect_error(status: u16, body: &[u8]) -> FlowerError {
    if status == 503
        && let Ok(envelope) = serde_json::from_slice::<Envelope>(body)
        && envelope.code.as_deref() == Some("forward")
    {
        return FlowerError::Forward {
            leader: envelope.leader,
        };
    }
    status_error(status, body)
}

/// Incremental parser for the WHATWG `text/event-stream` format.
///
/// Lines end in CRLF, LF or a lone CR, including a CRLF split across
/// chunks. A blank line dispatches the buffered event; lines starting
/// with `:` are comments; `field: value` drops one leading space from
/// the value; a line without a colon is a field with an empty value.
/// `event` names the type, `data` lines join with LF, and other
/// fields are ignored. Blocks without data (heartbeats, comments) and
/// unknown event types dispatch nothing. Buffered bytes per event are
/// bounded: an event past the bound is discarded through its
/// terminating blank line and surfaces one decode error. An event the
/// stream ends before terminating is discarded.
///
/// [`Wire`]: one dispatched `snapshot`, `reset` or `patch` event.
struct SseParser {
    /// Maximum buffered bytes per event.
    limit: usize,
    history: Option<flower_protocol::WatchHistory>,
    /// The current line, without its terminator.
    line: Vec<u8>,
    /// Whether the current line had any bytes, buffered or not.
    line_seen: bool,
    /// The previous line ended in CR: a leading LF completes CRLF.
    after_cr: bool,
    /// The event type buffer.
    kind: Vec<u8>,
    /// The data buffer, each line followed by LF.
    data: Vec<u8>,
    /// The current event overflowed the bound and is being discarded.
    oversized: bool,
    /// Dispatched events awaiting delivery.
    ready: VecDeque<Result<Wire, FlowerError>>,
}

/// One event as the server sent it.
#[derive(Clone, Debug, PartialEq)]
enum Wire {
    /// A whole value: `snapshot`, or `reset` with `reset` set.
    Value {
        reset: bool,
        revision: u64,
        /// Its identity, for patches; servers without deltas send none.
        id: Option<u64>,
        value: serde_json::Value,
    },
    /// A change to the value identified by `base`.
    Patch {
        revision: u64,
        id: u64,
        base: u64,
        /// RFC 6902 operations.
        operations: Vec<serde_json::Value>,
    },
}

impl SseParser {
    /// A parser bounding each event at `limit` bytes.
    fn new(limit: usize) -> Self {
        SseParser {
            limit,
            history: None,
            line: Vec::new(),
            line_seen: false,
            after_cr: false,
            kind: Vec::new(),
            data: Vec::new(),
            oversized: false,
            ready: VecDeque::new(),
        }
    }

    /// Feed a chunk, dispatching every event it completes.
    fn push(&mut self, mut chunk: &[u8]) {
        while let Some((&first, rest)) = chunk.split_first() {
            if std::mem::take(&mut self.after_cr) && first == b'\n' {
                chunk = rest;
                continue;
            }
            match chunk.iter().position(|byte| matches!(byte, b'\n' | b'\r')) {
                None => {
                    self.buffer_line(chunk);
                    return;
                }
                Some(end) => {
                    self.buffer_line(&chunk[..end]);
                    self.after_cr = chunk[end] == b'\r';
                    chunk = &chunk[end + 1..];
                    self.end_line();
                }
            }
        }
    }

    /// Pop the next dispatched event, if any.
    fn pop(&mut self) -> Option<Result<Wire, FlowerError>> {
        self.ready.pop_front()
    }

    /// Bytes currently buffered for the event in progress.
    fn buffered(&self) -> usize {
        self.line.len() + self.kind.len() + self.data.len()
    }

    /// Append bytes to the current line within the bound.
    fn buffer_line(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        self.line_seen = true;
        if self.oversized {
            return;
        }
        if self.buffered() + bytes.len() > self.limit {
            self.overflow();
            return;
        }
        self.line.extend_from_slice(bytes);
    }

    /// Drop the event in progress; it surfaces as an error at its
    /// terminating blank line.
    fn overflow(&mut self) {
        self.oversized = true;
        self.line = Vec::new();
        self.kind = Vec::new();
        self.data = Vec::new();
    }

    /// Process one complete line.
    fn end_line(&mut self) {
        let line = std::mem::take(&mut self.line);
        if !std::mem::take(&mut self.line_seen) {
            self.dispatch();
            return;
        }
        if self.oversized || line.first() == Some(&b':') {
            return;
        }
        let (field, value) = match line.iter().position(|byte| *byte == b':') {
            Some(colon) => {
                let value = &line[colon + 1..];
                (&line[..colon], value.strip_prefix(b" ").unwrap_or(value))
            }
            None => (line.as_slice(), &[][..]),
        };
        match field {
            b"event" => self.kind = value.to_vec(),
            b"data" => {
                self.data.extend_from_slice(value);
                self.data.push(b'\n');
                if self.buffered() > self.limit {
                    self.overflow();
                }
            }
            // `id`, `retry` and unknown fields carry nothing here.
            _ => {}
        }
    }

    /// Dispatch the buffered event at a blank line.
    fn dispatch(&mut self) {
        let kind = std::mem::take(&mut self.kind);
        let mut data = std::mem::take(&mut self.data);
        if std::mem::take(&mut self.oversized) {
            self.ready.push_back(Err(FlowerError::Decode(format!(
                "watch event exceeds {} bytes",
                self.limit
            ))));
            return;
        }
        if data.is_empty() {
            return;
        }
        data.pop();
        if matches!(kind.as_slice(), b"snapshot" | b"reset") {
            let history = serde_json::from_slice::<serde_json::Value>(&data)
                .ok()
                .and_then(|v| v.get("history").cloned())
                .filter(|v| !v.is_null());
            if let Some(history) = history {
                match serde_json::from_value::<flower_protocol::WatchHistory>(history) {
                    Ok(history) => {
                        if let Err(error) = history.validate() {
                            self.ready.push_back(Err(FlowerError::Decode(error)));
                            return;
                        }
                        self.history = Some(history);
                    }
                    Err(error) => {
                        self.ready
                            .push_back(Err(FlowerError::Decode(error.to_string())));
                        return;
                    }
                }
            }
        }
        let event = match kind.as_slice() {
            b"error" => Err(stream_error(&data)),
            b"snapshot" => decode_value(&data, false),
            b"reset" => decode_value(&data, true),
            b"patch" => decode_patch(&data),
            // Unknown events are forward-compatible noise.
            _ => return,
        };
        self.ready.push_back(event);
    }
}

fn stream_error(data: &[u8]) -> FlowerError {
    let mut json = match serde_json::from_slice::<serde_json::Value>(data) {
        Ok(serde_json::Value::Object(fields)) => serde_json::Value::Object(fields),
        _ => return FlowerError::Decode("invalid watch error envelope".into()),
    };
    let status = json["problem"]["status"]
        .as_u64()
        .and_then(|n| u16::try_from(n).ok())
        .filter(|status| (100..=999).contains(status))
        .unwrap_or(500);
    // SSE's event name already identifies an error; its body omits unary's ok discriminator.
    json["ok"] = serde_json::Value::Bool(false);
    FlowerError::Http(Box::new(flower_protocol::Problem::from_body(
        status,
        &serde_json::to_vec(&json).expect("JSON value serializes"),
        None,
    )))
}

/// Parse an event's JSON payload object.
fn payload(data: &[u8]) -> Result<serde_json::Map<String, serde_json::Value>, FlowerError> {
    match serde_json::from_slice(data) {
        Ok(serde_json::Value::Object(fields)) => Ok(fields),
        Ok(_) => Err(FlowerError::Decode(
            "watch event is not an object".to_string(),
        )),
        Err(error) => Err(FlowerError::Decode(format!("watch event: {error}"))),
    }
}

/// A required unsigned field of an event payload.
fn number(
    fields: &serde_json::Map<String, serde_json::Value>,
    name: &str,
) -> Result<u64, FlowerError> {
    fields
        .get(name)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| FlowerError::Decode(format!("watch event has no {name}")))
}

/// Decode a `snapshot` or `reset` event's JSON data.
fn decode_value(data: &[u8], reset: bool) -> Result<Wire, FlowerError> {
    let mut fields = payload(data)?;
    Ok(Wire::Value {
        reset,
        revision: number(&fields, "revision")?,
        id: fields.get("id").and_then(serde_json::Value::as_u64),
        value: fields
            .remove("value")
            .ok_or_else(|| FlowerError::Decode("watch event has no value".to_string()))?,
    })
}

/// Decode a `patch` event's JSON data.
fn decode_patch(data: &[u8]) -> Result<Wire, FlowerError> {
    let mut fields = payload(data)?;
    let operations = match fields.remove("patch") {
        Some(serde_json::Value::Array(operations)) => operations,
        _ => {
            return Err(FlowerError::Decode(
                "watch patch has no operations".to_string(),
            ));
        }
    };
    Ok(Wire::Patch {
        revision: number(&fields, "revision")?,
        id: number(&fields, "id")?,
        base: number(&fields, "base")?,
        operations,
    })
}

/// Apply RFC 6902 `add`, `remove` and `replace` operations in order.
/// A failure leaves `value` partly patched; callers resync.
fn apply_patch(
    value: &mut serde_json::Value,
    operations: &[serde_json::Value],
) -> Result<(), String> {
    for operation in operations {
        let op = operation
            .get("op")
            .and_then(serde_json::Value::as_str)
            .ok_or("operation has no op")?;
        let path = operation
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or("operation has no path")?;
        let replacement = operation.get("value");
        if !matches!(op, "add" | "remove" | "replace") {
            return Err(format!("unsupported operation `{op}`"));
        }
        if op != "remove" && replacement.is_none() {
            return Err(format!("`{op}` has no value"));
        }
        if path.is_empty() {
            match (op, replacement) {
                (_, Some(replacement)) => *value = replacement.clone(),
                _ => return Err("cannot remove the whole value".to_string()),
            }
            continue;
        }
        let (parent, last) = path
            .rsplit_once('/')
            .ok_or_else(|| format!("bad pointer `{path}`"))?;
        let key = last.replace("~1", "/").replace("~0", "~");
        let target = value
            .pointer_mut(parent)
            .ok_or_else(|| format!("no parent at `{path}`"))?;
        match target {
            serde_json::Value::Object(fields) => match (op, replacement) {
                ("add", Some(replacement)) => {
                    fields.insert(key, replacement.clone());
                }
                ("replace", Some(replacement)) => match fields.get_mut(&key) {
                    Some(slot) => *slot = replacement.clone(),
                    None => return Err(format!("nothing to replace at `{path}`")),
                },
                _ => {
                    if fields.remove(&key).is_none() {
                        return Err(format!("nothing to remove at `{path}`"));
                    }
                }
            },
            serde_json::Value::Array(items) => {
                let index = if op == "add" && key == "-" {
                    items.len()
                } else {
                    key.parse::<usize>()
                        .map_err(|_| format!("bad array index at `{path}`"))?
                };
                match (op, replacement) {
                    ("add", Some(replacement)) if index <= items.len() => {
                        items.insert(index, replacement.clone());
                    }
                    ("replace", Some(replacement)) if index < items.len() => {
                        items[index] = replacement.clone();
                    }
                    ("remove", _) if index < items.len() => {
                        items.remove(index);
                    }
                    _ => return Err(format!("index out of range at `{path}`")),
                }
            }
            _ => return Err(format!("`{path}` names inside a scalar")),
        }
    }
    Ok(())
}

/// One step of a watch connection.
enum Frame {
    /// A dispatched event, or a decode or transport failure.
    Event(Result<WatchEvent, FlowerError>),
    /// Bytes arrived without completing an event: heartbeats,
    /// comments or part of an event. Proof the connection lives.
    Activity,
}

/// What reopening a watch connection needs.
struct Reopen {
    client: FlowerClient,
    db: String,
    module: String,
    name: String,
    args: serde_json::Value,
    options: WatchOptions,
}

/// A raw watch: one logical watch yielding whole values. Patches apply
/// to the value it holds; a gap reopens the connection once for a fresh
/// snapshot, yielded as [`WatchEvent::Reset`].
pub struct DeltaStream {
    stream: ByteStream,
    parser: SseParser,
    finished: bool,
    /// The value the stream holds and the identity the server gave it.
    current: Option<(Option<u64>, serde_json::Value)>,
    /// How to reconnect after a gap; `None` cannot.
    reopen: Option<Reopen>,
    /// Whether the connection asks for deltas.
    deltas: bool,
    /// Gaps since a patch last applied.
    gaps: u32,
    /// The next whole value follows a resync.
    resynced: bool,
}

impl std::fmt::Debug for DeltaStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaStream")
            .field("buffered", &self.parser.buffered())
            .field("finished", &self.finished)
            .field("deltas", &self.deltas)
            .field("gaps", &self.gaps)
            .finish()
    }
}

impl DeltaStream {
    /// Parse a connected body stream.
    fn new(stream: ByteStream, reopen: Option<Reopen>, deltas: bool) -> Self {
        DeltaStream {
            stream,
            parser: SseParser::new(MAX_EVENT_BYTES),
            finished: false,
            current: None,
            reopen,
            deltas,
            gaps: 0,
            resynced: false,
        }
    }

    /// Poll the next event: `None` is a clean EOF.
    pub async fn next_event(&mut self) -> Option<Result<WatchEvent, FlowerError>> {
        loop {
            if self.finished && self.parser.ready.is_empty() {
                return None;
            }
            let cancel = self.reopen.as_ref().and_then(|r| r.options.cancel.clone());
            let next = if let Some(cancel) = cancel {
                tokio::select! {biased;()=cancel.cancelled()=>{
                    self.finished=true;self.current=None;self.parser.ready.clear();self.stream=Box::pin(futures_util::stream::empty());
                    return Some(Err(FlowerError::Cancelled));
                },next=self.next_frame()=>next}
            } else {
                self.next_frame().await
            };
            if let Frame::Event(event) = next? {
                return Some(event);
            }
        }
    }

    /// Poll one step: a buffered event, else the next chunk read.
    /// Cancel-safe while reading: a dropped poll loses no bytes. A
    /// resync dropped mid-reconnect keeps the old connection without a
    /// value, so its next patch resyncs again.
    async fn next_frame(&mut self) -> Option<Frame> {
        if let Some(wire) = self.parser.pop() {
            return match self.absorb(wire) {
                Ok(event) => Some(Frame::Event(Ok(event))),
                Err(Gap::Event(error)) => {
                    if !matches!(error, FlowerError::Decode(_)) {
                        self.finished = true;
                        self.current = None;
                        self.parser.ready.clear();
                        self.stream = Box::pin(futures_util::stream::empty());
                    }
                    Some(Frame::Event(Err(error)))
                }
                Err(Gap::Resync(reason)) => match self.resync(&reason).await {
                    Ok(()) => Some(Frame::Activity),
                    Err(error) => {
                        self.finished = true;
                        Some(Frame::Event(Err(error)))
                    }
                },
            };
        }
        if self.finished {
            return None;
        }
        match self.stream.next().await {
            None => {
                self.finished = true;
                None
            }
            Some(Err(error)) => {
                self.finished = true;
                Some(Frame::Event(Err(error)))
            }
            Some(Ok(chunk)) => {
                self.parser.push(&chunk);
                Some(Frame::Activity)
            }
        }
    }

    /// Turn one wire event into a whole value: values replace what the
    /// stream holds, patches apply to it.
    fn absorb(&mut self, wire: Result<Wire, FlowerError>) -> Result<WatchEvent, Gap> {
        match wire {
            // A decode failure loses an event, which may be a value the
            // next patch builds on: forget the value so that patch resyncs.
            Err(error) => {
                self.current = None;
                Err(Gap::Event(error))
            }
            Ok(Wire::Value {
                reset,
                revision,
                id,
                value,
            }) => {
                self.current = Some((id, value.clone()));
                let reset = reset || std::mem::take(&mut self.resynced);
                Ok(if reset {
                    WatchEvent::Reset { revision, value }
                } else {
                    WatchEvent::Snapshot { revision, value }
                })
            }
            Ok(Wire::Patch {
                revision,
                id,
                base,
                operations,
            }) => {
                let Some((Some(held), value)) = self.current.as_mut() else {
                    self.current = None;
                    return Err(Gap::Resync(format!(
                        "watch patch {id} names base {base}, but no identified value is held"
                    )));
                };
                if *held != base {
                    let reason = format!("watch patch {id} names base {base}, but {held} is held");
                    self.current = None;
                    return Err(Gap::Resync(reason));
                }
                if let Err(error) = apply_patch(value, &operations) {
                    self.current = None;
                    return Err(Gap::Resync(format!(
                        "watch patch {id} does not apply: {error}"
                    )));
                }
                *held = id;
                self.gaps = 0;
                Ok(WatchEvent::Snapshot {
                    revision,
                    value: value.clone(),
                })
            }
        }
    }

    /// Reconnect for a fresh snapshot after a gap, without deltas once
    /// [`MAX_GAPS`] gaps came in a row.
    async fn resync(&mut self, reason: &str) -> Result<(), FlowerError> {
        let Some(reopen) = &self.reopen else {
            return Err(FlowerError::Decode(format!(
                "{reason}; reconnect to resync"
            )));
        };
        self.gaps += 1;
        let deltas = self.deltas && self.gaps < MAX_GAPS;
        let stream = reopen
            .client
            .open_watch(
                &reopen.db,
                &reopen.module,
                &reopen.name,
                &reopen.args,
                &reopen.options,
                deltas,
            )
            .await?;
        self.stream = stream;
        self.parser = SseParser::new(MAX_EVENT_BYTES);
        self.finished = false;
        self.deltas = deltas;
        self.resynced = true;
        Ok(())
    }
}

/// Why an event could not become a value.
enum Gap {
    /// A failed event, surfaced as is.
    Event(FlowerError),
    /// The stream lost track of the value: resync.
    Resync(String),
}

/// A typed watch: one connection yielding decoded values.
pub struct ValueWatch<T> {
    deltas: DeltaStream,
    marker: PhantomData<T>,
}

impl<T: DeserializeOwned> ValueWatch<T> {
    /// Poll the next decoded value: `None` is a clean EOF.
    pub async fn next(&mut self) -> Option<Result<QueryResult<T>, FlowerError>> {
        match self.deltas.next_event().await? {
            Ok(WatchEvent::Snapshot { value, revision })
            | Ok(WatchEvent::Reset { value, revision }) => Some(
                serde_json::from_value(value)
                    .map(|value| QueryResult {
                        value,
                        staleness: Some(crate::Staleness::Unknown),
                        read_token: self.deltas.parser.history.as_ref().and_then(|history| {
                            self.deltas
                                .reopen
                                .as_ref()
                                .map(|reopen| watch_token(&reopen.db, history, revision))
                        }),
                    })
                    .map_err(|error| FlowerError::Decode(error.to_string())),
            ),
            Err(error) => Some(Err(error)),
        }
    }
}

/// A reconnecting typed subscription.
pub struct Subscription<T> {
    client: FlowerClient,
    db: String,
    module: String,
    name: String,
    args: serde_json::Value,
    options: SubscribeOptions,
    deltas: Option<DeltaStream>,
    max_revision: Option<u64>,
    history: Option<flower_protocol::WatchHistory>,
    fresh: bool,
    reconnects: u32,
    done: bool,
    marker: PhantomData<T>,
}

impl<T: DeserializeOwned> Subscription<T> {
    /// Adapt the pull API to a standard futures stream. Dropping it releases its connection.
    pub fn into_stream(self) -> impl futures_util::Stream<Item = Result<Update<T>, FlowerError>> {
        futures_util::stream::unfold(self, |mut subscription| async move {
            subscription.next().await.map(|item| (item, subscription))
        })
    }
    /// Poll the next update: `None` ends the subscription after its
    /// reconnect budget or a deterministic failure.
    pub async fn next(&mut self) -> Option<Result<Update<T>, FlowerError>> {
        loop {
            if self.done {
                return None;
            }
            if self
                .options
                .watch
                .cancel
                .as_ref()
                .is_some_and(crate::CancellationToken::is_cancelled)
            {
                self.done = true;
                self.deltas = None;
                return Some(Err(FlowerError::Cancelled));
            }
            if self.done {
                return None;
            }
            if self.deltas.is_none() {
                match self.connect().await {
                    Ok(deltas) => {
                        self.deltas = Some(deltas);
                        self.fresh = true;
                    }
                    Err(error) if transient(&error) => {
                        if !self.spend_reconnect() {
                            self.done = true;
                            return Some(Err(FlowerError::RetryFailed {
                                execution: error.execution(),
                                last: Box::new(error),
                                attempts: self.reconnects,
                                reason: crate::error::RetryStop::Attempts,
                            }));
                        }
                        self.sleep_backoff_hint(error.problem().and_then(|p| p.retry_after_ms))
                            .await;
                        continue;
                    }
                    Err(error) => {
                        self.done = true;
                        return Some(Err(error));
                    }
                }
            }
            let deltas = self.deltas.as_mut().expect("connected above");
            // The stall timer restarts on every frame: any bytes,
            // heartbeat comments included, prove the connection lives.
            let frame = {
                let next = tokio::time::timeout(self.options.stall_timeout, deltas.next_frame());
                if let Some(cancel) = &self.options.watch.cancel {
                    tokio::select! {biased;()=cancel.cancelled()=>{self.done=true;self.deltas=None;return Some(Err(FlowerError::Cancelled));},frame=next=>frame}
                } else {
                    next.await
                }
            };
            let frame = match frame {
                Ok(frame) => frame,
                Err(_) => {
                    self.deltas = None;
                    if !self.spend_reconnect() {
                        self.done = true;
                        return Some(Err(FlowerError::RetryFailed {
                            last: Box::new(FlowerError::Timeout),
                            attempts: self.reconnects,
                            reason: crate::error::RetryStop::Attempts,
                            execution: flower_protocol::Execution::Unknown,
                        }));
                    }
                    self.sleep_backoff().await;
                    continue;
                }
            };
            let event = match frame {
                Some(Frame::Activity) => continue,
                None => {
                    // Clean EOF (a lagged drop or server close):
                    // reconnect immediately for a fresh snapshot.
                    self.deltas = None;
                    if !self.spend_reconnect() {
                        self.done = true;
                        return None;
                    }
                    continue;
                }
                Some(Frame::Event(Err(error))) if transient(&error) => {
                    self.deltas = None;
                    if !self.spend_reconnect() {
                        self.done = true;
                        return Some(Err(FlowerError::RetryFailed {
                            execution: error.execution(),
                            last: Box::new(error),
                            attempts: self.reconnects,
                            reason: crate::error::RetryStop::Attempts,
                        }));
                    }
                    self.sleep_backoff_hint(error.problem().and_then(|p| p.retry_after_ms))
                        .await;
                    continue;
                }
                // One corrupt frame must not kill the subscription.
                Some(Frame::Event(Err(error))) => {
                    if !matches!(error, FlowerError::Decode(_)) {
                        self.done = true;
                    }
                    return Some(Err(error));
                }
                Some(Frame::Event(Ok(event))) => event,
            };
            // A connection that delivers data is healthy: the
            // reconnect budget and backoff start over.
            self.reconnects = 0;
            let (revision, value, reset) = match event {
                WatchEvent::Snapshot { revision, value } => (revision, value, self.fresh),
                WatchEvent::Reset { revision, value } => (revision, value, true),
            };
            let history = self.deltas.as_ref().and_then(|d| d.parser.history.clone());
            let changed = self.history != history;
            if changed || (self.fresh && history.is_none()) {
                self.max_revision = None;
            }
            self.history = history.clone();
            let reset = reset || changed;
            self.fresh = false;
            if self.options.monotonic && self.max_revision.is_some_and(|max| revision < max) {
                continue;
            }
            self.max_revision = Some(self.max_revision.map_or(revision, |max| max.max(revision)));
            return Some(
                serde_json::from_value(value)
                    .map(|value| Update {
                        revision,
                        value,
                        reset,
                        read_token: history
                            .as_ref()
                            .map(|history| watch_token(&self.db, history, revision)),
                        history,
                    })
                    .map_err(|error| FlowerError::Decode(error.to_string())),
            );
        }
    }

    /// Open one watch connection.
    async fn connect(&self) -> Result<DeltaStream, FlowerError> {
        self.client
            .watch_deltas(
                &self.db,
                &self.module,
                &self.name,
                &self.args,
                &self.options.watch,
            )
            .await
    }

    /// Spend one reconnect, if budgeted.
    fn spend_reconnect(&mut self) -> bool {
        if matches!(self.options.retry, Retry::Off) {
            return false;
        }
        if self
            .options
            .max_reconnects
            .is_some_and(|max| self.reconnects >= max)
        {
            return false;
        }
        self.reconnects += 1;
        true
    }

    /// Sleep the reconnect backoff for the spent attempt.
    async fn sleep_backoff(&self) {
        self.sleep_backoff_hint(None).await;
    }
    async fn sleep_backoff_hint(&self, hint: Option<u64>) {
        let policy = self.options.retry.policy().unwrap_or_default();
        let sleep = backoff(&policy, self.reconnects.saturating_sub(1), hint);
        if let Some(cancel) = &self.options.watch.cancel {
            tokio::select! {()=cancel.cancelled()=>{},()=sleep=>{}}
        } else {
            sleep.await;
        }
    }
}

impl FlowerClient {
    /// Open one raw watch: yields whole values until the connection
    /// ends, applying the patches the server sends and reconnecting
    /// once per gap for a fresh snapshot (see [`DeltaStream`]). No other
    /// reconnect; use [`FlowerClient::subscribe`] for that.
    pub async fn watch_deltas(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &serde_json::Value,
        options: &WatchOptions,
    ) -> Result<DeltaStream, FlowerError> {
        let stream = self
            .open_watch(db, module, name, args, options, true)
            .await?;
        let reopen = Reopen {
            client: self.clone(),
            db: db.to_string(),
            module: module.to_string(),
            name: name.to_string(),
            args: args.clone(),
            options: options.clone(),
        };
        Ok(DeltaStream::new(stream, Some(reopen), true))
    }

    /// Open one watch connection's body, asking for deltas when
    /// `deltas`.
    async fn open_watch(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &serde_json::Value,
        options: &WatchOptions,
        deltas: bool,
    ) -> Result<ByteStream, FlowerError> {
        let body = watch_body(db, module, name, args, deltas)?;
        let timeout = options.timeout.unwrap_or(self.timeout);
        let credentials = options.credentials.as_ref().or(self.credentials.as_ref());
        let request = tokio::time::timeout(
            timeout,
            self.transport.post_stream("/v1/watch", &body, credentials),
        );
        let response = if let Some(cancel) = &options.cancel {
            tokio::select! {biased;()=cancel.cancelled()=>return Err(FlowerError::Cancelled),response=request=>response}
        } else {
            request.await
        };
        let response = response.map_err(|_| FlowerError::Timeout)??;
        if response.status != 200 {
            let mut body = Vec::new();
            let mut stream = response.body;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                body.extend_from_slice(&chunk);
            }
            let error = connect_error(response.status, &body);
            return Err(match error {
                FlowerError::Http(mut p) => {
                    p.retry_after_ms = response.retry_after_ms.or(p.retry_after_ms);
                    FlowerError::Http(p)
                }
                other => other,
            });
        }
        Ok(response.body)
    }

    /// Open one typed watch: yields decoded values until it ends.
    pub async fn watch<T: DeserializeOwned>(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &serde_json::Value,
        options: &WatchOptions,
    ) -> Result<ValueWatch<T>, FlowerError> {
        Ok(ValueWatch {
            deltas: self.watch_deltas(db, module, name, args, options).await?,
            marker: PhantomData,
        })
    }

    /// Open a reconnecting typed subscription. Connections open
    /// lazily on the first `next`.
    pub fn subscribe<T: DeserializeOwned>(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &serde_json::Value,
        options: &SubscribeOptions,
    ) -> Subscription<T> {
        Subscription {
            client: self.clone(),
            db: db.to_string(),
            module: module.to_string(),
            name: name.to_string(),
            args: args.clone(),
            options: options.clone(),
            deltas: None,
            max_revision: None,
            history: None,
            fresh: true,
            reconnects: 0,
            done: false,
            marker: PhantomData,
        }
    }

    /// Resolve on the first update satisfying the predicate.
    pub async fn wait_until<T, F>(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &serde_json::Value,
        predicate: F,
        options: &SubscribeOptions,
    ) -> Result<Update<T>, FlowerError>
    where
        T: DeserializeOwned,
        F: FnMut(&T) -> bool,
    {
        let mut subscription = self.subscribe(db, module, name, args, options);
        let mut predicate = predicate;
        loop {
            match subscription.next().await {
                None => {
                    return Err(FlowerError::RetryExhausted(
                        "subscription ended".to_string(),
                    ));
                }
                Some(Err(error)) => return Err(error),
                Some(Ok(update)) => {
                    if predicate(&update.value) {
                        return Ok(update);
                    }
                }
            }
        }
    }

    /// Resolve on the first truthy dynamic-JSON update. Null, false,
    /// zero, empty string, array and object are falsy; everything
    /// else is truthy.
    pub async fn wait_until_truthy(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &serde_json::Value,
        options: &SubscribeOptions,
    ) -> Result<Update<serde_json::Value>, FlowerError> {
        self.wait_until(db, module, name, args, truthy, options)
            .await
    }
}

/// Dynamic-JSON truthiness for [`FlowerClient::wait_until_truthy`].
fn truthy(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => false,
        serde_json::Value::Bool(flag) => *flag,
        serde_json::Value::Number(number) => {
            number.as_i64().is_some_and(|n| n != 0)
                || number.as_u64().is_some_and(|n| n != 0)
                || number.as_f64().is_some_and(|n| n != 0.0)
        }
        serde_json::Value::String(text) => !text.is_empty(),
        serde_json::Value::Array(items) => !items.is_empty(),
        serde_json::Value::Object(fields) => !fields.is_empty(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Feed chunks and collect every dispatched event.
    fn parse(limit: usize, chunks: &[&[u8]]) -> Vec<Result<Wire, FlowerError>> {
        let mut parser = SseParser::new(limit);
        let mut out = Vec::new();
        for chunk in chunks {
            parser.push(chunk);
            while let Some(event) = parser.pop() {
                out.push(event);
            }
        }
        out
    }

    fn revision(event: &Result<Wire, FlowerError>) -> u64 {
        match event {
            Ok(Wire::Value {
                reset: false,
                revision,
                ..
            }) => *revision,
            other => panic!("expected a snapshot, got {other:?}"),
        }
    }

    /// A stream over canned SSE text that cannot reconnect.
    fn canned(text: &str) -> DeltaStream {
        let chunk: Result<Vec<u8>, FlowerError> = Ok(text.as_bytes().to_vec());
        DeltaStream::new(
            Box::pin(futures_util::stream::iter(vec![chunk])),
            None,
            true,
        )
    }

    #[test]
    fn patches_apply_rfc_6902_operations() {
        let mut value = serde_json::json!({"a/b": {"~": 1}, "list": [1, 2, 3], "gone": true});
        let operations = serde_json::json!([
            {"op": "replace", "path": "/a~1b/~0", "value": 2},
            {"op": "remove", "path": "/list/0"},
            {"op": "add", "path": "/list/2", "value": 4},
            {"op": "add", "path": "/list/-", "value": 5},
            {"op": "remove", "path": "/gone"},
            {"op": "add", "path": "/new", "value": null}
        ]);
        apply_patch(&mut value, operations.as_array().unwrap()).unwrap();
        assert_eq!(
            value,
            serde_json::json!({"a/b": {"~": 2}, "list": [2, 3, 4, 5], "new": null})
        );
        let mut whole = serde_json::json!(1);
        apply_patch(
            &mut whole,
            &[serde_json::json!({"op": "replace", "path": "", "value": [true]})],
        )
        .unwrap();
        assert_eq!(whole, serde_json::json!([true]));
        for bad in [
            serde_json::json!({"op": "move", "from": "/a", "path": "/b"}),
            serde_json::json!({"op": "remove", "path": "/missing"}),
            serde_json::json!({"op": "replace", "path": "/missing", "value": 1}),
            serde_json::json!({"op": "add", "path": "/list/9", "value": 1}),
            serde_json::json!({"op": "add", "path": "/nowhere/x", "value": 1}),
            serde_json::json!({"op": "add", "path": "/list"}),
        ] {
            let mut value = serde_json::json!({"list": []});
            assert!(
                apply_patch(&mut value, std::slice::from_ref(&bad)).is_err(),
                "{bad}"
            );
        }
    }

    #[tokio::test]
    async fn streams_apply_patches_to_the_value_they_hold() {
        let mut stream = canned(concat!(
            "event: snapshot\ndata: {\"revision\":1,\"id\":10,\"value\":{\"n\":1}}\n\n",
            "event: patch\ndata: {\"revision\":2,\"id\":11,\"base\":10,",
            "\"patch\":[{\"op\":\"replace\",\"path\":\"/n\",\"value\":2}]}\n\n",
            "event: patch\ndata: {\"revision\":3,\"id\":12,\"base\":11,",
            "\"patch\":[{\"op\":\"add\",\"path\":\"/m\",\"value\":true}]}\n\n",
        ));
        let mut seen = Vec::new();
        while let Some(event) = stream.next_event().await {
            match event.unwrap() {
                WatchEvent::Snapshot { revision, value } => seen.push((revision, value)),
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(
            seen,
            vec![
                (1, serde_json::json!({"n": 1})),
                (2, serde_json::json!({"n": 2})),
                (3, serde_json::json!({"n": 2, "m": true})),
            ]
        );
    }

    #[tokio::test]
    async fn streams_that_cannot_reconnect_surface_gaps() {
        // The patch names a value the stream never held.
        let mut stream = canned(concat!(
            "event: snapshot\ndata: {\"revision\":1,\"id\":10,\"value\":1}\n\n",
            "event: patch\ndata: {\"revision\":2,\"id\":12,\"base\":11,\"patch\":[]}\n\n",
        ));
        assert!(matches!(
            stream.next_event().await,
            Some(Ok(WatchEvent::Snapshot { revision: 1, .. }))
        ));
        assert!(matches!(
            stream.next_event().await,
            Some(Err(FlowerError::Decode(message))) if message.contains("base 11")
        ));
        assert!(stream.next_event().await.is_none());
    }

    #[test]
    fn sse_dispatches_crlf_framed_events() {
        let events = parse(
            1024,
            &[b"event: snapshot\r\ndata: {\"revision\":1,\"value\":null}\r\n\r\n:keepalive\r\n\r\nevent: snapshot\r\ndata: {\"revision\":2,\"value\":null}\r\n\r\n"],
        );
        assert_eq!(events.iter().map(revision).collect::<Vec<_>>(), vec![1, 2]);
    }

    #[test]
    fn sse_dispatches_lone_cr_framed_events() {
        let events = parse(
            1024,
            &[b"event: snapshot\rdata: {\"revision\":3,\"value\":true}\r\revent: snapshot\rdata: {\"revision\":4,\"value\":false}\r\r"],
        );
        assert_eq!(events.iter().map(revision).collect::<Vec<_>>(), vec![3, 4]);
    }

    #[test]
    fn sse_pairs_crlf_split_across_chunks() {
        // A CR ending one chunk and the LF starting the next form one
        // line break, not a blank line that would dispatch early.
        let events = parse(
            1024,
            &[
                b"event: snapshot\r",
                b"\ndata: {\"revision\":5,",
                b"\"value\":1}\r",
                b"\n\r",
                b"\n",
            ],
        );
        assert_eq!(events.iter().map(revision).collect::<Vec<_>>(), vec![5]);
    }

    #[test]
    fn sse_joins_data_lines_and_ignores_unknown_fields() {
        let events = parse(
            1024,
            &[b"id: 7\nretry: 10\nevent:reset\ndata:{\"revision\":6,\ndata: \"value\":[1]}\n\n"],
        );
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            Ok(Wire::Value { reset: true, revision: 6, id: None, value })
                if *value == serde_json::json!([1])
        ));
        // Comment-only blocks and data-less events dispatch nothing.
        assert!(parse(1024, &[b": ping\n\nevent: snapshot\n\n"]).is_empty());
    }

    #[test]
    fn sse_bounds_oversized_events_and_recovers() {
        let mut parser = SseParser::new(64);
        parser.push(b"event: snapshot\ndata: ");
        for _ in 0..100 {
            parser.push(&[b'x'; 50]);
            assert!(
                parser.buffered() <= 64,
                "buffer grew to {}",
                parser.buffered()
            );
        }
        parser.push(b"\n\nevent: snapshot\ndata: {\"revision\":9,\"value\":0}\n\n");
        assert!(matches!(parser.pop(), Some(Err(FlowerError::Decode(_)))));
        assert_eq!(revision(&parser.pop().unwrap()), 9);
        assert!(parser.pop().is_none());
    }

    #[test]
    fn sse_discards_unterminated_trailing_event() {
        let mut parser = SseParser::new(1024);
        parser.push(b"event: snapshot\ndata: {\"revision\":1,\"value\":0}\n");
        assert!(parser.pop().is_none());
    }
}
