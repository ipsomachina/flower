//! Declarative access policies: parsing, deploy-time validation and
//! enforcement.
//!
//! A collection may declare who can read, insert, update and delete its
//! rows, and who can read or write single top-level fields. The host
//! enforces those rules on every collection operation a caller performs:
//! hidden rows read as absent and drop out of scans and range pages before
//! limits and cursors, unreadable fields are redacted (the member is
//! removed, never nulled), and denied writes fail with `ACCESS_DENIED`, a
//! guest-recoverable failure. A derived value may declare who can read it
//! over `principal`, `args` and `now`.
//!
//! Only callers are guarded. Code without one runs with definer rights and
//! sees every row: derived evaluations, triggers, timer handlers and the
//! authorization hook. A definer's call into another module drops those
//! rights there and runs as an anonymous caller.
//!
//! Rules are data the host evaluates natively; [`flower_sdk::access`]
//! builds them. Comparisons with a missing or null side are false, so a row
//! without an owner never matches an anonymous caller's missing subject.
//! Only numbers order against numbers and strings against strings (by code
//! point). `stored(collection, key)` holds when the module's collection has
//! a row at the key; `readable(collection, key)` when it has one the caller
//! may read by that collection's read rule, which may not use `readable`
//! itself, so one lookup never leads to another. Each lookup records the
//! row as a dependency, so results depending on it follow its changes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use flower_sdk::error::codes;
use flower_sdk::{Principal, Value, fail};

use crate::host::{FUEL_ROW, Host, OpError, OpResult};
use crate::json::render;
use crate::storage::keys::encode_record_dep;

/// Most nodes one rule may hold.
pub const MAX_RULE_NODES: usize = 256;
/// Deepest one rule may nest.
pub const MAX_RULE_DEPTH: usize = 32;
/// Most parts a list key holds.
pub const MAX_KEY_PARTS: usize = 16;

/// A collection's access policy. Operations without a rule deny.
#[derive(Clone, Debug, PartialEq)]
pub struct CollectionPolicy {
    /// Who may read a stored row.
    pub read: Rule,
    /// Who may insert a row.
    pub insert: Rule,
    /// Who may replace a stored row.
    pub update: Rule,
    /// Who may delete a stored row.
    pub delete: Rule,
    /// Rules for single top-level fields, on top of the row's.
    pub fields: Vec<FieldPolicy>,
}

/// Rules for one top-level field. Without `write`, a caller may change
/// the field only where it may read it.
#[derive(Clone, Debug, PartialEq)]
pub struct FieldPolicy {
    /// Field name.
    pub name: String,
    /// Who may read it; others see the row without it.
    pub read: Option<Rule>,
    /// Who may change it.
    pub write: Option<Rule>,
}

/// A boolean rule.
#[derive(Clone, Debug, PartialEq)]
pub enum Rule {
    /// Always or never.
    Const(bool),
    /// Every rule holds.
    All(Vec<Rule>),
    /// Some rule holds.
    Any(Vec<Rule>),
    /// The rule does not hold.
    Not(Box<Rule>),
    /// A comparison of two operands.
    Compare(Op, Operand, Operand),
    /// The operand is present and not null.
    Exists(Operand),
    /// The module's collection has a row at the key the caller may read.
    Readable(String, Operand),
    /// The module's collection has a row at the key.
    Stored(String, Operand),
}

/// A comparison.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    /// Equal.
    Eq,
    /// Different.
    Ne,
    /// Element of an array.
    In,
    /// Strictly below.
    Lt,
    /// At or below.
    Lte,
    /// Strictly above.
    Gt,
    /// At or above.
    Gte,
    /// Text prefix.
    StartsWith,
}

/// A rule's input value.
#[derive(Clone, Debug, PartialEq)]
pub enum Operand {
    /// A literal.
    Value(Value),
    /// A path below a root.
    Ref(Root, Vec<String>),
    /// An array of operands, as a lookup key only.
    List(Vec<Operand>),
}

/// Where a path starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Root {
    /// The caller: `subject`, `tenant`, `claims…`.
    Principal,
    /// The stored row.
    Row,
    /// The row being written.
    Next,
    /// The row key.
    Key,
    /// A derived value's arguments.
    Args,
    /// The invocation time in milliseconds.
    Now,
}

/// Which inputs a rule may use.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Scope {
    Read,
    Insert,
    Update,
    Delete,
    FieldRead,
    FieldWrite,
    Derived,
}

impl Scope {
    fn allows(self, root: Root) -> bool {
        match root {
            Root::Principal | Root::Now => true,
            Root::Key => self != Scope::Derived,
            Root::Row => !matches!(self, Scope::Insert | Scope::Derived),
            Root::Next => matches!(self, Scope::Insert | Scope::Update | Scope::FieldWrite),
            Root::Args => self == Scope::Derived,
        }
    }
}

/// Parse and validate a collection policy from its manifest encoding.
pub fn parse_policy(value: &Value) -> Result<CollectionPolicy, String> {
    let map = value.as_object().ok_or("access is not an object")?;
    for key in map.keys() {
        if !matches!(
            key.as_str(),
            "read" | "insert" | "update" | "delete" | "fields"
        ) {
            return Err(format!("access has unknown member `{key}`"));
        }
    }
    let rule = |name: &str, scope: Scope| match map.get(name) {
        None => Ok(Rule::Const(false)),
        Some(value) => parse_rule(value, scope).map_err(|error| format!("{name}: {error}")),
    };
    let mut fields = Vec::new();
    if let Some(declared) = map.get("fields") {
        let declared = declared
            .as_object()
            .ok_or("access fields is not an object")?;
        for (name, entry) in declared {
            if name.is_empty() {
                return Err("access field names must be non-empty".to_string());
            }
            let entry = entry
                .as_object()
                .ok_or_else(|| format!("access field `{name}` is not an object"))?;
            if entry.keys().any(|key| key != "read" && key != "write") {
                return Err(format!("access field `{name}` has an unknown member"));
            }
            let read = entry
                .get("read")
                .map(|rule| parse_rule(rule, Scope::FieldRead))
                .transpose()
                .map_err(|error| format!("fields.{name}.read: {error}"))?;
            let write = entry
                .get("write")
                .map(|rule| parse_rule(rule, Scope::FieldWrite))
                .transpose()
                .map_err(|error| format!("fields.{name}.write: {error}"))?;
            if read.is_none() && write.is_none() {
                return Err(format!("access field `{name}` declares no rule"));
            }
            fields.push(FieldPolicy {
                name: name.clone(),
                read,
                write,
            });
        }
    }
    Ok(CollectionPolicy {
        read: rule("read", Scope::Read)?,
        insert: rule("insert", Scope::Insert)?,
        update: rule("update", Scope::Update)?,
        delete: rule("delete", Scope::Delete)?,
        fields,
    })
}

/// Parse and validate a derived value's access rule.
pub fn parse_derived_rule(value: &Value) -> Result<Rule, String> {
    parse_rule(value, Scope::Derived)
}

fn parse_rule(value: &Value, scope: Scope) -> Result<Rule, String> {
    let mut nodes = 0;
    Parser { scope }.rule(value, 1, &mut nodes)
}

struct Parser {
    scope: Scope,
}

impl Parser {
    fn rule(&self, value: &Value, depth: usize, nodes: &mut usize) -> Result<Rule, String> {
        *nodes += 1;
        if *nodes > MAX_RULE_NODES {
            return Err(format!("a rule may have at most {MAX_RULE_NODES} nodes"));
        }
        if depth > MAX_RULE_DEPTH {
            return Err(format!("a rule may nest at most {MAX_RULE_DEPTH} deep"));
        }
        let map = value.as_object().ok_or("a rule is an object")?;
        let mut entries = map.iter();
        let (Some((op, body)), None) = (entries.next(), entries.next()) else {
            return Err("a rule has exactly one member".to_string());
        };
        let pair = |body: &Value| -> Result<(Operand, Operand), String> {
            match body.as_array().map(Vec::as_slice) {
                Some([left, right]) => Ok((self.operand(left)?, self.operand(right)?)),
                _ => Err(format!("`{op}` takes two operands")),
            }
        };
        let compare = |op: Op| pair(body).map(|(left, right)| Rule::Compare(op, left, right));
        match op.as_str() {
            "const" => match body {
                Value::Bool(value) => Ok(Rule::Const(*value)),
                _ => Err("`const` takes a boolean".to_string()),
            },
            "all" | "any" => {
                let items = body
                    .as_array()
                    .ok_or_else(|| format!("`{op}` takes an array"))?;
                let rules = items
                    .iter()
                    .map(|item| self.rule(item, depth + 1, nodes))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(if op == "all" {
                    Rule::All(rules)
                } else {
                    Rule::Any(rules)
                })
            }
            "not" => Ok(Rule::Not(Box::new(self.rule(body, depth + 1, nodes)?))),
            "eq" => compare(Op::Eq),
            "ne" => compare(Op::Ne),
            "in" => compare(Op::In),
            "lt" => compare(Op::Lt),
            "lte" => compare(Op::Lte),
            "gt" => compare(Op::Gt),
            "gte" => compare(Op::Gte),
            "starts_with" => compare(Op::StartsWith),
            "exists" => Ok(Rule::Exists(self.operand(body)?)),
            "readable" | "stored" => {
                let (collection, key) = match body.as_array().map(Vec::as_slice) {
                    Some([Value::String(collection), key]) if !collection.is_empty() => {
                        (collection.clone(), self.key(key)?)
                    }
                    _ => return Err(format!("`{op}` takes a collection name and a key")),
                };
                Ok(if op == "readable" {
                    Rule::Readable(collection, key)
                } else {
                    Rule::Stored(collection, key)
                })
            }
            other => Err(format!("unknown rule `{other}`")),
        }
    }

    /// A lookup key: an operand, or a list of them.
    fn key(&self, value: &Value) -> Result<Operand, String> {
        if let Some(Value::Array(parts)) = value.as_object().and_then(|map| map.get("list")) {
            if value.as_object().is_some_and(|map| map.len() != 1) {
                return Err("an operand has exactly one member".to_string());
            }
            if parts.is_empty() || parts.len() > MAX_KEY_PARTS {
                return Err(format!("a list key holds 1 to {MAX_KEY_PARTS} operands"));
            }
            let parts = parts
                .iter()
                .map(|part| self.operand(part))
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(Operand::List(parts));
        }
        self.operand(value)
    }

    fn operand(&self, value: &Value) -> Result<Operand, String> {
        let map = value.as_object().ok_or("an operand is an object")?;
        let mut entries = map.iter();
        let (Some((kind, body)), None) = (entries.next(), entries.next()) else {
            return Err("an operand has exactly one member".to_string());
        };
        match kind.as_str() {
            "value" => Ok(Operand::Value(body.clone())),
            "list" => Err("a list is only the key of readable or stored".to_string()),
            "ref" => {
                let segments = body
                    .as_array()
                    .ok_or("`ref` takes an array of strings")?
                    .iter()
                    .map(|segment| segment.as_str().map(str::to_string))
                    .collect::<Option<Vec<String>>>()
                    .ok_or("`ref` takes an array of strings")?;
                let (root, path) = segments.split_first().ok_or("`ref` needs a root")?;
                let root = match root.as_str() {
                    "principal" => Root::Principal,
                    "row" => Root::Row,
                    "next" => Root::Next,
                    "key" => Root::Key,
                    "args" => Root::Args,
                    "now" => Root::Now,
                    other => return Err(format!("unknown operand root `{other}`")),
                };
                let nonempty = path.iter().all(|segment| !segment.is_empty());
                let valid = self.scope.allows(root)
                    && nonempty
                    && match root {
                        Root::Principal => match path {
                            [field] => matches!(field.as_str(), "subject" | "tenant" | "claims"),
                            [field, ..] => field == "claims",
                            [] => false,
                        },
                        Root::Row | Root::Next => !path.is_empty(),
                        Root::Key | Root::Args => true,
                        Root::Now => path.is_empty(),
                    };
                if !valid {
                    return Err(format!("{} cannot be used here", segments.join(".")));
                }
                Ok(Operand::Ref(root, path.to_vec()))
            }
            other => Err(format!("unknown operand `{other}`")),
        }
    }
}

impl Rule {
    /// Visit every rule node.
    fn each<'r>(&'r self, visit: &mut dyn FnMut(&'r Rule)) {
        visit(self);
        match self {
            Rule::All(rules) | Rule::Any(rules) => rules.iter().for_each(|rule| rule.each(visit)),
            Rule::Not(rule) => rule.each(visit),
            _ => {}
        }
    }

    /// The collections its `readable`s name.
    fn readable_targets(&self) -> Vec<&str> {
        let mut targets = Vec::new();
        self.each(&mut |rule| {
            if let Rule::Readable(collection, _) = rule {
                targets.push(collection.as_str());
            }
        });
        targets
    }

    /// The collections its `stored`s and `readable`s name.
    fn lookup_targets(&self) -> Vec<&str> {
        let mut targets = Vec::new();
        self.each(&mut |rule| {
            if let Rule::Readable(collection, _) | Rule::Stored(collection, _) = rule {
                targets.push(collection.as_str());
            }
        });
        targets
    }
}

impl CollectionPolicy {
    fn rules(&self) -> impl Iterator<Item = &Rule> {
        [&self.read, &self.insert, &self.update, &self.delete]
            .into_iter()
            .chain(
                self.fields
                    .iter()
                    .flat_map(|field| field.read.iter().chain(field.write.iter())),
            )
    }

    /// Whether some field's read rule can hide it.
    #[cfg(test)]
    fn redacts(&self) -> bool {
        self.fields.iter().any(|field| {
            field
                .read
                .as_ref()
                .is_some_and(|rule| *rule != Rule::Const(true))
        })
    }
}

/// Cross-collection checks over one module: every lookup names a declared
/// collection, and every `readable` names one with a policy whose read
/// rule uses no `readable` itself.
pub fn validate_targets<'a>(
    declared: &BTreeSet<&str>,
    policies: &BTreeMap<&'a str, &'a CollectionPolicy>,
    derived: impl Iterator<Item = (&'a str, &'a Rule)>,
) -> Result<(), String> {
    let rules = policies
        .iter()
        .flat_map(|(name, policy)| policy.rules().map(move |rule| (*name, rule)))
        .chain(derived);
    for (owner, rule) in rules {
        if let Some(missing) = rule
            .lookup_targets()
            .into_iter()
            .find(|target| !declared.contains(target))
        {
            return Err(format!("{owner}: `{missing}` is not a declared collection"));
        }
        for target in rule.readable_targets() {
            match policies.get(target) {
                None => {
                    return Err(format!(
                        "{owner}: readable(`{target}`) needs a collection with an access policy"
                    ));
                }
                Some(policy) if !policy.read.readable_targets().is_empty() => {
                    return Err(format!(
                        "{owner}: readable(`{target}`) names a collection whose read rule uses readable"
                    ));
                }
                Some(_) => {}
            }
        }
    }
    Ok(())
}

/// The caller as rules see it: anonymous callers are an empty object, so
/// every path into them is missing.
pub(crate) fn caller_view(principal: Option<&Principal>) -> Value {
    let mut map = BTreeMap::new();
    if let Some(principal) = principal {
        map.insert(
            "subject".to_string(),
            Value::String(principal.subject.clone()),
        );
        if let Some(tenant) = &principal.tenant {
            map.insert("tenant".to_string(), Value::String(tenant.clone()));
        }
        if let Some(claims) = &principal.claims {
            map.insert("claims".to_string(), claims.clone());
        }
    }
    Value::Object(map)
}

fn lookup<'a>(value: &'a Value, path: &[String]) -> Option<&'a Value> {
    path.iter().try_fold(value, |value, segment| match value {
        Value::Object(fields) => fields.get(segment),
        Value::Array(items) => segment
            .parse::<usize>()
            .ok()
            .and_then(|index| items.get(index)),
        _ => None,
    })
}

/// A resolved operand: `None` when missing.
type Resolved<'a> = Option<std::borrow::Cow<'a, Value>>;

fn present(value: &Resolved<'_>) -> bool {
    value.as_deref().is_some_and(|value| !value.is_null())
}

impl Op {
    fn holds(self, left: &Resolved<'_>, right: &Resolved<'_>) -> bool {
        use std::cmp::Ordering;
        let both = present(left) && present(right);
        let (left_value, right_value) = (left.as_deref(), right.as_deref());
        let order = || match (left_value, right_value) {
            (Some(Value::Number(left)), Some(Value::Number(right))) => left.partial_cmp(right),
            (Some(Value::String(left)), Some(Value::String(right))) => Some(left.cmp(right)),
            _ => None,
        };
        match self {
            Op::Eq => both && left_value == right_value,
            Op::Ne => both && left_value != right_value,
            Op::In => match right_value {
                Some(Value::Array(items)) => {
                    present(left) && items.iter().any(|item| Some(item) == left_value)
                }
                _ => false,
            },
            Op::Lt => order() == Some(Ordering::Less),
            Op::Lte => matches!(order(), Some(Ordering::Less | Ordering::Equal)),
            Op::Gt => order() == Some(Ordering::Greater),
            Op::Gte => matches!(order(), Some(Ordering::Greater | Ordering::Equal)),
            Op::StartsWith => match (left_value, right_value) {
                (Some(Value::String(text)), Some(Value::String(prefix))) => {
                    text.starts_with(prefix.as_str())
                }
                _ => false,
            },
        }
    }
}

/// Where rules find the rows `stored` and `readable` name: a guarded
/// host's view of its module, its own writes included.
pub(crate) trait Rows {
    /// The row of the module's `collection` at `key`, counted and
    /// recorded as read.
    fn row(&mut self, collection: &str, key: &Value) -> OpResult<Option<Value>>;
}

/// A rule with the caller and the invocation's clock folded in, once per
/// invocation: what remains depends only on the row (and, for `now`
/// compared against row fields, the clock). Rules about the caller alone
/// become constants, so an admin's reads cost nothing per row.
#[derive(Debug)]
enum Check {
    /// Holds or not, whatever the row.
    Const(bool),
    /// Holds or not whatever the row, but only at this instant: it
    /// compared the clock, so evaluating it polls time.
    Clocked(bool),
    All(Vec<Check>),
    Any(Vec<Check>),
    Not(Box<Check>),
    Compare(Op, Term, Term),
    Exists(Term),
    /// The module's `collection` has a row at `key` on which `read`, that
    /// collection's read rule folded for the same caller, holds.
    Readable {
        collection: String,
        read: Box<Check>,
        key: Term,
    },
    /// The module's `collection` has a row at `key`.
    Stored {
        collection: String,
        key: Term,
    },
}

/// A folded operand.
#[derive(Debug)]
enum Term {
    /// A literal, or a path into the caller (missing reads as null, which
    /// every comparison treats alike).
    Value(Value),
    /// The invocation clock: reading it polls time.
    Now(f64),
    /// A path into the row's inputs: `row`, `next`, `key` or `args`.
    Ref(Root, Vec<String>),
    /// A lookup key made of several operands.
    List(Vec<Term>),
}

/// One row's inputs to a folded check.
#[derive(Clone, Copy, Default)]
struct At<'a> {
    key: Option<&'a Value>,
    row: Option<&'a Value>,
    next: Option<&'a Value>,
    args: Option<&'a Value>,
}

impl Term {
    fn of(operand: &Operand, caller: &Value, now: f64) -> Term {
        match operand {
            Operand::Value(value) => Term::Value(value.clone()),
            Operand::List(parts) => Term::List(
                parts
                    .iter()
                    .map(|part| Term::of(part, caller, now))
                    .collect(),
            ),
            Operand::Ref(Root::Principal, path) => {
                Term::Value(lookup(caller, path).cloned().unwrap_or(Value::Null))
            }
            Operand::Ref(Root::Now, _) => Term::Now(now),
            Operand::Ref(root, path) => Term::Ref(*root, path.clone()),
        }
    }

    /// Known whatever the row.
    fn constant(&self) -> bool {
        matches!(self, Term::Value(_) | Term::Now(_))
    }

    fn resolve<'s>(&'s self, at: At<'s>, timed: &mut bool) -> Resolved<'s> {
        use std::borrow::Cow;
        match self {
            Term::Value(value) => Some(Cow::Borrowed(value)),
            Term::Now(now) => {
                *timed = true;
                Some(Cow::Owned(Value::Number(*now)))
            }
            Term::List(_) => None,
            Term::Ref(root, path) => {
                let base = match root {
                    Root::Row => at.row,
                    Root::Next => at.next,
                    Root::Key => at.key,
                    Root::Args => at.args,
                    // Folded into values.
                    Root::Principal | Root::Now => None,
                };
                base.and_then(|base| lookup(base, path)).map(Cow::Borrowed)
            }
        }
    }

    /// The key a lookup names: a present value, or the array of a list's
    /// present parts.
    fn key(&self, at: At<'_>, timed: &mut bool) -> Option<Value> {
        match self {
            Term::List(parts) => {
                let mut values = Vec::with_capacity(parts.len());
                for part in parts {
                    let resolved = part.resolve(at, timed);
                    if !present(&resolved) {
                        return None;
                    }
                    values.push(resolved?.into_owned());
                }
                Some(Value::Array(values))
            }
            _ => {
                let resolved = self.resolve(at, timed);
                if present(&resolved) {
                    resolved.map(std::borrow::Cow::into_owned)
                } else {
                    None
                }
            }
        }
    }
}

impl Check {
    /// Fold `rule` for `caller` at `now`. `policies` finds the read rules
    /// of the collections `readable` names, which fold for the caller
    /// too: one the caller can never satisfy needs no lookup.
    fn fold(
        rule: &Rule,
        caller: &Value,
        now: f64,
        policies: &dyn Fn(&str) -> Option<Arc<CollectionPolicy>>,
    ) -> Check {
        let term = |operand: &Operand| Term::of(operand, caller, now);
        match rule {
            Rule::Const(value) => Check::Const(*value),
            Rule::All(rules) => Check::join(rules, caller, now, policies, true),
            Rule::Any(rules) => Check::join(rules, caller, now, policies, false),
            Rule::Not(rule) => match Check::fold(rule, caller, now, policies) {
                Check::Const(value) => Check::Const(!value),
                Check::Clocked(value) => Check::Clocked(!value),
                check => Check::Not(Box::new(check)),
            },
            Rule::Compare(op, left, right) => {
                let (left, right) = (term(left), term(right));
                if left.constant() && right.constant() {
                    let mut timed = false;
                    let at = At::default();
                    let held = op.holds(
                        &left.resolve(at, &mut timed),
                        &right.resolve(at, &mut timed),
                    );
                    if timed {
                        Check::Clocked(held)
                    } else {
                        Check::Const(held)
                    }
                } else {
                    Check::Compare(*op, left, right)
                }
            }
            Rule::Exists(operand) => match term(operand) {
                Term::Value(value) => Check::Const(!value.is_null()),
                // Present at every instant.
                Term::Now(_) => Check::Const(true),
                term => Check::Exists(term),
            },
            Rule::Readable(collection, key) => {
                let read = policies(collection)
                    .map(|policy| Check::fold(&policy.read, caller, now, policies));
                match read {
                    None | Some(Check::Const(false)) => Check::Const(false),
                    Some(Check::Clocked(false)) => Check::Clocked(false),
                    Some(read) => Check::Readable {
                        collection: collection.clone(),
                        read: Box::new(read),
                        key: term(key),
                    },
                }
            }
            Rule::Stored(collection, key) => Check::Stored {
                collection: collection.clone(),
                key: term(key),
            },
        }
    }

    /// `all` (a conjunction) or `any`, dropping neutral constants and
    /// short-circuiting on absorbing ones. Clocked outcomes stay, so
    /// evaluating them still polls time.
    fn join(
        rules: &[Rule],
        caller: &Value,
        now: f64,
        policies: &dyn Fn(&str) -> Option<Arc<CollectionPolicy>>,
        conjunction: bool,
    ) -> Check {
        let mut checks = Vec::new();
        for rule in rules {
            match Check::fold(rule, caller, now, policies) {
                Check::Const(value) if value == conjunction => {}
                Check::Const(value) => return Check::Const(value),
                check => checks.push(check),
            }
        }
        match checks.len() {
            0 => Check::Const(conjunction),
            1 => checks.pop().unwrap_or(Check::Const(conjunction)),
            _ if conjunction => Check::All(checks),
            _ => Check::Any(checks),
        }
    }

    /// Whether it holds for one row. Sets `timed` when the outcome read
    /// the clock.
    fn holds(&self, at: At<'_>, rows: &mut dyn Rows, timed: &mut bool) -> OpResult<bool> {
        Ok(match self {
            Check::Const(value) => *value,
            Check::Clocked(value) => {
                *timed = true;
                *value
            }
            Check::All(checks) => {
                for check in checks {
                    if !check.holds(at, rows, timed)? {
                        return Ok(false);
                    }
                }
                true
            }
            Check::Any(checks) => {
                for check in checks {
                    if check.holds(at, rows, timed)? {
                        return Ok(true);
                    }
                }
                false
            }
            Check::Not(check) => !check.holds(at, rows, timed)?,
            Check::Compare(op, left, right) => {
                let left = left.resolve(at, timed);
                let right = right.resolve(at, timed);
                op.holds(&left, &right)
            }
            Check::Exists(term) => present(&term.resolve(at, timed)),
            Check::Stored { collection, key } => match key.key(at, timed) {
                None => false,
                Some(key) => rows.row(collection, &key)?.is_some(),
            },
            Check::Readable {
                collection,
                read,
                key,
            } => {
                let Some(key) = key.key(at, timed) else {
                    return Ok(false);
                };
                match rows.row(collection, &key)? {
                    None => false,
                    Some(row) => read.holds(
                        At {
                            key: Some(&key),
                            row: Some(&row),
                            ..At::default()
                        },
                        rows,
                        timed,
                    )?,
                }
            }
        })
    }

    fn always(&self) -> bool {
        matches!(self, Check::Const(true))
    }
}

/// One field's folded rules.
#[derive(Debug)]
struct FieldCheck {
    name: String,
    read: Option<Check>,
    write: Option<Check>,
}

/// One caller's view of a collection's policy, folded once per
/// invocation (see [`Check`]).
#[derive(Debug)]
pub(crate) struct Folded {
    read: Check,
    insert: Check,
    update: Check,
    delete: Check,
    fields: Vec<FieldCheck>,
    /// Some field's read rule can hide it from this caller.
    redacts: bool,
}

/// Folded policies of one invocation, by module and collection, for the
/// principal it currently runs as; `None` for collections without one.
pub(crate) type Folds = std::collections::HashMap<(String, String), Option<Arc<Folded>>>;

/// Most fields one write may clear.
pub const MAX_CLEARED_FIELDS: usize = 256;

/// What a guarded write does with a stored row.
#[derive(Debug, PartialEq)]
pub(crate) enum Admitted {
    /// Store this value, or delete with `None`.
    Write(Option<Value>),
    /// Leave the row alone: a denied delete of a row the caller can't see.
    Skip,
}

impl Folded {
    /// Fold `policy` for `caller` at `now`.
    pub(crate) fn new(
        policy: &CollectionPolicy,
        caller: &Value,
        now: f64,
        policies: &dyn Fn(&str) -> Option<Arc<CollectionPolicy>>,
    ) -> Folded {
        let fold = |rule: &Rule| Check::fold(rule, caller, now, policies);
        let fields: Vec<FieldCheck> = policy
            .fields
            .iter()
            .map(|field| FieldCheck {
                name: field.name.clone(),
                read: field.read.as_ref().map(fold),
                write: field.write.as_ref().map(fold),
            })
            .collect();
        Folded {
            read: fold(&policy.read),
            insert: fold(&policy.insert),
            update: fold(&policy.update),
            delete: fold(&policy.delete),
            redacts: fields
                .iter()
                .any(|field| field.read.as_ref().is_some_and(|check| !check.always())),
            fields,
        }
    }

    fn field(&self, name: &str) -> Option<&FieldCheck> {
        self.fields.iter().find(|field| field.name == name)
    }

    fn field_readable(
        field: &FieldCheck,
        key: &Value,
        row: &Value,
        rows: &mut dyn Rows,
        timed: &mut bool,
    ) -> OpResult<bool> {
        match &field.read {
            None => Ok(true),
            Some(check) => check.holds(
                At {
                    key: Some(key),
                    row: Some(row),
                    ..At::default()
                },
                rows,
                timed,
            ),
        }
    }

    /// Whether the caller sees a stored row. `fields` are the index
    /// fields that selected it: finding a row through a field the caller
    /// can't read would reveal that field, so such rows stay hidden too.
    fn visible(
        &self,
        key: &Value,
        row: &Value,
        fields: &[String],
        rows: &mut dyn Rows,
        timed: &mut bool,
    ) -> OpResult<bool> {
        let at = At {
            key: Some(key),
            row: Some(row),
            ..At::default()
        };
        if !self.read.holds(at, rows, timed)? {
            return Ok(false);
        }
        for name in fields {
            if let Some(field) = self.field(name)
                && !Folded::field_readable(field, key, row, rows, timed)?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// The row without the fields the caller can't read.
    fn redact(
        &self,
        key: &Value,
        row: Value,
        rows: &mut dyn Rows,
        timed: &mut bool,
    ) -> OpResult<Value> {
        if !self.redacts {
            return Ok(row);
        }
        let Value::Object(entries) = &row else {
            return Ok(row);
        };
        let mut hidden = Vec::new();
        for field in &self.fields {
            if entries.contains_key(&field.name)
                && !Folded::field_readable(field, key, &row, rows, timed)?
            {
                hidden.push(field.name.clone());
            }
        }
        if hidden.is_empty() {
            return Ok(row);
        }
        let Value::Object(mut entries) = row else {
            return Ok(row);
        };
        for name in hidden {
            entries.remove(&name);
        }
        Ok(Value::Object(entries))
    }

    /// Admit a delete of `previous`: a denied delete of a row the caller
    /// can't see skips, like deleting a missing key, so it learns nothing
    /// of the row; one it can see fails.
    fn admit_delete(
        &self,
        collection: &str,
        key: &Value,
        previous: Option<&Value>,
        rows: &mut dyn Rows,
        timed: &mut bool,
    ) -> OpResult<Admitted> {
        let Some(previous) = previous else {
            return Ok(Admitted::Write(None));
        };
        let at = At {
            key: Some(key),
            row: Some(previous),
            ..At::default()
        };
        if self.delete.holds(at, rows, timed)? {
            Ok(Admitted::Write(None))
        } else if !self.read.holds(at, rows, timed)? {
            Ok(Admitted::Skip)
        } else {
            Err(denied(collection))
        }
    }

    /// Admit a write of `next` over `previous`, returning the value to
    /// store. Fields the caller can't read and didn't write keep their
    /// stored values, so a read-modify-write of a redacted row preserves
    /// them, except those `clear` names: the write removes them, which
    /// takes the right to change them. A hidden field named in `clear`
    /// takes that right whether or not the row holds it, so a denied
    /// clear never tells a caller whether the hidden field was there.
    /// Denied inserts and updates fail alike.
    #[allow(clippy::too_many_arguments)]
    fn admit_put(
        &self,
        collection: &str,
        key: &Value,
        previous: Option<&Value>,
        mut next: Value,
        clear: &[String],
        rows: &mut dyn Rows,
        timed: &mut bool,
    ) -> OpResult<Value> {
        // Hidden fields named in `clear`, whether or not stored.
        let mut cleared_hidden: Vec<&str> = Vec::new();
        if let Some(stored) = previous {
            for field in &self.fields {
                if next
                    .as_object()
                    .is_some_and(|written| written.contains_key(&field.name))
                {
                    // Written: an ordinary change.
                    continue;
                }
                let named = clear.contains(&field.name);
                let kept = stored
                    .as_object()
                    .and_then(|entries| entries.get(&field.name));
                if !named && (kept.is_none() || next.as_object().is_none()) {
                    continue;
                }
                if Folded::field_readable(field, key, stored, rows, timed)? {
                    continue;
                }
                if named {
                    cleared_hidden.push(&field.name);
                } else if let (Some(value), Value::Object(written)) = (kept, &mut next) {
                    written.insert(field.name.clone(), value.clone());
                }
            }
        }
        let at = At {
            key: Some(key),
            row: previous,
            next: Some(&next),
            args: None,
        };
        let row_rule = if previous.is_some() {
            &self.update
        } else {
            &self.insert
        };
        if !row_rule.holds(at, rows, timed)? {
            return Err(denied(collection));
        }
        for field in &self.fields {
            let before = previous.and_then(|row| lookup(row, std::slice::from_ref(&field.name)));
            let after = lookup(&next, std::slice::from_ref(&field.name));
            let forced = cleared_hidden.contains(&field.name.as_str());
            if before == after && !forced {
                continue;
            }
            let allowed = match (&field.write, &field.read) {
                (Some(write), _) => write.holds(at, rows, timed)?,
                // Change only what you can read: in the stored row, or in
                // the new one for an insert.
                (None, Some(_)) => {
                    Folded::field_readable(field, key, previous.unwrap_or(&next), rows, timed)?
                }
                (None, None) => true,
            };
            if !allowed {
                return Err(denied(collection));
            }
        }
        Ok(next)
    }

    /// When every row the caller may read holds one string in one
    /// top-level field (the read rule is, or has a conjunct,
    /// `row.field == "…"` once the caller folds in, like `row.owner ==
    /// principal.subject`): that field and string.
    pub(crate) fn bucket(&self) -> Option<(&str, &str)> {
        fn equality(check: &Check) -> Option<(&str, &str)> {
            match check {
                Check::Compare(
                    Op::Eq,
                    Term::Ref(Root::Row, path),
                    Term::Value(Value::String(value)),
                )
                | Check::Compare(
                    Op::Eq,
                    Term::Value(Value::String(value)),
                    Term::Ref(Root::Row, path),
                ) if path.len() == 1 => Some((path[0].as_str(), value.as_str())),
                Check::All(checks) => checks.iter().find_map(equality),
                _ => None,
            }
        }
        equality(&self.read)
    }
}

fn denied(collection: &str) -> OpError {
    OpError::from(fail(
        codes::ACCESS_DENIED,
        format!("access policy denies this write to `{collection}`"),
    ))
}

/// The fields `set`'s optional fifth member names to clear: `null` or an
/// array of at most [`MAX_CLEARED_FIELDS`] non-empty field names.
pub(crate) fn cleared_fields(value: Option<&Value>) -> OpResult<Vec<String>> {
    let invalid = || {
        OpError::from(fail(
            codes::INVALID_VALUE,
            format!("set clears at most {MAX_CLEARED_FIELDS} non-empty field names"),
        ))
    };
    match value {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(fields)) if fields.len() <= MAX_CLEARED_FIELDS => fields
            .iter()
            .map(|field| match field {
                Value::String(name) if !name.is_empty() => Ok(name.clone()),
                _ => Err(invalid()),
            })
            .collect(),
        Some(_) => Err(invalid()),
    }
}

/// Rows a guarded host's check looks up in its module.
struct HostRows<'h, 'a> {
    host: &'h mut Host<'a>,
    module: &'h str,
}

impl Rows for HostRows<'_, '_> {
    fn row(&mut self, collection: &str, key: &Value) -> OpResult<Option<Value>> {
        let Ok(key_bytes) = render(key) else {
            return Ok(None);
        };
        let Ok(id) = self.host.collection_id(self.module, collection) else {
            return Ok(None);
        };
        self.host.session.spend(FUEL_ROW)?;
        self.host
            .session
            .record_dep(encode_record_dep(id, &key_bytes)?);
        match self.host.read_record(id, &key_bytes)? {
            None => Ok(None),
            Some(json) => Ok(Some(crate::json::parse_value(&json)?)),
        }
    }
}

fn policy_of(host: &Host<'_>, module: &str, collection: &str) -> Option<Arc<CollectionPolicy>> {
    host.descriptors
        .get(module)
        .and_then(|defs| defs.collection(collection))
        .and_then(|def| def.access.clone())
}

impl<'a> Host<'a> {
    /// The policy guarding a caller's operations on a module's
    /// collection, folded for the principal the invocation runs as: `None`
    /// without one, and for code running with definer rights. Each
    /// collection folds once per invocation and principal.
    pub(crate) fn guard(&mut self, module: &str, collection: &str) -> Option<Arc<Folded>> {
        if !self.session.guarded() {
            return None;
        }
        let slot = (module.to_string(), collection.to_string());
        if let Some(folded) = self.session.folds.get(&slot) {
            return folded.clone();
        }
        let folded = policy_of(self, module, collection).map(|policy| {
            let caller = caller_view(self.session.principal.as_ref());
            let now = self.session.timestamp_ms as f64;
            Arc::new(Folded::new(&policy, &caller, now, &|target| {
                policy_of(self, module, target)
            }))
        });
        self.session.folds.insert(slot, folded.clone());
        folded
    }

    /// Run one folded check over the module's rows, polling the clock
    /// when its outcome read it.
    fn checking<T>(
        &mut self,
        module: &str,
        check: impl FnOnce(&mut dyn Rows, &mut bool) -> OpResult<T>,
    ) -> OpResult<T> {
        let mut timed = false;
        let outcome = check(&mut HostRows { host: self, module }, &mut timed);
        if timed {
            self.session.poll_time();
        }
        outcome
    }

    /// A guarded read of one stored row: `None` when hidden (`fields` are
    /// the index fields that selected it, see [`Folded::visible`]), else
    /// the row redacted.
    pub(crate) fn shown(
        &mut self,
        module: &str,
        policy: &Folded,
        key: &Value,
        row: Value,
        fields: &[String],
    ) -> OpResult<Option<Value>> {
        self.checking(module, |rows, timed| {
            if !policy.visible(key, &row, fields, rows, timed)? {
                return Ok(None);
            }
            policy.redact(key, row, rows, timed).map(Some)
        })
    }

    /// Admit a caller's delete of `previous`: whether to stage it (see
    /// [`Folded::admit_delete`]).
    pub(crate) fn admit_delete(
        &mut self,
        module: &str,
        collection: &str,
        policy: &Folded,
        key: &Value,
        previous: Option<&Value>,
    ) -> OpResult<bool> {
        let admitted = self.checking(module, |rows, timed| {
            policy.admit_delete(collection, key, previous, rows, timed)
        })?;
        Ok(admitted != Admitted::Skip)
    }

    /// Admit a caller's write of `next` over `previous`, returning the
    /// value to store (see [`Folded::admit_put`]).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn admit_put(
        &mut self,
        module: &str,
        collection: &str,
        policy: &Folded,
        key: &Value,
        previous: Option<&Value>,
        next: Value,
        clear: &[String],
    ) -> OpResult<Value> {
        self.checking(module, |rows, timed| {
            policy.admit_put(collection, key, previous, next, clear, rows, timed)
        })
    }

    /// A caller may read a derived value only where its access rule
    /// allows. Values without a rule, and reads with definer rights, are
    /// open.
    pub(crate) fn admit_derived_read(
        &mut self,
        module: &str,
        name: &str,
        args: &Value,
    ) -> OpResult<()> {
        if !self.session.guarded() {
            return Ok(());
        }
        let Some(rule) = self
            .descriptors
            .get(module)
            .and_then(|defs| defs.derived_access.get(name))
            .cloned()
        else {
            return Ok(());
        };
        let caller = caller_view(self.session.principal.as_ref());
        let now = self.session.timestamp_ms as f64;
        let check = Check::fold(&rule, &caller, now, &|target| {
            policy_of(self, module, target)
        });
        let allowed = self.checking(module, |rows, timed| {
            check.holds(
                At {
                    args: Some(args),
                    ..At::default()
                },
                rows,
                timed,
            )
        })?;
        if allowed {
            Ok(())
        } else {
            Err(OpError::from(fail(
                codes::ACCESS_DENIED,
                format!("access rule denies reading `{name}`"),
            )))
        }
    }
}

/// Per-row evaluation, as the host did before folding: every rule
/// resolves the caller and evaluates in full for every row. Kept as the
/// oracle folded checks must agree with.
#[cfg(test)]
pub(crate) mod reference {
    use super::*;

    /// What one per-row check sees.
    #[derive(Clone, Copy)]
    struct Subject<'a> {
        policies: &'a dyn Fn(&str) -> Option<Arc<CollectionPolicy>>,
        caller: &'a Value,
        now: f64,
        key: Option<&'a Value>,
        row: Option<&'a Value>,
        next: Option<&'a Value>,
        args: Option<&'a Value>,
    }

    fn resolve<'s>(operand: &'s Operand, subject: Subject<'s>, timed: &mut bool) -> Resolved<'s> {
        use std::borrow::Cow;
        match operand {
            Operand::Value(value) => Some(Cow::Borrowed(value)),
            Operand::List(_) => None,
            Operand::Ref(root, path) => {
                let base = match root {
                    Root::Principal => Some(subject.caller),
                    Root::Row => subject.row,
                    Root::Next => subject.next,
                    Root::Key => subject.key,
                    Root::Args => subject.args,
                    Root::Now => {
                        *timed = true;
                        return Some(Cow::Owned(Value::Number(subject.now)));
                    }
                };
                base.and_then(|base| lookup(base, path)).map(Cow::Borrowed)
            }
        }
    }

    fn key(operand: &Operand, subject: Subject<'_>, timed: &mut bool) -> Option<Value> {
        match operand {
            Operand::List(parts) => {
                let mut values = Vec::with_capacity(parts.len());
                for part in parts {
                    let resolved = resolve(part, subject, timed);
                    if !present(&resolved) {
                        return None;
                    }
                    values.push(resolved?.into_owned());
                }
                Some(Value::Array(values))
            }
            _ => {
                let resolved = resolve(operand, subject, timed);
                if present(&resolved) {
                    resolved.map(std::borrow::Cow::into_owned)
                } else {
                    None
                }
            }
        }
    }

    fn holds(
        rule: &Rule,
        subject: Subject<'_>,
        rows: &mut dyn Rows,
        timed: &mut bool,
    ) -> OpResult<bool> {
        Ok(match rule {
            Rule::Const(value) => *value,
            Rule::All(rules) => {
                for rule in rules {
                    if !holds(rule, subject, rows, timed)? {
                        return Ok(false);
                    }
                }
                true
            }
            Rule::Any(rules) => {
                for rule in rules {
                    if holds(rule, subject, rows, timed)? {
                        return Ok(true);
                    }
                }
                false
            }
            Rule::Not(rule) => !holds(rule, subject, rows, timed)?,
            Rule::Compare(op, left, right) => {
                let left = resolve(left, subject, timed);
                let right = resolve(right, subject, timed);
                op.holds(&left, &right)
            }
            Rule::Exists(operand) => present(&resolve(operand, subject, timed)),
            Rule::Stored(collection, operand) => match key(operand, subject, timed) {
                None => false,
                Some(key) => rows.row(collection, &key)?.is_some(),
            },
            Rule::Readable(collection, operand) => {
                let Some(key) = key(operand, subject, timed) else {
                    return Ok(false);
                };
                let Some(policy) = (subject.policies)(collection) else {
                    return Ok(false);
                };
                match rows.row(collection, &key)? {
                    None => false,
                    Some(row) => holds(
                        &policy.read,
                        Subject {
                            key: Some(&key),
                            row: Some(&row),
                            next: None,
                            args: None,
                            ..subject
                        },
                        rows,
                        timed,
                    )?,
                }
            }
        })
    }

    /// A policy evaluated per row for one caller.
    pub(crate) struct PerRow<'p> {
        pub policy: &'p CollectionPolicy,
        pub policies: &'p dyn Fn(&str) -> Option<Arc<CollectionPolicy>>,
        pub caller: Value,
        pub now: f64,
    }

    impl PerRow<'_> {
        fn check(
            &self,
            rule: &Rule,
            key: Option<&Value>,
            row: Option<&Value>,
            next: Option<&Value>,
            rows: &mut dyn Rows,
            timed: &mut bool,
        ) -> OpResult<bool> {
            if let Rule::Const(value) = rule {
                return Ok(*value);
            }
            let subject = Subject {
                policies: self.policies,
                caller: &self.caller,
                now: self.now,
                key,
                row,
                next,
                args: None,
            };
            holds(rule, subject, rows, timed)
        }

        fn field_readable(
            &self,
            field: &FieldPolicy,
            key: &Value,
            row: &Value,
            rows: &mut dyn Rows,
            timed: &mut bool,
        ) -> OpResult<bool> {
            match &field.read {
                None => Ok(true),
                Some(rule) => self.check(rule, Some(key), Some(row), None, rows, timed),
            }
        }

        pub(crate) fn visible(
            &self,
            key: &Value,
            row: &Value,
            rows: &mut dyn Rows,
            timed: &mut bool,
        ) -> OpResult<bool> {
            self.check(&self.policy.read, Some(key), Some(row), None, rows, timed)
        }

        pub(crate) fn redact(
            &self,
            key: &Value,
            row: Value,
            rows: &mut dyn Rows,
            timed: &mut bool,
        ) -> OpResult<Value> {
            if !self.policy.redacts() {
                return Ok(row);
            }
            let Value::Object(entries) = &row else {
                return Ok(row);
            };
            let mut hidden = Vec::new();
            for field in &self.policy.fields {
                if entries.contains_key(&field.name)
                    && !self.field_readable(field, key, &row, rows, timed)?
                {
                    hidden.push(field.name.clone());
                }
            }
            let Value::Object(mut entries) = row else {
                return Ok(row);
            };
            for name in hidden {
                entries.remove(&name);
            }
            Ok(Value::Object(entries))
        }

        pub(crate) fn admit_delete(
            &self,
            key: &Value,
            previous: Option<&Value>,
            rows: &mut dyn Rows,
            timed: &mut bool,
        ) -> OpResult<Admitted> {
            let Some(previous) = previous else {
                return Ok(Admitted::Write(None));
            };
            if self.check(
                &self.policy.delete,
                Some(key),
                Some(previous),
                None,
                rows,
                timed,
            )? {
                Ok(Admitted::Write(None))
            } else if !self.check(
                &self.policy.read,
                Some(key),
                Some(previous),
                None,
                rows,
                timed,
            )? {
                Ok(Admitted::Skip)
            } else {
                Err(denied("c"))
            }
        }

        /// The write admission before `clear` existed: hidden fields always
        /// carry over.
        pub(crate) fn admit_put(
            &self,
            key: &Value,
            previous: Option<&Value>,
            mut next: Value,
            rows: &mut dyn Rows,
            timed: &mut bool,
        ) -> OpResult<Value> {
            if let (Some(stored @ Value::Object(entries)), Value::Object(written)) =
                (previous, &mut next)
            {
                for field in &self.policy.fields {
                    if !written.contains_key(&field.name)
                        && let Some(value) = entries.get(&field.name)
                        && !self.field_readable(field, key, stored, rows, timed)?
                    {
                        written.insert(field.name.clone(), value.clone());
                    }
                }
            }
            let row_rule = if previous.is_some() {
                &self.policy.update
            } else {
                &self.policy.insert
            };
            if !self.check(row_rule, Some(key), previous, Some(&next), rows, timed)? {
                return Err(denied("c"));
            }
            for field in &self.policy.fields {
                let before =
                    previous.and_then(|row| lookup(row, std::slice::from_ref(&field.name)));
                let after = lookup(&next, std::slice::from_ref(&field.name));
                if before == after {
                    continue;
                }
                let allowed = match (&field.write, &field.read) {
                    (Some(write), _) => {
                        self.check(write, Some(key), previous, Some(&next), rows, timed)?
                    }
                    (None, Some(read)) => {
                        let seen = previous.unwrap_or(&next);
                        self.check(read, Some(key), Some(seen), None, rows, timed)?
                    }
                    (None, None) => true,
                };
                if !allowed {
                    return Err(denied("c"));
                }
            }
            Ok(next)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(text: &str) -> Value {
        crate::json::parse_value(text.as_bytes()).unwrap()
    }

    #[test]
    fn policies_parse_and_default_to_deny() {
        let policy = parse_policy(&json(
            r#"{"read":{"eq":[{"ref":["row","owner"]},{"ref":["principal","subject"]}]},"fields":{"secret":{"read":{"const":false}}}}"#,
        ))
        .unwrap();
        assert_eq!(policy.insert, Rule::Const(false));
        assert_eq!(policy.delete, Rule::Const(false));
        assert!(policy.redacts());
        assert_eq!(policy.fields.len(), 1);
    }

    #[test]
    fn operands_outside_their_scope_reject() {
        // `next` only exists while writing; `row` not on insert; `args`
        // only in derived rules; `now` takes no path.
        for (text, what) in [
            (r#"{"read":{"exists":{"ref":["next","a"]}}}"#, "next"),
            (r#"{"insert":{"exists":{"ref":["row","a"]}}}"#, "row"),
            (r#"{"read":{"exists":{"ref":["args","a"]}}}"#, "args"),
            (r#"{"read":{"exists":{"ref":["now","x"]}}}"#, "now"),
            (
                r#"{"read":{"exists":{"ref":["principal","email"]}}}"#,
                "principal",
            ),
            (r#"{"read":{"exists":{"list":[{"value":1}]}}}"#, "list"),
            (r#"{"read":{"bogus":true}}"#, "unknown"),
            (r#"{"fields":{"a":{}}}"#, "no rule"),
            (r#"{"write":{"const":true}}"#, "unknown member"),
        ] {
            assert!(parse_policy(&json(text)).is_err(), "{what} accepted");
        }
        assert!(parse_derived_rule(&json(r#"{"exists":{"ref":["args","a"]}}"#)).is_ok());
        assert!(parse_derived_rule(&json(r#"{"exists":{"ref":["row","a"]}}"#)).is_err());
    }

    #[test]
    fn rules_are_bounded() {
        let mut rule = r#"{"const":true}"#.to_string();
        for _ in 0..MAX_RULE_DEPTH {
            rule = format!(r#"{{"not":{rule}}}"#);
        }
        let error = parse_policy(&json(&format!(r#"{{"read":{rule}}}"#))).unwrap_err();
        assert!(error.contains("nest"), "{error}");
        let wide: Vec<String> = (0..MAX_RULE_NODES)
            .map(|_| r#"{"const":true}"#.to_string())
            .collect();
        let error = parse_policy(&json(&format!(
            r#"{{"read":{{"all":[{}]}}}}"#,
            wide.join(",")
        )))
        .unwrap_err();
        assert!(error.contains("nodes"), "{error}");
    }

    #[test]
    fn comparisons_with_missing_sides_are_false() {
        let missing: Resolved<'_> = None;
        let null: Resolved<'_> = Some(std::borrow::Cow::Owned(Value::Null));
        let one: Resolved<'_> = Some(std::borrow::Cow::Owned(Value::Number(1.0)));
        let text: Resolved<'_> = Some(std::borrow::Cow::Owned(Value::String("a".into())));
        assert!(!Op::Eq.holds(&missing, &missing));
        assert!(!Op::Eq.holds(&null, &null));
        assert!(!Op::Ne.holds(&missing, &one));
        assert!(Op::Eq.holds(&one, &one));
        assert!(Op::Ne.holds(&one, &text));
        assert!(!Op::Lt.holds(&one, &text));
        assert!(Op::StartsWith.holds(&text, &text));
    }

    #[test]
    fn readable_targets_are_checked_across_policies() {
        let plain = parse_policy(&json(r#"{"read":{"const":true}}"#)).unwrap();
        let chained = parse_policy(&json(
            r#"{"read":{"readable":["plain",{"ref":["row","parent"]}]}}"#,
        ))
        .unwrap();
        let twice = parse_policy(&json(
            r#"{"read":{"readable":["chained",{"ref":["row","parent"]}]}}"#,
        ))
        .unwrap();
        let declared: BTreeSet<&str> = ["plain", "chained", "twice", "open"].into();
        let mut policies = BTreeMap::new();
        policies.insert("plain", &plain);
        policies.insert("chained", &chained);
        assert!(validate_targets(&declared, &policies, std::iter::empty()).is_ok());
        policies.insert("twice", &twice);
        assert!(validate_targets(&declared, &policies, std::iter::empty()).is_err());
        let open =
            parse_policy(&json(r#"{"read":{"readable":["open",{"ref":["key"]}]}}"#)).unwrap();
        let mut policies = BTreeMap::new();
        policies.insert("reader", &open);
        assert!(validate_targets(&declared, &policies, std::iter::empty()).is_err());
        let stored =
            parse_policy(&json(r#"{"read":{"stored":["missing",{"ref":["key"]}]}}"#)).unwrap();
        let mut policies = BTreeMap::new();
        policies.insert("plain", &stored);
        assert!(validate_targets(&declared, &policies, std::iter::empty()).is_err());
    }

    /// Rows held in memory, for checks without a host.
    #[derive(Default)]
    struct MapRows {
        rows: BTreeMap<(String, Vec<u8>), Value>,
        lookups: usize,
    }

    impl MapRows {
        fn put(&mut self, collection: &str, key: &Value, row: Value) {
            self.rows
                .insert((collection.to_string(), render(key).unwrap()), row);
        }
    }

    impl Rows for MapRows {
        fn row(&mut self, collection: &str, key: &Value) -> OpResult<Option<Value>> {
            self.lookups += 1;
            let Ok(key) = render(key) else {
                return Ok(None);
            };
            Ok(self.rows.get(&(collection.to_string(), key)).cloned())
        }
    }

    fn principal(subject: &str, role: &str) -> Value {
        caller_view(Some(
            &Principal::new(
                subject.to_string(),
                None,
                Some(json(&format!(r#"{{"role":"{role}"}}"#))),
            )
            .unwrap(),
        ))
    }

    fn no_policies(_: &str) -> Option<Arc<CollectionPolicy>> {
        None
    }

    /// Random rules, rows and callers for the equivalence test.
    struct Gen(rand::rngs::StdRng);

    impl Gen {
        fn pick<T: Clone>(&mut self, items: &[T]) -> T {
            use rand::Rng;
            items[self.0.random_range(0..items.len())].clone()
        }

        fn chance(&mut self, percent: u32) -> bool {
            use rand::Rng;
            self.0.random_range(0..100) < percent
        }

        fn value(&mut self) -> Value {
            match self.pick(&[0, 1, 2, 3, 4, 5, 6]) {
                0 => Value::Null,
                1 => Value::Bool(self.chance(50)),
                2 => Value::Number(self.pick(&[1.0, 2.0, 950.0, 1000.0, 1050.0])),
                3 => Value::Array(vec![
                    Value::String(self.pick(&["a", "b"]).into()),
                    Value::Number(self.pick(&[1.0, 2.0])),
                ]),
                _ => Value::String(self.pick(&["a", "b", "c", "admin", "user", "ab"]).into()),
            }
        }

        fn path(&mut self, root: Root) -> Vec<String> {
            let field = match root {
                Root::Principal => {
                    return match self.pick(&[0, 1, 2, 3]) {
                        0 => vec!["subject".into()],
                        1 => vec!["tenant".into()],
                        2 => vec!["claims".into(), "role".into()],
                        _ => vec!["claims".into(), "missing".into()],
                    };
                }
                Root::Key => {
                    return match self.pick(&[0, 1, 2]) {
                        0 => vec![],
                        1 => vec!["0".into()],
                        _ => vec!["1".into()],
                    };
                }
                Root::Now => return vec![],
                _ => self.pick(&["owner", "team", "rank", "tags", "secret", "parent", "nope"]),
            };
            let mut path = vec![field.to_string()];
            if field == "tags" && self.chance(30) {
                path.push(self.pick(&["0", "1"]).to_string());
            }
            path
        }

        fn operand(&mut self, roots: &[Root]) -> Operand {
            if self.chance(25) {
                return Operand::Value(self.value());
            }
            let root = self.pick(roots);
            let path = self.path(root);
            Operand::Ref(root, path)
        }

        fn key(&mut self, roots: &[Root]) -> Operand {
            if self.chance(30) {
                Operand::List(vec![self.operand(roots), self.operand(roots)])
            } else {
                self.operand(roots)
            }
        }

        fn rule(&mut self, roots: &[Root], depth: usize, lookups: bool) -> Rule {
            let leaf = depth == 0 || self.chance(35);
            if leaf {
                return match self.pick(&[0, 1, 1, 1, 1, 2, 3, 4]) {
                    0 => Rule::Const(self.chance(50)),
                    1 => {
                        let op = self.pick(&[
                            Op::Eq,
                            Op::Ne,
                            Op::In,
                            Op::Lt,
                            Op::Lte,
                            Op::Gt,
                            Op::Gte,
                            Op::StartsWith,
                        ]);
                        Rule::Compare(op, self.operand(roots), self.operand(roots))
                    }
                    2 => Rule::Exists(self.operand(roots)),
                    3 if lookups => Rule::Stored("parents".into(), self.key(roots)),
                    4 if lookups => Rule::Readable("parents".into(), self.key(roots)),
                    _ => Rule::Const(self.chance(50)),
                };
            }
            match self.pick(&[0, 1, 2]) {
                0 => {
                    let count = self.pick(&[1, 2, 3]);
                    Rule::All(
                        (0..count)
                            .map(|_| self.rule(roots, depth - 1, lookups))
                            .collect(),
                    )
                }
                1 => {
                    let count = self.pick(&[1, 2, 3]);
                    Rule::Any(
                        (0..count)
                            .map(|_| self.rule(roots, depth - 1, lookups))
                            .collect(),
                    )
                }
                _ => Rule::Not(Box::new(self.rule(roots, depth - 1, lookups))),
            }
        }

        /// A read rule that sometimes pins `owner` to the caller, as
        /// owner-bucket scans look for.
        fn read_rule(&mut self, roots: &[Root], lookups: bool) -> Rule {
            let rule = self.rule(roots, 3, lookups);
            if self.chance(40) {
                let pin = Rule::Compare(
                    Op::Eq,
                    Operand::Ref(Root::Row, vec!["owner".into()]),
                    Operand::Ref(Root::Principal, vec!["subject".into()]),
                );
                Rule::All(vec![pin, rule])
            } else {
                rule
            }
        }

        fn policy(&mut self, lookups: bool) -> CollectionPolicy {
            use Root::{Key, Next, Now, Principal as Caller, Row};
            let mut fields = Vec::new();
            for name in ["secret", "rank"] {
                if !self.chance(60) {
                    continue;
                }
                let read = self
                    .chance(80)
                    .then(|| self.rule(&[Caller, Row, Key, Now], 2, lookups));
                let write = self
                    .chance(50)
                    .then(|| self.rule(&[Caller, Row, Next, Key, Now], 2, lookups));
                if read.is_none() && write.is_none() {
                    continue;
                }
                fields.push(FieldPolicy {
                    name: name.to_string(),
                    read,
                    write,
                });
            }
            CollectionPolicy {
                read: self.read_rule(&[Caller, Row, Key, Now], lookups),
                insert: self.rule(&[Caller, Next, Key, Now], 3, lookups),
                update: self.rule(&[Caller, Row, Next, Key, Now], 3, lookups),
                delete: self.rule(&[Caller, Row, Key, Now], 3, lookups),
                fields,
            }
        }

        fn row(&mut self) -> Value {
            let mut map = BTreeMap::new();
            for field in ["owner", "team", "rank", "tags", "secret", "parent"] {
                if self.chance(80) {
                    let value = match field {
                        "owner" if self.chance(70) => {
                            Value::String(self.pick(&["a", "b", "c"]).into())
                        }
                        "parent" if self.chance(70) => Value::String(self.pick(&["p", "q"]).into()),
                        _ => self.value(),
                    };
                    map.insert(field.to_string(), value);
                }
            }
            Value::Object(map)
        }

        fn row_key(&mut self) -> Value {
            if self.chance(50) {
                Value::String(self.pick(&["k1", "k2", "a"]).into())
            } else {
                Value::Array(vec![
                    Value::String(self.pick(&["a", "b"]).into()),
                    Value::Number(self.pick(&[1.0, 2.0])),
                ])
            }
        }

        fn caller(&mut self) -> Value {
            match self.pick(&[0, 1, 2, 3]) {
                0 => caller_view(None),
                1 => principal("a", "admin"),
                2 => principal("b", "user"),
                _ => principal("c", "user"),
            }
        }
    }

    fn agree<T: PartialEq + std::fmt::Debug>(
        what: &str,
        folded: &OpResult<T>,
        per_row: &OpResult<T>,
    ) {
        match (folded, per_row) {
            (Ok(folded), Ok(per_row)) => assert_eq!(folded, per_row, "{what} differs"),
            (Err(folded), Err(per_row)) => {
                assert_eq!(folded.failure.code, per_row.failure.code, "{what} differs");
            }
            _ => panic!("{what}: folded {folded:?}, per row {per_row:?}"),
        }
    }

    /// Folding the caller into rules once per invocation decides exactly
    /// what evaluating every rule in full for every row decides: which
    /// rows show, how they are redacted, and which writes and deletes are
    /// admitted with what value, over random rules (lookups, the clock,
    /// keys, list keys and missing paths included), rows and callers. An
    /// outcome the folded check reports clock-free never changes with
    /// the clock.
    #[test]
    fn folded_checks_decide_what_per_row_evaluation_decides() {
        use rand::SeedableRng;
        let mut generator = Gen(rand::rngs::StdRng::seed_from_u64(0x5eed));
        let now = 1000.0;
        let mut compared = 0usize;
        for round in 0..400 {
            let parents = Arc::new(generator.policy(false));
            let policy = generator.policy(round % 4 != 0);
            let lookup = |name: &str| (name == "parents").then(|| parents.clone());
            let mut rows = MapRows::default();
            for parent in ["p", "q"] {
                if generator.chance(70) {
                    let row = generator.row();
                    rows.put("parents", &Value::String(parent.into()), row);
                }
            }
            let mut stored = Vec::new();
            for _ in 0..12 {
                stored.push((generator.row_key(), generator.row()));
            }
            for _ in 0..4 {
                let caller = generator.caller();
                let folded = Folded::new(&policy, &caller, now, &lookup);
                let later = Folded::new(&policy, &caller, now + 500.0, &lookup);
                let per_row = reference::PerRow {
                    policy: &policy,
                    policies: &lookup,
                    caller: caller.clone(),
                    now,
                };
                for (key, row) in &stored {
                    let mut timed = false;
                    let mut ignored = false;
                    let seen = folded.visible(key, row, &[], &mut rows, &mut timed);
                    let oracle = per_row.visible(key, row, &mut rows, &mut ignored);
                    agree("visibility", &seen, &oracle);
                    if !timed {
                        let at_later = later.visible(key, row, &[], &mut rows, &mut ignored);
                        agree("clock-free visibility", &at_later, &seen);
                    }
                    if matches!(seen, Ok(true)) {
                        agree(
                            "redaction",
                            &folded.redact(key, row.clone(), &mut rows, &mut timed),
                            &per_row.redact(key, row.clone(), &mut rows, &mut ignored),
                        );
                    }
                    agree(
                        "delete",
                        &folded.admit_delete("c", key, Some(row), &mut rows, &mut timed),
                        &per_row.admit_delete(key, Some(row), &mut rows, &mut ignored),
                    );
                    let next = generator.row();
                    agree(
                        "update",
                        &folded.admit_put(
                            "c",
                            key,
                            Some(row),
                            next.clone(),
                            &[],
                            &mut rows,
                            &mut timed,
                        ),
                        &per_row.admit_put(key, Some(row), next.clone(), &mut rows, &mut ignored),
                    );
                    agree(
                        "insert",
                        &folded.admit_put("c", key, None, next.clone(), &[], &mut rows, &mut timed),
                        &per_row.admit_put(key, None, next, &mut rows, &mut ignored),
                    );
                    compared += 1;
                }
            }
        }
        assert_eq!(compared, 400 * 4 * 12);
    }

    #[test]
    fn rules_about_the_caller_alone_fold_into_constants_once() {
        let policy = parse_policy(&json(
            r#"{"read":{"any":[{"eq":[{"ref":["principal","claims","role"]},{"value":"admin"}]},
                {"stored":["members",{"list":[{"ref":["row","team"]},{"ref":["principal","subject"]}]}]}]},
                "fields":{"secret":{"read":{"eq":[{"ref":["principal","claims","role"]},{"value":"admin"}]}}}}"#,
        ))
        .unwrap();
        let admin = Folded::new(&policy, &principal("a", "admin"), 0.0, &no_policies);
        assert!(admin.read.always() && !admin.redacts);
        // Rows cost an admin no lookup at all.
        let mut rows = MapRows::default();
        let row = json(r#"{"team":"t","secret":"s"}"#);
        let mut timed = false;
        assert!(
            admin
                .visible(&json(r#""k""#), &row, &[], &mut rows, &mut timed)
                .unwrap()
        );
        assert_eq!(rows.lookups, 0);
        // Others keep only the membership lookup, with the caller folded in.
        let user = Folded::new(&policy, &principal("b", "user"), 0.0, &no_policies);
        assert!(matches!(user.read, Check::Stored { .. }) && user.redacts);
        assert!(
            !user
                .visible(&json(r#""k""#), &row, &[], &mut rows, &mut timed)
                .unwrap()
        );
        assert_eq!(rows.lookups, 1);
        // A target whose read rule the caller can never meet needs no
        // lookup.
        let parents = Arc::new(
            parse_policy(&json(
                r#"{"read":{"eq":[{"ref":["principal","claims","role"]},{"value":"admin"}]}}"#,
            ))
            .unwrap(),
        );
        let child = parse_policy(&json(
            r#"{"read":{"readable":["parents",{"ref":["row","parent"]}]}}"#,
        ))
        .unwrap();
        let lookup = |name: &str| (name == "parents").then(|| parents.clone());
        let user = Folded::new(&child, &principal("b", "user"), 0.0, &lookup);
        assert!(matches!(user.read, Check::Const(false)));
        // A comparison with the clock stays clocked: evaluating it polls.
        let expiring =
            parse_policy(&json(r#"{"read":{"lt":[{"value":5},{"ref":["now"]}]}}"#)).unwrap();
        let folded = Folded::new(&expiring, &principal("b", "user"), 10.0, &no_policies);
        assert!(matches!(folded.read, Check::Clocked(true)));
        let mut timed = false;
        assert!(
            folded
                .visible(&json(r#""k""#), &row, &[], &mut rows, &mut timed)
                .unwrap()
        );
        assert!(timed);
        // So does a `readable` whose target only the clock refuses: no
        // lookup, but the outcome changes with time.
        let expiring = Arc::new(expiring);
        let lookup = |name: &str| (name == "parents").then(|| expiring.clone());
        let early = Folded::new(&child, &principal("b", "user"), 1.0, &lookup);
        assert!(matches!(early.read, Check::Clocked(false)));
        let mut timed = false;
        assert!(
            !early
                .visible(&json(r#""k""#), &row, &[], &mut rows, &mut timed)
                .unwrap()
        );
        assert!(timed);
    }

    #[test]
    fn owner_pins_name_their_bucket() {
        let owned = parse_policy(&json(
            r#"{"read":{"all":[{"eq":[{"ref":["principal","subject"]},{"ref":["row","owner"]}]},
                {"exists":{"ref":["row","title"]}}]}}"#,
        ))
        .unwrap();
        let folded = Folded::new(&owned, &principal("b", "user"), 0.0, &no_policies);
        assert_eq!(folded.bucket(), Some(("owner", "b")));
        // Anonymous callers pin nothing (their subject is missing); nested
        // paths and alternatives pin nothing either.
        let anonymous = Folded::new(&owned, &caller_view(None), 0.0, &no_policies);
        assert_eq!(anonymous.bucket(), None);
        for text in [
            r#"{"read":{"eq":[{"ref":["row","owner","id"]},{"ref":["principal","subject"]}]}}"#,
            r#"{"read":{"any":[{"eq":[{"ref":["row","owner"]},{"ref":["principal","subject"]}]},
                {"exists":{"ref":["row","public"]}}]}}"#,
        ] {
            let policy = parse_policy(&json(text)).unwrap();
            let folded = Folded::new(&policy, &principal("b", "user"), 0.0, &no_policies);
            assert_eq!(folded.bucket(), None, "{text}");
        }
    }

    fn cleared(
        folded: &Folded,
        previous: &str,
        next: &str,
        clear: &[&str],
    ) -> Result<Value, String> {
        let clear: Vec<String> = clear.iter().map(|name| name.to_string()).collect();
        let mut rows = MapRows::default();
        let mut timed = false;
        folded
            .admit_put(
                "notes",
                &json(r#""k""#),
                Some(&json(previous)),
                json(next),
                &clear,
                &mut rows,
                &mut timed,
            )
            .map_err(|error| error.failure.code)
    }

    #[test]
    fn clear_removes_hidden_fields_only_with_the_right_to_change_them() {
        // `token` is write-only for its owner; `audit` is hidden from
        // everyone but admins and unwritable by others.
        let policy = parse_policy(&json(
            r#"{"read":{"const":true},"update":{"const":true},
                "fields":{"token":{"read":{"const":false},
                                  "write":{"eq":[{"ref":["row","owner"]},{"ref":["principal","subject"]}]}},
                          "audit":{"read":{"eq":[{"ref":["principal","claims","role"]},{"value":"admin"}]}}}}"#,
        ))
        .unwrap();
        let owner = Folded::new(&policy, &principal("a", "user"), 0.0, &no_policies);
        let other = Folded::new(&policy, &principal("b", "user"), 0.0, &no_policies);
        let stored = r#"{"owner":"a","token":"t","audit":"x","text":"old"}"#;
        let next = r#"{"owner":"a","text":"new"}"#;
        // Without clear, hidden fields carry over.
        assert_eq!(
            cleared(&owner, stored, next, &[]).unwrap(),
            json(r#"{"owner":"a","token":"t","audit":"x","text":"new"}"#)
        );
        // The owner may clear the write-only token; the audit stays.
        assert_eq!(
            cleared(&owner, stored, next, &["token"]).unwrap(),
            json(r#"{"owner":"a","audit":"x","text":"new"}"#)
        );
        // Nobody but an admin may clear the audit, stored or not: the
        // outcome never tells whether a hidden field was there.
        let absent = r#"{"owner":"a","text":"old"}"#;
        for previous in [stored, absent] {
            assert_eq!(
                cleared(&owner, previous, next, &["audit"]).unwrap_err(),
                codes::ACCESS_DENIED
            );
            assert_eq!(
                cleared(&other, previous, next, &["token"]).unwrap_err(),
                codes::ACCESS_DENIED
            );
        }
        // Clearing a field the caller can read is an ordinary removal.
        assert_eq!(
            cleared(&owner, stored, next, &["text", "missing"]).unwrap(),
            json(r#"{"owner":"a","token":"t","audit":"x","text":"new"}"#)
        );
        let admin = Folded::new(&policy, &principal("c", "admin"), 0.0, &no_policies);
        assert_eq!(
            cleared(&admin, stored, next, &["audit"]).unwrap(),
            json(r#"{"owner":"a","token":"t","text":"new"}"#)
        );
    }

    #[test]
    fn cleared_field_lists_are_bounded_names() {
        assert!(cleared_fields(None).unwrap().is_empty());
        assert!(cleared_fields(Some(&Value::Null)).unwrap().is_empty());
        assert_eq!(
            cleared_fields(Some(&json(r#"["a","b"]"#))).unwrap(),
            vec!["a".to_string(), "b".to_string()]
        );
        for bad in [r#"[""]"#, r#"[1]"#, r#""a""#, r#"{"a":1}"#] {
            assert!(cleared_fields(Some(&json(bad))).is_err(), "{bad}");
        }
        let many = Value::Array(
            (0..=MAX_CLEARED_FIELDS)
                .map(|n| Value::String(format!("f{n}")))
                .collect(),
        );
        assert!(cleared_fields(Some(&many)).is_err());
    }

    /// Folding against per-row evaluation over 10,000 rows of a docs-like
    /// policy (owner, team membership through `stored`, an admin claim, an
    /// owner-only field), for an admin and a team member: visibility plus
    /// redaction per row. Ignored: a measurement. Run it in release with
    /// `cargo test --release -p flower-engine --lib -- --ignored --nocapture
    /// folding_against_per_row_evaluation`.
    #[test]
    #[ignore = "a measurement: run in release on a quiet machine"]
    fn folding_against_per_row_evaluation() {
        use std::hint::black_box;
        let policy = parse_policy(&json(
            r#"{"read":{"any":[
                {"eq":[{"ref":["row","owner"]},{"ref":["principal","subject"]}]},
                {"stored":["members",{"list":[{"ref":["row","team"]},{"ref":["principal","subject"]}]}]},
                {"eq":[{"ref":["principal","claims","role"]},{"value":"admin"}]}]},
              "fields":{"secret":{"read":{"eq":[{"ref":["row","owner"]},{"ref":["principal","subject"]}]}}}}"#,
        ))
        .unwrap();
        let mut rows = MapRows::default();
        for team in 0..10 {
            rows.put(
                "members",
                &json(&format!(r#"["t{team}","b"]"#)),
                Value::Bool(true),
            );
        }
        let stored: Vec<(Value, Value)> = (0..10_000)
            .map(|n| {
                (
                    Value::String(format!("d{n}")),
                    json(&format!(
                        r#"{{"owner":"o{}","team":"t{}","title":"x","secret":"s"}}"#,
                        n % 100,
                        n % 20
                    )),
                )
            })
            .collect();
        let passes = 5;
        for (name, caller) in [
            ("admin", principal("a", "admin")),
            ("member", principal("b", "user")),
        ] {
            let mut per_row_ns = Vec::new();
            let mut folded_ns = Vec::new();
            for _ in 0..passes {
                let started = std::time::Instant::now();
                let per_row = reference::PerRow {
                    policy: &policy,
                    policies: &no_policies,
                    caller: caller.clone(),
                    now: 0.0,
                };
                let mut timed = false;
                for (key, row) in &stored {
                    if per_row.visible(key, row, &mut rows, &mut timed).unwrap() {
                        black_box(
                            per_row
                                .redact(key, row.clone(), &mut rows, &mut timed)
                                .unwrap(),
                        );
                    }
                }
                per_row_ns.push(started.elapsed().as_nanos() as f64 / stored.len() as f64);
                let started = std::time::Instant::now();
                let folded = Folded::new(&policy, &caller, 0.0, &no_policies);
                for (key, row) in &stored {
                    if folded
                        .visible(key, row, &[], &mut rows, &mut timed)
                        .unwrap()
                    {
                        black_box(
                            folded
                                .redact(key, row.clone(), &mut rows, &mut timed)
                                .unwrap(),
                        );
                    }
                }
                folded_ns.push(started.elapsed().as_nanos() as f64 / stored.len() as f64);
            }
            per_row_ns.sort_by(f64::total_cmp);
            folded_ns.sort_by(f64::total_cmp);
            let (per_row, folded) = (per_row_ns[passes / 2], folded_ns[passes / 2]);
            println!(
                "{name}: per-row {per_row:.0} ns/row, folded {folded:.0} ns/row ({:.1}x)",
                per_row / folded
            );
        }
    }
}
