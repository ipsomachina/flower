//! Typed index handles and source pages.
//!
//! [`Index<K, V, F>`] names one declared index of a collection with key
//! type `K`, row type `V` and indexed field types `F` (a tuple, one type
//! per field in order). A range over it fixes a [`Prefix`] of the fields,
//! then optionally bounds the next field, both type-checked against `F`:
//!
//! ```ignore
//! // index(name = "by_owner_due", fields(owner, due), types(String, u64))
//! let page = Tasks::BY_OWNER_DUE
//!     .prefix((owner,))
//!     .gte(&since)
//!     .reverse()
//!     .page(ctx, 20)?;
//! ```
//!
//! A prefix covering every field takes no bounds: its next component is
//! [`Full`], which no value has. Components render to canonical keys
//! at the call, as raw ranges do; the host owns the
//! order and the cursors, and checks that a cursor belongs to the range it
//! continues. [`Source<K, V>`] pages a whole collection in host order: a
//! complete inventory, rows the caller may not read left out.
//!
//! Both handles address the calling module's collection, or another
//! module's exported one through [`Index::in_module`] /
//! [`Source::in_module`] (by name or `$slot`); imported collections are
//! read-only, so neither handle writes.

use alloc::string::String;
use alloc::vec::Vec;
use core::marker::PhantomData;

use crate::ctx::{CollectionRef, RangeBounds, ReadCtx};
use crate::error::{Failure, Result, codes, fail};
use crate::typed::{FlowerType, Page, decode_page};
use crate::value::Value;

/// The component after a prefix that covers every indexed field: no
/// value has this type, so such a range takes no bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Full {}

/// A typed prefix of the index fields `F`.
pub trait Prefix<F> {
    /// The type of the field after the prefix, which bounds compare
    /// against; [`Full`] when the prefix covers every field.
    type Next;

    /// The prefix components, one value per field.
    fn components(&self) -> Result<Vec<Value>>;
}

/// `prefix!([A, B] [0: A] => B)`: the prefix `(A,)` of fields `(A, B)`,
/// whose next component is a `B`.
macro_rules! prefix {
    ([$($field:ident),+] [$($position:tt : $ty:ident),*] => $next:ty) => {
        impl<$($field: FlowerType),+> Prefix<($($field,)+)> for ($($ty,)*) {
            type Next = $next;

            fn components(&self) -> Result<Vec<Value>> {
                #[allow(unused_mut)]
                let mut components = Vec::new();
                $( components.push(self.$position.encode()?); )*
                Ok(components)
            }
        }
    };
}

prefix!([A] [] => A);
prefix!([A] [0: A] => Full);
prefix!([A, B] [] => A);
prefix!([A, B] [0: A] => B);
prefix!([A, B] [0: A, 1: B] => Full);
prefix!([A, B, C] [] => A);
prefix!([A, B, C] [0: A] => B);
prefix!([A, B, C] [0: A, 1: B] => C);
prefix!([A, B, C] [0: A, 1: B, 2: C] => Full);
prefix!([A, B, C, D] [] => A);
prefix!([A, B, C, D] [0: A] => B);
prefix!([A, B, C, D] [0: A, 1: B] => C);
prefix!([A, B, C, D] [0: A, 1: B, 2: C] => D);
prefix!([A, B, C, D] [0: A, 1: B, 2: C, 3: D] => Full);

/// Marks the types a handle stands for, without owning any.
type Types<K, V, F> = fn() -> (K, V, F);

/// A typed handle on one declared index.
pub struct Index<K, V, F> {
    module: &'static str,
    collection: &'static str,
    name: &'static str,
    _types: PhantomData<Types<K, V, F>>,
}

impl<K, V, F> Clone for Index<K, V, F> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K, V, F> Copy for Index<K, V, F> {}

impl<K, V, F> core::fmt::Debug for Index<K, V, F> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Index")
            .field("module", &self.module)
            .field("collection", &self.collection)
            .field("name", &self.name)
            .finish()
    }
}

impl<K, V, F> Index<K, V, F> {
    /// The index `name` of the calling module's `collection`.
    pub const fn new(collection: &'static str, name: &'static str) -> Self {
        Index {
            module: "",
            collection,
            name,
            _types: PhantomData,
        }
    }

    /// The same index of another module's exported collection, by name
    /// or `$slot`.
    pub const fn in_module(self, module: &'static str) -> Self {
        Index { module, ..self }
    }

    /// The index name.
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// A range over the whole index.
    pub fn query(&self) -> IndexRange<K, V, F, ()>
    where
        (): Prefix<F>,
    {
        self.prefix(())
    }

    /// A range over the rows whose leading indexed fields equal `prefix`.
    pub fn prefix<P: Prefix<F>>(&self, prefix: P) -> IndexRange<K, V, F, P> {
        IndexRange {
            index: *self,
            prefix,
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            after: None,
            reverse: false,
            error: None,
        }
    }
}

/// A range being built over an [`Index`]: prefix, optional bounds on the
/// next field, direction and continuation.
pub struct IndexRange<K, V, F, P> {
    index: Index<K, V, F>,
    prefix: P,
    gt: Option<Value>,
    gte: Option<Value>,
    lt: Option<Value>,
    lte: Option<Value>,
    after: Option<String>,
    reverse: bool,
    error: Option<Failure>,
}

impl<K, V, F, P> IndexRange<K, V, F, P>
where
    P: Prefix<F>,
{
    fn bound(mut self, slot: fn(&mut Self) -> &mut Option<Value>, value: Result<Value>) -> Self {
        match value {
            Ok(text) => *slot(&mut self) = Some(text),
            Err(failure) => {
                self.error.get_or_insert(failure);
            }
        }
        self
    }

    /// Rows whose next field is greater than `bound`.
    pub fn gt(self, bound: &P::Next) -> Self
    where
        P::Next: FlowerType,
    {
        self.bound(|range| &mut range.gt, bound.encode())
    }

    /// Rows whose next field is at least `bound`.
    pub fn gte(self, bound: &P::Next) -> Self
    where
        P::Next: FlowerType,
    {
        self.bound(|range| &mut range.gte, bound.encode())
    }

    /// Rows whose next field is less than `bound`.
    pub fn lt(self, bound: &P::Next) -> Self
    where
        P::Next: FlowerType,
    {
        self.bound(|range| &mut range.lt, bound.encode())
    }

    /// Rows whose next field is at most `bound`.
    pub fn lte(self, bound: &P::Next) -> Self
    where
        P::Next: FlowerType,
    {
        self.bound(|range| &mut range.lte, bound.encode())
    }

    /// Continue after the cursor an earlier page of the same range
    /// returned.
    pub fn after(mut self, cursor: &str) -> Self {
        self.after = Some(String::from(cursor));
        self
    }

    /// Walk the range in descending order.
    pub fn reverse(mut self) -> Self {
        self.reverse = true;
        self
    }

    /// The raw bounds this range sends, with at most `limit` rows.
    pub fn bounds(&self, limit: u32) -> Result<RangeBounds> {
        if let Some(failure) = &self.error {
            return Err(failure.clone());
        }
        if self.gt.is_some() && self.gte.is_some() || self.lt.is_some() && self.lte.is_some() {
            return Err(fail(
                codes::INVALID_VALUE,
                "a range bounds each side once: gt or gte, lt or lte",
            ));
        }
        Ok(RangeBounds {
            prefix: self.prefix.components()?,
            gt: self.gt.clone(),
            gte: self.gte.clone(),
            lt: self.lt.clone(),
            lte: self.lte.clone(),
            limit,
            after: self.after.clone(),
            reverse: self.reverse,
        })
    }
}

impl<K: FlowerType, V: FlowerType, F, P: Prefix<F>> IndexRange<K, V, F, P> {
    /// Read one page of at most `limit` rows, decoded.
    pub fn page(self, ctx: &mut impl ReadCtx, limit: u32) -> Result<Page<K, V>> {
        let bounds = self.bounds(limit)?;
        let collection = CollectionRef::new(self.index.module, self.index.collection);
        decode_page(ctx.range(collection, self.index.name, bounds)?)
    }
}

/// A typed read handle on a whole collection: point reads by typed key
/// and complete pages in host order.
pub struct Source<K, V> {
    module: &'static str,
    collection: &'static str,
    _types: PhantomData<fn() -> (K, V)>,
}

impl<K, V> Clone for Source<K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K, V> Copy for Source<K, V> {}

impl<K, V> core::fmt::Debug for Source<K, V> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Source")
            .field("module", &self.module)
            .field("collection", &self.collection)
            .finish()
    }
}

impl<K: FlowerType, V: FlowerType> Source<K, V> {
    /// The calling module's `collection`.
    pub const fn new(collection: &'static str) -> Self {
        Source {
            module: "",
            collection,
            _types: PhantomData,
        }
    }

    /// Another module's exported collection, by name or `$slot`.
    pub const fn in_module(self, module: &'static str) -> Self {
        Source { module, ..self }
    }

    fn reference(&self) -> CollectionRef<'static> {
        CollectionRef::new(self.module, self.collection)
    }

    /// Read one row by typed key.
    pub fn get(&self, ctx: &mut impl ReadCtx, key: &K) -> Result<Option<V>> {
        match ctx.get(self.reference(), &key.encode()?)? {
            None => Ok(None),
            Some(value) => Ok(Some(V::decode(value)?)),
        }
    }

    /// One page of at most `limit` rows in host order, after `cursor`:
    /// a complete inventory when walked to the end, rows the caller may
    /// not read left out. Cursors from other collections fail.
    pub fn page(
        &self,
        ctx: &mut impl ReadCtx,
        limit: u32,
        after: Option<&str>,
    ) -> Result<Page<K, V>> {
        decode_page(ctx.scan(self.reference(), limit, after)?)
    }
}

/// One row change a reducer folds: the row's key, its image leaving
/// the group (`old`) and its image entering it (`new`). An insert into
/// the group has only `new`, a delete or a move out only `old`, and an
/// update within the group both.
#[derive(Clone, Debug, PartialEq)]
pub struct RowChange {
    /// The row's key, decoded.
    pub key: Value,
    /// The image to take out of the accumulator.
    pub old: Option<Value>,
    /// The image to fold into it.
    pub new: Option<Value>,
}

/// What the host hands an aggregate's reducer
/// ([`Aggregation::reduce`][crate::app::Aggregation::reduce]): the
/// group, the cached accumulator and the group's row changes since it
/// was computed, in key order.
#[derive(Clone, Debug, PartialEq)]
pub struct ReduceCall {
    /// The group, as the value's `{"group": ...}` argument holds it.
    pub group: Value,
    /// The accumulator the changes apply to.
    pub previous: Value,
    /// The changes, in key order.
    pub changes: Vec<RowChange>,
}

impl ReduceCall {
    /// Decode the reducer's argument `{group, previous, changes: [{key,
    /// old?, new?}]}`.
    pub fn decode(args: Value) -> Result<Self> {
        let mut map = match args {
            Value::Object(map) => map,
            _ => return Err(fail(codes::TYPE_MISMATCH, "a reduce call is an object")),
        };
        let group = map.remove("group").unwrap_or(Value::Null);
        let previous = map.remove("previous").unwrap_or(Value::Null);
        let changes = match map.remove("changes") {
            Some(Value::Array(changes)) => changes,
            _ => {
                return Err(fail(
                    codes::TYPE_MISMATCH,
                    "a reduce call lists its changes",
                ));
            }
        };
        let mut decoded = Vec::with_capacity(changes.len());
        for change in changes {
            let mut change = match change {
                Value::Object(change) => change,
                _ => return Err(fail(codes::TYPE_MISMATCH, "a row change is an object")),
            };
            decoded.push(RowChange {
                key: change.remove("key").unwrap_or(Value::Null),
                old: change.remove("old"),
                new: change.remove("new"),
            });
        }
        Ok(ReduceCall {
            group,
            previous,
            changes: decoded,
        })
    }
}

/// A reducer's answer.
#[derive(Clone, Debug, PartialEq)]
pub enum Reduced {
    /// The accumulator with every change folded in.
    Value(Value),
    /// The accumulator cannot take a change out (a minimum, say): build
    /// the group again from its rows.
    Rebuild,
}

impl Reduced {
    /// Encode as the host reads it: `{"value": ...}` or `{"rebuild":
    /// true}`.
    pub fn encode(self) -> Value {
        let mut map = alloc::collections::BTreeMap::new();
        match self {
            Reduced::Value(value) => {
                map.insert(String::from("value"), value);
            }
            Reduced::Rebuild => {
                map.insert(String::from("rebuild"), Value::Bool(true));
            }
        }
        Value::Object(map)
    }
}

/// A per-group accumulator over one declared index of `Source`: one
/// derived value per equality group, `{"group": <group>}`. A group
/// builds by folding its rows from [`initial`][Self::initial] through
/// [`add`][Self::add] in index order. The value caches per group and
/// depends on that group's entries only, so writes to other groups leave
/// it cached. A write to its group's rows hands the cached value the
/// changes instead, in the writing transaction: [`remove`][Self::remove]
/// takes each changed row's old image out and `add` folds its new image
/// in, so the group never builds again while `remove` can invert `add`.
/// Without `remove` (its default answers `None`), or when it answers
/// `None` or fails, the group builds from its rows as before. Register
/// [`DERIVED`][Self::DERIVED] as a derived definition (retain it with
/// `Materialize::Always` to keep groups computed and maintained).
///
/// `add` and `remove` must be deterministic inverses that do not depend
/// on order: the changes come in key order, not write order. Prefer
/// integer units for sums.
pub trait TypedAggregate: Sized + 'static {
    /// The source collection's marker.
    type Source: crate::typed::TypedCollection;
    /// The index's field types, as for [`Index`].
    type Fields;
    /// The group: a full prefix of `Fields`.
    type Group: Prefix<Self::Fields, Next = Full> + FlowerType;
    /// The accumulator.
    type Accumulator: FlowerType;
    /// Stable derived name.
    const NAME: &'static str;
    /// The index the groups come from.
    const INDEX: &'static str;
    /// Rows read per range page while folding.
    const PAGE: u32 = 256;
    /// Keep accumulators across deployments while this stays the same
    /// (1 to 128 bytes, no control characters); `None` builds every
    /// group again under new code. Change it whenever `initial`, `add` or
    /// `remove` would compute differently.
    const VERSION: Option<&'static str> = None;

    /// The empty group's accumulator.
    fn initial(group: &Self::Group) -> Result<Self::Accumulator>;

    /// Fold one row in.
    fn add(
        accumulator: Self::Accumulator,
        key: &<Self::Source as crate::typed::TypedCollection>::Key,
        row: &<Self::Source as crate::typed::TypedCollection>::Value,
        group: &Self::Group,
    ) -> Result<Self::Accumulator>;

    /// Take one row out: the inverse of [`add`][Self::add], or `None`
    /// when the accumulator cannot be inverted, which builds the group
    /// again from its rows. The default cannot invert.
    fn remove(
        accumulator: Self::Accumulator,
        key: &<Self::Source as crate::typed::TypedCollection>::Key,
        row: &<Self::Source as crate::typed::TypedCollection>::Value,
        group: &Self::Group,
    ) -> Result<Option<Self::Accumulator>> {
        let _ = (accumulator, key, row, group);
        Ok(None)
    }

    /// One group's accumulator, as this module reads it: built from the
    /// rows the first time, maintained by deltas after (while the
    /// definition is materialized).
    fn get(ctx: &mut impl ReadCtx, group: &Self::Group) -> Result<Self::Accumulator> {
        let mut args = alloc::collections::BTreeMap::new();
        args.insert(String::from("group"), group.encode()?);
        Self::Accumulator::decode(ctx.get_derived("", Self::NAME, &Value::Object(args))?)
    }

    /// The derived definition to register.
    const DERIVED: crate::app::Derived =
        crate::app::Derived::new(Self::NAME, aggregate_shim::<Self>)
            .args(crate::schema::v::object(&[crate::schema::v::field(
                "group",
                <Self::Group as FlowerType>::SCHEMA,
            )]))
            .result(<Self::Accumulator as FlowerType>::SCHEMA)
            .aggregate(crate::app::Aggregation {
                collection: <Self::Source as crate::typed::TypedCollection>::NAME,
                index: Self::INDEX,
                reduce: reduce_shim::<Self>,
                version: Self::VERSION,
            });
}

/// Fold a [`ReduceCall`] through a typed aggregate's `remove` and `add`.
fn reduce_shim<T: TypedAggregate>(args: Value) -> Result<Value> {
    use crate::typed::TypedCollection;
    let call = ReduceCall::decode(args)?;
    let group = T::Group::decode(call.group)?;
    let mut accumulator = T::Accumulator::decode(call.previous)?;
    for change in call.changes {
        let key = <T::Source as TypedCollection>::Key::decode(change.key)?;
        if let Some(old) = change.old {
            let row = <T::Source as TypedCollection>::Value::decode(old)?;
            accumulator = match T::remove(accumulator, &key, &row, &group)? {
                Some(accumulator) => accumulator,
                None => return Ok(Reduced::Rebuild.encode()),
            };
        }
        if let Some(new) = change.new {
            let row = <T::Source as TypedCollection>::Value::decode(new)?;
            accumulator = T::add(accumulator, &key, &row, &group)?;
        }
    }
    Ok(Reduced::Value(accumulator.encode()?).encode())
}

fn aggregate_shim<'c, 'r, T: TypedAggregate>(
    ctx: &'c mut crate::ctx::Ctx<'c, 'r>,
    args: crate::value::Value,
) -> Result<crate::value::Value> {
    use crate::typed::TypedCollection;
    let ctx = crate::ctx::wrap_derived(ctx);
    let group = match args {
        crate::value::Value::Object(mut map) => match map.remove("group") {
            Some(group) => T::Group::decode(group)?,
            None => return Err(fail(codes::MISSING_FIELD, "aggregate needs a group")),
        },
        _ => return Err(fail(codes::TYPE_MISMATCH, "aggregate args are an object")),
    };
    let index: Index<
        <T::Source as TypedCollection>::Key,
        <T::Source as TypedCollection>::Value,
        T::Fields,
    > = Index::new(<T::Source as TypedCollection>::NAME, T::INDEX);
    let mut accumulator = T::initial(&group)?;
    let mut after: Option<String> = None;
    loop {
        let components = group.components()?;
        let mut range = index.prefix(Components(components));
        if let Some(cursor) = &after {
            range = range.after(cursor);
        }
        let page = range.page(ctx, T::PAGE)?;
        for row in &page.rows {
            accumulator = T::add(accumulator, &row.key, &row.value, &group)?;
        }
        match page.cursor {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    accumulator.encode()
}

/// Already-rendered prefix components: the aggregate's group, whatever
/// its arity.
struct Components(Vec<Value>);

impl<F> Prefix<F> for Components {
    type Next = Full;

    fn components(&self) -> Result<Vec<Value>> {
        Ok(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_render_their_components_in_order() {
        type Fields = (String, u32, bool);
        assert!(<() as Prefix<Fields>>::components(&()).unwrap().is_empty());
        let one: Vec<Value> = Prefix::<Fields>::components(&(String::from("ada"),)).unwrap();
        assert_eq!(one, alloc::vec![Value::from("ada")]);
        let two = Prefix::<Fields>::components(&(String::from("ada"), 7u32)).unwrap();
        assert_eq!(two, alloc::vec![Value::from("ada"), Value::Number(7.0)]);
        let four = Prefix::<(u8, u8, u8, u8)>::components(&(1u8, 2u8, 3u8, 4u8)).unwrap();
        assert_eq!(four.len(), 4);
    }

    static BY_OWNER: Index<String, Value, (String, u64)> = Index::new("tasks", "by_owner");

    #[test]
    fn ranges_build_typed_bounds() {
        let bounds = BY_OWNER
            .prefix((String::from("ada"),))
            .gte(&5)
            .lt(&9)
            .after("cursor")
            .reverse()
            .bounds(10)
            .unwrap();
        assert_eq!(bounds.prefix, alloc::vec![Value::from("ada")]);
        assert_eq!(bounds.gte, Some(Value::Number(5.0)));
        assert_eq!(bounds.lt, Some(Value::Number(9.0)));
        assert_eq!(bounds.after.as_deref(), Some("cursor"));
        assert!(bounds.reverse);
        assert_eq!(bounds.limit, 10);
        // The whole index, bounded on the first field.
        let all = BY_OWNER.query().gt(&String::from("a")).bounds(1).unwrap();
        assert!(all.prefix.is_empty());
        assert_eq!(all.gt, Some(Value::from("a")));
        // One side bounded twice is refused.
        let doubled = BY_OWNER.query().gt(&String::new()).gte(&String::new());
        assert_eq!(doubled.bounds(1).unwrap_err().code, codes::INVALID_VALUE);
        // A bound that cannot render fails when the range is used.
        let nan: Index<String, Value, (f64,)> = Index::new("t", "i");
        let failure = nan.query().gt(&f64::NAN).bounds(1).unwrap_err();
        assert_eq!(failure.code, codes::INVALID_VALUE);
    }

    fn object(pairs: &[(&str, Value)]) -> Value {
        Value::Object(
            pairs
                .iter()
                .map(|(key, value)| (String::from(*key), value.clone()))
                .collect(),
        )
    }

    #[test]
    fn reduce_calls_decode_and_answers_encode() {
        let text = |text: &str| Value::String(String::from(text));
        let call = ReduceCall::decode(object(&[
            ("group", Value::Array(alloc::vec![text("north")])),
            ("previous", Value::Number(3.0)),
            (
                "changes",
                Value::Array(alloc::vec![
                    object(&[("key", text("a")), ("new", Value::Number(1.0))]),
                    object(&[("key", text("b")), ("old", Value::Number(2.0))]),
                ]),
            ),
        ]))
        .unwrap();
        assert_eq!(call.previous, Value::Number(3.0));
        assert_eq!(
            call.changes,
            alloc::vec![
                RowChange {
                    key: text("a"),
                    old: None,
                    new: Some(Value::Number(1.0)),
                },
                RowChange {
                    key: text("b"),
                    old: Some(Value::Number(2.0)),
                    new: None,
                },
            ]
        );
        assert!(ReduceCall::decode(Value::Null).is_err());
        assert!(ReduceCall::decode(object(&[("changes", Value::Null)])).is_err());
        assert_eq!(
            Reduced::Value(Value::Number(4.0)).encode(),
            object(&[("value", Value::Number(4.0))])
        );
        assert_eq!(
            Reduced::Rebuild.encode(),
            object(&[("rebuild", Value::Bool(true))])
        );
    }
}
