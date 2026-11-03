//! Concepts › Queries and mutations.

use alloc::string::String;

use flower_sdk::typed::decode_outcome;
use flower_sdk::{
    App, CallError, CallOutcome, Consistency, Failure, FlowerError, FlowerType, MethodCtx,
    MutationCtx, Op, QueryCtx, Result, Value, collection, mutation, query,
};

#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Account {
    pub balance: u64,
}

#[collection(name = "accounts", key = String, value = Account)]
pub struct Accounts;

// ANCHOR: query
/// An infallible query: the return type is the result contract.
#[query(http)]
pub fn greet(ctx: &mut QueryCtx<'_, '_>, name: String) -> String {
    let _ = ctx;
    alloc::format!("Hello, {name}!")
}

/// A fallible query returning the SDK's own `Result`, with its own
/// deadline.
#[query(http, timeout = "500ms")]
pub fn balance(ctx: &mut QueryCtx<'_, '_>, account: String) -> Result<Option<u64>> {
    let account = Accounts::get(ctx, &account)?;
    Ok(account.map(|account| account.balance))
}

/// The same callback under another name, served from any replica's own
/// state at most two seconds stale.
pub static BALANCE_NEARBY: Op<BalanceArgs, Option<u64>> = BALANCE
    .named("balance_nearby")
    .consistency(Consistency::ReplicaLocalWithin(2_000));
// ANCHOR_END: query

// ANCHOR: errors
/// The failures `transfer` declares. Codes discriminate the variants; a
/// payload travels as the failure's `details`.
#[derive(FlowerError, Clone, Debug, PartialEq)]
pub enum TransferError {
    #[flower(code = "ACCOUNT_NOT_FOUND", message = "no account has this ID")]
    NotFound(String),
    #[flower(code = "INSUFFICIENT_FUNDS", message = "the balance is too low")]
    InsufficientFunds(u64),
    #[flower(code = "ACCOUNT_LOCKED", retryable)]
    Locked,
    /// Every other failure (a codec, the host): `?` converts into it.
    #[flower(other)]
    Host(Failure),
}

#[mutation(http)]
pub fn transfer(
    ctx: &mut MutationCtx<'_, '_>,
    from: String,
    to: String,
    amount: u64,
) -> core::result::Result<u64, TransferError> {
    let mut source = Accounts::get(ctx, &from)?.ok_or(TransferError::NotFound(from.clone()))?;
    let mut target = Accounts::get(ctx, &to)?.ok_or(TransferError::NotFound(to.clone()))?;
    if source.balance < amount {
        return Err(TransferError::InsufficientFunds(source.balance));
    }
    source.balance -= amount;
    target.balance += amount;
    Accounts::set(ctx, &from, &source)?;
    Accounts::set(ctx, &to, &target)?;
    Ok(source.balance)
}
// ANCHOR_END: errors

// ANCHOR: app
pub static APP: App = App::new(&[&Accounts, &GREET, &BALANCE, &BALANCE_NEARBY, &TRANSFER]);
// ANCHOR_END: app

// ANCHOR: calls
/// Pay through the `bank` module's exported `transfer`, turning its
/// declared failures into an answer instead of failing the whole call.
/// `TransferArgs` is the argument struct `#[mutation]` derived for
/// `transfer`.
#[mutation]
pub fn pay(ctx: &mut MutationCtx<'_, '_>, from: String, amount: u64) -> Result<String> {
    let args = TransferArgs {
        from,
        to: String::from("merchant"),
        amount,
    };
    let outcome = match ctx.try_mutation("bank", "transfer", &args.encode()?) {
        Ok(outcome) => decode_outcome::<u64, TransferError>(outcome)?,
        Err(CallError::Host(failure)) => return Err(failure),
    };
    Ok(match outcome {
        CallOutcome::Success(left) => alloc::format!("paid; {left} left"),
        CallOutcome::Failure(TransferError::InsufficientFunds(balance)) => {
            alloc::format!("declined: balance {balance}")
        }
        CallOutcome::Failure(other) => alloc::format!("declined: {other:?}"),
    })
}

/// Read another module's exported query. A failure here fails the
/// whole invocation, even if the caller ignores the `Err`.
#[query]
pub fn merchant_balance(ctx: &mut QueryCtx<'_, '_>) -> Result<Value> {
    let mut args = alloc::collections::BTreeMap::new();
    args.insert(
        String::from("account"),
        Value::String(String::from("merchant")),
    );
    ctx.call("bank", "balance", &Value::Object(args))
}
// ANCHOR_END: calls

/// Keeps `MethodCtx` in scope for the `ctx.call` above.
pub fn _uses_method_ctx(ctx: &mut impl MethodCtx) -> Result<Option<flower_sdk::Principal>> {
    ctx.principal()
}
