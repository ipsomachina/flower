//! Concepts › Queues and timers.

use alloc::format;
use alloc::string::String;

use core::time::Duration;

use flower_sdk::queue::{EnqueueOptions, Job, Queue, TypedQueue};
use flower_sdk::timer::Timers;
use flower_sdk::{App, FlowerType, MutationCtx, QueryCtx, Result, collection, mutation, query};

// ANCHOR: queue
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Email {
    pub to: String,
    pub subject: String,
}

#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Sent {
    pub message_id: String,
}

/// The queue, its contract and its handle in one declaration. Listed
/// among the app's components, it declares the queue: the host refuses
/// any other payload, from guests and native workers alike, and checks
/// kept results too.
pub static MAIL: TypedQueue<Email, Sent> = TypedQueue::new("mail");

/// Queue an invitation in the tenant's scope. The job exists exactly
/// when this mutation commits; naming it makes a second invitation to
/// the same address fail `JOB_EXISTS` instead of queueing twice.
#[mutation(http)]
pub fn invite(ctx: &mut MutationCtx<'_, '_>, tenant: String, to: String) -> Result<String> {
    let email = Email {
        to: to.clone(),
        subject: String::from("You are invited"),
    };
    let options = EnqueueOptions::default()
        .scope(&tenant)
        .named(&format!("invite:{to}"))
        .max_attempts(10);
    MAIL.enqueue(ctx, &email, &options)
}

/// Whether an invitation went out, from the result the worker kept.
#[query(http)]
pub fn invitation_sent(ctx: &mut QueryCtx<'_, '_>, tenant: String, to: String) -> Result<bool> {
    let name = format!("invite:{to}");
    let job = Job::Named {
        scope: &tenant,
        name: &name,
    };
    Ok(MAIL.result(ctx, job)?.is_some())
}
// ANCHOR_END: queue

/// Keeps `Queue` in scope for the page's prose about raw handles.
pub fn _raw_stats(ctx: &mut QueryCtx<'_, '_>) -> Result<u64> {
    Ok(Queue::stats(ctx, "mail", None)?.pending)
}

#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Trial {
    pub active: bool,
}

#[collection(name = "trials", key = String, value = Trial)]
pub struct Trials;

// ANCHOR: timer
const TRIAL: Duration = Duration::from_secs(14 * 24 * 60 * 60);

/// Timer IDs are any text up to 128 bytes, without control characters.
fn trial_timer(account: &str) -> String {
    format!("trial:{account}")
}

#[mutation(http)]
pub fn start_trial(ctx: &mut MutationCtx<'_, '_>, account: String) -> Result<()> {
    Trials::set(ctx, &account, &Trial { active: true })?;
    // The handler is the mutation's descriptor and its arguments its own
    // argument struct, checked now. Scheduling an existing ID replaces
    // that timer.
    let args = EndTrialArgs {
        account: account.clone(),
    };
    Timers::after(ctx, &trial_timer(&account), TRIAL, &END_TRIAL, &args)
}

/// The timer's handler: an ordinary mutation of this module. Without
/// `http`, nothing outside the module can call it.
#[mutation]
pub fn end_trial(ctx: &mut MutationCtx<'_, '_>, account: String) -> Result<()> {
    Trials::set(ctx, &account, &Trial { active: false })
}

#[mutation(http)]
pub fn convert(ctx: &mut MutationCtx<'_, '_>, account: String) -> Result<bool> {
    Trials::delete(ctx, &account)?;
    Timers::cancel(ctx, &trial_timer(&account))
}
// ANCHOR_END: timer

// ANCHOR: app
pub static APP: App = App::new(&[
    &MAIL,
    &Trials,
    &INVITE,
    &INVITATION_SENT,
    &START_TRIAL,
    &END_TRIAL,
    &CONVERT,
]);
// ANCHOR_END: app
