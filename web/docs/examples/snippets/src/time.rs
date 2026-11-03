//! Concepts › Time.

use alloc::string::String;

use flower_sdk::{FlowerType, QueryCtx, ReadCtx, Result, collection, query};

#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Coupon {
    pub percent: u64,
    /// Epoch milliseconds.
    pub expires_at: f64,
}

#[collection(name = "coupons", key = String, value = Coupon)]
pub struct Coupons;

// ANCHOR: changes-at
/// The discount a coupon gives right now. The answer can change at the
/// coupon's expiry without any write, so the query says when: caches
/// and watches hold the result until then, and re-evaluate at that
/// instant.
#[query(http)]
pub fn discount(ctx: &mut QueryCtx<'_, '_>, code: String) -> Result<u64> {
    let Some(coupon) = Coupons::get(ctx, &code)? else {
        return Ok(0);
    };
    let now = ctx.clock()?;
    if now >= coupon.expires_at {
        return Ok(0);
    }
    ctx.changes_at(Some(coupon.expires_at))?;
    Ok(coupon.percent)
}
// ANCHOR_END: changes-at

// ANCHOR: now
/// Seconds left on a coupon. This changes every instant, so it reads
/// `now()`: the result is never cached and a watch re-evaluates it
/// on every poll.
#[query(http)]
pub fn seconds_left(ctx: &mut QueryCtx<'_, '_>, code: String) -> Result<f64> {
    let Some(coupon) = Coupons::get(ctx, &code)? else {
        return Ok(0.0);
    };
    let now = ctx.now()?;
    Ok(((coupon.expires_at - now) / 1000.0).max(0.0).floor())
}
// ANCHOR_END: now
