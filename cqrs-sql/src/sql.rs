pub(crate) mod command;
mod ident;
mod row;

pub use ident::{Ident, IdentPart};
pub(crate) use row::{Context, IntoRows, Row};

use std::ops::Bound::{self, Excluded, Included};

#[inline]
fn op<T: Copy>(
    bound: &Bound<T>,
    op1: &'static str,
    op2: &'static str,
) -> Option<(T, &'static str)> {
    match bound {
        Included(value) => Some((*value, op1)),
        Excluded(value) => Some((*value, op2)),
        _ => None,
    }
}

#[inline]
pub(crate) fn greater_than<T: Copy>(bound: &Bound<T>) -> Option<(T, &'static str)> {
    op(bound, ">=", ">")
}
#[inline]
pub(crate) fn less_than<T: Copy>(bound: &Bound<T>) -> Option<(T, &'static str)> {
    op(bound, "<=", "<")
}
