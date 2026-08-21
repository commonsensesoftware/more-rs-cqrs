use std::ops::Bound::{self, Excluded, Included};

fn op<T: Copy>(bound: &Bound<T>, op1: &'static str, op2: &'static str) -> Option<(T, &'static str)> {
    match bound {
        Included(value) => Some((*value, op1)),
        Excluded(value) => Some((*value, op2)),
        _ => None,
    }
}

/// Maps a lower [bound](Bound) to its value and comparison operator, if any.
pub(crate) fn greater_than<T: Copy>(bound: &Bound<T>) -> Option<(T, &'static str)> {
    op(bound, ">=", ">")
}

/// Maps an upper [bound](Bound) to its value and comparison operator, if any.
pub(crate) fn less_than<T: Copy>(bound: &Bound<T>) -> Option<(T, &'static str)> {
    op(bound, "<=", "<")
}
