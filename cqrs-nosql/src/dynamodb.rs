mod builder;
mod event;
mod snapshot;

pub use builder::Builder;
pub use event::EventStore;
pub use snapshot::SnapshotStore;

use aws_sdk_dynamodb::types::AttributeValue;
use cfg_if::cfg_if;
use std::{
    collections::HashMap,
    ops::Bound::{self, Excluded, Included},
    str::FromStr,
};

cfg_if! {
    if #[cfg(feature = "migrate")] {
        mod migration;
        pub use migration::{EventStoreMigration, SnapshotStoreMigration};
    }
}

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
fn greater_than<T: Copy>(bound: &Bound<T>) -> Option<(T, &'static str)> {
    op(bound, ">=", ">")
}
#[inline]
fn less_than<T: Copy>(bound: &Bound<T>) -> Option<(T, &'static str)> {
    op(bound, "<=", "<")
}

fn coerce<T: FromStr + Default>(
    name: &str,
    attributes: &HashMap<String, AttributeValue>,
    select: fn(&AttributeValue) -> Result<&String, &AttributeValue>,
) -> T {
    if let Some(attribute) = attributes.get(name) {
        if let Ok(value) = select(attribute) {
            return value.parse::<T>().unwrap_or_default();
        }
    }

    T::default()
}
