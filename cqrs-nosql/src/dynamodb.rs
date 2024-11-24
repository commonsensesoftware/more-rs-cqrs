mod builder;
mod event;
mod snapshot;

pub use builder::Builder;
pub use event::EventStore;
pub use snapshot::SnapshotStore;

use crate::BoxErr;
use aws_sdk_dynamodb::{
    types::{
        AttributeValue::{self, S},
        DeleteRequest, WriteRequest,
    },
    Client,
};
use cfg_if::cfg_if;
use cqrs::snapshot::Retention;
use std::{
    collections::HashMap,
    error::Error,
    ops::Bound::{self, Excluded, Included},
    str::FromStr,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
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

// REMARKS: there is no efficient way to know how many items there are and delete them in an atomic
// manner. a transaction only allows 100 items, but there could be more. this operation is idempotent.
// if a failure occurs, it is transient (expect bugs) and can safely be retried until it succeeds.
async fn delete_all(
    client: &Client,
    table: &str,
    id: String,
    retention: Option<&Retention>,
) -> Result<(), Box<dyn Error + Send>> {
    const MAX_BATCH_SIZE: usize = 25;

    let query = client
        .query()
        .table_name(table)
        .scan_index_forward(false)
        .key_condition_expression("id = :id")
        .expression_attribute_values(":id", S(id))
        .projection_expression("id, version, takenOn")
        .into_paginator();
    let mut keys = query.items().send();
    let mut batch = Vec::with_capacity(MAX_BATCH_SIZE);
    let mut kept = 0u8;
    let mut count = 0usize;

    while let Some(key) = keys.next().await {
        let mut attributes = key.box_err()?;

        if let Some(retention) = retention {
            if let Some(age) = retention.age {
                let age = (SystemTime::now() - age)
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let taken_on = attributes
                    .remove("takenOn")
                    .unwrap()
                    .as_n()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();

                if age >= taken_on {
                    if let Some(keep) = retention.count {
                        if kept < keep {
                            kept += 1;
                            continue;
                        }
                    } else {
                        continue;
                    }
                }
            } else if let Some(keep) = retention.count {
                if kept < keep {
                    kept += 1;
                    continue;
                }
            }
        }

        batch.push(
            WriteRequest::builder()
                .delete_request(
                    DeleteRequest::builder()
                        .key("id", attributes.remove("id").unwrap())
                        .key("version", attributes.remove("version").unwrap())
                        .build()
                        .unwrap(),
                )
                .build(),
        );

        if batch.len() == MAX_BATCH_SIZE {
            client
                .batch_write_item()
                .request_items(table, batch)
                .send()
                .await
                .box_err()?;
            batch = Vec::with_capacity(MAX_BATCH_SIZE);
            count += 1;

            // add an artificial yield so we don't get throttled
            // no there is no direct dependency on tokio to use the async variant
            // REF: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TroubleshootingThrottlingOnDemand.html
            if (count * MAX_BATCH_SIZE) % 750 == 0 {
                thread::sleep(Duration::from_secs(1));
            }
        }
    }

    if !batch.is_empty() {
        client
            .batch_write_item()
            .request_items(table, batch)
            .send()
            .await
            .box_err()?;
    }

    Ok(())
}
