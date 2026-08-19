mod builder;
mod event;
mod snapshot;

pub use builder::{Builder, BuilderError};
pub use event::EventStore;
pub use snapshot::SnapshotStore;

use crate::{
    BoxErr,
    bound::{greater_than, less_than},
    prune::Prune,
};
use aws_sdk_dynamodb::{
    Client,
    types::{
        AttributeValue::{self, S},
        DeleteRequest, WriteRequest,
    },
};
use cqrs::snapshot::Retention;
use std::{
    collections::HashMap,
    error::Error,
    str::FromStr,
    thread,
    time::{Duration, SystemTime},
};

cfg_select! {
    feature = "migrate" => {
        mod migration;
        pub use migration::{EventStoreMigration, SnapshotStoreMigration};
    }
    _ => {}
}

// REMARKS: a transaction is limited to 100 items, which is the number of events that can be saved atomically
// REF: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
pub(crate) const MAX_TRANSACTION_SIZE: usize = 100;

// a batch write is limited to 25 items, which is unrelated to the size of a transaction
// REF: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
const MAX_WRITE_SIZE: usize = 25;

fn coerce<T: FromStr + Default>(
    name: &str,
    attributes: &HashMap<String, AttributeValue>,
    select: fn(&AttributeValue) -> Result<&String, &AttributeValue>,
) -> T {
    if let Some(attribute) = attributes.get(name)
        && let Ok(value) = select(attribute)
    {
        return value.parse::<T>().unwrap_or_default();
    }

    T::default()
}

// REMARKS: there is no efficient way to know how many items there are and delete them in an atomic manner. a
// transaction only allows 100 items, but there could be more. this operation is idempotent. if a failure occurs, it is
// transient (expect bugs) and can safely be retried until it succeeds.
async fn delete_all(
    client: &Client,
    table: &str,
    id: String,
    retention: Option<&Retention>,
    now: SystemTime,
) -> Result<(), Box<dyn Error + Send>> {
    let query = client
        .query()
        .table_name(table)
        .scan_index_forward(false)
        .key_condition_expression("id = :id")
        .expression_attribute_values(":id", S(id))
        .projection_expression("id, version, takenOn")
        .into_paginator();
    let mut keys = query.items().send();
    let mut batch = Vec::with_capacity(MAX_WRITE_SIZE);
    let mut prune = Prune::new(retention, now);
    let mut count = 0usize;

    while let Some(key) = keys.next().await {
        let mut attributes = key.box_err()?;

        if !prune.expired(Duration::from_secs(coerce(
            "takenOn",
            &attributes,
            AttributeValue::as_n,
        ))) {
            continue;
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

        if batch.len() == MAX_WRITE_SIZE {
            client
                .batch_write_item()
                .request_items(table, batch)
                .send()
                .await
                .box_err()?;
            batch = Vec::with_capacity(MAX_WRITE_SIZE);
            count += 1;

            // yield so we don't get throttled; there's no direct dependency on tokio for the async variant
            // REF: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TroubleshootingThrottlingOnDemand.html
            if (count * MAX_WRITE_SIZE).is_multiple_of(750) {
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
