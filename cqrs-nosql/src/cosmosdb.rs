mod builder;
mod event;
mod snapshot;

pub use builder::{Builder, BuilderError};
pub use event::EventStore;
pub use snapshot::SnapshotStore;

use crate::{BoxErr, prune::Prune};
use azure_data_cosmos::{
    CosmosClient, CosmosError, clients::ContainerClient, feed::FeedScope, models::TransactionalBatch,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use cqrs::snapshot::Retention;
use futures::StreamExt;
use serde::Deserialize;
use std::{
    error::Error,
    time::{Duration, SystemTime},
};
use thiserror::Error as ThisError;

cfg_select! {
    feature = "migrate" => {
        mod migration;
        pub use migration::{EventStoreMigration, SnapshotStoreMigration};
    }
    _ => {}
}

// a transactional batch is limited to 100 operations, all of which must target the same partition key. an aggregate is
// a partition, which makes a batch the natural unit of atomicity for the events appended by a single command
pub(crate) const MAX_BATCH_SIZE: usize = 100;

pub(crate) const CONFLICT: u16 = 409;
pub(crate) const NOT_FOUND: u16 = 404;

// an operation that would have succeeded, but was rolled back because another operation
// in the same batch failed. it never identifies the cause of a failure
const FAILED_DEPENDENCY: u16 = 424;

/// Represents the error of a failed transactional batch.
#[derive(ThisError, Debug)]
#[error("the transactional batch failed with status code {0}")]
pub(crate) struct BatchError(pub u16);

/// Represents a reference to an Azure Cosmos DB container.
///
/// # Remarks
///
/// The [container client](ContainerClient) is resolved on demand rather than up front because
/// resolution is asynchronous and requires the container to already exist; for example, after a
/// migration has been run. The underlying client caches the container metadata it resolves.
#[derive(Clone)]
pub(crate) struct Container {
    client: CosmosClient,
    database: String,
    container: String,
}

impl Container {
    /// Initializes a new [Container].
    ///
    /// # Arguments
    ///
    /// * `client` - the underlying [client](CosmosClient)
    /// * `database` - the database identifier
    /// * `container` - the container identifier
    pub fn new(client: CosmosClient, database: String, container: String) -> Self {
        Self {
            client,
            database,
            container,
        }
    }

    /// Resolves the [container client](ContainerClient) the container refers to.
    pub async fn resolve(&self) -> Result<ContainerClient, CosmosError> {
        self.client
            .database_client(&self.database)
            .container_client(&self.container)
            .await
    }
}

/// Encodes a [version](cqrs::Version) sort key as an item identifier.
///
/// # Arguments
///
/// * `sort_key` - the version sort key to encode
///
/// # Remarks
///
/// An item identifier must be a string that is unique within a logical partition. The value is
/// padded so that identifiers also sort lexicographically, which makes them easier to read in
/// tools such as the Data Explorer. Ordering is always applied to the numeric version.
#[inline]
pub(crate) fn key(sort_key: u32) -> String {
    format!("{sort_key:010}")
}

/// Encodes message content for storage.
///
/// # Arguments
///
/// * `content` - the content to encode
///
/// # Remarks
///
/// An item is JSON, which has no representation for binary content, so the encoded message
/// is stored as a Base64 string.
#[inline]
pub(crate) fn encode(content: &[u8]) -> String {
    STANDARD.encode(content)
}

/// Decodes stored message content.
///
/// # Arguments
///
/// * `content` - the content to decode
#[inline]
pub(crate) fn decode(content: &str) -> Result<Vec<u8>, Box<dyn Error + Send>> {
    STANDARD.decode(content).box_err()
}

/// Executes a transactional batch and returns the status code of the operation that failed, if any.
///
/// # Arguments
///
/// * `client` - the [container client](ContainerClient) to execute the batch with
/// * `batch` - the [batch](TransactionalBatch) to execute
///
/// # Remarks
///
/// A batch that fails still succeeds as a request. The outcome of each operation is reported by
/// the corresponding result in the response.
pub(crate) async fn execute(client: &ContainerClient, batch: TransactionalBatch) -> Result<Option<u16>, CosmosError> {
    let results = client.execute_transactional_batch(batch, None).await?.into_model()?;
    let mut status = None;

    for result in results.results() {
        if result.is_success() {
            continue;
        }

        let code = result.status_code();

        if code != FAILED_DEPENDENCY {
            return Ok(Some(code));
        }

        status = Some(code);
    }

    Ok(status)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Key {
    id: String,

    #[serde(default)]
    taken_on: u64,
}

// there is no efficient way to know how many items there are and delete them in an atomic manner. a transaction only
// allows 100 items, but there could be more. this operation is idempotent. if a failure occurs, it is transient
// (expect bugs) and can safely be retried until it succeeds.
async fn delete_all(
    container: &Container,
    id: String,
    retention: Option<&Retention>,
    now: SystemTime,
) -> Result<(), Box<dyn Error + Send>> {
    let client = container.resolve().await.box_err()?;
    let mut items = client
        .query_items::<Key>(
            "SELECT c.id, c.takenOn FROM c ORDER BY c.version DESC",
            FeedScope::partition(id.clone()),
            None,
        )
        .await
        .box_err()?;
    let mut prune = Prune::new(retention, now);
    let mut batch = TransactionalBatch::new(id.clone());
    let mut count = 0usize;

    while let Some(item) = items.next().await {
        let key = item.box_err()?;

        if !prune.expired(Duration::from_secs(key.taken_on)) {
            continue;
        }

        batch = batch.delete_item(key.id, None);
        count += 1;

        if count == MAX_BATCH_SIZE {
            if let Some(status) = execute(&client, batch).await.box_err()? {
                return Err(Box::new(BatchError(status)) as Box<dyn Error + Send>);
            }

            batch = TransactionalBatch::new(id.clone());
            count = 0;
        }
    }

    if count > 0
        && let Some(status) = execute(&client, batch).await.box_err()?
    {
        return Err(Box::new(BatchError(status)) as Box<dyn Error + Send>);
    }

    Ok(())
}
