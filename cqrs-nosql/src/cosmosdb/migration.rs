use async_trait::async_trait;
use azure_data_cosmos::{
    CosmosClient,
    models::{ContainerProperties, PartitionKeyDefinition},
};
use cqrs::StoreMigration;
use std::error::Error;

// an aggregate is a partition, which keeps the events or snapshots it owns collocated
// and makes them eligible for a transactional batch
const PARTITION_KEY: &str = "/aggregateId";

// a migration is idempotent. a database or container that already exists reports a conflict, which means the migration
// has already been run
async fn migrate(client: &CosmosClient, database: &str, container: &str) -> Result<(), Box<dyn Error + 'static>> {
    if let Err(error) = client.create_database(database, None).await
        && !error.status().is_conflict()
    {
        return Err(Box::new(error));
    }

    let properties = ContainerProperties::new(container.to_string(), PartitionKeyDefinition::from(PARTITION_KEY));

    if let Err(error) = client
        .database_client(database)
        .create_container(properties, None)
        .await
        && !error.status().is_conflict()
    {
        return Err(Box::new(error));
    }

    Ok(())
}

/// Represents the migrations for an Azure Cosmos DB [event store](super::EventStore).
pub struct EventStoreMigration {
    client: CosmosClient,
    database: String,
    container: String,
}

impl EventStoreMigration {
    /// Initializes a new [EventStoreMigration].
    ///
    /// # Arguments
    ///
    /// * `client` - the [client](CosmosClient) to perform the migration with
    /// * `database` - the identifier of the database to migrate
    /// * `container` - the identifier of the container to migrate
    pub fn new<D: Into<String>, C: Into<String>>(client: CosmosClient, database: D, container: C) -> Self {
        Self {
            client,
            database: database.into(),
            container: container.into(),
        }
    }
}

#[async_trait]
impl StoreMigration for EventStoreMigration {
    async fn run(&self) -> Result<(), Box<dyn Error + 'static>> {
        migrate(&self.client, &self.database, &self.container).await
    }
}

/// Represents the migrations for an Azure Cosmos DB [snapshot store](super::SnapshotStore).
pub struct SnapshotStoreMigration {
    client: CosmosClient,
    database: String,
    container: String,
}

impl SnapshotStoreMigration {
    /// Initializes a new [SnapshotStoreMigration].
    ///
    /// # Arguments
    ///
    /// * `client` - the [client](CosmosClient) to perform the migration with
    /// * `database` - the identifier of the database to migrate
    /// * `container` - the identifier of the container to migrate
    pub fn new<D: Into<String>, C: Into<String>>(client: CosmosClient, database: D, container: C) -> Self {
        Self {
            client,
            database: database.into(),
            container: container.into(),
        }
    }
}

#[async_trait]
impl StoreMigration for SnapshotStoreMigration {
    async fn run(&self) -> Result<(), Box<dyn Error + 'static>> {
        migrate(&self.client, &self.database, &self.container).await
    }
}
