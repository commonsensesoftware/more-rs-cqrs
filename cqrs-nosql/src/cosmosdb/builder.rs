use self::BuilderError::*;
use super::{Container, EventStore, SnapshotStore};
use azure_data_cosmos::CosmosClient;
use cqrs::{
    Clock, Concurrency, Mask, WallClock,
    event::{Delete, Event, StoreOptions as EventStoreOptions},
    message::{Message, Transcoder},
    snapshot::{Snapshot, StoreOptions as SnapshotStoreOptions},
};
use std::sync::Arc;
use thiserror::Error;

type DynSnapshotStore<ID> = dyn cqrs::snapshot::Store<ID>;

/// Represents the possible Azure Cosmos DB store builder errors.
#[derive(Error, Debug, PartialEq, Eq)]
pub enum BuilderError {
    /// Indicates the client is missing because it has not been configured.
    ///
    /// # Remarks
    ///
    /// Unlike other storage providers, there is no ambient configuration that can be used to create an Azure Cosmos DB
    /// [client](CosmosClient); an endpoint and credential are always required. A [client](CosmosClient) is also created
    /// asynchronously, which a store cannot do on behalf of a consumer while it is being built.
    #[error("a client has not been configured")]
    MissingClient,

    /// Indicates the target database is missing because it has not been configured.
    #[error("a database has not been configured")]
    MissingDatabase,

    /// Indicates the target container is missing because it has not been configured.
    #[error("a container has not been configured")]
    MissingContainer,
}

/// Represents builder for Azure Cosmos DB stores.
pub struct Builder<ID, M: Message + ?Sized> {
    database: Option<String>,
    container: Option<String>,
    client: Option<CosmosClient>,
    concurrency: Concurrency,
    delete: Delete,
    mask: Option<Arc<dyn Mask>>,
    clock: Option<Arc<dyn Clock>>,
    transcoder: Option<Arc<Transcoder<M>>>,
    snapshots: Option<Arc<DynSnapshotStore<ID>>>,
}

impl<ID, M: Message + ?Sized> Default for Builder<ID, M> {
    fn default() -> Self {
        Self {
            database: Default::default(),
            container: Default::default(),
            client: Default::default(),
            concurrency: Default::default(),
            delete: Default::default(),
            mask: Default::default(),
            clock: Default::default(),
            transcoder: Default::default(),
            snapshots: Default::default(),
        }
    }
}

impl<ID, M: Message + ?Sized> Builder<ID, M> {
    /// Configures the identifier of the database containing the store.
    ///
    /// # Arguments
    ///
    /// * `value` - the identifier of the database
    pub fn database<V: Into<String>>(mut self, value: V) -> Self {
        self.database = Some(value.into());
        self
    }

    /// Configures the identifier of the container representing the store.
    ///
    /// # Arguments
    ///
    /// * `value` - the identifier of the storage container
    pub fn container<V: Into<String>>(mut self, value: V) -> Self {
        self.container = Some(value.into());
        self
    }

    /// Configures the client to use.
    ///
    /// # Arguments
    ///
    /// * `value` - the underlying [client](CosmosClient)
    pub fn client<V: Into<CosmosClient>>(mut self, value: V) -> Self {
        self.client = Some(value.into());
        self
    }

    /// Configures the mask associated with the store.
    ///
    /// # Arguments
    ///
    /// * `value` - the [mask](Mask) used to obfuscate [versions](cqrs::Version)
    pub fn mask<V: Into<Arc<dyn Mask>>>(mut self, value: V) -> Self {
        self.mask = Some(value.into());
        self
    }

    /// Configures the clock associated with the store.
    ///
    /// # Arguments
    ///
    /// * `value` - the associated [clock](Clock)
    pub fn clock<V: Into<Arc<dyn Clock>>>(mut self, value: V) -> Self {
        self.clock = Some(value.into());
        self
    }

    /// Configures the transcoder used to encode and decode store messages.
    ///
    /// # Arguments
    ///
    /// * `value` - the associated [transcoder](Transcoder)
    pub fn transcoder<V: Into<Arc<Transcoder<M>>>>(mut self, value: V) -> Self {
        self.transcoder = Some(value.into());
        self
    }

    fn resolve_container(&mut self, suffix: &str) -> Result<Container, BuilderError> {
        let client = self.client.take().ok_or(MissingClient)?;
        let database = self.database.take().ok_or(MissingDatabase)?;
        let container = self.container.take().ok_or(MissingContainer)?;

        Ok(Container::new(client, database, format!("{container}{suffix}")))
    }
}

impl<ID> Builder<ID, dyn Event> {
    /// Enforces concurrency, which not enforced by default.
    pub fn enforce_concurrency(mut self) -> Self {
        self.concurrency = Concurrency::Enforced;
        self
    }

    /// Configures the store to support deletes.
    pub fn with_deletes(mut self) -> Self {
        self.delete = Delete::Supported;
        self
    }

    /// Configures the snapshots associated with the store.
    ///
    /// # Arguments
    ///
    /// * `value` - the associated [snapshot store](cqrs::snapshot::Store)
    pub fn snapshots<V: Into<Arc<DynSnapshotStore<ID>>>>(mut self, value: V) -> Self {
        self.snapshots = Some(value.into());
        self
    }

    /// Builds and returns a new [event store](EventStore).
    pub fn build(mut self) -> Result<EventStore<ID>, BuilderError> {
        let container = self.resolve_container("_Events")?;
        let options = EventStoreOptions::<ID>::new(
            self.concurrency,
            self.delete,
            self.mask,
            self.clock.unwrap_or_else(|| Arc::new(WallClock::new())),
            self.transcoder.unwrap_or_default(),
            self.snapshots,
        );

        Ok(EventStore::new(container, options))
    }
}

impl<ID> Builder<ID, dyn Snapshot> {
    /// Builds and returns a new [snapshot store](SnapshotStore).
    pub fn build(mut self) -> Result<SnapshotStore<ID>, BuilderError> {
        let container = self.resolve_container("_Snapshots")?;
        let options = SnapshotStoreOptions::new(
            self.mask,
            self.clock.unwrap_or_else(|| Arc::new(WallClock::new())),
            self.transcoder.unwrap_or_default(),
        );

        Ok(SnapshotStore::new(container, options))
    }
}
