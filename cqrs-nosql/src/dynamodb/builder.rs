use self::BuilderError::*;
use super::{EventStore, SnapshotStore};
use aws_config::SdkConfig;
use aws_sdk_dynamodb::Client;
use cqrs::{
    Clock, Concurrency, Mask, WallClock,
    event::{Delete, Event, StoreOptions as EventStoreOptions},
    message::{Message, Transcoder},
    snapshot::{Snapshot, StoreOptions as SnapshotStoreOptions},
};
use futures::executor;
use std::sync::Arc;
use thiserror::Error;

type DynSnapshotStore<ID> = dyn cqrs::snapshot::Store<ID>;

/// Represents the possible Amazon DynamoDB store builder errors.
#[derive(Error, Debug, PartialEq, Eq)]
pub enum BuilderError {
    /// Indicates the target table is missing because it has not been configured.
    #[error("a table has not been configured")]
    MissingTable,
}

/// Represents builder for Amazon DynamoDB stores.
pub struct Builder<ID, M: Message + ?Sized> {
    table: Option<&'static str>,
    config: Option<SdkConfig>,
    client: Option<Client>,
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
            table: Default::default(),
            config: Default::default(),
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
    /// Configures the identifier of the table representing the store.
    ///
    /// # Arguments
    ///
    /// * `value` - the name of the storage table
    pub fn table(mut self, value: &'static str) -> Self {
        self.table = Some(value);
        self
    }

    /// Configures the service configuration.
    ///
    /// # Arguments
    ///
    /// * `value` - the service [configuration](SdkConfig)
    ///
    /// # Remarks
    ///
    /// Providing a configuration has no effect if a [Self::client] is specified.
    pub fn config(mut self, value: SdkConfig) -> Self {
        self.config = Some(value);
        self
    }

    /// Configures the client to use.
    ///
    /// # Arguments
    ///
    /// * `value` - the underlying [client](Client)
    ///
    /// # Remarks
    ///
    /// Specifying a [Client] is useful when it has already been configured externally or it is being
    /// reused across configurations. This configuration supersedes any previous [configuration](Self::config).
    pub fn client<V: Into<Client>>(mut self, value: V) -> Self {
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

    fn resolve_client(&mut self) -> Result<Client, BuilderError> {
        if let Some(client) = self.client.take() {
            Ok(client)
        } else {
            let config = if let Some(config) = self.config.take() {
                config
            } else {
                use aws_config::{BehaviorVersion, meta::region::RegionProviderChain};
                let region = RegionProviderChain::default_provider();
                executor::block_on(
                    aws_config::defaults(BehaviorVersion::latest())
                        .region(region)
                        .load(),
                )
            };

            Ok(Client::new(&config))
        }
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
        let client = self.resolve_client()?;
        let table = format!("{}_Events", self.table.ok_or(MissingTable)?);
        let options = EventStoreOptions::<ID>::new(
            self.concurrency,
            self.delete,
            self.mask,
            self.clock.unwrap_or_else(|| Arc::new(WallClock::new())),
            self.transcoder.unwrap_or_default(),
            self.snapshots,
        );

        Ok(EventStore::new(client, table, options))
    }
}

impl<ID> Builder<ID, dyn Snapshot> {
    /// Builds and returns a new [snapshot store](SnapshotStore).
    pub fn build(mut self) -> Result<SnapshotStore<ID>, BuilderError> {
        let client = self.resolve_client()?;
        let table = format!("{}_Snapshots", self.table.ok_or(MissingTable)?);
        let options = SnapshotStoreOptions::new(
            self.mask,
            self.clock.unwrap_or_else(|| Arc::new(WallClock::new())),
            self.transcoder.unwrap_or_default(),
        );

        Ok(SnapshotStore::new(client, table, options))
    }
}
