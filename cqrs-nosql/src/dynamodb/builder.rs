use self::BuilderError::*;
use super::{EventStore, SnapshotStore};
use aws_sdk_dynamodb::{config::Credentials, Client};
use cqrs::{
    event::{Delete, Event},
    message::{Message, Transcoder},
    snapshot::{Retention, Snapshot},
    Clock, Mask, WallClock,
};
use futures::executor;
use std::sync::Arc;
use thiserror::Error;

type DynSnapshotStore<ID> = dyn cqrs::snapshot::Store<ID>;

/// Represents the possible Amazon DynamoDB store builder errors.
#[derive(Error, Debug)]
pub enum BuilderError {
    /// Indicates the target table is missing because it has not been configured.
    #[error("a table has not been configured")]
    MissingTable,

    /// Indicates the endpoint URL is missing because it has not been configured.
    #[error("an endpoint URL has not been configured")]
    MissingUrl,

    /// Indicates authentication credentials are missing because they have not been configured.
    #[error("authentication credentials have not been configured")]
    MissingCredentials,
}

/// Represents builder for Amazon DynamoDB stores.
pub struct Builder<ID, M: Message + ?Sized> {
    table: Option<&'static str>,
    url: Option<String>,
    credentials: Option<Credentials>,
    client: Option<Client>,
    delete: Delete,
    mask: Option<Arc<dyn Mask>>,
    clock: Option<Arc<dyn Clock>>,
    transcoder: Option<Arc<Transcoder<M>>>,
    snapshots: Option<Arc<DynSnapshotStore<ID>>>,
    retention: Option<Retention>,
}

impl<ID, M: Message + ?Sized> Default for Builder<ID, M> {
    fn default() -> Self {
        Self {
            table: Default::default(),
            url: Default::default(),
            credentials: Default::default(),
            client: Default::default(),
            delete: Default::default(),
            mask: Default::default(),
            clock: Default::default(),
            transcoder: Default::default(),
            snapshots: Default::default(),
            retention: Default::default(),
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

    /// Configures the service endpoint URL.
    ///
    /// # Arguments
    ///
    /// * `value` - the endpoint URL
    ///
    /// # Remarks
    ///
    /// Configuring a URL has no effect if a [`Self::client`] is specified.
    pub fn url<V: Into<String>>(mut self, value: V) -> Self {
        self.url = Some(value.into());
        self
    }

    /// Configures the authentication credentials.
    ///
    /// # Arguments
    ///
    /// * `value` - the authentication [credentials](Credentials)
    ///
    /// # Remarks
    ///
    /// Configuring credentials has no effect if a [`Self::client`] is specified.
    pub fn credentials<V: Into<Credentials>>(mut self, value: V) -> Self {
        self.credentials = Some(value.into());
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
    /// Specifying a [`Client`] is useful when it has already been configured externally or it is being
    /// reused across configurations. This configuration supersedes any configuration by [`Self::url`]
    /// or [`Self::credentials`] as they are mutually exclusive.
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
            use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
            let region = RegionProviderChain::default_provider();
            let loader = aws_config::defaults(BehaviorVersion::latest())
                .region(region)
                .endpoint_url(self.url.take().ok_or(MissingUrl)?)
                .credentials_provider(self.credentials.take().ok_or(MissingCredentials)?);

            Ok(Client::new(&executor::block_on(loader.load())))
        }
    }
}

impl<ID> Builder<ID, dyn Event> {
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

        Ok(EventStore::new(
            client,
            table,
            self.mask,
            self.clock.unwrap_or_else(|| Arc::new(WallClock::new())),
            self.transcoder.unwrap_or_default(),
            self.snapshots,
            self.delete,
        ))
    }
}

impl<ID> Builder<ID, dyn Snapshot> {
    /// Configures the snapshot retention.
    ///
    /// # Arguments
    ///
    /// * `value` - the snapshot [retention](Retention) policy
    pub fn retention<V: Into<Retention>>(mut self, value: V) -> Self {
        self.retention = Some(value.into());
        self
    }

    /// Builds and returns a new [snapshot store](SnapshotStore).
    pub fn build(mut self) -> Result<SnapshotStore<ID>, BuilderError> {
        let client = self.resolve_client()?;
        let table = format!("{}_Snapshots", self.table.ok_or(MissingTable)?);

        Ok(SnapshotStore::new(
            client,
            table,
            self.mask,
            self.clock.unwrap_or_else(|| Arc::new(WallClock::new())),
            self.transcoder.unwrap_or_default(),
            self.retention.unwrap_or_default(),
        ))
    }
}
