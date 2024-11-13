use self::SqlStoreBuilderError::*;
use crate::{event, snapshot, sql::Ident};
use cqrs::{
    event::Event, message::Message, message::Transcoder, snapshot::Snapshot, Clock, WallClock,
};
use sqlx::{pool::PoolOptions, Database};
use std::sync::Arc;
use thiserror::Error;

type DynSnapshotStore<ID> = dyn cqrs::snapshot::Store<ID>;

/// Represents the possible SQL store builder errors.
#[derive(Error, Debug)]
pub enum SqlStoreBuilderError {
    /// Indicates the target table is missing because it has not been configured.
    #[error("a table has not been configured")]
    MissingTable,

    /// Indicates the URL for the database connection string is missing because it has not been configured.
    #[error("a database URL has not been configured")]
    MissingUrl,

    /// Indicates a database [error](sqlx::Error).
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Represents a SQL store builder.
pub struct SqlStoreBuilder<ID, M, DB>
where
    M: Message + ?Sized,
    DB: Database,
{
    schema: &'static str,
    table: Option<&'static str>,
    pub(crate) url: Option<String>,
    pub(crate) options: Option<PoolOptions<DB>>,
    clock: Option<Arc<dyn Clock>>,
    transcoder: Option<Arc<Transcoder<M>>>,
    snapshots: Option<Arc<DynSnapshotStore<ID>>>,
}

impl<ID, DB: Database> Default for SqlStoreBuilder<ID, dyn Event, DB> {
    fn default() -> Self {
        Self {
            schema: "events",
            table: None,
            url: None,
            options: None,
            clock: None,
            transcoder: None,
            snapshots: None,
        }
    }
}

impl<ID, DB: Database> Default for SqlStoreBuilder<ID, dyn Snapshot, DB> {
    fn default() -> Self {
        Self {
            schema: "snapshots",
            table: None,
            url: None,
            options: None,
            clock: None,
            transcoder: None,
            snapshots: None,
        }
    }
}

impl<ID, M, DB> SqlStoreBuilder<ID, M, DB>
where
    M: Message + ?Sized,
    DB: Database,
{
    /// Configures the name of the schema for the table representing the store.
    ///
    /// # Arguments
    ///
    /// * `value` - the name of the schema
    pub fn schema(mut self, value: &'static str) -> Self {
        self.schema = value;
        self
    }

    /// Configures the identifier of the table representing the store.
    ///
    /// # Arguments
    ///
    /// * `value` - the name of the storage table
    pub fn table(mut self, value: &'static str) -> Self {
        self.table = Some(value);
        self
    }

    /// Configures the URL representing the database connection string.
    ///
    /// # Arguments
    ///
    /// * `value` - the URL for the database connection string
    pub fn url<V: Into<String>>(mut self, value: V) -> Self {
        self.url = Some(value.into());
        self
    }

    /// Configures the options for the underlying database.
    ///
    /// # Arguments
    ///
    /// * `value` - the [options](PoolOptions) for the underlying database
    pub fn options(mut self, value: PoolOptions<DB>) -> Self {
        self.options = Some(value);
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

    /// Configures the transcoder used to encode and decode store events.
    ///
    /// # Arguments
    ///
    /// * `value` - the associated [transcoder](Transcoder)
    pub fn transcoder<V: Into<Arc<Transcoder<M>>>>(mut self, value: V) -> Self {
        self.transcoder = Some(value.into());
        self
    }
}

impl<ID, DB: Database> SqlStoreBuilder<ID, dyn Event, DB> {
    /// Configures the snapshots associated with the store.
    ///
    /// # Arguments
    ///
    /// * `value` - the associated [snapshot store](cqrs::snapshot::Store)
    pub fn snapshots<V: Into<Arc<DynSnapshotStore<ID>>>>(mut self, value: V) -> Self {
        self.snapshots = Some(value.into());
        self
    }

    /// Builds and returns a new [`event::SqlStore`].
    pub fn build(self) -> Result<event::SqlStore<ID, DB>, SqlStoreBuilderError> {
        let url = self.url.ok_or(MissingUrl)?;
        let options = self.options.unwrap_or_default();
        let table = self.table.ok_or(MissingTable)?;
        let table = if self.schema.is_empty() {
            Ident::unqualified(table)
        } else {
            Ident::qualified(self.schema, table)
        };

        Ok(event::SqlStore::new(
            table,
            options.connect_lazy(&url)?,
            self.clock.unwrap_or_else(|| Arc::new(WallClock::new())),
            self.transcoder.unwrap_or_default(),
            self.snapshots
        ))
    }
}

impl<ID, DB: Database> SqlStoreBuilder<ID, dyn Snapshot, DB> {
    /// Builds and returns a new [`snapshot::SqlStore`].
    pub fn build(self) -> Result<snapshot::SqlStore<ID, DB>, SqlStoreBuilderError> {
        let url = self.url.ok_or(MissingUrl)?;
        let options = self.options.unwrap_or_default();
        let table = self.table.ok_or(MissingTable)?;
        let table = if self.schema.is_empty() {
            Ident::unqualified(table)
        } else {
            Ident::qualified(self.schema, table)
        };

        Ok(snapshot::SqlStore::new(
            table,
            options.connect_lazy(&url)?,
            self.clock.unwrap_or_else(|| Arc::new(WallClock::new())),
            self.transcoder.unwrap_or_default(),
        ))
    }
}
