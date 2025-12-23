use super::{Predicate, Retention, Snapshot};
use crate::{
    Clock, Mask, StoreOptionsBuilder, Version, event::StoreError, message::{Descriptor, EncodingError, Saved, Transcoder}
};
use async_trait::async_trait;
use std::{error::Error, fmt::Debug, sync::Arc};
use thiserror::Error;
use uuid::Uuid;

/// Defines the behavior of a snapshot store.
#[async_trait]
pub trait Store<T: Debug + Send = Uuid>: Send + Sync {
    /// Loads a snapshot.
    ///
    /// # Arguments
    ///
    /// * `id` - the identifier of the [snapshot](Snapshot) to load
    /// * `predicate` - the optional [predicate](Predicate) used to filter the snapshot
    async fn load(
        &self,
        id: &T,
        predicate: Option<&Predicate>,
    ) -> Result<Option<Saved<Box<dyn Snapshot>>>, SnapshotError>;

    /// Loads a raw snapshot into a [message descriptor](Descriptor).
    ///
    /// # Arguments
    ///
    /// * `id` - the identifier of the [snapshot](Snapshot) to load
    /// * `predicate` - the optional [predicate](Predicate) used to filter the snapshot
    async fn load_raw(
        &self,
        id: &T,
        predicate: Option<&Predicate>,
    ) -> Result<Option<Descriptor>, SnapshotError>;

    /// Saves a snapshot.
    ///
    /// # Arguments
    ///
    /// * `id` - the identifier of the [snapshot](Snapshot) to save
    /// * `version` - the [version](Version) of the [snapshot](Snapshot) to save
    /// * `snapshot` - the [snapshot](Snapshot) to save
    async fn save(
        &self,
        id: &T,
        version: Version,
        snapshot: Box<dyn Snapshot>,
    ) -> Result<(), SnapshotError>;

    /// Prunes snapshots for the specified identifier using the specified retention policy.
    ///
    /// # Arguments
    ///
    /// * `id` - the identifier of the snapshots to delete
    /// * `retention` - the optional [retention](Retention) policy
    ///
    /// # Remarks
    ///
    /// If a [retention](Retention) policy is unspecified, then all snapshots are deleted.
    async fn prune(&self, id: &T, retention: Option<&Retention>) -> Result<(), SnapshotError>;
}

/// Represents [snapshot store](Store) options.
#[derive(Clone)]
pub struct StoreOptions {
    mask: Option<Arc<dyn Mask>>,
    clock: Arc<dyn Clock>,
    transcoder: Arc<Transcoder<dyn Snapshot>>,
}

impl StoreOptions {
    /// Initializes a new [StoreOptions].
    ///
    /// # Arguments
    ///
    /// * `mask` - the optional [mask](Mask) used to obfuscate [versions](Version)
    /// * `clock` - the associated [clock](Clock)
    /// * `transcoder` - the associated [transcoder](Transcoder)
    pub fn new(
        mask: Option<Arc<dyn Mask>>,
        clock: Arc<dyn Clock>,
        transcoder: Arc<Transcoder<dyn Snapshot>>,
    ) -> Self {
        Self {
            mask,
            clock,
            transcoder,
        }
    }

    /// Creates and returns a new [builder](StoreOptionsBuilder).
    pub fn builder() -> StoreOptionsBuilder<dyn Snapshot> {
        StoreOptionsBuilder::default()
    }

    /// Gets the configured [mask](Mask), if any.
    pub fn mask(&self) -> Option<&(dyn Mask + 'static)> {
        self.mask.as_deref()
    }

    /// Gets the configured [clock](Clock).
    pub fn clock(&self) -> &dyn Clock {
        &*self.clock
    }

    /// Gets the configured [transcoder](Transcoder).
    pub fn transcoder(&self) -> &Transcoder<dyn Snapshot> {
        &self.transcoder
    }
}

impl From<&StoreOptions> for Arc<dyn Clock> {
    fn from(options: &StoreOptions) -> Self {
        options.clock.clone()
    }
}

/// Represents the possible snapshot errors.
#[derive(Error, Debug)]
pub enum SnapshotError {
    /// Indicates an invalid snapshot [encoding](crate::message::Encoding).
    #[error(transparent)]
    InvalidEncoding(#[from] EncodingError),

    /// Indicates the [snapshot](Snapshot) [version](Version) is invalid.
    ///
    /// # Remarks
    ///
    /// An invalid version can most likely happen in one of the following scenarios:
    ///
    /// * A user attempted to generate a [version](Version) explicitly
    /// * The backing store changed the [mask](crate::Mask) it uses
    /// * The backing store itself has changed
    #[error("the specified version is invalid")]
    InvalidVersion,

    /// Indicates an unknown store [error](Error).
    #[error(transparent)]
    Unknown(#[from] Box<dyn Error + Send>),
}

impl<T: Debug + Send> From<SnapshotError> for StoreError<T> {
    fn from(value: SnapshotError) -> Self {
        match value {
            SnapshotError::InvalidEncoding(error) => Self::InvalidEncoding(error),
            SnapshotError::InvalidVersion => Self::InvalidVersion,
            SnapshotError::Unknown(error) => Self::Unknown(error),
        }
    }
}
