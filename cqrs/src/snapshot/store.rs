use super::{Predicate, Snapshot};
use crate::{
    event::StoreError,
    message::{Descriptor, EncodingError},
};
use async_trait::async_trait;
use std::{error::Error, fmt::Debug};
use thiserror::Error;
use uuid::Uuid;

/// Represents the possible snapshot errors.
#[derive(Error, Debug)]
pub enum SnapshotError {
    /// Indicates an invalid snapshot [encoding](crate::message::Encoding).
    #[error(transparent)]
    InvalidEncoding(#[from] EncodingError),

    /// Indicates an unknown store [error](Error).
    #[error(transparent)]
    Unknown(#[from] Box<dyn Error + Send>),
}

impl<T: Debug + Send> From<SnapshotError> for StoreError<T> {
    fn from(value: SnapshotError) -> Self {
        match value {
            SnapshotError::InvalidEncoding(error) => Self::InvalidEncoding(error),
            SnapshotError::Unknown(error) => Self::Unknown(error),
        }
    }
}

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
    ) -> Result<Option<Box<dyn Snapshot>>, SnapshotError>;

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
    /// * `snapshot` - the [snapshot](Snapshot) to save
    async fn save(&self, id: &T, snapshot: Box<dyn Snapshot>) -> Result<(), SnapshotError>;
}
