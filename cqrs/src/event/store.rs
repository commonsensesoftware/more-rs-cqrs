use super::{Event, Predicate};
use crate::{message::EncodingError, Clock, Range, Version};
use async_trait::async_trait;
use futures::Stream;
use std::{error::Error, fmt::Debug, pin::Pin, sync::Arc, time::SystemTime};
use thiserror::Error;
use uuid::Uuid;

/// Represents an event identifier [stream](Stream).
pub type IdStream<T> = Pin<Box<dyn Stream<Item = Result<T, StoreError<T>>> + Send>>;

/// Represents a stored event [stream](Stream).
pub type EventStream<'a, T> =
    Pin<Box<dyn Stream<Item = Result<Box<dyn Event>, StoreError<T>>> + Send + 'a>>;

/// Defines the behavior of an event store.
#[async_trait]
pub trait Store<T: Debug + Send = Uuid>: Send + Sync {
    /// Gets the store [clock](Clock).
    fn clock(&self) -> Arc<dyn Clock>;

    /// Streams the unique sets of all identifiers in the store.
    ///
    /// # Arguments
    ///
    /// * `stored_on` - the [date](SystemTime) [range](Range) used to filter results
    ///
    /// # Remarks
    ///
    /// The stream of identifiers are expected to be unique. If a bounded `stored_on` [range](Range)
    /// is specified, the stream will only include the identifiers of entities that had their first
    /// event recorded within the range.
    async fn ids(&self, stored_on: Range<SystemTime>) -> IdStream<T>;

    /// Loads a sequence of [events](Event).
    ///
    /// # Arguments
    ///
    /// * `predicate` - the optional [predicate](Predicate) used to filter events
    async fn load<'a>(&self, predicate: Option<&'a Predicate<'a, T>>) -> EventStream<'a, T>;

    /// Saves a collection of events.
    ///
    /// # Arguments
    ///
    /// * `id` - the identifier of the events to save
    /// * `events` - the list of [events](Event) to save
    /// * `expected_version` - the current, expected [version](Version)
    async fn save(
        &self,
        id: &T,
        events: &mut [Box<dyn Event>],
        expected_version: Version,
    ) -> Result<(), StoreError<T>>;

    /// Deletes a collection of events.
    ///
    /// # Arguments
    ///
    /// * `id` - the identifier of the events to delete
    ///
    /// # Remarks
    ///
    /// In general, events should never be deleted; however, there is a use case for events that are
    /// tombstoned to be permanently deleted from a store. A valid and safe scenario might be if the
    /// events have been copied to different, cheaper, colder, but long-live store.
    ///
    /// A store is not required to support deletes and the assumed expectation should be that a store
    /// does not support deletes. A store that does support deletes is expected to prevent saving new
    /// events that occur after the delete.
    #[allow(unused_variables)]
    async fn delete(&self, id: &T) -> Result<(), StoreError<T>> {
        Err(StoreError::Unsupported)
    }
}

/// Represents the possible store errors.
#[derive(Error, Debug)]
pub enum StoreError<T: Debug + Send> {
    /// Indicates a concurrency conflict occurred.
    #[error("the item with identifier {0:?} and version {1:?} already exists")]
    Conflict(T, u32),

    /// Indicates the specified identifier has been deleted.
    #[error("the item with identifier {0:?} has been deleted")]
    Deleted(T),

    /// Indicates an invalid store [encoding](Encoding).
    #[error(transparent)]
    InvalidEncoding(#[from] EncodingError),

    /// Indicates the specified [version](Version) is invalid.
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

    /// Indicates that a batch size is too large and provides maximum size allowed.
    #[error("the batch to store is greater than {0}, which is the maximum size allowed")]
    BatchTooLarge(u8),

    /// Indicates that an operation is unsupported; for example, deletion.
    #[error("the requested operation is unsupported")]
    Unsupported,

    /// Indicates an unknown store [error](Error).
    #[error(transparent)]
    Unknown(#[from] Box<dyn Error + Send>),
}

impl<T: Debug + PartialEq + Send> PartialEq for StoreError<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Conflict(l0, l1), Self::Conflict(r0, r1)) => l0 == r0 && l1 == r1,
            (Self::Deleted(l0), Self::Deleted(r0)) => l0 == r0,
            (Self::InvalidEncoding(l0), Self::InvalidEncoding(r0)) => l0 == r0,
            (Self::BatchTooLarge(l0), Self::BatchTooLarge(r0)) => l0 == r0,
            (Self::Unknown(_), Self::Unknown(_)) => false,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

impl<T: Debug + Eq + Send> Eq for StoreError<T> {}
