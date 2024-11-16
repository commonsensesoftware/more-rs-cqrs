use super::{Event, Predicate};
use crate::{message::EncodingError, Range, Version};
use async_trait::async_trait;
use futures::Stream;
use std::{error::Error, fmt::Debug, pin::Pin, time::SystemTime};
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
}

/// Represents the possible store errors.
#[derive(Error, Debug)]
pub enum StoreError<T: Debug + Send> {
    /// Indicates a concurrency conflict occurred.
    #[error("the item with identifier {0:?} and version {1:?} already exists")]
    Conflict(T, u32),

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

    /// Indicates an unknown store [error](Error).
    #[error(transparent)]
    Unknown(#[from] Box<dyn Error + Send>),
}
