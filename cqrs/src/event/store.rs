use super::{Event, Predicate};
use crate::{message::EncodingError, Version};
use async_trait::async_trait;
use futures::Stream;
use std::{error::Error, fmt::Debug, pin::Pin};
use thiserror::Error;
use uuid::Uuid;

/// Represents a stored event [`Stream`].
pub type EventStream<'a, T> =
    Pin<Box<dyn Stream<Item = Result<Box<dyn Event>, StoreError<T>>> + Send + 'a>>;

/// Defines the behavior of an event store.
#[async_trait]
pub trait Store<T: Debug + Send = Uuid>: Send + Sync {
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

    /// Indicates that a batch size is too large and provides maximum size allowed.
    #[error("the batch to store is greater than {0}, which is the maximum size allowed")]
    BatchTooLarge(u8),

    /// Indicates an unknown store [error](Error).
    #[error(transparent)]
    Unknown(#[from] Box<dyn Error + Send>),
}
