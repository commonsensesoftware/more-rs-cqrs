use crate::{
    NoSqlVersion,
    NoSqlVersionPart::{Sequence, Version as ByOne},
};
use async_trait::async_trait;
use cqrs::{
    Version,
    event::{Event, StoreError, StoreOptions},
};
use std::fmt::Debug;

/// Defines the behavior used to append events to a NoSQL [store](cqrs::event::Store).
///
/// # Remarks
///
/// Storage providers differ in how a batch of events is written, but they do not differ in how a [version](Version) is
/// resolved while writing them. This trait is only intended to be implemented by the storage providers in this crate.
#[async_trait]
pub(crate) trait Append<ID>
where
    ID: Clone + Debug + Send + Sync + 'static,
{
    /// Gets the maximum number of operations allowed in a single, atomic batch.
    const MAX_BATCH_SIZE: usize;

    /// Gets the associated [store options](StoreOptions).
    fn options(&self) -> &StoreOptions<ID>;

    /// Writes a single [event](Event) and returns the [version](Version) it was written with.
    ///
    /// # Arguments
    ///
    /// * `id` - the identifier of the events to write
    /// * `version` - the [version](Version) to write with
    /// * `event` - the [event](Event) to write
    async fn write_one(
        &self,
        id: &ID,
        version: Version,
        event: &(dyn Event + 'static),
    ) -> Result<Version, StoreError<ID>>;

    /// Writes multiple [events](Event) atomically and returns the last [version](Version) they were written with.
    ///
    /// # Arguments
    ///
    /// * `id` - the identifier of the events to write
    /// * `version` - the first [version](Version) to write with
    /// * `events` - the [events](Event) to write
    async fn write_all(&self, id: &ID, version: Version, events: &[Box<dyn Event>]) -> Result<Version, StoreError<ID>>;

    /// Gets a value indicating whether the specified [event](Event) has already been written with the specified
    /// [version](Version).
    ///
    /// # Arguments
    ///
    /// * `id` - the identifier of the event to match
    /// * `version` - the [version](Version) the event was written with
    /// * `event` - the [event](Event) to match
    ///
    /// # Remarks
    ///
    /// The stored event is only a match if it is the same event; specifically, the same schema and content. The time it
    /// was stored on is never compared because it is measured for each attempt to write it.
    ///
    /// A store is expected to read the event with strong consistency. A stale read reports that nothing was written,
    /// which reintroduces the very duplicate the read exists to prevent.
    async fn written(&self, id: &ID, version: Version, event: &(dyn Event + 'static)) -> Result<bool, StoreError<ID>>;

    /// Appends a collection of events and returns the new [version](Version).
    ///
    /// # Arguments
    ///
    /// * `id` - the identifier of the events to append
    /// * `expected_version` - the current, expected [version](Version)
    /// * `events` - the list of [events](Event) to append
    async fn append(
        &self,
        id: &ID,
        expected_version: Version,
        events: &[Box<dyn Event>],
    ) -> Result<Version, StoreError<ID>> {
        if events.is_empty() {
            return Ok(expected_version);
        }

        let options = self.options();
        let mut version = if expected_version != Version::default()
            && let Some(mask) = options.mask()
        {
            expected_version.unmask(mask)
        } else {
            expected_version
        };

        if version.invalid() {
            return Err(StoreError::InvalidVersion);
        }

        if events.len() > 1 {
            // deletes support reserves an operation in the batch to check that the previous version still exists
            let reserved = usize::from(options.delete().supported() && version.number() > 0);
            let max = Self::MAX_BATCH_SIZE - reserved;

            if events.len() > max {
                return Err(StoreError::BatchTooLarge(max as u8));
            }
        }

        loop {
            version = version.increment(ByOne);

            let result = if events.len() == 1 {
                self.write_one(id, version, &*events[0]).await
            } else {
                self.write_all(id, version, events).await
            };

            match result {
                Ok(current) => {
                    version = current;
                    break;
                }
                Err(error) => {
                    if !matches!(error, StoreError::Conflict(_, _)) {
                        return Err(error);
                    }

                    // REMARKS: a conflict is not proof that another writer won the version. a client retries a request
                    // whose response it never observed, so the conflict can be this same write echoing back the event
                    // the original request already committed. the stored event settles it: if it is the event being
                    // written, the append is already durable and retrying it would append the event a second time
                    if self.written(id, version, &*events[0]).await? {
                        // events are written atomically, so the last version is durable whenever the first one is
                        for _ in 1..events.len() {
                            version = version.increment(Sequence);
                        }

                        break;
                    }

                    if options.concurrency().enforced() {
                        return Err(error);
                    }
                }
            }
        }

        if let Some(mask) = options.mask() {
            version = version.mask(mask);
        }

        Ok(version)
    }
}
