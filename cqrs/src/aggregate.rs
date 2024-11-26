use crate::{event::Event, snapshot::Snapshot, Clock, Version};
use async_trait::async_trait;
use futures::Stream;
use std::{error::Error, fmt::Debug, sync::Arc};

/// Represents a [stream](Stream) of historic [events](Event).
pub type EventHistory<'a> =
    dyn Stream<Item = Result<Box<dyn Event>, Box<dyn Error + Send>>> + Send + Unpin + 'a;

/// Defines the behavior of an aggregate root.
#[async_trait]
pub trait Aggregate: Send {
    type ID: Debug + Send;

    /// Gets the aggregate identifier.
    fn id(&self) -> &Self::ID;

    /// Gets the [version](Version) of the aggregate.
    fn version(&self) -> Version;

    /// Gets a set of uncommitted [changes](ChangeSet).
    fn changes(&mut self) -> ChangeSet;

    /// Replays the specified event.
    ///
    /// # Arguments
    ///
    /// * `event` - the [event](Event) to replay.
    fn replay(&mut self, event: &dyn Event);

    /// Replays the specified sequence of events.
    ///
    /// # Arguments
    ///
    /// * `history` - the sequence of [events](Event) to replay.
    async fn replay_all(&mut self, history: &mut EventHistory)
        -> Result<(), Box<dyn Error + Send>>;

    /// Creates and returns a new [snapshot](Snapshot) of the aggregate.
    fn snapshot(&self) -> Option<Box<dyn Snapshot>> {
        None
    }

    /// Sets the clock associated with the aggregate.
    /// 
    /// # Arguments
    /// 
    /// * `clock` - the [clock](Clock) associated with the aggregate
    fn set_clock(&mut self, clock: Arc<dyn Clock>);
}

/// Represent a set of uncommitted changes made to an [aggregate](Aggregate).
pub struct ChangeSet<'a> {
    events: &'a mut Vec<Box<dyn Event>>,
    version: &'a mut Version,
    expected_version: Version,
}

impl<'a> ChangeSet<'a> {
    /// Initializes a new [`ChangeSet`].
    ///
    /// # Arguments
    ///
    /// * `events` - the set of uncommitted [events](Event)
    /// * `version` - the current aggregate [version](Version)
    pub fn new(events: &'a mut Vec<Box<dyn Event>>, version: &'a mut Version) -> Self {
        let expected_version = *version;

        Self {
            events,
            version,
            expected_version,
        }
    }

    /// Gets a value indicating whether there are any changes.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Gets the expected [version](Version) associated with the changes.
    pub fn expected_version(&self) -> Version {
        self.expected_version
    }

    /// Gets the set of uncommitted [events](Event).
    pub fn uncommitted(&mut self) -> &mut [Box<dyn Event>] {
        self.events
    }

    /// Accepts all of the changes in the [change set](ChangeSet).
    pub fn accept(&mut self) {
        if let Some(event) = self.events.last() {
            *self.version = event.version();
            self.events.clear();
        }
    }
}
