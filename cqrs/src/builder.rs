use crate::{
    Clock, Concurrency, Mask, WallClock,
    event::{self, Delete, Event},
    message::{Message, Transcoder},
    snapshot::{self, Snapshot},
};
use std::sync::Arc;

/// Represents a store options builder.
pub struct StoreOptionsBuilder<M: Message + ?Sized, ID = ()> {
    concurrency: Concurrency,
    delete: Delete,
    mask: Option<Arc<dyn Mask>>,
    clock: Option<Arc<dyn Clock>>,
    transcoder: Option<Arc<Transcoder<M>>>,
    snapshots: Option<Arc<dyn snapshot::Store<ID>>>,
}

impl<M: Message + ?Sized, ID> Default for StoreOptionsBuilder<M, ID> {
    fn default() -> Self {
        Self {
            concurrency: Default::default(),
            delete: Default::default(),
            mask: Default::default(),
            clock: Default::default(),
            transcoder: Default::default(),
            snapshots: Default::default(),
        }
    }
}

impl<M: Message + ?Sized, ID> StoreOptionsBuilder<M, ID> {
    /// Configures the mask associated with the store.
    ///
    /// # Arguments
    ///
    /// * `value` - the [mask](Mask) used to obfuscate [versions](crate::Version)
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
}

impl<ID> StoreOptionsBuilder<dyn Event, ID> {
    /// Configures the store to enforce concurrency.
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
    /// * `value` - the associated [snapshot store](snapshot::Store)
    pub fn snapshots<V: Into<Arc<dyn snapshot::Store<ID>>>>(mut self, value: V) -> Self {
        self.snapshots = Some(value.into());
        self
    }

    /// Builds and returns a new [event store options](event::StoreOptions).
    pub fn build(self) -> event::StoreOptions<ID> {
        event::StoreOptions::<ID>::new(
            self.concurrency,
            self.delete,
            self.mask,
            self.clock.unwrap_or_else(|| Arc::new(WallClock::new())),
            self.transcoder.unwrap_or_default(),
            self.snapshots,
        )
    }
}

impl StoreOptionsBuilder<dyn Snapshot, ()> {
    /// Builds and returns a new [snapshot store options](snapshot::StoreOptions).
    pub fn build(self) -> snapshot::StoreOptions {
        snapshot::StoreOptions::new(
            self.mask,
            self.clock.unwrap_or_else(|| Arc::new(WallClock::new())),
            self.transcoder.unwrap_or_default(),
        )
    }
}
