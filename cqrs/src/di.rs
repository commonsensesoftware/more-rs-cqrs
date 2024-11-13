mod builder;
mod options;

pub use builder::AggregateBuilder;
pub use options::*;

use crate::{
    message::{Message, Transcoder}, StoreMigrator, WallClock
};
use cfg_if::cfg_if;
use di::{existing_as_self, Injectable, ServiceCollection};

cfg_if! {
    if #[cfg(feature = "mem")] {
        mod mem;
        pub use mem::*;
    }
}

fn merge<M: Message + ?Sized>(mut transcoders: Vec<Transcoder<M>>) -> Transcoder<M> {
    if let Some(mut merged) = transcoders.pop() {
        for transcoder in transcoders.drain(..) {
            merged.merge(transcoder);
        }

        merged
    } else {
        Transcoder::default()
    }
}

/// Provides the extension functions for Command Query Responsibility Segregation (CQRS).
pub trait CqrsExt: Sized {
    /// Adds the CQRS services.
    ///
    /// # Arguments
    ///
    /// * `setup` - the function used to setup [CQRS options](CqrsOptions)
    fn add_cqrs<F>(&mut self, setup: F) -> &mut Self
    where
        F: FnOnce(&mut CqrsOptions);
}

impl CqrsExt for ServiceCollection {
    fn add_cqrs<F>(&mut self, setup: F) -> &mut Self
    where
        F: FnOnce(&mut CqrsOptions),
    {
        let mut options = CqrsOptions::new(self);

        setup(&mut options);

        let events = options.transcoders.events;
        let snapshots = options.transcoders.snapshots;

        self.try_add(existing_as_self(merge(events)))
            .try_add(existing_as_self(merge(snapshots)))
            .try_add(WallClock::singleton())
            .try_add(StoreMigrator::transient())
    }
}
