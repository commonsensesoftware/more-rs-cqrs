use super::AggregateBuilder;
use crate::{
    mem::{EventStore, SnapshotStore},
    Aggregate, Repository,
};
use di::{Injectable, ServiceCollection};
use std::{fmt::Debug, hash::Hash, marker::PhantomData};

/// Represents the in-memory storage configuration extensions.
pub trait InMemoryExt<'a, A: Aggregate + Default>: Sized
where
    A: Aggregate + Default + 'static,
    A::ID: Clone + Debug + Eq + Hash + Send + Sync,
{
    /// Configures an [aggregate](Aggregate) with in-memory storage.
    fn in_memory(self) -> InMemoryBuilder<'a, A>;
}

impl<'a, A> InMemoryExt<'a, A> for AggregateBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + Debug + Eq + Hash + Send + Sync,
{
    fn in_memory(self) -> InMemoryBuilder<'a, A> {
        InMemoryBuilder::new(self.services)
    }
}

/// Represents the builder for in-memory storage.
pub struct InMemoryBuilder<'a, A>
where
    A: Aggregate + Default + 'static,
    A::ID: Clone + Debug + Eq + Hash + Send + Sync,
{
    services: &'a mut ServiceCollection,
    _aggregate: PhantomData<A>,
}

impl<'a, A> InMemoryBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + Debug + Eq + Hash + Send + Sync,
{
    fn new(services: &'a mut ServiceCollection) -> Self {
        services.try_add(Repository::<A>::singleton());

        Self {
            services,
            _aggregate: PhantomData,
        }
    }

    /// Adds additional in-memory configuration options.
    pub fn with(self) -> InMemoryOptionsBuilder<'a, A> {
        InMemoryOptionsBuilder::new(self)
    }
}

impl<'a, A> Drop for InMemoryBuilder<'a, A>
where
    A: Aggregate + Default + 'static,
    A::ID: Clone + Debug + Eq + Hash + Send + Sync,
{
    fn drop(&mut self) {
        self.services
            .add(EventStore::<A::ID>::singleton().with_key::<A>());
    }
}

/// Represents a builder for in-memory configuration options.
pub struct InMemoryOptionsBuilder<'a, A>
where
    A: Aggregate + Default + 'static,
    A::ID: Clone + Debug + Eq + Hash + Send + Sync,
{
    parent: InMemoryBuilder<'a, A>,
}

impl<'a, A> InMemoryOptionsBuilder<'a, A>
where
    A: Aggregate + Default + 'static,
    A::ID: Clone + Debug + Eq + Hash + Send + Sync,
{
    fn new(parent: InMemoryBuilder<'a, A>) -> Self {
        Self { parent }
    }

    /// Configures in-memory storage with in-memory snapshots.
    pub fn snapshots(self) {
        self.parent
            .services
            .add(SnapshotStore::<A::ID>::singleton().with_key::<A>());
    }
}
