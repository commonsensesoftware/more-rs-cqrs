use super::AggregateBuilder;
use crate::{
    Aggregate, Clock, Concurrency, Mask, Repository, WallClock,
    event::{self, Delete, Event},
    in_memory,
    message::Transcoder,
    snapshot::{self, Snapshot},
};
use di::{
    Injectable, Ref, ServiceCollection, exactly_one, singleton_with_key, zero_or_one,
    zero_or_one_with_key,
};
use std::{fmt::Debug, hash::Hash, marker::PhantomData, sync::Arc};

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
    mask: Option<Arc<dyn Mask>>,
    enforce_concurrency: bool,
    allow_delete: bool,
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
            mask: None,
            enforce_concurrency: false,
            allow_delete: false,
        }
    }

    /// Configures the associated mask.
    ///
    /// # Arguments
    ///
    /// * `value` - the [mask](Mask) used to obfuscate [versions](cqrs::Version)
    pub fn mask<V: Mask + 'static>(mut self, value: V) -> Self {
        self.mask = Some(Arc::new(value));
        self
    }

    /// Enforces concurrency, which not enforced by default.
    pub fn enforce_concurrency(mut self) -> Self {
        self.enforce_concurrency = true;
        self
    }

    /// Enables support for deletes, which is unsupported by default.
    pub fn deletes(mut self) -> Self {
        self.allow_delete = true;
        self
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
        let mask = self.mask.clone();
        let enforce_concurrency = self.enforce_concurrency;
        let allow_delete = self.allow_delete;

        self.services.try_add(
            singleton_with_key::<A, dyn event::Store<A::ID>, in_memory::EventStore<A::ID>>()
                .depends_on(zero_or_one::<dyn Mask>())
                .depends_on(zero_or_one::<dyn Clock>())
                .depends_on(exactly_one::<Transcoder<dyn Event>>())
                .depends_on(zero_or_one_with_key::<A, dyn snapshot::Store<A::ID>>())
                .from(move |sp| {
                    let concurrency = if enforce_concurrency {
                        Concurrency::Enforced
                    } else {
                        Concurrency::None
                    };
                    let delete = if allow_delete {
                        Delete::Supported
                    } else {
                        Delete::Unsupported
                    };
                    let snapshots =
                        if let Some(snapshots) = sp.get_by_key::<A, dyn snapshot::Store<A::ID>>() {
                            Some(Ref::<dyn snapshot::Store<A::ID>>::from(snapshots))
                        } else {
                            None
                        };
                    let options = event::StoreOptions::<A::ID>::new(
                        concurrency,
                        delete,
                        mask.clone().or_else(|| sp.get::<dyn Mask>()),
                        sp.get::<dyn Clock>()
                            .unwrap_or_else(|| Arc::new(WallClock::default())),
                        sp.get_required::<Transcoder<dyn Event>>(),
                        snapshots,
                    );

                    Ref::new(in_memory::EventStore::new(options))
                }),
        );
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
        let mask = self.parent.mask.clone();

        self.parent.services.try_add(
            singleton_with_key::<A, dyn snapshot::Store<A::ID>, in_memory::SnapshotStore<A::ID>>()
                .depends_on(zero_or_one::<dyn Mask>())
                .depends_on(exactly_one::<dyn Clock>())
                .depends_on(exactly_one::<Transcoder<dyn Snapshot>>())
                .from(move |sp| {
                    let options = snapshot::StoreOptions::new(
                        mask.clone().or_else(|| sp.get::<dyn Mask>()),
                        sp.get::<dyn Clock>()
                            .unwrap_or_else(|| Arc::new(WallClock::default())),
                        sp.get_required::<Transcoder<dyn Snapshot>>(),
                    );

                    Ref::new(in_memory::SnapshotStore::new(options))
                }),
        );
    }
}
