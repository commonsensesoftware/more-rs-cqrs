use crate::{
    event::{Predicate, PredicateBuilder, Store, StoreError},
    message::EncodingError,
    Aggregate,
};
use cfg_if::cfg_if;
use futures::{stream, StreamExt, TryStreamExt};
use std::{error::Error, future::Future, pin::Pin};
use std::{fmt::Debug, sync::Arc};
use thiserror::Error;

/// Represents the possible repository errors.
#[derive(Error, Debug)]
pub enum RepositoryError<T: Debug + Send> {
    /// Indicates the specified aggregate was not found.
    #[error("an aggregate with the identifier {0:?} was not found")]
    NotFound(T),

    /// Indicates a concurrency conflict occurred.
    #[error("the aggregate with identifier {0:?} and version {1:?} already exists")]
    Conflict(T, u32),

    /// Indicates an [encoding error](EncodingError) for an event.
    #[error(transparent)]
    InvalidEncoding(#[from] EncodingError),

    /// Indicates the [aggregate](Aggregate) [version](Version) is invalid.
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

    /// Indicates an unknown [error](Error).
    #[error(transparent)]
    Unknown(#[from] Box<dyn Error + Send>),
}

impl<T: Debug + Send + 'static> From<StoreError<T>> for RepositoryError<T> {
    fn from(value: StoreError<T>) -> Self {
        match value {
            StoreError::Conflict(id, version) => Self::Conflict(id, version),
            StoreError::InvalidEncoding(error) => Self::InvalidEncoding(error),
            StoreError::InvalidVersion => Self::InvalidVersion,
            StoreError::Unknown(error) => Self::Unknown(error),
            _ => Self::Unknown(Box::new(value)),
        }
    }
}

/// Represents an [aggregate](Aggregate) repository.
pub struct Repository<A: Aggregate> {
    store: Arc<dyn Store<A::ID>>,
}

impl<A> Repository<A>
where
    A: Aggregate + Default + Sync,
    A::ID: Clone + Debug + Send + Sync + 'static,
{
    /// Initializes a new [`Repository`].
    ///
    /// # Arguments
    ///
    /// * `store` - the underlying [store](Store)
    pub fn new<S: Store<A::ID> + 'static>(store: S) -> Self {
        Self {
            store: Arc::new(store),
        }
    }

    /// Gets an [aggregate](Aggregate) with the specified identifier.
    ///
    /// # Arguments
    ///
    /// * `id` - the aggregate identifier
    /// * `predicate` - the [predicate](Predicate) used to filter [events](crate::event::Event), if any
    ///
    #[allow(clippy::type_complexity)] // REF: https://github.com/rust-lang/rust/issues/112792
    pub fn get<'a, 'b>(
        &'a self,
        id: &'b A::ID,
        predicate: Option<&'b Predicate<'a, A::ID>>,
    ) -> Pin<Box<dyn Future<Output = Result<A, RepositoryError<A::ID>>> + Send + 'b>>
    where
        'a: 'b,
        Self: 'a,
    {
        Box::pin(async move {
            let mut builder = PredicateBuilder::new(Some(id));

            if let Some(predicate) = predicate {
                builder = builder.merge(predicate);
            }

            let predicate = builder.build();
            let mut history = self.store.load(Some(&predicate)).await;

            if let Some(first) = history.next().await {
                let mut history = Box::pin(
                    stream::iter(vec![first])
                        .chain(history)
                        .map_err(|e| Box::new(e) as Box<dyn Error + Send>),
                );
                let mut aggregate = A::default();

                aggregate.replay_all(&mut history).await?;
                Ok(aggregate)
            } else {
                Err(RepositoryError::NotFound(id.clone()))
            }
        })
    }

    /// Saves the specified [aggregate](Aggregate).
    ///
    /// # Arguments
    ///
    /// * `aggregate` - the [aggregate](Aggregate) to save
    pub async fn save(&self, aggregate: &mut A) -> Result<(), RepositoryError<A::ID>> {
        let id = aggregate.id().clone();
        let mut changes = aggregate.changes();

        if changes.is_empty() {
            return Ok(());
        }

        let expected_version = changes.expected_version();

        self.store
            .save(&id, changes.uncommitted(), expected_version)
            .await?;

        changes.accept();

        Ok(())
    }
}

impl<A> From<Arc<dyn Store<A::ID>>> for Repository<A>
where
    A: Aggregate + Default,
    A::ID: Clone + Debug + Send + 'static,
{
    fn from(value: Arc<dyn Store<A::ID>>) -> Self {
        Repository {
            store: value.clone(),
        }
    }
}

cfg_if! {
    if #[cfg(feature = "di")] {
        use di::{inject, injectable, KeyedRef};

        #[injectable]
        impl<A> Repository<A>
        where
            A: Aggregate + Default + Sync + 'static,
            A::ID: Clone + Debug + Send + Sync + 'static,
        {
            #[inject]
            fn _new(store: KeyedRef<A, dyn Store<A::ID>>) -> Self {
                Self { store: store.into() }
            }
        }
    }
}
