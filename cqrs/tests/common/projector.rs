use super::domain::{Credited, Debited, Statement};
use async_trait::async_trait;
use cqrs::{
    event::{Event, EventStream, IdStream, Predicate, Receiver, Store, StoreError},
    projection::{FilterBuilder, Projector},
    projectors, Clock, Range, Version, WallClock,
};
use std::{error::Error, sync::Arc, time::SystemTime};
use std::{fmt::Debug, marker::PhantomData};

#[projectors]
mod projectors {
    // convention-based used 'output' and 'store' field names
    #[projector]
    pub struct StatementGenerator {
        pub output: Statement,
        pub store: Arc<dyn Store<String>>,
    }

    impl StatementGenerator {
        pub fn new(store: Arc<dyn Store<String>>) -> Self {
            Self {
                output: Default::default(),
                store,
            }
        }

        pub async fn run(&mut self, id: &String) -> Result<Statement, Box<dyn Error + Send>> {
            let filter = FilterBuilder::new(Some(id)).build();

            self.output.id = id.clone();
            self.output.date = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            <Self as Projector<String, Statement>>::run(self, Some(&filter)).await
        }
    }

    #[async_trait]
    impl Receiver<Debited> for StatementGenerator {
        async fn receive(&mut self, event: &Debited) -> Result<(), Box<dyn Error + Send>> {
            self.output.set_version(event.version());
            self.output.balance -= event.amount;
            Ok(())
        }
    }

    #[async_trait]
    impl Receiver<Credited> for StatementGenerator {
        async fn receive(&mut self, event: &Credited) -> Result<(), Box<dyn Error + Send>> {
            self.output.set_version(event.version());
            self.output.balance += event.amount;
            Ok(())
        }
    }

    #[projector]
    pub struct DeclarativeFieldNames {
        #[output]
        projection: Statement,

        #[store]
        events: Arc<dyn Store<String>>,
    }

    #[async_trait]
    impl Receiver<Debited> for DeclarativeFieldNames {
        async fn receive(&mut self, event: &Debited) -> Result<(), Box<dyn Error + Send>> {
            self.projection.balance -= event.amount;
            Ok(())
        }
    }

    #[async_trait]
    impl Receiver<Credited> for DeclarativeFieldNames {
        async fn receive(&mut self, event: &Credited) -> Result<(), Box<dyn Error + Send>> {
            self.projection.balance += event.amount;
            Ok(())
        }
    }

    #[projector]
    pub struct GenericProjector<K, S>
    where
        K: Debug + Default + Send + Sync + 'static,
        S: Store<K>,
    {
        output: Statement,
        store: Arc<S>,
        _marker: PhantomData<K>,
    }

    #[async_trait]
    impl<K, S> Receiver<Debited> for GenericProjector<K, S>
    where
        K: Debug + Default + Send + Sync,
        S: Store<K>,
    {
        async fn receive(&mut self, event: &Debited) -> Result<(), Box<dyn Error + Send>> {
            self.output.balance -= event.amount;
            Ok(())
        }
    }

    #[async_trait]
    impl<K, S> Receiver<Credited> for GenericProjector<K, S>
    where
        K: Debug + Default + Send + Sync,
        S: Store<K>,
    {
        async fn receive(&mut self, event: &Credited) -> Result<(), Box<dyn Error + Send>> {
            self.output.balance += event.amount;
            Ok(())
        }
    }

    #[projector]
    pub struct NoOutput {
        store: Arc<dyn Store<String>>,
    }

    #[async_trait]
    impl Receiver<Debited> for NoOutput {
        async fn receive(&mut self, _event: &Debited) -> Result<(), Box<dyn Error + Send>> {
            Ok(())
        }
    }

    #[async_trait]
    impl Receiver<Credited> for NoOutput {
        async fn receive(&mut self, _event: &Credited) -> Result<(), Box<dyn Error + Send>> {
            Ok(())
        }
    }

    pub struct CustomStore;

    #[async_trait]
    impl Store<u64> for CustomStore {
        fn clock(&self) -> Arc<dyn Clock> {
            Arc::new(WallClock::new())
        }

        async fn ids(&self, _stored_on: Range<SystemTime>) -> IdStream<u64> {
            Box::pin(futures::stream::iter(std::iter::empty()))
        }

        async fn load<'a>(
            &self,
            _predicate: Option<&'a Predicate<'a, u64>>,
        ) -> EventStream<'a, u64> {
            Box::pin(futures::stream::iter(std::iter::empty()))
        }

        async fn save(
            &self,
            _id: &u64,
            _events: &mut [Box<dyn Event>],
            _expected_version: Version,
        ) -> Result<(), StoreError<u64>> {
            Ok(())
        }
    }

    #[projector(u64)]
    pub struct NoIdTypeByConvention {
        store: CustomStore,
    }

    #[async_trait]
    impl Receiver<Debited> for NoIdTypeByConvention {
        async fn receive(&mut self, _event: &Debited) -> Result<(), Box<dyn Error + Send>> {
            Ok(())
        }
    }

    #[async_trait]
    impl Receiver<Credited> for NoIdTypeByConvention {
        async fn receive(&mut self, _event: &Credited) -> Result<(), Box<dyn Error + Send>> {
            Ok(())
        }
    }
}
