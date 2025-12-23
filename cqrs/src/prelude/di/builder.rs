use crate::Aggregate;
use di::ServiceCollection;
use std::marker::PhantomData;

/// Represents builder to configure an [aggregate](Aggregate).
pub struct AggregateBuilder<'a, A: Aggregate> {
    pub services: &'a mut ServiceCollection,
    _aggregate: PhantomData<A>,
}

impl<'a, A: Aggregate> AggregateBuilder<'a, A> {
    pub(crate) fn new(services: &'a mut ServiceCollection) -> Self {
        Self {
            services,
            _aggregate: PhantomData,
        }
    }
}
