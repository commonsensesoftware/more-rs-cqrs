use super::AggregateBuilder;
use crate::{event::Event, message::Transcoder, snapshot::Snapshot, Aggregate};
use di::ServiceCollection;

/// Represents the supported [transcoder](Transcoder) options.
#[derive(Default)]
pub struct TranscoderOptions {
    /// Gets or sets the list [event](Event) [transcoders](Transcoder)s.
    pub events: Vec<Transcoder<dyn Event>>,

    /// Gets or sets the list [snapshot](Snapshot) [transcoders](Transcoder)s.
    pub snapshots: Vec<Transcoder<dyn Snapshot>>,
}

/// Represents the supported Command Query Responsibility Segregation (CQRS) options.
pub struct CqrsOptions<'a> {
    /// Gets the associated [services](ServiceCollection).
    pub services: &'a mut ServiceCollection,

    /// Gets or sets the supported [transcoder options](TranscoderOptions).
    pub transcoders: TranscoderOptions,
}

impl<'a> CqrsOptions<'a> {
    /// Initializes new [CQRS options](CqrsOptions).
    ///
    /// # Arguments
    ///
    /// * `services` - the [services](ServiceCollection) associated with the options
    pub fn new(services: &'a mut ServiceCollection) -> Self {
        Self {
            services,
            transcoders: Default::default(),
        }
    }

    /// Configures the storage for an [aggregate](Aggregate).
    pub fn store<A: Aggregate>(&mut self) -> AggregateBuilder<'_, A> {
        AggregateBuilder::new(self.services)
    }
}
