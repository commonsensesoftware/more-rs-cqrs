mod delete;
mod message;
mod predicate;
mod receiver;
mod store;

pub use delete::Delete;
pub use message::Event;
pub use predicate::{LoadOptions, Predicate, PredicateBuilder};
pub use receiver::Receiver;
pub use store::{EventStream, IdStream, Store, StoreError};

/// Creates and returns a new [event](Event) [transcoder](crate::message::Transcoder).
#[inline]
pub fn transcoder() -> crate::message::Transcoder<dyn Event> {
    crate::message::Transcoder::<dyn Event>::new()
}
