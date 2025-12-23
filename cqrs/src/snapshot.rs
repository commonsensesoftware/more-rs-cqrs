mod message;
mod predicate;
mod retention;
mod store;

pub use message::Snapshot;
pub use predicate::{Predicate, PredicateBuilder};
pub use retention::Retention;
pub use store::{SnapshotError, Store, StoreOptions};

/// Creates and returns a new [snapshot](Snapshot) [transcoder](crate::message::Transcoder).
#[inline]
pub fn transcoder() -> crate::message::Transcoder<dyn Snapshot> {
    crate::message::Transcoder::<dyn Snapshot>::new()
}
