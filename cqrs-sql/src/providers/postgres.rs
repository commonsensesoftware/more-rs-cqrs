use crate::{event, snapshot};
use sqlx::Postgres;

impl snapshot::Upsert for Postgres {
    fn on_conflict() -> &'static str {
        concat!(
            "ON CONFLICT (id) DO UPDATE SET ",
            "taken_on = EXCLUDED.taken_on, ",
            "revision = EXCLUDED.revision, ",
            "type = EXCLUDED.type"
        )
    }
}

/// Represents a Postgres [event store](event::SqlStore).
pub type EventStore<ID> = event::SqlStore<ID, Postgres>;

/// Represents a Postgres [snapshot store](snapshot::SqlStore).
pub type SnapshotStore<ID> = snapshot::SqlStore<ID, Postgres>;

cfg_if::cfg_if! {
    if #[cfg(feature = "migrate")] {
        mod migration;
        pub use migration::PostgresMigrator;
    }
}
