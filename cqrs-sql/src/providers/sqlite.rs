use crate::{event, snapshot};
use sqlx::Sqlite;

impl snapshot::Upsert for Sqlite {
    fn on_conflict() -> &'static str {
        concat!(
            "ON CONFLICT (id) DO UPDATE SET ",
            "taken_on = EXCLUDED.taken_on, ",
            "revision = EXCLUDED.revision, ",
            "type = EXCLUDED.type"
        )
    }
}

/// Represents a SQLite [event store](event::SqlStore).
pub type EventStore<ID> = event::SqlStore<ID, Sqlite>;

/// Represents a SQLite [snapshot store](snapshot::SqlStore).
pub type SnapshotStore<ID> = snapshot::SqlStore<ID, Sqlite>;

cfg_if::cfg_if! {
    if #[cfg(feature = "sqlite")] {
        mod migration;
        pub use migration::SqliteMigrator;
    }
}
