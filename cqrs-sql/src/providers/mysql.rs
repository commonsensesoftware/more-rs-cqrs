use crate::{event, snapshot};
use sqlx::MySql;

impl snapshot::Upsert for MySql {
    fn on_conflict() -> &'static str {
        concat!(
            "ON DUPLICATE KEY UPDATE ",
            "taken_on = VALUES(taken_on), ",
            "revision = VALUES(revision), ",
            "type = VALUES(type)"
        )
    }
}

/// Represents a MySql [event store](event::SqlStore).
pub type EventStore<ID> = event::SqlStore<ID, MySql>;

/// Represents a MySql [snapshot store](snapshot::SqlStore).
pub type SnapshotStore<ID> = snapshot::SqlStore<ID, MySql>;

cfg_if::cfg_if! {
    if #[cfg(feature = "migrate")] {
        mod migration;
        pub use migration::MySqlMigrator;
    }
}
