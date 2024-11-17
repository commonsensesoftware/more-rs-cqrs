mod event_command;
mod event_store;
mod snapshot_command;
mod snapshot_store;

pub use event_store::EventStore;
pub use snapshot_store::SnapshotStore;

impl crate::snapshot::Upsert for sqlx::Sqlite {
    fn on_conflict() -> &'static str {
        concat!(
            "ON CONFLICT (id) DO UPDATE SET ",
            "taken_on = EXCLUDED.taken_on, ",
            "revision = EXCLUDED.revision, ",
            "type = EXCLUDED.type"
        )
    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "migrate")] {
        mod migration;
        pub use migration::SqliteMigrator;
    }
}
