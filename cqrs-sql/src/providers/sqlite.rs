mod event_command;
mod event_store;
mod snapshot_command;
mod snapshot_store;

pub use event_store::EventStore;
pub use snapshot_store::SnapshotStore;

use crate::{snapshot, sql};
use cqrs::{snapshot::Retention, Clock};
use sqlx::{Encode, QueryBuilder, Sqlite, Type};
use std::time::UNIX_EPOCH;

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

impl<'a, ID> snapshot::Prune<'a, ID, Sqlite> for Sqlite
where
    ID: Encode<'a, Sqlite> + Type<Sqlite>,
{
    fn prune(
        table: &'a sql::Ident<'a>,
        id: &'a ID,
        clock: &'a dyn Clock,
        retention: &'a Retention,
    ) -> sqlx::QueryBuilder<'a, Sqlite> {
        let mut delete = QueryBuilder::new("DELETE FROM ");

        delete.push(table.quote()).push("WHERE id = ").push_bind(id);

        // SAFETY: unwrap is allowed here as before epoch is a bug in the clock
        // LIMIT must be specified so use the largest possible value
        if let Some(count) = retention.count {
            if let Some(age) = retention.age {
                let taken_on = (clock.now() - age).duration_since(UNIX_EPOCH).unwrap();
                delete
                    .push(" AND taken_on >= ")
                    .push_bind(taken_on.as_secs() as i64);
            }

            delete
                .push(" ORDER BY taken_on DESC LIMIT 2305843009213693951 OFFSET ")
                .push_bind(count as i16);
        } else if let Some(age) = retention.age {
            let taken_on = (clock.now() - age).duration_since(UNIX_EPOCH).unwrap();
            delete
                .push(" AND taken_on <= ")
                .push_bind(taken_on.as_secs() as i64);
        }

        delete.push(';');
        delete
    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "migrate")] {
        mod migration;
        pub use migration::SqliteMigrator;
    }
}
