use crate::{event, snapshot, sql};
use cqrs::{snapshot::Retention, Clock};
use sqlx::{Encode, Postgres, QueryBuilder, Type};
use std::time::UNIX_EPOCH;

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

impl<'a, ID> snapshot::Prune<'a, ID, Postgres> for Postgres
where
    ID: Encode<'a, Postgres> + Type<Postgres>,
{
    fn prune(
        table: &'a sql::Ident<'a>,
        id: &'a ID,
        clock: &'a dyn Clock,
        retention: &'a Retention,
    ) -> sqlx::QueryBuilder<'a, Postgres> {
        let mut delete = QueryBuilder::new("WITH s2 AS (");

        delete
            .push("SELECT id, version FROM ")
            .push(table.quote())
            .push("WHERE id = ")
            .push_bind(id);

        // SAFETY: unwrap is allowed here as before epoch is a bug in the clock
        if let Some(count) = retention.count {
            if let Some(age) = retention.age {
                let taken_on = (clock.now() - age).duration_since(UNIX_EPOCH).unwrap();
                delete
                    .push(" AND taken_on >= ")
                    .push_bind(taken_on.as_secs() as i64);
            }

            delete
                .push(" ORDER BY taken_on DESC OFFSET ")
                .push_bind(count as i16);
        } else if let Some(age) = retention.age {
            let taken_on = (clock.now() - age).duration_since(UNIX_EPOCH).unwrap();
            delete
                .push(" AND taken_on <= ")
                .push_bind(taken_on.as_secs() as i64);
        }

        delete
            .push(") DELETE FROM ")
            .push(table.quote())
            .push(" s1 USING s2 WHERE s1.id = s2.id AND s1.version = s2.version;");

        delete
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
