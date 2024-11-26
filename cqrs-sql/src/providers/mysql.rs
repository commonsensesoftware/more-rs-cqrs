use crate::{event, snapshot, sql};
use cqrs::{snapshot::Retention, Clock};
use sqlx::{Encode, MySql, QueryBuilder, Type};
use std::time::UNIX_EPOCH;

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

impl<'a, ID> snapshot::Prune<'a, ID, MySql> for MySql
where
    ID: Encode<'a, MySql> + Type<MySql>,
{
    fn prune(
        table: &'a sql::Ident<'a>,
        id: &'a ID,
        clock: &'a dyn Clock,
        retention: &'a Retention,
    ) -> sqlx::QueryBuilder<'a, MySql> {
        let mut delete = QueryBuilder::new("WITH s2 AS (");

        delete
            .push("SELECT id, version FROM ")
            .push(table.quote())
            .push("WHERE id = ")
            .push_bind(id);

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
                .push(" ORDER BY taken_on DESC LIMIT 18446744073709551615 OFFSET ")
                .push_bind(count as i16);
        } else if let Some(age) = retention.age {
            let taken_on = (clock.now() - age).duration_since(UNIX_EPOCH).unwrap();
            delete
                .push(" AND taken_on <= ")
                .push_bind(taken_on.as_secs() as i64);
        }

        delete
            .push(") DELETE s1 FROM ")
            .push(table.quote())
            .push(" s1 INNER JOIN s2 WHERE s1.id = s2.id AND s1.version = s2.version;");

        delete
    }
}

/// Represents a MySql [event store](event::SqlStore).
pub type EventStore<ID> = event::SqlStore<ID, MySql>;

/// Represents a MySql [snapshot store](snapshot::SqlStore).
pub type SnapshotStore<ID> = snapshot::SqlStore<ID, MySql>;

cfg_if::cfg_if! {
    if #[cfg(feature = "migrate")] {
        mod migration;
        pub use migration::Migrator;
    }
}
