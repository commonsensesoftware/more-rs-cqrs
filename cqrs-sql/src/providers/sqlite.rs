use crate::{
    event, snapshot,
    sql::{self, Provider},
};
use cqrs::{Clock, snapshot::Retention};
use sqlx::{Encode, QueryBuilder, Sqlite, Type};
use std::time::UNIX_EPOCH;

impl sql::Provider for Sqlite {
    // SQLite has no notion of a schema, so "events"."orders" is folded into events_orders
    fn supports_schemas() -> bool {
        false
    }
}

impl snapshot::Upsert for Sqlite {
    fn on_conflict() -> &'static str {
        concat!(
            "ON CONFLICT (id, version) DO UPDATE SET ",
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
    ) -> sqlx::QueryBuilder<Sqlite> {
        let mut delete = QueryBuilder::new("DELETE FROM ");

        delete.push(Sqlite::quote(table));

        // SAFETY: unwrap is allowed here as before epoch is a bug in the clock
        if let Some(count) = retention.count {
            // ORDER BY and LIMIT are only accepted on DELETE when SQLite is built with
            // SQLITE_ENABLE_UPDATE_DELETE_LIMIT, which is not the default, so the rows to
            // remove are matched by a subquery, where both are always allowed. OFFSET
            // requires LIMIT and -1 is the SQLite idiom for an unbounded one
            delete
                .push(" WHERE (id, version) IN (SELECT id, version FROM ")
                .push(Sqlite::quote(table))
                .push(" WHERE id = ")
                .push_bind(id);

            if let Some(age) = retention.age {
                let taken_on = (clock.now() - age).duration_since(UNIX_EPOCH).unwrap();
                delete.push(" AND taken_on >= ").push_bind(taken_on.as_secs() as i64);
            }

            delete
                .push(" ORDER BY taken_on DESC LIMIT -1 OFFSET ")
                .push_bind(count as i16)
                .push(')');
        } else {
            delete.push(" WHERE id = ").push_bind(id);

            if let Some(age) = retention.age {
                let taken_on = (clock.now() - age).duration_since(UNIX_EPOCH).unwrap();
                delete.push(" AND taken_on <= ").push_bind(taken_on.as_secs() as i64);
            }
        }

        delete.push(';');
        delete
    }
}

/// Represents a SQLite [event store](event::SqlStore).
pub type EventStore<ID> = event::SqlStore<ID, Sqlite>;

/// Represents a SQLite [snapshot store](snapshot::SqlStore).
pub type SnapshotStore<ID> = snapshot::SqlStore<ID, Sqlite>;

cfg_select! {
    feature = "migrate" => {
        mod migration;
        pub use migration::Migrator;
    }
    _ => {}
}
