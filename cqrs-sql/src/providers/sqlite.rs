use crate::{
    event, snapshot,
    sql::{self, Provider},
};
use cqrs::{Clock, snapshot::Retention};
use sqlx::{Encode, QueryBuilder, Sqlite, Type};

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

        // ORDER BY and LIMIT are only accepted on DELETE when SQLite is built with SQLITE_ENABLE_UPDATE_DELETE_LIMIT,
        // which is not the default, so match by a subquery, where the retention is always allowed
        delete
            .push(Sqlite::quote(table))
            .push(" WHERE (id, version) IN (SELECT id, version FROM (SELECT id, version");

        snapshot::prune::columns(&mut delete, retention);

        delete
            .push(" FROM ")
            .push(Sqlite::quote(table))
            .push(" WHERE id = ")
            .push_bind(id)
            .push(") WHERE id = ")
            .push_bind(id);

        snapshot::prune::stale(&mut delete, "", clock, retention);
        delete.push(");");
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
