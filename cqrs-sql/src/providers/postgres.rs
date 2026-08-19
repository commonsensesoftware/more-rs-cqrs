use crate::{
    event, snapshot,
    sql::{self, Provider},
};
use cqrs::{Clock, snapshot::Retention};
use sqlx::{Encode, Postgres, QueryBuilder, Type};

impl sql::Provider for Postgres {}

impl snapshot::Upsert for Postgres {
    fn on_conflict() -> &'static str {
        concat!(
            "ON CONFLICT (id, version) DO UPDATE SET ",
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
    ) -> sqlx::QueryBuilder<Postgres> {
        let mut delete = QueryBuilder::new("WITH s2 AS (");

        delete.push("SELECT id, version");
        snapshot::prune::columns(&mut delete, retention);
        delete
            .push(" FROM ")
            .push(Postgres::quote(table))
            .push(" WHERE id = ")
            .push_bind(id)
            .push(") DELETE FROM ")
            .push(Postgres::quote(table))
            .push(" s1 USING s2 WHERE s1.id = s2.id AND s1.version = s2.version");

        snapshot::prune::stale(&mut delete, "s2.", clock, retention);
        delete.push(';');
        delete
    }
}

/// Represents a Postgres [event store](event::SqlStore).
pub type EventStore<ID> = event::SqlStore<ID, Postgres>;

/// Represents a Postgres [snapshot store](snapshot::SqlStore).
pub type SnapshotStore<ID> = snapshot::SqlStore<ID, Postgres>;

cfg_select! {
    feature = "migrate" => {
        mod migration;
        pub use migration::Migrator;
    }
    _ => {}
}
