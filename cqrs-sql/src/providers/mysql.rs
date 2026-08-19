use crate::{
    event, snapshot,
    sql::{self, Delimiters, Provider},
};
use cqrs::{Clock, snapshot::Retention};
use sqlx::{Encode, MySql, QueryBuilder, Type};

impl sql::Provider for MySql {
    // MySQL only treats a double quote as an identifier delimiter when ANSI_QUOTES is among
    // its sql_mode flags, which is neither the default nor something a library can assume,
    // so identifiers are quoted with backticks instead
    fn delimiters() -> Delimiters {
        Delimiters::BACKTICK
    }
}

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
    ) -> sqlx::QueryBuilder<MySql> {
        let mut delete = QueryBuilder::new("WITH s2 AS (");

        delete.push("SELECT id, version");
        snapshot::prune::columns(&mut delete, retention);
        delete
            .push(" FROM ")
            .push(MySql::quote(table))
            .push(" WHERE id = ")
            .push_bind(id)
            .push(") DELETE s1 FROM ")
            .push(MySql::quote(table))
            .push(" s1 INNER JOIN s2 WHERE s1.id = s2.id AND s1.version = s2.version");

        snapshot::prune::stale(&mut delete, "s2.", clock, retention);
        delete.push(';');
        delete
    }
}

/// Represents a MySql [event store](event::SqlStore).
pub type EventStore<ID> = event::SqlStore<ID, MySql>;

/// Represents a MySql [snapshot store](snapshot::SqlStore).
pub type SnapshotStore<ID> = snapshot::SqlStore<ID, MySql>;

cfg_select! {
    feature = "migrate" => {
        mod migration;
        pub use migration::Migrator;
    }
    _ => {}
}
