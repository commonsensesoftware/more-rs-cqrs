mod options;

pub use options::SqlOptions;

type DynEventStore<ID> = dyn cqrs::event::Store<ID>;
type DynSnapshotStore<ID> = dyn cqrs::snapshot::Store<ID>;

use ::di::Ref;
use ::options::Snapshot;
use cqrs::message::Message;
use sqlx::{Database, pool::PoolOptions};

fn merge<ID, M, DB>(
    mut builder: crate::SqlStoreBuilder<ID, M, DB>,
    name: &str,
    url: Option<&str>,
    cfg_options: Option<&PoolOptions<DB>>,
    di_options: Option<&Ref<dyn Snapshot<SqlOptions<DB>>>>,
) -> crate::SqlStoreBuilder<ID, M, DB>
where
    M: Message + ?Sized,
    DB: Database,
{
    if let Some(options) = cfg_options {
        builder = builder.options(options.clone());
    } else if let Some(snapshot) = di_options
        && let Ok(db) = snapshot.get_named(name)
    {
        builder = builder.options(db.options.clone());
    }

    if let Some(url) = url {
        builder = builder.url(url);
    } else if let Some(snapshot) = di_options
        && let Ok(db) = snapshot.get_named(name)
    {
        if !db.url.is_empty() {
            builder = builder.url(db.url.clone());
        }
    }

    builder
}

cfg_select! {
    any(feature = "mysql", feature = "postgres") => {
        mod builder;
        pub use builder::{SqlStoreBuilder, SqlStoreOptionsBuilder};
    }
    _ => {}
}

cfg_select! {
    all(any(feature = "mysql", feature = "postgres"), feature = "migrate") => {
        mod migration;
        pub use migration::SqlMigrationsBuilder;
    }
    _ => {}
}

cfg_select! {
    feature = "mysql" => {
        mod mysql;
        pub use mysql::MySqlExt;
    }
    _ => {}
}

cfg_select! {
    feature = "postgres" => {
        mod postgres;
        pub use postgres::PostgresExt;
    }
    _ => {}
}

cfg_select! {
    feature = "sqlite" => {
        mod sqlite;
        pub use sqlite::SqliteExt;
    }
    _ => {}
}
