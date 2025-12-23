use cfg_if::cfg_if;

mod options;

pub use options::SqlOptions;

type DynEventStore<ID> = dyn cqrs::event::Store<ID>;
type DynSnapshotStore<ID> = dyn cqrs::snapshot::Store<ID>;

use ::di::Ref;
use ::options::OptionsSnapshot;
use cqrs::message::Message;
use sqlx::{pool::PoolOptions, Database};

fn merge<ID, M, DB>(
    mut builder: crate::SqlStoreBuilder<ID, M, DB>,
    name: &str,
    url: Option<&str>,
    cfg_options: Option<&PoolOptions<DB>>,
    di_options: Option<&Ref<dyn OptionsSnapshot<SqlOptions<DB>>>>,
) -> crate::SqlStoreBuilder<ID, M, DB>
where
    M: Message + ?Sized,
    DB: Database,
{
    if let Some(options) = cfg_options {
        builder = builder.options(options.clone());
    } else if let Some(snapshot) = di_options {
        let db = snapshot.get(Some(name));
        builder = builder.options(db.options.clone());
    }

    if let Some(url) = url {
        builder = builder.url(url);
    } else if let Some(snapshot) = di_options {
        let db = snapshot.get(Some(name));

        if !db.url.is_empty() {
            builder = builder.url(db.url.clone());
        }
    }

    builder
}

cfg_if! {
    if #[cfg(any(feature = "mysql", feature = "postgres"))] {
        mod builder;
        pub use builder::{SqlStoreBuilder, SqlStoreOptionsBuilder};
    }
}

cfg_if! {
    if #[cfg(all(any(feature = "mysql", feature = "postgres"), feature = "migrate"))] {
        mod migration;
        pub use migration::SqlMigrationsBuilder;
    }
}

cfg_if! {
    if #[cfg(feature = "mysql")] {
        mod mysql;
        pub use mysql::MySqlExt;
    }
}

cfg_if! {
    if #[cfg(feature = "postgres")] {
        mod postgres;
        pub use postgres::PostgresExt;
    }
}

cfg_if! {
    if #[cfg(feature = "sqlite")] {
        mod sqlite;
        pub use sqlite::SqliteExt;
    }
}
