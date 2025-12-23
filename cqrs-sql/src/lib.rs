mod builder;

/// Provides event storage using a SQL database.
pub mod event;

/// Provides snapshot storage using a SQL database.
pub mod snapshot;

/// Provides SQL-specific functionality.
pub mod sql;

mod version;

pub use builder::{SqlStoreBuilder, SqlStoreBuilderError};
pub(crate) use version::new_version;
pub use version::{SqlVersion, SqlVersionDisplay, SqlVersionPart};

#[cfg(any(feature = "mysql", feature = "postgres", feature = "sqlite"))]
mod providers;

/// Contains library prelude.
pub mod prelude;

#[cfg(feature = "mysql")]
/// Provides storage using MySQL.
pub use providers::mysql;

#[cfg(feature = "postgres")]
/// Provides storage using Postgres.
pub use providers::postgres;

#[cfg(feature = "sqlite")]
/// Provides storage using SQLite.
pub use providers::sqlite;

use cfg_if::cfg_if;
use std::{
    error::Error,
    time::{SystemTime, UNIX_EPOCH},
};

cfg_if! {
    if #[cfg(feature = "migrate")] {
        mod migrate;
        pub use migrate::{SqlStoreMigrator, SqlStoreMigration};
    }
}

pub(crate) trait BoxErr<T> {
    fn box_err(self) -> Result<T, Box<dyn Error + Send>>;
}

impl<T, E: Error + Send + 'static> BoxErr<T> for Result<T, E> {
    fn box_err(self) -> Result<T, Box<dyn Error + Send>> {
        self.map_err(|e| Box::new(e) as Box<dyn Error + Send>)
    }
}

pub(crate) fn to_secs(timestamp: SystemTime) -> i64 {
    timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}
