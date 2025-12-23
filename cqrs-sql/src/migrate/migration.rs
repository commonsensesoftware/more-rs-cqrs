use futures::future::{self, BoxFuture};
use sqlx::{
    error::BoxDynError,
    migrate::{Migrate, MigrateError, Migration, MigrationSource, Migrator},
    pool::PoolOptions,
    Database, Pool,
};
use std::borrow::Cow;

enum Either<DB: Database> {
    Pool(Pool<DB>),
    Options(PoolOptions<DB>),
}

#[derive(Debug)]
struct Source(Migration);

impl<'s> MigrationSource<'s> for Source {
    fn resolve(self) -> BoxFuture<'s, Result<Vec<Migration>, BoxDynError>> {
        Box::pin(future::ready(Ok(vec![self.0])))
    }
}

/// Represents the migration for a SQL-based storage.
pub struct SqlStoreMigration<DB: Database> {
    migration: Migration,
    url: String,
    either: Either<DB>,
}

impl<DB> SqlStoreMigration<DB>
where
    DB: Database,
    DB::Connection: Migrate,
{
    /// Initializes a new [SqlStoreMigration].
    ///
    ///  # Argument
    ///
    /// * `migration` - the migration to execute
    /// * `url` - the URL representing the database connection string
    /// * `options` - the [connection pool options](PoolOptions) used during the migration
    pub fn new<M, S>(migration: M, url: S, options: PoolOptions<DB>) -> Self
    where
        M: Into<Migration>,
        S: AsRef<str>,
    {
        Self {
            migration: migration.into(),
            url: url.as_ref().into(),
            either: Either::Options(options),
        }
    }

    /// Initializes a new [SqlStoreMigration].
    ///
    ///  # Argument
    ///
    /// * `migration` - the migration to execute
    /// * `pool` - the [connection pool](Pool) used during the migration
    pub fn with_pool(migration: impl Into<Migration>, pool: Pool<DB>) -> Self {
        Self {
            migration: migration.into(),
            url: Default::default(),
            either: Either::Pool(pool),
        }
    }

    /// Gets the URL representing the database connection string.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Gets the migration version.
    pub fn version(&self) -> i64 {
        self.migration.version
    }

    /// Merges one migration into another.
    ///
    /// # Arguments
    ///
    /// * `other` - the other [migration](SqlStoreMigration) to merge
    pub fn merge(&mut self, other: Self) {
        let m1 = &self.migration;
        let m2 = other.migration;

        self.migration = Migration::new(
            m1.version,
            Cow::Owned(format!("{} {}", m1.description, m2.description)),
            m1.migration_type,
            Cow::Owned(format!("{}\n{}", m1.sql, m2.sql)),
            m1.no_tx,
        )
    }

    /// Runs the migration.
    pub async fn run(self) -> Result<(), MigrateError> {
        let migrator = Migrator::new(Source(self.migration)).await?;
        let pool = match self.either {
            Either::Pool(pool) => pool,
            Either::Options(options) => options.connect(&self.url).await?,
        };
        migrator.run(&pool).await
    }
}

impl<DB: Database> From<SqlStoreMigration<DB>> for Migration {
    fn from(value: SqlStoreMigration<DB>) -> Self {
        value.migration
    }
}
