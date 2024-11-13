use futures::future::{self, BoxFuture};
use sqlx::{
    error::BoxDynError,
    migrate::{Migrate, MigrateError, Migration, MigrationSource, Migrator},
    pool::PoolOptions,
    Database,
};

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
    options: PoolOptions<DB>,
}

impl<DB> SqlStoreMigration<DB>
where
    DB: Database,
    DB::Connection: Migrate,
{
    /// Initializes a new [`SqlStoreMigration`].
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
            options,
        }
    }

    /// Runs the migration.
    pub async fn run(self) -> Result<(), MigrateError> {
        let migrator = Migrator::new(Source(self.migration)).await?;
        let pool = self.options.connect(&self.url).await?;
        migrator.run(&pool).await
    }
}
