use super::migration::SqlStoreMigration;
use async_trait::async_trait;
use cqrs::StoreMigration;
use sqlx::{
    migrate::{Migrate, MigrateError, Migration},
    pool::PoolOptions,
    Database,
};
use std::{error::Error, sync::Mutex};

/// Represents a SQL-base storage migrator.
pub struct SqlStoreMigrator<DB>
where
    DB: Database,
    DB::Connection: Migrate,
{
    migrations: Mutex<Vec<SqlStoreMigration<DB>>>,
}

impl<DB> Default for SqlStoreMigrator<DB>
where
    DB: Database,
    DB::Connection: Migrate,
{
    fn default() -> Self {
        Self {
            migrations: Default::default(),
        }
    }
}

impl<DB> SqlStoreMigrator<DB>
where
    DB: Database,
    DB::Connection: Migrate,
{
    /// Initializes new [`SqlStoreMigrator`]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a new migration.
    ///
    /// # Arguments
    ///
    /// * `migration` - the [migration](Migration) to add
    /// * `url` - the URL representing the database connection string
    /// * `options` - the [connection pool options](PoolOptions) to use during the migration
    pub fn add<M, S>(&self, migration: M, url: S, options: PoolOptions<DB>)
    where
        M: Into<Migration>,
        S: AsRef<str>,
        DB: Database,
        DB::Connection: Migrate,
    {
        let migration = SqlStoreMigration::new(migration, url, options);
        self.migrations.lock().unwrap().push(migration);
    }

    /// Runs the configured migrations.
    ///
    /// # Remarks
    ///
    /// The underlying migrations are dropped after each run.
    pub async fn run(&self) -> Result<(), MigrateError> {
        let migrations = std::mem::take(&mut *self.migrations.lock().unwrap());

        for migration in migrations {
            migration.run().await?;
        }

        Ok(())
    }
}

#[async_trait]
impl<DB> StoreMigration for SqlStoreMigrator<DB>
where
    DB: Database,
    DB::Connection: Migrate
{
    async fn run(&self) -> Result<(), Box<dyn Error + 'static>> {
        self.run().await.map_err(|e| Box::new(e) as Box<dyn Error + 'static>)
    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "di")] {
        use di::{inject, injectable, Ref};

        #[injectable(StoreMigration)]
        impl<DB> SqlStoreMigrator<DB>
        where
            DB: Database,
            DB::Connection: Migrate,
        {
            #[inject]
            fn _new(migrations: impl Iterator<Item = Ref<SqlStoreMigration<DB>>>) -> Self {
                Self {
                    migrations: Mutex::new(migrations.filter_map(Ref::into_inner).collect())
                }
            }
        }
    }
}
