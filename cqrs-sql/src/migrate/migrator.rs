use super::migration::SqlStoreMigration;
use async_trait::async_trait;
use cqrs::StoreMigration;
use sqlx::{
    migrate::{Migrate, MigrateError, Migration},
    pool::PoolOptions,
    Database,
};
use std::{collections::HashMap, error::Error, sync::Mutex};

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
    /// Initializes new [SqlStoreMigrator]
    pub fn new() -> Self {
        Self::default()
    }

    /// Configures a new migration.
    ///
    /// # Arguments
    ///
    /// * `migration` - the [migration](Migration) to configure
    /// * `url` - the URL representing the database connection string
    /// * `options` - the [connection pool options](PoolOptions) to use during the migration
    #[inline]
    pub fn configure<M, S>(&self, migration: M, url: S, options: PoolOptions<DB>)
    where
        M: Into<Migration>,
        S: AsRef<str>,
        DB: Database,
        DB::Connection: Migrate,
    {
        self.add(SqlStoreMigration::new(migration, url, options));
    }

    /// Adds a migration.
    /// 
    /// # Arguments
    /// 
    /// * `migration` - the [migration](SqlStoreMigration) to add
    pub fn add(&self, migration: SqlStoreMigration<DB>)
    where
        DB: Database,
        DB::Connection: Migrate,
    {
        let mut migrations = self.migrations.lock().unwrap();

        for existing in migrations.iter_mut() {
            if existing.version() == migration.version() && existing.url() == migration.url() {
                existing.merge(migration);
                return;
            }
        }

        migrations.push(migration);
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
    DB::Connection: Migrate,
{
    async fn run(&self) -> Result<(), Box<dyn Error + 'static>> {
        self.run()
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error + 'static>)
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
                let mut migrations: Vec<_> = migrations.filter_map(Ref::into_inner).collect();
                let count = migrations.len();
                let mut buckets: HashMap<String, SqlStoreMigration<DB>> = HashMap::with_capacity(count);

                for migration in migrations.drain(..) {
                    let key = migration.url().to_owned();

                    if buckets.contains_key(&key) {
                        buckets.entry(key).and_modify(|m| m.merge(migration));
                    } else {
                        buckets.entry(key).or_insert(migration);
                    }
                }

                Self {
                    migrations: Mutex::new(buckets.into_values().collect()),
                }
            }
        }
    }
}
