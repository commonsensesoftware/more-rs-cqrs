use async_trait::async_trait;
use std::{
    error::Error,
    sync::{Arc, Mutex},
};

/// Defines the behavior to migrate storage.
#[async_trait]
pub trait StoreMigration: Send + Sync {
    /// Runs the storage migration.
    async fn run(&self) -> Result<(), Box<dyn Error + 'static>>;
}

/// Represents a storage migrator.
#[derive(Default)]
pub struct StoreMigrator {
    migrations: Mutex<Vec<Arc<dyn StoreMigration>>>,
}

impl StoreMigrator {
    /// Initializes a new [StoreMigrator].
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a new migration.
    ///
    /// # Arguments
    ///
    /// * `migration` - the [migration](StoreMigration) to add
    pub fn add(&self, migration: impl StoreMigration + 'static) {
        self.migrations.lock().unwrap().push(Arc::new(migration));
    }

    /// Runs the configured migrations.
    ///
    /// # Remarks
    ///
    /// The underlying migrations are dropped after each run.
    pub async fn run(&self) -> Result<(), Box<dyn Error + 'static>> {
        let migrations = std::mem::take(&mut *self.migrations.lock().unwrap());

        for migration in migrations {
            migration.run().await?;
        }

        Ok(())
    }
}

#[cfg_attr(feature = "di", di::injectable)]
impl StoreMigrator {
    #[cfg_attr(feature = "di", di::inject)]
    fn _new(migrations: Vec<di::Ref<dyn StoreMigration>>) -> Self {
        Self {
            migrations: Mutex::new(migrations),
        }
    }
}
