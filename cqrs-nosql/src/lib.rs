mod prune;
mod version;

#[cfg(any(feature = "cosmosdb", feature = "dynamodb"))]
mod append;

#[cfg(any(feature = "cosmosdb", feature = "dynamodb"))]
mod bound;

#[cfg(any(feature = "cosmosdb", feature = "dynamodb"))]
mod snapshot;

pub use version::{NoSqlVersion, NoSqlVersionPart};

#[cfg(feature = "cosmosdb")]
/// Provides storage using Azure Cosmos DB.
pub mod cosmosdb;

#[cfg(feature = "dynamodb")]
/// Provides storage using Amazon DynamoDB.
pub mod dynamodb;

/// Contains library prelude.
pub mod prelude;

use std::{
    error::Error,
    time::{SystemTime, UNIX_EPOCH},
};

#[allow(dead_code)]
pub(crate) trait BoxErr<T> {
    fn box_err(self) -> Result<T, Box<dyn Error + Send>>;
}

impl<T, E: Error + Send + 'static> BoxErr<T> for Result<T, E> {
    fn box_err(self) -> Result<T, Box<dyn Error + Send>> {
        self.map_err(|e| Box::new(e) as Box<dyn Error + Send>)
    }
}

#[allow(dead_code)]
pub(crate) fn to_secs(timestamp: SystemTime) -> u64 {
    timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs()
}
