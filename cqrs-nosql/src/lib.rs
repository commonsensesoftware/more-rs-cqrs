mod version;

pub use version::{NoSqlVersion, NoSqlVersionPart};

#[cfg(feature = "dynamodb")]
/// Provides storage using Amazon DynamoDB.
pub mod dynamodb;

use cfg_if::cfg_if;
use std::{
    error::Error,
    time::{SystemTime, UNIX_EPOCH},
};

cfg_if! {
    if #[cfg(feature = "di")] {
        mod di;
        pub use di::*;
    }
}

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
pub(crate) fn to_secs(timestamp: SystemTime) -> i64 {
    timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}