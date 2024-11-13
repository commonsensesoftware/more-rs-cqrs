mod builder;
mod options;

pub use builder::{SqlStoreBuilder, SqlStoreOptionsBuilder};
pub use options::SqlOptions;

use cfg_if::cfg_if;

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

// cfg_if! {
//     if #[cfg(feature = "sqlite")] {
//         mod sqlite;
//         pub use sqlite::SqliteExt;
//     }
// }
