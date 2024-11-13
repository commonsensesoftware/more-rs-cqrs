use crate::SqlStoreMigrator;
use crate::{sqlite, sql::Ident};
use sqlx::{
    migrate::{Migration, MigrationType::Simple},
    Sqlite,
};
use std::any::type_name;
use std::borrow::Cow;

/// Represents a SQLite [migrator](SqlStoreMigrator).
pub type SqliteMigrator = SqlStoreMigrator<Sqlite>;

impl<ID> From<&sqlite::EventStore<ID>> for Migration {
    fn from(value: &sqlite::EventStore<ID>) -> Self {
        Self::new(
            1,
            Cow::Owned(format!("'{}' events table", value.table.name())),
            Simple,
            Cow::Owned(events_table(&value.table, db_type::<ID>())),
            false,
        )
    }
}

impl<ID> From<&sqlite::SnapshotStore<ID>> for Migration {
    fn from(value: &sqlite::SnapshotStore<ID>) -> Self {
        Self::new(
            1,
            Cow::Owned(format!("'{}' snapshots table", value.table.name())),
            Simple,
            Cow::Owned(snapshots_table(&value.table, db_type::<ID>())),
            false,
        )
    }
}

#[inline]
fn db_type<ID>() -> &'static str {
    let name = type_name::<ID>();

    match name {
        "i8" | "u8" | "i16" | "u16" | "i32" | "u32" | "i64" | "u64" | "isize" | "usize" => {
            "INTEGER"
        }
        "uuid::Uuid" => "BLOB",
        "alloc::string::String" => "TEXT",
        _ => panic!("type '{}' is not a supported database type", name),
    }
}

fn events_table(table: &Ident, db_type: &str) -> String {
    let mut sql = String::new();

    sql.push_str("CREATE TABLE IF NOT EXISTS ");
    sql.push_str(&table.as_object_name());
    sql.push('(');
    sql.push_str("id ");
    sql.push_str(db_type);
    sql.push_str(" NOT NULL, ");
    sql.push_str("version INTEGER NOT NULL, ");
    sql.push_str("sequence INTEGER NOT NULL, ");
    sql.push_str("revision INTEGER NOT NULL, ");
    sql.push_str("stored_on INTEGER NOT NULL, ");
    sql.push_str("type TEXT NOT NULL, ");
    sql.push_str("content BLOB NOT NULL, ");
    sql.push_str("PRIMARY KEY (id, version, sequence)");
    sql.push_str(");");

    sql
}

fn snapshots_table(table: &Ident, db_type: &str) -> String {
    let mut sql = String::new();

    sql.push_str("CREATE TABLE IF NOT EXISTS ");
    sql.push_str(&table.as_object_name());
    sql.push('(');
    sql.push_str("id ");
    sql.push_str(db_type);
    sql.push_str(" NOT NULL, ");
    sql.push_str("version INTEGER NOT NULL, ");
    sql.push_str("revision INTEGER NOT NULL, ");
    sql.push_str("taken_on INTEGER NOT NULL, ");
    sql.push_str("type TEXT NOT NULL, ");
    sql.push_str("content BLOB NOT NULL, ");
    sql.push_str("correlation_id TEXT DEFAULT NULL, ");
    sql.push_str("PRIMARY KEY (id, version)");
    sql.push_str(");");

    sql
}
