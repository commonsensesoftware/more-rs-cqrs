use crate::SqlStoreMigrator;
use crate::{
    postgres,
    sql::{Ident, IdentPart::Schema},
};
use sqlx::{
    migrate::{Migration, MigrationType::Simple},
    Postgres,
};
use std::any::type_name;
use std::borrow::Cow;
use std::mem::size_of;

/// Represents a Postgres [migrator](SqlStoreMigrator).
pub type Migrator = SqlStoreMigrator<Postgres>;

impl<ID> From<&postgres::EventStore<ID>> for Migration {
    fn from(value: &postgres::EventStore<ID>) -> Self {
        Self::new(
            1,
            Cow::Owned(format!("'{}' events table.", value.table.name())),
            Simple,
            Cow::Owned(events_table(&value.table, db_type::<ID>())),
            false,
        )
    }
}

impl<ID> From<&postgres::SnapshotStore<ID>> for Migration {
    fn from(value: &postgres::SnapshotStore<ID>) -> Self {
        Self::new(
            1,
            Cow::Owned(format!("'{}' snapshots table.", value.table.name())),
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
        "i8" | "u8" | "i16" | "u16" => "SMALLINT",
        "i32" | "u32" => "INTEGER",
        "i64" | "u64" => "BIGINT",
        "isize" | "usize" => match size_of::<isize>() {
            4 => "INTEGER",
            8 => "BIGINT",
            _ => unreachable!(),
        },
        "uuid::Uuid" => "UUID",
        "alloc::string::String" => "VARCHAR(50)",
        _ => panic!("type '{}' is not a supported database type", name),
    }
}

fn events_table(table: &Ident, db_type: &str) -> String {
    let mut sql = String::new();

    if let Some(schema) = table.quote_part(Schema) {
        sql.push_str("CREATE SCHEMA IF NOT EXISTS ");
        sql.push_str(&schema);
        sql.push_str(";\n");
    }

    sql.push_str("CREATE TABLE IF NOT EXISTS ");
    sql.push_str(&table.quote());
    sql.push('(');
    sql.push_str("id ");
    sql.push_str(db_type);
    sql.push_str(" NOT NULL, ");
    sql.push_str("version INTEGER NOT NULL, ");
    sql.push_str("sequence SMALLINT NOT NULL, ");
    sql.push_str("revision SMALLINT NOT NULL, ");
    sql.push_str("stored_on BIGINT NOT NULL, ");
    sql.push_str("type VARCHAR(128) NOT NULL, ");
    sql.push_str("content BYTEA NOT NULL, ");
    sql.push_str("correlation_id VARCHAR(50) DEFAULT NULL, ");
    sql.push_str("PRIMARY KEY(id, version, sequence)");
    sql.push_str(");");

    sql
}

fn snapshots_table(table: &Ident, db_type: &str) -> String {
    let mut sql = String::new();

    if let Some(schema) = table.quote_part(Schema) {
        sql.push_str("CREATE SCHEMA IF NOT EXISTS ");
        sql.push_str(&schema);
        sql.push_str(";\n");
    }

    sql.push_str("CREATE TABLE IF NOT EXISTS ");
    sql.push_str(&table.quote());
    sql.push('(');
    sql.push_str("id ");
    sql.push_str(db_type);
    sql.push_str(" NOT NULL, ");
    sql.push_str("version INTEGER NOT NULL, ");
    sql.push_str("revision SMALLINT NOT NULL, ");
    sql.push_str("taken_on BIGINT NOT NULL, ");
    sql.push_str("type VARCHAR(128) NOT NULL, ");
    sql.push_str("content BYTEA NOT NULL, ");
    sql.push_str("correlation_id VARCHAR(50) DEFAULT NULL, ");
    sql.push_str("PRIMARY KEY(id, version)");
    sql.push_str(");");

    sql
}
