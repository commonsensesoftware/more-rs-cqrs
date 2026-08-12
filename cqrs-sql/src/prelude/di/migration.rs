use super::SqlStoreOptionsBuilder;
use crate::{
    SqlStoreMigration, SqlStoreMigrator, event,
    snapshot::{self, Upsert},
    sql::Provider,
};
use cqrs::Aggregate;
use di::{Injectable, Ref, transient_as_self, zero_or_one_with_key};
use sqlx::{
    ColumnIndex, Database, Decode, Encode, Executor, FromRow, IntoArguments, Type,
    migrate::{Migrate, Migration},
};

/// Represents the configuration for SQL storage migration.
pub struct SqlMigrationsBuilder<'a, A, DB>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Sync + Type<DB>,
    DB: Database + Provider + Upsert,
    <DB as Database>::Connection: Migrate,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'db> &'db mut <DB as Database>::Connection: Executor<'db, Database = DB>,
    i16: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i32: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i64: for<'db> Encode<'db, DB> + Type<DB>,
    usize: ColumnIndex<<DB as Database>::Row>,
    String: for<'db> Encode<'db, DB> + Type<DB>,
    for<'db> &'db str: Decode<'db, DB> + Type<DB>,
    for<'db> &'db [u8]: Encode<'db, DB> + Decode<'db, DB> + Type<DB>,
    for<'c> &'c event::SqlStore<A::ID, DB>: Into<Migration>,
    for<'c> &'c snapshot::SqlStore<A::ID, DB>: Into<Migration>,
    (bool,): for<'db> FromRow<'db, DB::Row>,
{
    parent: SqlStoreOptionsBuilder<'a, A, DB>,
}

impl<'a, A, DB> SqlMigrationsBuilder<'a, A, DB>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Sync + Type<DB>,
    DB: Database + Provider + Upsert,
    <DB as Database>::Connection: Migrate,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'db> &'db mut <DB as Database>::Connection: Executor<'db, Database = DB>,
    i16: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i32: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i64: for<'db> Encode<'db, DB> + Type<DB>,
    usize: ColumnIndex<<DB as Database>::Row>,
    String: for<'db> Encode<'db, DB> + Type<DB>,
    for<'db> &'db str: Decode<'db, DB> + Type<DB>,
    for<'db> &'db [u8]: Encode<'db, DB> + Decode<'db, DB> + Type<DB>,
    for<'c> &'c event::SqlStore<A::ID, DB>: Into<Migration>,
    for<'c> &'c snapshot::SqlStore<A::ID, DB>: Into<Migration>,
    (bool,): for<'db> FromRow<'db, DB::Row>,
{
    pub(crate) fn new(parent: SqlStoreOptionsBuilder<'a, A, DB>) -> Self {
        parent
            .parent
            .services
            .try_add_to_all(SqlStoreMigrator::<DB>::transient());
        Self { parent }
    }
}

impl<'a, A, DB> Drop for SqlMigrationsBuilder<'a, A, DB>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Sync + Type<DB>,
    DB: Database + Provider + Upsert,
    <DB as Database>::Connection: Migrate,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'db> &'db mut <DB as Database>::Connection: Executor<'db, Database = DB>,
    i16: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i32: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i64: for<'db> Encode<'db, DB> + Type<DB>,
    usize: ColumnIndex<<DB as Database>::Row>,
    String: for<'db> Encode<'db, DB> + Type<DB>,
    for<'db> &'db str: Decode<'db, DB> + Type<DB>,
    for<'db> &'db [u8]: Encode<'db, DB> + Decode<'db, DB> + Type<DB>,
    for<'c> &'c event::SqlStore<A::ID, DB>: Into<Migration>,
    for<'c> &'c snapshot::SqlStore<A::ID, DB>: Into<Migration>,
    (bool,): for<'db> FromRow<'db, DB::Row>,
{
    fn drop(&mut self) {
        // the migration reuses the store's own connection pool rather than opening a second one. this is required for a
        // database such as SQLite, where connecting to 'sqlite::memory:' a second time yields a different, empty db
        self.parent.parent.services.add(
            transient_as_self::<SqlStoreMigration<DB>>()
                .depends_on(zero_or_one_with_key::<A, event::SqlStore<A::ID, DB>>())
                .from(move |sp| {
                    let store = sp.get_required_by_key::<A, event::SqlStore<A::ID, DB>>();
                    let migration = SqlStoreMigration::with_pool(&*store, store.pool.clone());

                    Ref::new(migration)
                }),
        );

        if !self.parent.use_snapshots {
            return;
        }

        self.parent.parent.services.add(
            transient_as_self::<SqlStoreMigration<DB>>()
                .depends_on(zero_or_one_with_key::<A, snapshot::SqlStore<A::ID, DB>>())
                .from(move |sp| {
                    let store = sp.get_required_by_key::<A, snapshot::SqlStore<A::ID, DB>>();
                    let migration = SqlStoreMigration::with_pool(&*store, store.pool.clone());

                    Ref::new(migration)
                }),
        );
    }
}
