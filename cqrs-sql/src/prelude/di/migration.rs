use super::{DynSnapshotStore, SqlOptions, SqlStoreOptionsBuilder, merge};
use crate::{
    SqlStoreMigration, SqlStoreMigrator, event,
    snapshot::{self, Upsert},
};
use cqrs::{Aggregate, Clock, event::Event, message::Transcoder};
use di::{Injectable, Ref, exactly_one, transient_as_self, zero_or_one, zero_or_one_with_key};
use options::OptionsSnapshot;
use sqlx::{
    ColumnIndex, Database, Decode, Encode, Executor, FromRow, IntoArguments, Type,
    migrate::{Migrate, Migration},
};

/// Represents the configuration for SQL storage migration.
pub struct SqlMigrationsBuilder<'a, A, DB>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Sync + Type<DB>,
    DB: Database + Upsert,
    <DB as Database>::Connection: Migrate,
    for<'args, 'db> <DB as Database>::Arguments<'args>: IntoArguments<'db, DB>,
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
    DB: Database + Upsert,
    <DB as Database>::Connection: Migrate,
    for<'args, 'db> <DB as Database>::Arguments<'args>: IntoArguments<'db, DB>,
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
    DB: Database + Upsert,
    <DB as Database>::Connection: Migrate,
    for<'args, 'db> <DB as Database>::Arguments<'args>: IntoArguments<'db, DB>,
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
        let name = self.parent.parent.name;
        let url = self.parent.url.clone();
        let cfg_options = self.parent.options.clone();

        self.parent.parent.services.add(
            transient_as_self::<SqlStoreMigration<DB>>()
                .depends_on(exactly_one::<dyn Clock>())
                .depends_on(exactly_one::<Transcoder<dyn Event>>())
                .depends_on(zero_or_one::<dyn OptionsSnapshot<SqlOptions<DB>>>())
                .from(move |sp| {
                    let di_options = sp.get::<dyn OptionsSnapshot<SqlOptions<DB>>>();
                    let builder = merge(
                        event::SqlStore::<A::ID, DB>::builder()
                            .table(name)
                            .clock(sp.get_required::<dyn Clock>())
                            .transcoder(sp.get_required::<Transcoder<dyn Event>>()),
                        name,
                        url.as_deref(),
                        cfg_options.as_ref(),
                        di_options.as_ref(),
                    );
                    let url = builder.url.clone().unwrap_or_default();
                    let options = builder.options.clone().unwrap_or_default();
                    let store = builder.build().unwrap();
                    let migration = SqlStoreMigration::new(&store, url, options);

                    Ref::new(migration)
                }),
        );

        if !self.parent.use_snapshots {
            return;
        }

        let url = self.parent.url.clone();
        let cfg_options = self.parent.options.clone();

        self.parent.parent.services.add(
            transient_as_self::<SqlStoreMigration<DB>>()
                .depends_on(exactly_one::<dyn Clock>())
                .depends_on(exactly_one::<Transcoder<dyn Event>>())
                .depends_on(zero_or_one_with_key::<A, DynSnapshotStore<A::ID>>())
                .depends_on(zero_or_one::<dyn OptionsSnapshot<SqlOptions<DB>>>())
                .from(move |sp| {
                    let di_options = sp.get::<dyn OptionsSnapshot<SqlOptions<DB>>>();
                    let builder = merge(
                        event::SqlStore::<A::ID, DB>::builder()
                            .table(name)
                            .clock(sp.get_required::<dyn Clock>())
                            .transcoder(sp.get_required::<Transcoder<dyn Event>>()),
                        name,
                        url.as_deref(),
                        cfg_options.as_ref(),
                        di_options.as_ref(),
                    );
                    let url = builder.url.clone().unwrap_or_default();
                    let options = builder.options.clone().unwrap_or_default();
                    let store = builder.build().unwrap();
                    let migration = SqlStoreMigration::new(&store, url, options);

                    Ref::new(migration)
                }),
        );
    }
}
