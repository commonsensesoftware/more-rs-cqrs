use super::{merge, DynEventStore, DynSnapshotStore, SqlOptions};
use crate::{
    event,
    snapshot::{self, Prune, Upsert},
};
use cfg_if::cfg_if;
use cqrs::{
    event::Event,
    message::Transcoder,
    snapshot::Snapshot,
    Aggregate, Clock, Mask, Repository,
};
use di::{
    exactly_one, exactly_one_with_key, singleton_as_self, singleton_with_key, zero_or_one,
    zero_or_one_with_key, Ref, ServiceCollection,
};
use options::OptionsSnapshot;
use sqlx::{
    pool::PoolOptions, ColumnIndex, Database, Decode, Encode, Executor, IntoArguments, Type,
};
use std::{any::type_name, marker::PhantomData, sync::Arc};

/// Represents a builder to configure SQL storage.
pub struct SqlStoreBuilder<'a, A, DB: Database>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Sync + Type<DB>,
    DB: Database,
    for<'args, 'db> <DB as Database>::Arguments<'args>: IntoArguments<'db, DB>,
    for<'db> &'db mut <DB as Database>::Connection: Executor<'db, Database = DB>,
    i16: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i32: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i64: for<'db> Encode<'db, DB> + Type<DB>,
    usize: ColumnIndex<<DB as Database>::Row>,
    String: for<'db> Encode<'db, DB> + Type<DB>,
    for<'db> &'db str: Decode<'db, DB> + Type<DB>,
    for<'db> &'db [u8]: Encode<'db, DB> + Decode<'db, DB> + Type<DB>,
{
    pub(crate) services: &'a mut ServiceCollection,
    pub(crate) name: &'static str,
    _db: PhantomData<DB>,
    _aggregate: PhantomData<A>,
}

impl<'a, A, DB> SqlStoreBuilder<'a, A, DB>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Sync + Type<DB>,
    DB: Database,
    for<'args, 'db> <DB as Database>::Arguments<'args>: IntoArguments<'db, DB>,
    for<'db> &'db mut <DB as Database>::Connection: Executor<'db, Database = DB>,
    i16: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i32: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i64: for<'db> Encode<'db, DB> + Type<DB>,
    usize: ColumnIndex<<DB as Database>::Row>,
    String: for<'db> Encode<'db, DB> + Type<DB>,
    for<'db> &'db str: Decode<'db, DB> + Type<DB>,
    for<'db> &'db [u8]: Encode<'db, DB> + Decode<'db, DB> + Type<DB>,
{
    /// Initializes a new [`SqlStoreBuilder`].
    ///
    /// # Arguments
    ///
    /// * `services` - the associated [services](ServiceCollection)
    pub fn new(services: &'a mut ServiceCollection) -> Self {
        services.try_add(
            singleton_as_self::<Repository<A>>()
                .depends_on(exactly_one_with_key::<A, DynEventStore<A::ID>>())
                .from(|sp| {
                    let store = sp.get_required_by_key::<A, DynEventStore<A::ID>>();
                    let repository: Repository<A> = Ref::<DynEventStore<A::ID>>::from(store).into();
                    Ref::new(repository)
                }),
        );

        Self {
            services,
            name: type_name::<A>().rsplit_once("::").unwrap().1,
            _db: PhantomData,
            _aggregate: PhantomData,
        }
    }

    /// Adds additional SQL storage configuration options.
    pub fn with(self) -> SqlStoreOptionsBuilder<'a, A, DB> {
        SqlStoreOptionsBuilder::new(self)
    }
}

impl<'a, A, DB> Drop for SqlStoreBuilder<'a, A, DB>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Sync + Type<DB>,
    DB: Database,
    for<'args, 'db> <DB as Database>::Arguments<'args>: IntoArguments<'db, DB>,
    for<'db> &'db mut <DB as Database>::Connection: Executor<'db, Database = DB>,
    i16: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i32: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i64: for<'db> Encode<'db, DB> + Type<DB>,
    usize: ColumnIndex<<DB as Database>::Row>,
    String: for<'db> Encode<'db, DB> + Type<DB>,
    for<'db> &'db str: Decode<'db, DB> + Type<DB>,
    for<'db> &'db [u8]: Encode<'db, DB> + Decode<'db, DB> + Type<DB>,
{
    fn drop(&mut self) {
        let name = self.name;

        self.services.try_add(
            singleton_with_key::<A, DynEventStore<A::ID>, event::SqlStore<A::ID, DB>>()
                .depends_on(exactly_one::<dyn Clock>())
                .depends_on(exactly_one::<Transcoder<dyn Event>>())
                .depends_on(zero_or_one_with_key::<A, DynSnapshotStore<A::ID>>())
                .depends_on(zero_or_one::<dyn OptionsSnapshot<SqlOptions<DB>>>())
                .from(|sp| {
                    let options = sp.get::<dyn OptionsSnapshot<SqlOptions<DB>>>();
                    let mut builder = event::SqlStore::<A::ID, DB>::builder()
                        .table(name)
                        .clock(sp.get_required::<dyn Clock>())
                        .transcoder(sp.get_required::<Transcoder<dyn Event>>());

                    if let Some(snapshot) = &options {
                        let db = snapshot.get(Some(name));

                        if !db.url.is_empty() {
                            builder = builder.url(db.url.clone());
                        }

                        builder = builder.options(db.options.clone());
                    }

                    Ref::new(builder.build().unwrap())
                }),
        );
    }
}

/// Represents a builder for SQL storage configuration options.
pub struct SqlStoreOptionsBuilder<'a, A, DB: Database>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Sync + Type<DB>,
    DB: Database,
    for<'args, 'db> <DB as Database>::Arguments<'args>: IntoArguments<'db, DB>,
    for<'db> &'db mut <DB as Database>::Connection: Executor<'db, Database = DB>,
    i16: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i32: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i64: for<'db> Encode<'db, DB> + Type<DB>,
    usize: ColumnIndex<<DB as Database>::Row>,
    String: for<'db> Encode<'db, DB> + Type<DB>,
    for<'db> &'db str: Decode<'db, DB> + Type<DB>,
    for<'db> &'db [u8]: Encode<'db, DB> + Decode<'db, DB> + Type<DB>,
{
    pub(crate) parent: SqlStoreBuilder<'a, A, DB>,
    pub(crate) url: Option<String>,
    pub(crate) options: Option<PoolOptions<DB>>,
    pub(crate) mask: Option<Box<dyn Mask>>,
    pub(crate) allow_delete: bool,
    pub(crate) use_snapshots: bool,
}

impl<'a, A, DB> SqlStoreOptionsBuilder<'a, A, DB>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Sync + Type<DB>,
    DB: Database,
    for<'args, 'db> <DB as Database>::Arguments<'args>: IntoArguments<'db, DB>,
    for<'db> &'db mut <DB as Database>::Connection: Executor<'db, Database = DB>,
    i16: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i32: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i64: for<'db> Encode<'db, DB> + Type<DB>,
    usize: ColumnIndex<<DB as Database>::Row>,
    String: for<'db> Encode<'db, DB> + Type<DB>,
    for<'db> &'db str: Decode<'db, DB> + Type<DB>,
    for<'db> &'db [u8]: Encode<'db, DB> + Decode<'db, DB> + Type<DB>,
{
    fn new(parent: SqlStoreBuilder<'a, A, DB>) -> Self {
        Self {
            parent,
            url: None,
            options: None,
            mask: None,
            allow_delete: false,
            use_snapshots: false,
        }
    }

    /// Configures the name of the table used for events and snapshots.
    ///
    /// # Arguments
    ///
    /// * `value` - the name of the underlying database table names
    ///
    /// # Remarks
    ///
    /// The default name is the name of the configured [aggregate](Aggregate).
    /// The event and snapshot tables are created in different database schemas.
    /// If the database does not support schemas, the table names will have the
    /// naming format `events_<name>` and `snapshots_<name>`, respectively.
    pub fn table(mut self, value: &'static str) -> Self {
        self.parent.name = value;
        self
    }

    /// Configures the URL used as the database connection string.
    ///
    /// # Arguments
    ///
    /// * `value` - the database connection string URL
    ///
    /// # Remarks
    ///
    /// If a URL is not specified, the underlying configuration will attempt to use
    /// the value configured by [`SqlOptions::url`] via [`OptionsSnapshot`] using
    /// the configured [`Self::table`], which allows specifying [`SqlOptions`] for
    /// all or specific [aggregates](Aggregate). A URL specified by this function
    /// supersedes any other configuration.
    pub fn url<V: AsRef<str>>(mut self, value: V) -> Self {
        self.url = Some(value.as_ref().into());
        self
    }

    /// Configures the database connection pool options.
    ///
    /// # Arguments
    ///
    /// * `value` - the database [connection pool options](PoolOptions)
    pub fn options(mut self, value: PoolOptions<DB>) -> Self {
        self.options = Some(value);
        self
    }

    /// Configures the associated mask.
    ///
    /// # Arguments
    ///
    /// * `value` - the [mask](Mask) used to obfuscate [versions](cqrs::Version)
    pub fn mask<V: Mask + 'static>(mut self, value: V) -> Self {
        self.mask = Some(Box::new(value));
        self
    }

    // Enables support for deletes, which is unsupported by default.
    pub fn deletes(mut self) -> Self {
        self.allow_delete = true;
        self
    }
}

impl<'a, A, DB> SqlStoreOptionsBuilder<'a, A, DB>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Sync + Type<DB>,
    DB: Database + for<'db> Prune<'db, A::ID, DB> + Upsert,
    for<'args, 'db> <DB as Database>::Arguments<'args>: IntoArguments<'db, DB>,
    for<'db> &'db mut <DB as Database>::Connection: Executor<'db, Database = DB>,
    i16: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i32: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i64: for<'db> Encode<'db, DB> + Type<DB>,
    usize: ColumnIndex<<DB as Database>::Row>,
    String: for<'db> Encode<'db, DB> + Type<DB>,
    for<'db> &'db str: Decode<'db, DB> + Type<DB>,
    for<'db> &'db [u8]: Encode<'db, DB> + Decode<'db, DB> + Type<DB>,
{
    /// Configures storage with SQL-based snapshots.
    ///
    /// # Remarks
    ///
    /// In order to use a snapshot store which does not use SQL, a keyed service must be registered in the
    /// [`ServiceCollection`] for a [`cqrs::snapshot::Store`] using the type of [`Aggregate`] as the key.
    #[inline]
    pub fn snapshots(mut self) -> Self {
        let name = self.parent.name;
        let url = self.url.clone();
        let cfg_options = self.options.clone();
        let mask = self.mask.take().map(Arc::from);

        self.use_snapshots = true;
        self.parent.services.try_add(
            singleton_with_key::<A, DynSnapshotStore<A::ID>, snapshot::SqlStore<A::ID, DB>>()
                .depends_on(zero_or_one::<dyn Mask>())
                .depends_on(exactly_one::<dyn Clock>())
                .depends_on(exactly_one::<Transcoder<dyn Snapshot>>())
                .depends_on(zero_or_one::<dyn OptionsSnapshot<SqlOptions<DB>>>())
                .from(move |sp| {
                    let di_options = sp.get::<dyn OptionsSnapshot<SqlOptions<DB>>>();
                    let mut builder = merge(
                        snapshot::SqlStore::<A::ID, DB>::builder()
                            .table(name)
                            .clock(sp.get_required::<dyn Clock>())
                            .transcoder(sp.get_required::<Transcoder<dyn Snapshot>>()),
                        name,
                        url.as_deref(),
                        cfg_options.as_ref(),
                        di_options.as_ref(),
                    );

                    if let Some(mask) = mask.clone().or_else(|| sp.get::<dyn Mask>()) {
                        builder = builder.mask(mask);
                    }

                    Ref::new(builder.build().unwrap())
                }),
        );

        self
    }
}

impl<'a, A, DB> Drop for SqlStoreOptionsBuilder<'a, A, DB>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Sync + Type<DB>,
    DB: Database,
    for<'args, 'db> <DB as Database>::Arguments<'args>: IntoArguments<'db, DB>,
    for<'db> &'db mut <DB as Database>::Connection: Executor<'db, Database = DB>,
    i16: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i32: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i64: for<'db> Encode<'db, DB> + Type<DB>,
    usize: ColumnIndex<<DB as Database>::Row>,
    String: for<'db> Encode<'db, DB> + Type<DB>,
    for<'db> &'db str: Decode<'db, DB> + Type<DB>,
    for<'db> &'db [u8]: Encode<'db, DB> + Decode<'db, DB> + Type<DB>,
{
    fn drop(&mut self) {
        let name = self.parent.name;
        let url = self.url.clone();
        let cfg_options = self.options.clone();
        let mask = self.mask.take().map(Arc::from);
        let allow_delete = self.allow_delete;

        self.parent.services.try_add(
            singleton_with_key::<A, DynEventStore<A::ID>, event::SqlStore<A::ID, DB>>()
                .depends_on(zero_or_one::<dyn Mask>())
                .depends_on(exactly_one::<dyn Clock>())
                .depends_on(exactly_one::<Transcoder<dyn Event>>())
                .depends_on(zero_or_one_with_key::<A, DynSnapshotStore<A::ID>>())
                .depends_on(zero_or_one::<dyn OptionsSnapshot<SqlOptions<DB>>>())
                .from(move |sp| {
                    let di_options = sp.get::<dyn OptionsSnapshot<SqlOptions<DB>>>();
                    let mut builder = merge(
                        event::SqlStore::<A::ID, DB>::builder()
                            .table(name)
                            .clock(sp.get_required::<dyn Clock>())
                            .transcoder(sp.get_required::<Transcoder<dyn Event>>()),
                        name,
                        url.as_deref(),
                        cfg_options.as_ref(),
                        di_options.as_ref(),
                    );

                    if let Some(snapshots) = sp.get_by_key::<A, DynSnapshotStore<A::ID>>() {
                        builder =
                            builder.snapshots(Ref::<DynSnapshotStore<A::ID>>::from(snapshots));
                    }

                    if let Some(mask) = mask.clone().or_else(|| sp.get::<dyn Mask>()) {
                        builder = builder.mask(mask);
                    }

                    if allow_delete {
                        builder = builder.with_deletes();
                    }

                    Ref::new(builder.build().unwrap())
                }),
        );
    }
}

cfg_if! {
    if #[cfg(feature = "migrate")] {
        use super::SqlMigrationsBuilder;
        use sqlx::migrate::{Migrate, Migration};

        impl<'a, A, DB> SqlStoreOptionsBuilder<'a, A, DB>
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
        {
            /// Configures the database to use migrations.
            pub fn migrations(self) -> SqlMigrationsBuilder<'a, A, DB> {
                SqlMigrationsBuilder::new(self)
            }
        }
    }
}
