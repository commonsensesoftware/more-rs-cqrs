use super::{merge, DynEventStore, DynSnapshotStore, SqlOptions};
use crate::sqlite::{EventStore, SnapshotStore};
use cfg_if::cfg_if;
use cqrs::{
    di::AggregateBuilder, event::Event, message::Transcoder, snapshot::Snapshot, Aggregate, Clock,
    Mask, Repository,
};
use di::{
    exactly_one, exactly_one_with_key, singleton_as_self, singleton_with_key,
    singleton_with_key_factory, zero_or_one, zero_or_one_with_key, Ref, ServiceCollection,
};
use options::OptionsSnapshot;
use sqlx::{pool::PoolOptions, Decode, Encode, Sqlite, Type};
use std::{any::type_name, marker::PhantomData, sync::Arc};

/// Represents the SQLite storage configuration extensions.
pub trait SqliteExt<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID:
        Clone + for<'db> Encode<'db, Sqlite> + for<'db> Decode<'db, Sqlite> + Sync + Type<Sqlite>,
{
    /// Configures an [aggregate](Aggregate) with Sqlite storage.
    fn in_sqlite(self) -> SqliteStoreBuilder<'a, A>;
}

impl<'a, A> SqliteExt<'a, A> for AggregateBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID:
        Clone + for<'db> Encode<'db, Sqlite> + for<'db> Decode<'db, Sqlite> + Sync + Type<Sqlite>,
{
    fn in_sqlite(self) -> SqliteStoreBuilder<'a, A> {
        SqliteStoreBuilder::new(self.services)
    }
}

/// Represents a builder to configure [SQLite](Sqlite) storage.
pub struct SqliteStoreBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID:
        Clone + for<'db> Encode<'db, Sqlite> + for<'db> Decode<'db, Sqlite> + Sync + Type<Sqlite>,
{
    services: &'a mut ServiceCollection,
    name: &'static str,
    _aggregate: PhantomData<A>,
}

impl<'a, A> SqliteStoreBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID:
        Clone + for<'db> Encode<'db, Sqlite> + for<'db> Decode<'db, Sqlite> + Sync + Type<Sqlite>,
{
    /// Initializes a new [`SqliteStoreBuilder`].
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
            _aggregate: PhantomData,
        }
    }

    /// Adds additional [SQLite](Sqlite) storage configuration options.
    pub fn with(self) -> SqliteStoreOptionsBuilder<'a, A> {
        SqliteStoreOptionsBuilder::new(self)
    }
}

impl<'a, A> Drop for SqliteStoreBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID:
        Clone + for<'db> Encode<'db, Sqlite> + for<'db> Decode<'db, Sqlite> + Sync + Type<Sqlite>,
{
    fn drop(&mut self) {
        let name = self.name;

        self.services.try_add(
            singleton_with_key::<A, DynEventStore<A::ID>, EventStore<A::ID>>()
                .depends_on(exactly_one::<dyn Clock>())
                .depends_on(exactly_one::<Transcoder<dyn Event>>())
                .depends_on(zero_or_one_with_key::<A, DynSnapshotStore<A::ID>>())
                .depends_on(zero_or_one::<dyn OptionsSnapshot<SqlOptions<Sqlite>>>())
                .from(|sp| {
                    let options = sp.get::<dyn OptionsSnapshot<SqlOptions<Sqlite>>>();
                    let mut builder = EventStore::<A::ID>::builder()
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

                    Ref::new( EventStore::try_from(builder).unwrap())
                }),
        );
    }
}

/// Represents a builder for [SQLite](Sqlite) storage configuration options.
pub struct SqliteStoreOptionsBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID:
        Clone + for<'db> Encode<'db, Sqlite> + for<'db> Decode<'db, Sqlite> + Sync + Type<Sqlite>,
{
    parent: SqliteStoreBuilder<'a, A>,
    url: Option<String>,
    options: Option<PoolOptions<Sqlite>>,
    mask: Option<Box<dyn Mask>>,
    allow_delete: bool,
    use_snapshots: bool,
}

impl<'a, A> SqliteStoreOptionsBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID:
        Clone + for<'db> Encode<'db, Sqlite> + for<'db> Decode<'db, Sqlite> + Sync + Type<Sqlite>,
{
    fn new(parent: SqliteStoreBuilder<'a, A>) -> Self {
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
    pub fn options(mut self, value: PoolOptions<Sqlite>) -> Self {
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
        self.parent
            .services
            .try_add(
                singleton_with_key::<A, SnapshotStore<A::ID>, SnapshotStore<A::ID>>()
                    .depends_on(zero_or_one::<dyn Mask>())
                    .depends_on(exactly_one::<dyn Clock>())
                    .depends_on(exactly_one::<Transcoder<dyn Snapshot>>())
                    .depends_on(zero_or_one::<dyn OptionsSnapshot<SqlOptions<Sqlite>>>())
                    .from(move |sp| {
                        let di_options = sp.get::<dyn OptionsSnapshot<SqlOptions<Sqlite>>>();
                        let mut builder = merge(
                            SnapshotStore::<A::ID>::builder()
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

                        Ref::new(SnapshotStore::try_from(builder).unwrap())
                    }),
            )
            .try_add(singleton_with_key_factory::<A, DynSnapshotStore<A::ID>, _>(
                |sp| {
                    Ref::<SnapshotStore<A::ID>>::from(
                        sp.get_required_by_key::<A, SnapshotStore<A::ID>>(),
                    ) as Ref<DynSnapshotStore<A::ID>>
                },
            ));

        self
    }
}

impl<'a, A> Drop for SqliteStoreOptionsBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID:
        Clone + for<'db> Encode<'db, Sqlite> + for<'db> Decode<'db, Sqlite> + Sync + Type<Sqlite>,
{
    fn drop(&mut self) {
        let name = self.parent.name;
        let url = self.url.clone();
        let cfg_options = self.options.clone();
        let mask = self.mask.take().map(Arc::from);
        let allow_delete = self.allow_delete;

        self.parent
            .services
            .try_add(
                singleton_with_key::<A, EventStore<A::ID>, EventStore<A::ID>>()
                    .depends_on(zero_or_one::<dyn Mask>())
                    .depends_on(exactly_one::<dyn Clock>())
                    .depends_on(exactly_one::<Transcoder<dyn Event>>())
                    .depends_on(zero_or_one_with_key::<A, DynSnapshotStore<A::ID>>())
                    .depends_on(zero_or_one::<dyn OptionsSnapshot<SqlOptions<Sqlite>>>())
                    .from(move |sp| {
                        let di_options = sp.get::<dyn OptionsSnapshot<SqlOptions<Sqlite>>>();
                        let mut builder = merge(
                            EventStore::<A::ID>::builder()
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

                        Ref::new(EventStore::try_from(builder).unwrap())
                    }),
            )
            .try_add(singleton_with_key_factory::<A, DynEventStore<A::ID>, _>(
                |sp| {
                    Ref::<EventStore<A::ID>>::from(sp.get_required_by_key::<A, EventStore<A::ID>>())
                        as Ref<DynEventStore<A::ID>>
                },
            ));
    }
}

cfg_if! {
    if #[cfg(feature = "migrate")] {
        use crate::{SqlStoreMigration, SqlStoreMigrator};
        use di::{transient_as_self, Injectable};

        /// Represents the configuration for [SQLite](Sqlite) storage migration.
        pub struct SqliteMigrationsBuilder<'a, A>
        where
            A: Aggregate + Default + Sync + 'static,
            A::ID: Clone + for<'db> Encode<'db, Sqlite> + for<'db> Decode<'db, Sqlite> + Sync + Type<Sqlite>,
        {
            parent: SqliteStoreOptionsBuilder<'a, A>,
        }

        impl<'a, A> SqliteMigrationsBuilder<'a, A>
        where
            A: Aggregate + Default + Sync + 'static,
            A::ID: Clone + for<'db> Encode<'db, Sqlite> + for<'db> Decode<'db, Sqlite> + Sync + Type<Sqlite>,
        {
            fn new(parent: SqliteStoreOptionsBuilder<'a, A>) -> Self {
                parent
                    .parent
                    .services
                    .try_add_to_all(SqlStoreMigrator::<Sqlite>::transient());
                Self { parent }
            }
        }

        impl<'a, A> Drop for SqliteMigrationsBuilder<'a, A>
        where
            A: Aggregate + Default + Sync + 'static,
            A::ID: Clone + for<'db> Encode<'db, Sqlite> + for<'db> Decode<'db, Sqlite> + Sync + Type<Sqlite>,
        {
            fn drop(&mut self) {
                self.parent.parent.services.add(
                    transient_as_self::<SqlStoreMigration<Sqlite>>()
                        .depends_on(zero_or_one_with_key::<A, EventStore<A::ID>>())
                        .from(move |sp| {
                            let store = sp.get_required_by_key::<A, EventStore<A::ID>>();
                            let migration = SqlStoreMigration::with_pool(&*store, store.pool.clone());

                            Ref::new(migration)
                        }),
                );

                if !self.parent.use_snapshots {
                    return;
                }

                self.parent.parent.services.add(
                    transient_as_self::<SqlStoreMigration<Sqlite>>()
                        .depends_on(zero_or_one_with_key::<A, SnapshotStore<A::ID>>())
                        .from(move |sp| {
                            let store = sp.get_required_by_key::<A, SnapshotStore<A::ID>>();
                            let migration = SqlStoreMigration::with_pool(&*store, store.pool.clone());

                            Ref::new(migration)
                        }),
                );
            }
        }

        impl<'a, A> SqliteStoreOptionsBuilder<'a, A>
        where
            A: Aggregate + Default + Sync + 'static,
            A::ID:
                Clone + for<'db> Encode<'db, Sqlite> + for<'db> Decode<'db, Sqlite> + Sync + Type<Sqlite>,
        {
            /// Configures the database to use migrations.
            pub fn migrations(self) -> SqliteMigrationsBuilder<'a, A> {
                SqliteMigrationsBuilder::new(self)
            }
        }
    }
}
