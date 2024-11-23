use crate::dynamodb::{EventStore, SnapshotStore};
use aws_sdk_dynamodb::{config::Credentials, Client};
use cfg_if::cfg_if;
use cqrs::{
    di::AggregateBuilder,
    event::{self, Event},
    message::Transcoder,
    snapshot::{self, Retention, Snapshot},
    Aggregate, Clock, Mask, Repository,
};
use di::{
    exactly_one, exactly_one_with_key, singleton_as_self, singleton_with_key, zero_or_one,
    zero_or_one_with_key, Ref, ServiceCollection,
};
use options::OptionsSnapshot;
use std::{any::type_name, marker::PhantomData, str::FromStr, sync::Arc};

/// Represents the Amazon DynamoDB storage configuration extensions.
pub trait DynamoDbExt<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + Default + FromStr + Sync + ToString,
{
    /// Configures an [aggregate](Aggregate) with Amazon DynamoDB storage.
    fn in_dynamodb(self) -> DynamoDbBuilder<'a, A>;
}

impl<'a, A> DynamoDbExt<'a, A> for AggregateBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + Default + FromStr + Sync + ToString,
{
    fn in_dynamodb(self) -> DynamoDbBuilder<'a, A> {
        DynamoDbBuilder::new(self.services)
    }
}

/// Represents a builder to configure Amazon DynamoDB storage.
pub struct DynamoDbBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + Default + FromStr + Sync + ToString,
{
    services: &'a mut ServiceCollection,
    table: &'static str,
    _aggregate: PhantomData<A>,
}

impl<'a, A> DynamoDbBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + Default + FromStr + Sync + ToString,
{
    /// Initializes a new [`SqliteStoreBuilder`].
    ///
    /// # Arguments
    ///
    /// * `services` - the associated [services](ServiceCollection)
    pub fn new(services: &'a mut ServiceCollection) -> Self {
        services.try_add(
            singleton_as_self::<Repository<A>>()
                .depends_on(exactly_one_with_key::<A, dyn event::Store<A::ID>>())
                .from(|sp| {
                    let store = sp.get_required_by_key::<A, dyn event::Store<A::ID>>();
                    let repository: Repository<A> =
                        Ref::<dyn event::Store<A::ID>>::from(store).into();
                    Ref::new(repository)
                }),
        );

        Self {
            services,
            table: type_name::<A>().rsplit_once("::").unwrap().1.into(),
            _aggregate: PhantomData,
        }
    }

    /// Adds additional Amazon DynamoDB storage configuration options.
    pub fn with(self) -> DynamoDbOptionsBuilder<'a, A> {
        DynamoDbOptionsBuilder::new(self)
    }
}

impl<'a, A> Drop for DynamoDbBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + Default + FromStr + Sync + ToString,
{
    fn drop(&mut self) {
        let table = self.table;

        self.services.try_add(
            singleton_with_key::<A, dyn event::Store<A::ID>, EventStore<A::ID>>()
                .depends_on(exactly_one::<dyn Clock>())
                .depends_on(exactly_one::<Transcoder<dyn Event>>())
                .depends_on(zero_or_one_with_key::<A, dyn snapshot::Store<A::ID>>())
                .depends_on(zero_or_one::<dyn OptionsSnapshot<Option<Client>>>())
                .from(move |sp| {
                    let mut builder = EventStore::<A::ID>::builder()
                        .table(table)
                        .clock(sp.get_required::<dyn Clock>())
                        .transcoder(sp.get_required::<Transcoder<dyn Event>>());

                    if let Some(options) = sp.get::<dyn OptionsSnapshot<Option<Client>>>() {
                        if let Some(client) = options.get(Some(table)).as_ref() {
                            builder = builder.client(client.clone());
                        }
                    }

                    Ref::new(builder.build().unwrap())
                }),
        );
    }
}

// Represents a builder for Amazon DynamoDB storage configuration options.
pub struct DynamoDbOptionsBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + Default + FromStr + Sync + ToString,
{
    parent: DynamoDbBuilder<'a, A>,
    url: Option<String>,
    credentials: Option<Credentials>,
    client: Option<Client>,
    mask: Option<Box<dyn Mask>>,
    allow_delete: bool,
    use_snapshots: bool,
}

impl<'a, A> DynamoDbOptionsBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + Default + FromStr + Sync + ToString,
{
    fn new(parent: DynamoDbBuilder<'a, A>) -> Self {
        Self {
            parent,
            url: None,
            credentials: None,
            client: None,
            mask: None,
            allow_delete: false,
            use_snapshots: false,
        }
    }

    /// Configures the name of the table used for events and snapshots.
    ///
    /// # Arguments
    ///
    /// * `value` - the name of the underlying table names
    ///
    /// # Remarks
    ///
    /// The default name is the name of the configured [aggregate](Aggregate). Table
    /// names will use the suffix `<table>_Events` and `<table>_Snapshots`, respectively.
    pub fn table(mut self, value: &'static str) -> Self {
        self.parent.table = value;
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
    /// Configuring a URL has no effect if a [`Self::client`] is specified.
    pub fn url<V: AsRef<str>>(mut self, value: V) -> Self {
        self.url = Some(value.as_ref().into());
        self
    }

    /// Configures the authentication credentials.
    ///
    /// # Arguments
    ///
    /// * `value` - the authentication [credentials](Credentials)
    ///
    /// # Remarks
    ///
    /// Configuring credentials has no effect if a [`Self::client`] is specified.
    pub fn credentials<V: Into<Credentials>>(mut self, value: V) -> Self {
        self.credentials = Some(value.into());
        self
    }

    /// Configures the client to use.
    ///
    /// # Arguments
    ///
    /// * `value` - the underlying [client](Client)
    ///
    /// # Remarks
    ///
    /// Specifying a [`Client`] is useful when it has already been configured externally or it is being
    /// reused across configurations. This configuration supersedes any configuration by [`Self::url`]
    /// or [`Self::credentials`] as they are mutually exclusive.
    ///
    /// A [`Client`] may be configured via [`OptionsSnapshot`] using the configured [`Self::table`],
    /// which allows specifying [`Option`] of [`Client`] for all or a specific [aggregates](Aggregate).
    /// If a client is explicitly configured by this function, it supersedes any other configuration.
    pub fn client<V: Into<Client>>(mut self, value: V) -> Self {
        self.client = Some(value.into());
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

    /// Configures Amazon DynamoDB storage with snapshots.
    ///
    /// # Remarks
    ///
    /// In order to use a snapshot store which does not use Amazon DynamoDB, a keyed service must be registered in
    /// the [`ServiceCollection`] for a [`cqrs::snapshot::Store`] using the type of [`Aggregate`] as the key.
    #[inline]
    pub fn snapshots(self) -> Self {
        self.snapshots_with(Retention::default())
    }

    /// Configures Amazon DynamoDB storage with snapshots.
    ///
    /// # Arguments
    ///
    /// * `retention` - the [retention](Retention) policy applied to snapshots
    ///
    /// # Remarks
    ///
    /// In order to use a snapshot store which does not use Amazon DynamoDB, a keyed service must be registered in
    /// the [`ServiceCollection`] for a [`cqrs::snapshot::Store`] using the type of [`Aggregate`] as the key.
    pub fn snapshots_with(mut self, retention: Retention) -> Self {
        let table = self.parent.table;
        let url;
        let credentials;
        let client = self.client.clone();
        let mask = self.mask.take().map(Arc::from);

        if client.is_some() {
            url = None;
            credentials = None;
        } else {
            url = self.url.clone();
            credentials = self.credentials.take();
        }

        self.use_snapshots = true;
        self.parent.services.try_add(
            singleton_with_key::<A, dyn snapshot::Store<A::ID>, SnapshotStore<A::ID>>()
                .depends_on(zero_or_one::<dyn Mask>())
                .depends_on(exactly_one::<dyn Clock>())
                .depends_on(exactly_one::<Transcoder<dyn Snapshot>>())
                .depends_on(zero_or_one::<dyn OptionsSnapshot<Option<Client>>>())
                .from(move |sp| {
                    let mut builder = SnapshotStore::<A::ID>::builder()
                        .table(table)
                        .clock(sp.get_required::<dyn Clock>())
                        .transcoder(sp.get_required::<Transcoder<dyn Snapshot>>())
                        .retention(retention.clone());

                    if let Some(mask) = mask.clone().or_else(|| sp.get::<dyn Mask>()) {
                        builder = builder.mask(mask);
                    }

                    if let Some(url) = &url {
                        builder = builder.url(url);
                    }

                    if let Some(credentials) = &credentials {
                        builder = builder.credentials(credentials.clone())
                    }

                    if let Some(client) = &client {
                        builder = builder.client(client.clone());
                    } else if let Some(options) = sp.get::<dyn OptionsSnapshot<Option<Client>>>() {
                        if let Some(client) = options.get(Some(table)).as_ref() {
                            builder = builder.client(client.clone());
                        }
                    }

                    Ref::new(builder.build().unwrap())
                }),
        );

        self
    }
}

impl<'a, A> Drop for DynamoDbOptionsBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + Default + FromStr + Sync + ToString,
{
    fn drop(&mut self) {
        let table = self.parent.table;
        let url;
        let credentials;
        let client = self.client.clone();
        let mask = self.mask.take().map(Arc::from);
        let allow_delete = self.allow_delete;

        if client.is_some() {
            url = None;
            credentials = None;
        } else {
            url = self.url.clone();
            credentials = self.credentials.take();
        }

        self.parent.services.try_add(
            singleton_with_key::<A, dyn event::Store<A::ID>, EventStore<A::ID>>()
                .depends_on(zero_or_one::<dyn Mask>())
                .depends_on(exactly_one::<dyn Clock>())
                .depends_on(exactly_one::<Transcoder<dyn Event>>())
                .depends_on(zero_or_one_with_key::<A, dyn snapshot::Store<A::ID>>())
                .depends_on(zero_or_one::<dyn OptionsSnapshot<Option<Client>>>())
                .from(move |sp| {
                    let mut builder = EventStore::<A::ID>::builder()
                        .table(table)
                        .clock(sp.get_required::<dyn Clock>())
                        .transcoder(sp.get_required::<Transcoder<dyn Event>>());

                    if let Some(snapshots) = sp.get_by_key::<A, dyn snapshot::Store<A::ID>>() {
                        builder =
                            builder.snapshots(Ref::<dyn snapshot::Store<A::ID>>::from(snapshots));
                    }

                    if let Some(mask) = mask.clone().or_else(|| sp.get::<dyn Mask>()) {
                        builder = builder.mask(mask);
                    }

                    if let Some(url) = &url {
                        builder = builder.url(url);
                    }

                    if let Some(credentials) = &credentials {
                        builder = builder.credentials(credentials.clone())
                    }

                    if let Some(client) = &client {
                        builder = builder.client(client.clone());
                    } else if let Some(options) = sp.get::<dyn OptionsSnapshot<Option<Client>>>() {
                        if let Some(client) = options.get(Some(table)).as_ref() {
                            builder = builder.client(client.clone());
                        }
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
        use crate::dynamodb::{EventStoreMigration, SnapshotStoreMigration};
        use cqrs::StoreMigration;
        use di::{transient, ServiceProvider};
        use futures::executor;

        fn resolve_client(
            client: Option<&Client>,
            table: &str,
            url: Option<&String>,
            credentials: Option<&Credentials>,
            sp: &ServiceProvider,
        ) -> Client {
            if let Some(client) = client {
                return client.clone();
            } else if let Some(options) = sp.get::<dyn OptionsSnapshot<Option<Client>>>() {
                if let Some(client) = options.get(Some(table)).as_ref() {
                    return client.clone();
                }
            }

            use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
            let region = RegionProviderChain::default_provider();
            let mut loader = aws_config::defaults(BehaviorVersion::latest()).region(region);

            if let Some(url) = url {
                loader = loader.endpoint_url(url);
            }

            if let Some(credentials) = credentials {
                loader = loader.credentials_provider(credentials.clone());
            }

            Client::new(&executor::block_on(loader.load()))
        }

        /// Represents the configuration for Amazon DynamoDB storage migration.
        pub struct DynamoDbMigrationsBuilder<'a, A>
        where
            A: Aggregate + Default + Sync + 'static,
            A::ID: Clone + Default + FromStr + Sync + ToString,
        {
            parent: DynamoDbOptionsBuilder<'a, A>,
        }

        impl<'a, A> DynamoDbMigrationsBuilder<'a, A>
        where
            A: Aggregate + Default + Sync + 'static,
            A::ID: Clone + Default + FromStr + Sync + ToString,
        {
            fn new(parent: DynamoDbOptionsBuilder<'a, A>) -> Self {
                Self { parent }
            }
        }

        impl<'a, A> Drop for DynamoDbMigrationsBuilder<'a, A>
        where
            A: Aggregate + Default + Sync + 'static,
            A::ID: Clone + Default + FromStr + Sync + ToString,
        {
            fn drop(&mut self) {
                let table = self.parent.parent.table;
                let client = self.parent.client.clone();
                let url;
                let credentials;

                if client.is_some() {
                    url = None;
                    credentials = None;
                } else {
                    url = self.parent.url.clone();
                    credentials = self.parent.credentials.clone();
                }

                self.parent.parent.services.add(
                    transient::<dyn StoreMigration, EventStoreMigration>()
                    .depends_on(zero_or_one::<dyn OptionsSnapshot<Option<Client>>>())
                        .from(move |sp| {
                            let client = resolve_client(client.as_ref(), table, url.as_ref(), credentials.as_ref(), sp);
                            let migration = EventStoreMigration::new(client, table);

                            Ref::new(migration)
                        }),
                );

                if !self.parent.use_snapshots {
                    return;
                }

                let client = self.parent.client.clone();
                let url;
                let credentials;

                if client.is_some() {
                    url = None;
                    credentials = None;
                } else {
                    url = self.parent.url.clone();
                    credentials = self.parent.credentials.clone();
                }

                self.parent.parent.services.add(
                    transient::<dyn StoreMigration, SnapshotStoreMigration>()
                    .depends_on(zero_or_one::<dyn OptionsSnapshot<Option<Client>>>())
                        .from(move |sp| {
                            let client = resolve_client(client.as_ref(), table, url.as_ref(), credentials.as_ref(), sp);
                            let migration = SnapshotStoreMigration::new(client, table);

                            Ref::new(migration)
                        }),
                );
            }
        }

        impl<'a, A> DynamoDbOptionsBuilder<'a, A>
        where
            A: Aggregate + Default + Sync + 'static,
            A::ID: Clone + Default + FromStr + Sync + ToString,
        {
            /// Configures the database to use migrations.
            pub fn migrations(self) -> DynamoDbMigrationsBuilder<'a, A> {
                DynamoDbMigrationsBuilder::new(self)
            }
        }
    }
}
