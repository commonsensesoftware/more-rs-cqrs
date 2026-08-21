use crate::cosmosdb::{EventStore, SnapshotStore};
use azure_data_cosmos::CosmosClient;
use cqrs::{
    Aggregate, Clock, Mask, Repository,
    event::{self, Event},
    message::Transcoder,
    prelude::AggregateBuilder,
    snapshot::{self, Snapshot},
};
use di::{
    Ref, ServiceCollection, exactly_one, exactly_one_with_key, singleton_as_self, singleton_with_key, zero_or_one,
    zero_or_one_with_key,
};
use options::Snapshot as OptionsSnapshot;
use std::{any::type_name, marker::PhantomData, str::FromStr, sync::Arc};

/// Represents the Azure Cosmos DB storage configuration extensions.
pub trait CosmosDbExt<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + Default + FromStr + Sync + ToString,
{
    /// Configures an [aggregate](Aggregate) with Azure Cosmos DB storage.
    fn in_cosmosdb(self) -> CosmosDbBuilder<'a, A>;
}

impl<'a, A> CosmosDbExt<'a, A> for AggregateBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + Default + FromStr + Sync + ToString,
{
    fn in_cosmosdb(self) -> CosmosDbBuilder<'a, A> {
        CosmosDbBuilder::new(self.services)
    }
}

/// Represents a builder to configure Azure Cosmos DB storage.
pub struct CosmosDbBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + Default + FromStr + Sync + ToString,
{
    services: &'a mut ServiceCollection,
    database: Option<String>,
    container: &'static str,
    _aggregate: PhantomData<A>,
}

impl<'a, A> CosmosDbBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + Default + FromStr + Sync + ToString,
{
    /// Initializes a new [CosmosDbBuilder].
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
                    let repository: Repository<A> = Ref::<dyn event::Store<A::ID>>::from(store).into();
                    Ref::new(repository)
                }),
        );

        Self {
            services,
            database: None,
            container: type_name::<A>().rsplit_once("::").unwrap().1,
            _aggregate: PhantomData,
        }
    }

    /// Adds additional Azure Cosmos DB storage configuration options.
    pub fn with(self) -> CosmosDbOptionsBuilder<'a, A> {
        CosmosDbOptionsBuilder::new(self)
    }
}

impl<'a, A> Drop for CosmosDbBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + Default + FromStr + Sync + ToString,
{
    fn drop(&mut self) {
        let container = self.container;
        let database = self.database.clone();

        self.services.try_add(
            singleton_with_key::<A, dyn event::Store<A::ID>, EventStore<A::ID>>()
                .depends_on(zero_or_one::<dyn Mask>())
                .depends_on(exactly_one::<dyn Clock>())
                .depends_on(exactly_one::<Transcoder<dyn Event>>())
                .depends_on(zero_or_one_with_key::<A, dyn snapshot::Store<A::ID>>())
                .depends_on(zero_or_one::<dyn OptionsSnapshot<Option<CosmosClient>>>())
                .from(move |sp| {
                    let mut builder = EventStore::<A::ID>::builder()
                        .container(container)
                        .clock(sp.get_required::<dyn Clock>())
                        .transcoder(sp.get_required::<Transcoder<dyn Event>>());

                    if let Some(database) = &database {
                        builder = builder.database(database.clone());
                    }

                    if let Some(mask) = sp.get::<dyn Mask>() {
                        builder = builder.mask(mask);
                    }

                    if let Some(options) = sp.get::<dyn OptionsSnapshot<Option<CosmosClient>>>()
                        && let Ok(option) = options.get_named(container)
                        && let Some(client) = &*option
                    {
                        builder = builder.client(client.clone());
                    }

                    Ref::new(builder.build().unwrap())
                }),
        );
    }
}

// Represents a builder for Azure Cosmos DB storage configuration options.
pub struct CosmosDbOptionsBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + Default + FromStr + Sync + ToString,
{
    parent: CosmosDbBuilder<'a, A>,
    client: Option<CosmosClient>,
    mask: Option<Box<dyn Mask>>,
    enforce_concurrency: bool,
    allow_delete: bool,
    use_snapshots: bool,
}

impl<'a, A> CosmosDbOptionsBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + Default + FromStr + Sync + ToString,
{
    fn new(parent: CosmosDbBuilder<'a, A>) -> Self {
        Self {
            parent,
            client: None,
            mask: None,
            enforce_concurrency: false,
            allow_delete: false,
            use_snapshots: false,
        }
    }

    /// Configures the identifier of the database used for events and snapshots.
    ///
    /// # Arguments
    ///
    /// * `value` - the identifier of the underlying database
    ///
    /// # Remarks
    ///
    /// A database is required. Unlike a container, there is no default identifier.
    pub fn database<V: Into<String>>(mut self, value: V) -> Self {
        self.parent.database = Some(value.into());
        self
    }

    /// Configures the identifier of the container used for events and snapshots.
    ///
    /// # Arguments
    ///
    /// * `value` - the identifier of the underlying containers
    ///
    /// # Remarks
    ///
    /// The default identifier is the name of the configured [aggregate](Aggregate). Container
    /// identifiers will use the suffix `<container>_Events` and `<container>_Snapshots`,
    /// respectively.
    pub fn container(mut self, value: &'static str) -> Self {
        self.parent.container = value;
        self
    }

    /// Configures the client to use.
    ///
    /// # Arguments
    ///
    /// * `value` - the underlying [client](CosmosClient)
    ///
    /// # Remarks
    ///
    /// A [client](CosmosClient) is required and is created asynchronously, which means it must
    /// be created before the services it is used by are configured.
    ///
    /// A [client](CosmosClient) may also be configured via [OptionsSnapshot] using the configured
    /// [Self::container], which allows specifying [Option] of [CosmosClient] for all or a specific
    /// [aggregates](Aggregate). If a client is explicitly configured by this function, it supersedes
    /// any other configuration.
    pub fn client<V: Into<CosmosClient>>(mut self, value: V) -> Self {
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

    /// Enforces concurrency, which not enforced by default.
    pub fn enforce_concurrency(mut self) -> Self {
        self.enforce_concurrency = true;
        self
    }

    // Enables support for deletes, which is unsupported by default.
    pub fn deletes(mut self) -> Self {
        self.allow_delete = true;
        self
    }

    /// Configures Azure Cosmos DB storage with snapshots.
    ///
    /// # Remarks
    ///
    /// In order to use a snapshot store which does not use Azure Cosmos DB, a keyed service must be
    /// registered in the [ServiceCollection] for a [cqrs::snapshot::Store] using the type of
    /// [Aggregate] as the key.
    pub fn snapshots(mut self) -> Self {
        let container = self.parent.container;
        let database = self.parent.database.clone();
        let client = self.client.clone();
        let mask = self.mask.take().map(Arc::from);

        self.use_snapshots = true;
        self.parent.services.try_add(
            singleton_with_key::<A, dyn snapshot::Store<A::ID>, SnapshotStore<A::ID>>()
                .depends_on(zero_or_one::<dyn Mask>())
                .depends_on(exactly_one::<dyn Clock>())
                .depends_on(exactly_one::<Transcoder<dyn Snapshot>>())
                .depends_on(zero_or_one::<dyn OptionsSnapshot<Option<CosmosClient>>>())
                .from(move |sp| {
                    let mut builder = SnapshotStore::<A::ID>::builder()
                        .container(container)
                        .clock(sp.get_required::<dyn Clock>())
                        .transcoder(sp.get_required::<Transcoder<dyn Snapshot>>());

                    if let Some(database) = &database {
                        builder = builder.database(database.clone());
                    }

                    if let Some(mask) = mask.clone().or_else(|| sp.get::<dyn Mask>()) {
                        builder = builder.mask(mask);
                    }

                    if let Some(client) = &client {
                        builder = builder.client(client.clone());
                    } else if let Some(options) = sp.get::<dyn OptionsSnapshot<Option<CosmosClient>>>()
                        && let Ok(option) = options.get_named(container)
                        && let Some(client) = &*option
                    {
                        builder = builder.client(client.clone());
                    }

                    Ref::new(builder.build().unwrap())
                }),
        );

        self
    }
}

impl<'a, A> Drop for CosmosDbOptionsBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + Default + FromStr + Sync + ToString,
{
    fn drop(&mut self) {
        let container = self.parent.container;
        let database = self.parent.database.clone();
        let client = self.client.clone();
        let mask = self.mask.take().map(Arc::from);
        let enforce_concurrency = self.enforce_concurrency;
        let allow_delete = self.allow_delete;

        self.parent.services.try_add(
            singleton_with_key::<A, dyn event::Store<A::ID>, EventStore<A::ID>>()
                .depends_on(zero_or_one::<dyn Mask>())
                .depends_on(exactly_one::<dyn Clock>())
                .depends_on(exactly_one::<Transcoder<dyn Event>>())
                .depends_on(zero_or_one_with_key::<A, dyn snapshot::Store<A::ID>>())
                .depends_on(zero_or_one::<dyn OptionsSnapshot<Option<CosmosClient>>>())
                .from(move |sp| {
                    let mut builder = EventStore::<A::ID>::builder()
                        .container(container)
                        .clock(sp.get_required::<dyn Clock>())
                        .transcoder(sp.get_required::<Transcoder<dyn Event>>());

                    if let Some(database) = &database {
                        builder = builder.database(database.clone());
                    }

                    if let Some(snapshots) = sp.get_by_key::<A, dyn snapshot::Store<A::ID>>() {
                        builder = builder.snapshots(Ref::<dyn snapshot::Store<A::ID>>::from(snapshots));
                    }

                    if let Some(mask) = mask.clone().or_else(|| sp.get::<dyn Mask>()) {
                        builder = builder.mask(mask);
                    }

                    if let Some(client) = &client {
                        builder = builder.client(client.clone());
                    } else if let Some(options) = sp.get::<dyn OptionsSnapshot<Option<CosmosClient>>>()
                        && let Ok(option) = options.get_named(container)
                        && let Some(client) = &*option
                    {
                        builder = builder.client(client.clone());
                    }

                    if enforce_concurrency {
                        builder = builder.enforce_concurrency();
                    }

                    if allow_delete {
                        builder = builder.with_deletes();
                    }

                    Ref::new(builder.build().unwrap())
                }),
        );
    }
}

cfg_select! {
    feature = "migrate" => {
        use crate::cosmosdb::{EventStoreMigration, SnapshotStoreMigration};
        use cqrs::StoreMigration;
        use di::{transient, ServiceProvider};

        fn resolve_client(
            client: Option<&CosmosClient>,
            container: &str,
            sp: &ServiceProvider,
        ) -> CosmosClient {
            if let Some(client) = client {
                return client.clone();
            } else if let Some(options) = sp.get::<dyn OptionsSnapshot<Option<CosmosClient>>>()
                && let Ok(option) = options.get_named(container)
                && let Some(client) = &*option {
                    return client.clone();
                }

            panic!("a client has not been configured")
        }

        fn resolve_database(database: Option<&String>) -> String {
            database
                .cloned()
                .expect("a database has not been configured")
        }

        /// Represents the configuration for Azure Cosmos DB storage migration.
        pub struct CosmosDbMigrationsBuilder<'a, A>
        where
            A: Aggregate + Default + Sync + 'static,
            A::ID: Clone + Default + FromStr + Sync + ToString,
        {
            parent: CosmosDbOptionsBuilder<'a, A>,
        }

        impl<'a, A> CosmosDbMigrationsBuilder<'a, A>
        where
            A: Aggregate + Default + Sync + 'static,
            A::ID: Clone + Default + FromStr + Sync + ToString,
        {
            fn new(parent: CosmosDbOptionsBuilder<'a, A>) -> Self {
                Self { parent }
            }
        }

        impl<'a, A> Drop for CosmosDbMigrationsBuilder<'a, A>
        where
            A: Aggregate + Default + Sync + 'static,
            A::ID: Clone + Default + FromStr + Sync + ToString,
        {
            fn drop(&mut self) {
                let container = self.parent.parent.container;
                let database = self.parent.parent.database.clone();
                let client = self.parent.client.clone();

                self.parent.parent.services.add(
                    transient::<dyn StoreMigration, EventStoreMigration>()
                    .depends_on(zero_or_one::<dyn OptionsSnapshot<Option<CosmosClient>>>())
                        .from(move |sp| {
                            let client = resolve_client(client.as_ref(), container, sp);
                            let migration = EventStoreMigration::new(
                                client,
                                resolve_database(database.as_ref()),
                                format!("{container}_Events"));

                            Ref::new(migration)
                        }),
                );

                if !self.parent.use_snapshots {
                    return;
                }

                let database = self.parent.parent.database.clone();
                let client = self.parent.client.clone();

                self.parent.parent.services.add(
                    transient::<dyn StoreMigration, SnapshotStoreMigration>()
                    .depends_on(zero_or_one::<dyn OptionsSnapshot<Option<CosmosClient>>>())
                        .from(move |sp| {
                            let client = resolve_client(client.as_ref(), container, sp);
                            let migration = SnapshotStoreMigration::new(
                                client,
                                resolve_database(database.as_ref()),
                                format!("{container}_Snapshots"));

                            Ref::new(migration)
                        }),
                );
            }
        }

        impl<'a, A> CosmosDbOptionsBuilder<'a, A>
        where
            A: Aggregate + Default + Sync + 'static,
            A::ID: Clone + Default + FromStr + Sync + ToString,
        {
            /// Configures the database to use migrations.
            pub fn migrations(self) -> CosmosDbMigrationsBuilder<'a, A> {
                CosmosDbMigrationsBuilder::new(self)
            }
        }
    }
    _ => {}
}
