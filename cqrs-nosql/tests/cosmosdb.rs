mod common;

use azure_data_cosmos::{
    AccountEndpoint, AccountReference, CosmosClient, RoutingStrategy, feed::FeedScope, options::Region,
};
use common::{
    TestResult,
    domain::{self, Account, Statement},
    scenario,
};
use cqrs::{
    Aggregate, Clock, Range, Repository, RepositoryError, StoreMigration, VirtualClock,
    event::Store as EventStoreTrait,
    snapshot::{Retention, Store},
};
use cqrs_nosql::cosmosdb::{EventStore, EventStoreMigration, SnapshotStore, SnapshotStoreMigration};
use futures::StreamExt;
use std::{
    error::Error,
    sync::{Arc, Weak},
    time::{Duration, SystemTime},
};
use testcontainers_modules::testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::Mutex;

const IMAGE: &str = "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator";

// the vnext emulator is a fraction of the size of the original emulator, starts in seconds, and serves plain HTTP,
// which removes the need to trust a self-signed certificate
const TAG: &str = "vnext-preview";
const GATEWAY_PORT: u16 = 8081;

// the emulator only accepts the well-known key that is documented for it
// REF: https://learn.microsoft.com/azure/cosmos-db/how-to-develop-emulator
const KEY: &str = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

// the emulator reports the health of each of its parts every few seconds and only reports them all as healthy once it
// is ready to serve requests
const READY: &str = "PostgreSQL=OK, Gateway=OK, Explorer=OK";

const DATABASE: &str = "cqrs";

/// Represents a running Azure Cosmos DB emulator.
struct Emulator {
    _container: ContainerAsync<GenericImage>,
    endpoint: String,
}

impl Emulator {
    /// Creates and returns a new [client](CosmosClient) connected to the emulator.
    ///
    /// # Remarks
    ///
    /// The driver spawns the background tasks that maintain its routing state and connection pool onto the `tokio`
    /// runtime the client is created on. Each test has its own runtime, so a shared client stops working as soon as the
    /// test that created it finishes. Only the emulator itself is shared; a client is cheap by comparison.
    async fn client(&self) -> Result<CosmosClient, Box<dyn Error + 'static>> {
        let endpoint: AccountEndpoint = self.endpoint.parse()?;
        let account = AccountReference::with_authentication_key(endpoint, KEY);

        Ok(CosmosClient::builder()
            .build(account, RoutingStrategy::ProximityTo(Region::WEST_US))
            .await?)
    }
}

// the emulator is an entire database engine, which is far too heavy to start one per test. a weak reference shares a
// single emulator among the tests, while still removing the container when the last test using it completes
static EMULATOR: Mutex<Weak<Emulator>> = Mutex::const_new(Weak::new());

// the SDK only allows a plaintext endpoint for a known emulator host, of which the loopback address the container is
// published on is one
async fn emulator() -> Result<Arc<Emulator>, Box<dyn Error + 'static>> {
    let mut shared = EMULATOR.lock().await;

    if let Some(emulator) = shared.upgrade() {
        return Ok(emulator);
    }

    let container = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(GATEWAY_PORT.tcp())
        .with_wait_for(WaitFor::message_on_either_std(READY))
        .with_startup_timeout(Duration::from_secs(300))
        .start()
        .await?;
    let port = container.get_host_port_ipv4(GATEWAY_PORT).await?;
    let emulator = Arc::new(Emulator {
        _container: container,
        endpoint: format!("http://127.0.0.1:{port}/"),
    });

    *shared = Arc::downgrade(&emulator);
    Ok(emulator)
}

async fn count_snapshots(client: &CosmosClient, container: &str, id: &str) -> Result<usize, Box<dyn Error + 'static>> {
    let container = client
        .database_client(DATABASE)
        .container_client(&format!("{container}_Snapshots"))
        .await?;
    let mut items = container
        .query_items::<serde_json::Value>("SELECT c.id FROM c", FeedScope::partition(id.to_string()), None)
        .await?;
    let mut count = 0usize;

    while let Some(item) = items.next().await {
        item?;
        count += 1;
    }

    Ok(count)
}

async fn migrate(client: &CosmosClient, container: &str) -> TestResult {
    EventStoreMigration::new(client.clone(), DATABASE, format!("{container}_Events"))
        .run()
        .await?;
    SnapshotStoreMigration::new(client.clone(), DATABASE, format!("{container}_Snapshots"))
        .run()
        .await?;
    Ok(())
}

#[tokio::test]
async fn verify_cosmosdb_integration() -> TestResult {
    // arrange
    const CONTAINER: &str = "TMP_a17c4e08b9d24f6ea3c5b81d0f27e934";

    let emulator = emulator().await?;
    let client = &emulator.client().await?;

    migrate(client, CONTAINER).await?;

    let snapshots: Arc<SnapshotStore<String>> = Arc::new(
        SnapshotStore::builder()
            .client(client.clone())
            .database(DATABASE)
            .container(CONTAINER)
            .transcoder(domain::transcoder::snapshots())
            .build()?,
    );
    let events: EventStore<String> = EventStore::builder()
        .client(client.clone())
        .database(DATABASE)
        .container(CONTAINER)
        .transcoder(domain::transcoder::events())
        .snapshots(snapshots.clone() as Arc<dyn cqrs::snapshot::Store<String>>)
        .build()?;
    let repository = Repository::<Account>::new(events);

    // act
    let id = scenario::open_new_account(&repository, "12345", 50.0).await?;

    scenario::make_deposit(&repository, &id, 25.0).await?;
    scenario::make_withdrawal(&repository, &id, 10.0).await?;

    let account = repository.get(&id, None).await?;

    // assert
    assert_eq!(account.balance(), 65.0);

    // a snapshot summarizes the events that came before it, so replaying from it and then
    // replaying the events that follow reaches the same state
    scenario::new_monthly_statement(&repository, &id, &*snapshots).await?;
    scenario::make_deposit(&repository, &id, 5.0).await?;

    let account = repository.get(&id, None).await?;

    assert_eq!(account.balance(), 70.0);
    assert!(
        snapshots.load(&id, None).await?.is_some(),
        "expected the snapshot to round-trip"
    );
    Ok(())
}

#[tokio::test]
async fn verify_cosmosdb_seeds_load_from_snapshot() -> TestResult {
    // arrange
    const CONTAINER: &str = "TMP_d80f3a6b2c1e47d9a5b8c0e7f2a41935";

    let emulator = emulator().await?;
    let client = &emulator.client().await?;

    migrate(client, CONTAINER).await?;

    let snapshots: Arc<SnapshotStore<String>> = Arc::new(
        SnapshotStore::builder()
            .client(client.clone())
            .database(DATABASE)
            .container(CONTAINER)
            .transcoder(domain::transcoder::snapshots())
            .build()?,
    );
    let events: EventStore<String> = EventStore::builder()
        .client(client.clone())
        .database(DATABASE)
        .container(CONTAINER)
        .transcoder(domain::transcoder::events())
        .snapshots(snapshots.clone() as Arc<dyn cqrs::snapshot::Store<String>>)
        .build()?;
    let repository = Repository::<Account>::new(events);
    let id = scenario::open_new_account(&repository, "12345", 50.0).await?;

    scenario::make_deposit(&repository, &id, 25.0).await?;

    let account = repository.get(&id, None).await?;

    assert_eq!(account.balance(), 75.0);

    // a snapshot that does not agree with the events it summarizes is the only way to prove which of the two a load
    // actually replayed. a real snapshot always agrees, so seeding a load from one is otherwise indistinguishable from
    // replaying every event
    let statement = Statement::new(id.clone(), 1000.0, SystemTime::now());

    snapshots.save(&id, account.version(), Box::new(statement)).await?;

    // act
    scenario::make_deposit(&repository, &id, 5.0).await?;

    // assert
    let account = repository.get(&id, None).await?;

    assert_eq!(
        account.balance(),
        1005.0,
        "expected the load to be seeded from the snapshot, but every event was replayed"
    );
    Ok(())
}

#[tokio::test]
async fn verify_cosmosdb_streams_ids() -> TestResult {
    // arrange
    const CONTAINER: &str = "TMP_4e07b91c6a2d43f8b5c0e9a7d3106284";

    let emulator = emulator().await?;
    let client = &emulator.client().await?;

    migrate(client, CONTAINER).await?;

    let events: EventStore<String> = EventStore::builder()
        .client(client.clone())
        .database(DATABASE)
        .container(CONTAINER)
        .transcoder(domain::transcoder::events())
        .build()?;
    let repository = Repository::<Account>::new(events);
    let first = scenario::open_new_account(&repository, "12345", 50.0).await?;
    let second = scenario::open_new_account(&repository, "67890", 25.0).await?;

    // more than one event is recorded for an aggregate, but only the first identifies it
    scenario::make_deposit(&repository, &first, 10.0).await?;

    let events: EventStore<String> = EventStore::builder()
        .client(client.clone())
        .database(DATABASE)
        .container(CONTAINER)
        .transcoder(domain::transcoder::events())
        .build()?;

    // act
    let mut ids = EventStoreTrait::ids(&events, Range::default()).await;
    let mut found = Vec::new();

    while let Some(id) = ids.next().await {
        found.push(id?);
    }

    found.sort();

    // assert
    assert_eq!(found, vec![first, second]);
    Ok(())
}

#[tokio::test]
async fn verify_cosmosdb_saves_events_in_a_single_batch() -> TestResult {
    // arrange
    const CONTAINER: &str = "TMP_3f8b6d1c05a94e27b4d9c8e6a2f01b73";

    let emulator = emulator().await?;
    let client = &emulator.client().await?;

    migrate(client, CONTAINER).await?;

    let events: EventStore<String> = EventStore::builder()
        .client(client.clone())
        .database(DATABASE)
        .container(CONTAINER)
        .transcoder(domain::transcoder::events())
        .enforce_concurrency()
        .build()?;
    let repository = Repository::<Account>::new(events);
    let id = scenario::open_new_account(&repository, "12345", 50.0).await?;
    let mut account = repository.get(&id, None).await?;

    // act
    account.credit(10.0)?;
    account.credit(20.0)?;
    account.debit(5.0)?;
    repository.save(&mut account).await?;

    // assert
    let account = repository.get(&id, None).await?;

    assert_eq!(account.balance(), 75.0);
    Ok(())
}

#[tokio::test]
async fn verify_cosmosdb_enforces_concurrency() -> TestResult {
    // arrange
    const CONTAINER: &str = "TMP_c62a90e7b31d485fa8e4d0c7f5b31e28";

    let emulator = emulator().await?;
    let client = &emulator.client().await?;

    migrate(client, CONTAINER).await?;

    let events: EventStore<String> = EventStore::builder()
        .client(client.clone())
        .database(DATABASE)
        .container(CONTAINER)
        .transcoder(domain::transcoder::events())
        .enforce_concurrency()
        .build()?;
    let repository = Repository::<Account>::new(events);
    let id = scenario::open_new_account(&repository, "12345", 50.0).await?;
    let mut first = repository.get(&id, None).await?;
    let mut second = repository.get(&id, None).await?;

    // act
    first.credit(10.0)?;
    repository.save(&mut first).await?;
    second.credit(20.0)?;

    let result = repository.save(&mut second).await;

    // assert
    assert!(
        matches!(result, Err(RepositoryError::Conflict(_, _))),
        "expected a conflict, but was {result:?}"
    );
    Ok(())
}

#[tokio::test]
async fn verify_cosmosdb_does_not_allow_save_after_delete() -> TestResult {
    // arrange
    const CONTAINER: &str = "TMP_e5d02b7a4c1f49e8b6a3d9c07f2e18b4";

    let emulator = emulator().await?;
    let client = &emulator.client().await?;

    migrate(client, CONTAINER).await?;

    let events: EventStore<String> = EventStore::builder()
        .client(client.clone())
        .database(DATABASE)
        .container(CONTAINER)
        .transcoder(domain::transcoder::events())
        .with_deletes()
        .build()?;
    let repository = Repository::<Account>::new(events);

    // act
    let id = scenario::open_new_account(&repository, "12345", 50.0).await?;
    let mut account = repository.get(&id, None).await?;

    repository.delete(&id).await?;
    account.credit(100.0)?;

    // assert
    assert_eq!(
        repository.save(&mut account).await.unwrap_err(),
        RepositoryError::NotFound(id.clone())
    );
    Ok(())
}

#[tokio::test]
async fn verify_cosmosdb_prunes_snapshots() -> TestResult {
    // arrange
    const CONTAINER: &str = "TMP_0b94f7e2a6c34d18e5b7c0a3f9d26e51";

    let emulator = emulator().await?;
    let client = &emulator.client().await?;

    migrate(client, CONTAINER).await?;

    let snapshots: SnapshotStore<String> = SnapshotStore::builder()
        .client(client.clone())
        .database(DATABASE)
        .container(CONTAINER)
        .transcoder(domain::transcoder::snapshots())
        .build()?;
    let events: EventStore<String> = EventStore::builder()
        .client(client.clone())
        .database(DATABASE)
        .container(CONTAINER)
        .transcoder(domain::transcoder::events())
        .build()?;
    let repository = Repository::<Account>::new(events);
    let id = scenario::open_new_account(&repository, "12345", 50.0).await?;

    for _ in 0..3 {
        scenario::new_monthly_statement(&repository, &id, &snapshots).await?;
        scenario::make_deposit(&repository, &id, 10.0).await?;
    }

    // act / assert
    let retention = Retention::count(2);

    snapshots.prune(&id, Some(&retention)).await?;
    assert_eq!(
        count_snapshots(client, CONTAINER, &id).await?,
        2,
        "expected 2 retained by count"
    );

    // nothing is older than the retained age, so the snapshots survive
    let retention = Retention::age(Duration::from_secs(60));

    snapshots.prune(&id, Some(&retention)).await?;
    assert_eq!(
        count_snapshots(client, CONTAINER, &id).await?,
        2,
        "expected 2 retained by age"
    );

    snapshots.prune(&id, None).await?;
    assert_eq!(
        count_snapshots(client, CONTAINER, &id).await?,
        0,
        "expected all snapshots pruned"
    );
    Ok(())
}

#[tokio::test]
async fn verify_cosmosdb_prunes_stale_snapshots() -> TestResult {
    // arrange
    const CONTAINER: &str = "TMP_7a1e5c93b0d84f26a8c1e7b5d02f9346";

    let emulator = emulator().await?;
    let client = &emulator.client().await?;

    migrate(client, CONTAINER).await?;

    let clock = VirtualClock::new();
    let snapshots: SnapshotStore<String> = SnapshotStore::builder()
        .client(client.clone())
        .database(DATABASE)
        .container(CONTAINER)
        .clock(Arc::new(clock.clone()) as Arc<dyn Clock>)
        .transcoder(domain::transcoder::snapshots())
        .build()?;
    let events: EventStore<String> = EventStore::builder()
        .client(client.clone())
        .database(DATABASE)
        .container(CONTAINER)
        .transcoder(domain::transcoder::events())
        .build()?;
    let repository = Repository::<Account>::new(events);
    let id = scenario::open_new_account(&repository, "12345", 50.0).await?;

    for _ in 0..3 {
        scenario::new_monthly_statement(&repository, &id, &snapshots).await?;
        scenario::make_deposit(&repository, &id, 10.0).await?;
    }

    // everything taken so far is now older than the retained age, but the last snapshot is taken after the clock
    // moves forward
    clock.wind(Duration::from_secs(120));
    scenario::new_monthly_statement(&repository, &id, &snapshots).await?;

    assert_eq!(
        count_snapshots(client, CONTAINER, &id).await?,
        4,
        "expected 4 snapshots"
    );

    // act
    let retention = Retention {
        count: Some(2),
        age: Some(Duration::from_secs(60)),
    };

    snapshots.prune(&id, Some(&retention)).await?;

    // assert
    assert_eq!(
        count_snapshots(client, CONTAINER, &id).await?,
        1,
        "expected only the snapshot within the retained age"
    );
    Ok(())
}

#[tokio::test]
async fn verify_cosmosdb_ignores_a_replayed_write() -> TestResult {
    // arrange
    const CONTAINER: &str = "TMP_9c3d17ab5e0f42869b7c1d4e08a35f76";

    let emulator = emulator().await?;
    let client = &emulator.client().await?;

    migrate(client, CONTAINER).await?;

    let store = || {
        EventStore::<String>::builder()
            .client(client.clone())
            .database(DATABASE)
            .container(CONTAINER)
            .transcoder(domain::transcoder::events())
    };
    let repository = Repository::<Account>::new(store().build()?);
    let id = scenario::open_new_account(&repository, "12345", 50.0).await?;
    let version = repository.get(&id, None).await?.version();

    // a client resends a request whose response it never observed, which is indistinguishable from saving the very same
    // events with the same expected version
    let events: Vec<Box<dyn cqrs::event::Event>> =
        vec![Box::new(domain::Credited::new(id.clone(), 25.0, SystemTime::now()))];
    let relaxed = store().build()?;
    let strict = store().enforce_concurrency().build()?;

    // act
    let written = EventStoreTrait::save(&relaxed, &id, version, &events).await?;
    let replayed = EventStoreTrait::save(&relaxed, &id, version, &events).await?;

    // assert
    assert_eq!(replayed, written, "expected the replayed write to be a no-op");

    let account = repository.get(&id, None).await?;

    assert_eq!(
        account.balance(),
        75.0,
        "expected the replayed event to have been stored exactly once"
    );

    // enforcing concurrency must not report a conflict for a write that already succeeded
    let enforced = EventStoreTrait::save(&strict, &id, version, &events).await?;

    assert_eq!(
        enforced, written,
        "expected no conflict when the write is already durable"
    );

    // a batch of events is written atomically, so replaying one is a no-op for the same reason
    let version = repository.get(&id, None).await?.version();
    let batch: Vec<Box<dyn cqrs::event::Event>> = vec![
        Box::new(domain::Credited::new(id.clone(), 10.0, SystemTime::now())),
        Box::new(domain::Debited::new(id.clone(), 5.0, SystemTime::now())),
    ];
    let written = EventStoreTrait::save(&relaxed, &id, version, &batch).await?;
    let replayed = EventStoreTrait::save(&relaxed, &id, version, &batch).await?;

    assert_eq!(replayed, written, "expected the replayed batch to be a no-op");

    let account = repository.get(&id, None).await?;

    assert_eq!(
        account.balance(),
        80.0,
        "expected the replayed batch to have been stored exactly once"
    );
    Ok(())
}
