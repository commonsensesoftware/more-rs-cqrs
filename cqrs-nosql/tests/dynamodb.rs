mod common;

use aws_config::{BehaviorVersion, meta::region::RegionProviderChain};
use aws_sdk_dynamodb::{Client, config::Credentials};
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
use cqrs_nosql::dynamodb::{EventStore, EventStoreMigration, SnapshotStore, SnapshotStoreMigration};
use futures::StreamExt;
use std::{
    error::Error,
    sync::{Arc, Weak},
    time::{Duration, SystemTime},
};
use testcontainers_modules::{dynamodb_local::DynamoDb, testcontainers::runners::AsyncRunner};
use tokio::{sync::Mutex, time::sleep};

/// Represents a running Amazon DynamoDB Local instance.
struct Local {
    // the container is removed when it is dropped
    _container: testcontainers_modules::testcontainers::ContainerAsync<DynamoDb>,
    endpoint: String,
}

impl Local {
    // a client cannot be shared by the tests because the connection pool of the AWS SDK is bound to the runtime that
    // created it, and every test has its own runtime
    async fn client(&self) -> Client {
        let region = RegionProviderChain::default_provider().or_else("us-west-2");
        let credentials = Credentials::new("fakeKey", "fakeSecret", None, None, "test");
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region)
            .endpoint_url(&self.endpoint)
            .credentials_provider(credentials)
            .load()
            .await;

        Client::new(&config)
    }

    // REMARKS: the ready condition of the image is a log message the server writes as it starts,
    // followed by a fixed delay. the delay is a guess, and it is only long enough while nothing else
    // is competing for the machine. nextest runs each test in its own process, so several containers
    // start at once and the server is not listening yet when the delay expires, which surfaces as an
    // incomplete message. wait for the server to actually answer instead
    async fn wait_until_ready(&self) -> TestResult {
        const ATTEMPTS: u8 = 60;

        let client = self.client().await;

        for attempt in 1..=ATTEMPTS {
            if client.list_tables().send().await.is_ok() {
                return Ok(());
            }

            if attempt < ATTEMPTS {
                sleep(Duration::from_millis(250)).await;
            }
        }

        Err("DynamoDB Local did not start serving requests".into())
    }
}

// DynamoDB Local is inexpensive to start, but a single instance is still shared among the tests that overlap. a weak
// reference removes the container as soon as the last test using it completes
static LOCAL: Mutex<Weak<Local>> = Mutex::const_new(Weak::new());

async fn local() -> Result<Arc<Local>, Box<dyn Error + 'static>> {
    let mut shared = LOCAL.lock().await;

    if let Some(local) = shared.upgrade() {
        return Ok(local);
    }

    let container = DynamoDb::default().start().await?;
    let port = container.get_host_port_ipv4(8000).await?;
    let local = Arc::new(Local {
        _container: container,
        endpoint: format!("http://127.0.0.1:{port}"),
    });

    local.wait_until_ready().await?;

    *shared = Arc::downgrade(&local);
    Ok(local)
}

async fn migrate(client: &Client, table: &str) -> TestResult {
    EventStoreMigration::new(client.clone(), format!("{table}_Events"))
        .run()
        .await?;
    SnapshotStoreMigration::new(client.clone(), format!("{table}_Snapshots"))
        .run()
        .await?;
    Ok(())
}

async fn count_snapshots(client: &Client, table: &str, id: &str) -> Result<usize, Box<dyn Error + 'static>> {
    use aws_sdk_dynamodb::types::AttributeValue::S;

    let count = client
        .query()
        .table_name(format!("{table}_Snapshots"))
        .key_condition_expression("id = :id")
        .expression_attribute_values(":id", S(id.into()))
        .send()
        .await?
        .items
        .map(|items| items.len())
        .unwrap_or_default();

    Ok(count)
}

#[tokio::test]
async fn verify_dynamodb_integration() -> TestResult {
    // arrange
    const TABLE: &str = "TMP_f3a91c0e7b2d44a8e6c5b1d90f2a7e34";

    let local = local().await?;
    let client = &local.client().await;

    migrate(client, TABLE).await?;

    let snapshots: Arc<SnapshotStore<String>> = Arc::new(
        SnapshotStore::builder()
            .client(client.clone())
            .table(TABLE)
            .transcoder(domain::transcoder::snapshots())
            .build()?,
    );
    let events: EventStore<String> = EventStore::builder()
        .client(client.clone())
        .table(TABLE)
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

    // a snapshot summarizes the events that came before it, so replaying from it and then replaying the events that
    // follow reaches the same state
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
async fn verify_dynamodb_seeds_load_from_snapshot() -> TestResult {
    // arrange
    const TABLE: &str = "TMP_5c8e2b7f0a1d4936b8e4c0a7f2d51963";

    let local = local().await?;
    let client = &local.client().await;

    migrate(client, TABLE).await?;

    let snapshots: Arc<SnapshotStore<String>> = Arc::new(
        SnapshotStore::builder()
            .client(client.clone())
            .table(TABLE)
            .transcoder(domain::transcoder::snapshots())
            .build()?,
    );
    let events: EventStore<String> = EventStore::builder()
        .client(client.clone())
        .table(TABLE)
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
async fn verify_dynamodb_streams_ids() -> TestResult {
    // arrange
    const TABLE: &str = "TMP_6f2c9e1a83b04d75a9c3e0b7f4d18a52";

    let local = local().await?;
    let client = &local.client().await;

    migrate(client, TABLE).await?;

    let events: EventStore<String> = EventStore::builder()
        .client(client.clone())
        .table(TABLE)
        .transcoder(domain::transcoder::events())
        .build()?;
    let repository = Repository::<Account>::new(events);
    let first = scenario::open_new_account(&repository, "12345", 50.0).await?;
    let second = scenario::open_new_account(&repository, "67890", 25.0).await?;

    // more than one event is recorded for an aggregate, but only the first identifies it
    scenario::make_deposit(&repository, &first, 10.0).await?;

    let events: EventStore<String> = EventStore::builder()
        .client(client.clone())
        .table(TABLE)
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
async fn verify_dynamodb_saves_events_in_a_single_transaction() -> TestResult {
    // arrange
    const TABLE: &str = "TMP_9d1b6e30c8a54f27b3e9d5c8a0f41e26";

    let local = local().await?;
    let client = &local.client().await;

    migrate(client, TABLE).await?;

    let events: EventStore<String> = EventStore::builder()
        .client(client.clone())
        .table(TABLE)
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
async fn verify_dynamodb_enforces_concurrency() -> TestResult {
    // arrange
    const TABLE: &str = "TMP_2e7a4c91f0b8436da5c2e8b7f1d09364";

    let local = local().await?;
    let client = &local.client().await;

    migrate(client, TABLE).await?;

    let events: EventStore<String> = EventStore::builder()
        .client(client.clone())
        .table(TABLE)
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
async fn verify_dynamodb_does_not_allow_save_after_delete() -> TestResult {
    // arrange
    const TABLE: &str = "TMP_c0e58a2b7d1f49638a4c7e0b2f95d183";

    let local = local().await?;
    let client = &local.client().await;

    migrate(client, TABLE).await?;

    let events: EventStore<String> = EventStore::builder()
        .client(client.clone())
        .table(TABLE)
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
async fn verify_dynamodb_prunes_stale_snapshots() -> TestResult {
    // arrange
    const TABLE: &str = "TMP_8b3f1d60e2a74c95b1d8e6c3a0f27594";

    let local = local().await?;
    let client = &local.client().await;

    migrate(client, TABLE).await?;

    let clock = VirtualClock::new();
    let snapshots: SnapshotStore<String> = SnapshotStore::builder()
        .client(client.clone())
        .table(TABLE)
        .clock(Arc::new(clock.clone()) as Arc<dyn Clock>)
        .transcoder(domain::transcoder::snapshots())
        .build()?;
    let events: EventStore<String> = EventStore::builder()
        .client(client.clone())
        .table(TABLE)
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

    assert_eq!(count_snapshots(client, TABLE, &id).await?, 4, "expected 4 snapshots");

    // act
    let retention = Retention {
        count: Some(2),
        age: Some(Duration::from_secs(60)),
    };

    snapshots.prune(&id, Some(&retention)).await?;

    // assert
    assert_eq!(
        count_snapshots(client, TABLE, &id).await?,
        1,
        "expected only the snapshot within the retained age"
    );

    snapshots.prune(&id, None).await?;
    assert_eq!(
        count_snapshots(client, TABLE, &id).await?,
        0,
        "expected all snapshots pruned"
    );
    Ok(())
}

#[tokio::test]
async fn verify_dynamodb_ignores_a_replayed_write() -> TestResult {
    // arrange
    const TABLE: &str = "TMP_5b1e94c0d73a428fb6e0c9a2d18f7304";

    let local = local().await?;
    let client = &local.client().await;

    migrate(client, TABLE).await?;

    let store = || {
        EventStore::<String>::builder()
            .client(client.clone())
            .table(TABLE)
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
