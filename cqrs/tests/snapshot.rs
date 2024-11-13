mod common;

use common::{
    domain::{self, Account},
    projector::StatementGenerator,
    BoxErr, TestResult,
};
use cqrs::{
    event::Store,
    in_memory::{EventStore, SnapshotStore},
    snapshot::{self, Store as _},
    Aggregate, Repository, VirtualClock,
};
use std::sync::Arc;

#[test]
fn account_should_create_snapshot_via_trait() {
    // arrange
    let mut account = Account::open("42");

    account.credit(25.0);
    account.credit(25.0);
    account.credit(50.0);
    account.debit(25.0);

    let aggregate: &dyn Aggregate<ID = String> = &account;

    // act
    let snapshot = aggregate.snapshot();

    // assert
    assert_eq!(
        snapshot
            .unwrap()
            .as_any()
            .downcast_ref::<crate::domain::Statement>()
            .unwrap()
            .balance(),
        75.0
    );
}

#[tokio::test]
async fn account_should_replay_all_history_with_snapshot() -> TestResult {
    // arrange
    let clock = VirtualClock::new();
    let snapshots = Arc::new(SnapshotStore::<String>::new(domain::transcoder::snapshots()));
    let events = EventStore::<String>::with_snapshots(
        clock,
        domain::transcoder::events(),
        snapshots.clone(),
    );
    let repository = Repository::new(events);
    let id = String::from("42");
    let mut account = Account::open(id.clone());

    account.credit(25.0);
    account.credit(25.0);
    account.credit(50.0);
    account.debit(25.0);
    repository.save(&mut account).await.box_err()?;

    if let Some(snapshot) = account.snapshot() {
        snapshots.save(account.id(), snapshot).await.box_err()?;
    }

    account.credit(25.0);
    repository.save(&mut account).await.box_err()?;

    // act
    account = repository.get(&id, None).await.box_err()?;

    // assert
    assert_eq!(account.balance, 100.0);
    Ok(())
}

#[tokio::test]
async fn account_should_replay_all_history_with_snapshot_from_projector() -> TestResult {
    // arrange
    let clock = VirtualClock::new();
    let snapshots = Arc::new(SnapshotStore::<String>::new(domain::transcoder::snapshots()));
    let events: Arc<dyn Store<String>> = Arc::new(EventStore::<String>::with_snapshots(
        clock,
        domain::transcoder::events(),
        snapshots.clone(),
    ));
    let repository = Repository::from(events.clone());
    let mut projector = StatementGenerator::new(events);
    let id = String::from("42");
    let mut account = Account::open(id.clone());

    account.credit(25.0);
    account.credit(25.0);
    account.credit(50.0);
    account.debit(25.0);
    repository.save(&mut account).await.box_err()?;

    let snapshot = projector.run(&id).await?;

    snapshots
        .save(account.id(), Box::new(snapshot))
        .await
        .box_err()?;
    account.credit(25.0);
    repository.save(&mut account).await.box_err()?;

    // act
    account = repository.get(&id, None).await.box_err()?;

    // assert
    assert_eq!(account.balance, 100.0);
    Ok(())
}

#[test]
fn di_should_register_expected_descriptors_after_drop() {
    // arrange
    use common::domain::transcoder::{events, snapshots};
    use cqrs::di::*;  
    let mut services = di::ServiceCollection::new();

    // act
    services
        .add_cqrs(|options| {
            options.transcoders.events.push(events());
            options.transcoders.snapshots.push(snapshots());
            options.store::<Account>().in_memory().with().snapshots();
        });

    // assert
    assert_eq!(services.len(), 7);
}

#[tokio::test]
async fn account_should_replay_all_history_with_snapshot_using_di() -> TestResult {
    // arrange
    use common::domain::transcoder::{events, snapshots};
    use cqrs::di::*;

    let provider = di::ServiceCollection::new()
        .add_cqrs(|options| {
            options.transcoders.events.push(events());
            options.transcoders.snapshots.push(snapshots());
            options.store::<Account>().in_memory().with().snapshots();
        })
        .build_provider()
        .unwrap();

    let snapshots = provider.get_required_by_key::<Account, dyn snapshot::Store<String>>();
    let repository = provider.get_required::<Repository<Account>>();
    let id = String::from("42");
    let mut account = Account::open(id.clone());

    account.credit(25.0);
    account.credit(25.0);
    account.credit(50.0);
    account.debit(25.0);
    repository.save(&mut account).await.box_err()?;

    if let Some(snapshot) = account.snapshot() {
        snapshots.save(account.id(), snapshot).await.box_err()?;
    }

    account.credit(25.0);
    repository.save(&mut account).await.box_err()?;

    // act
    account = repository.get(&id, None).await.box_err()?;

    // assert
    assert_eq!(account.balance, 100.0);
    Ok(())
}
