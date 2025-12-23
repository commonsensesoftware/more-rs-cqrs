mod common;

use common::{
    TestResult,
    domain::{Account, transcoder::events},
};
use cqrs::{
    Repository, RepositoryError, VirtualClock,
    event::{Store, StoreOptions},
    in_memory::EventStore,
    prelude::*,
};
use std::sync::Arc;

#[tokio::test]
async fn repository_should_save_aggregate() -> TestResult<RepositoryError<String>> {
    // arrange
    let options = StoreOptions::builder()
        .clock(VirtualClock::new())
        .transcoder(events())
        .build();
    let store = Arc::new(EventStore::<String>::new(options));
    let repository: Repository<Account> = (store.clone() as Arc<dyn Store<String>>).into();
    let mut account = Account::open("42");

    account.credit(25.0);
    account.credit(25.0);
    account.credit(50.0);
    account.debit(25.0);

    // act
    repository.save(&mut account).await?;

    // assert
    assert_eq!(store.size(), 4);
    Ok(())
}

#[tokio::test]
async fn repository_should_get_aggregate_by_id() -> TestResult<RepositoryError<String>> {
    // arrange
    let options = StoreOptions::builder()
        .clock(VirtualClock::new())
        .transcoder(events())
        .build();
    let store = EventStore::<String>::new(options);
    let repository = Repository::new(store);
    let id = String::from("42");
    let mut account = Account::open(id.clone());

    account.credit(25.0);
    account.credit(25.0);
    account.credit(50.0);
    account.debit(25.0);
    repository.save(&mut account).await?;

    // act
    account = repository.get(&id, None).await?;

    // assert
    assert_eq!(account.balance, 75.0);
    Ok(())
}

#[tokio::test]
async fn repository_should_get_aggregate_by_id_using_di() -> TestResult<RepositoryError<String>> {
    // arrange
    let provider = di::ServiceCollection::new()
        .add_cqrs(|options| {
            options.transcoders.events.push(events());
            options.store::<Account>().in_memory();
        })
        .build_provider()
        .unwrap();

    let repository = provider.get_required::<Repository<Account>>();
    let id = String::from("42");
    let mut account = Account::open(id.clone());

    account.credit(25.0);
    account.credit(25.0);
    account.credit(50.0);
    account.debit(25.0);
    repository.save(&mut account).await?;

    // act
    account = repository.get(&id, None).await?;

    // assert
    assert_eq!(account.balance, 75.0);
    Ok(())
}
