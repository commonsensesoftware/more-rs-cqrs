mod common;

use common::{domain::{self, Account}, TestResult};
use cqrs::{
    event::Store, in_memory::EventStore, Repository, RepositoryError, VirtualClock,
};
use std::sync::Arc;

#[tokio::test]
async fn repository_should_save_aggregate() -> TestResult<RepositoryError<String>> {
    // arrange
    let transcoder = domain::transcoder::events();
    let clock = VirtualClock::new();
    let store = Arc::new(EventStore::<String>::new(clock, transcoder));
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
    let transcoder = domain::transcoder::events();
    let clock = VirtualClock::new();
    let store = EventStore::<String>::new(clock, transcoder);
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
    use common::domain::transcoder::events;
    use cqrs::di::*;

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
