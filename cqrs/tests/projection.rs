mod common;

use common::{domain::{self, Account}, projector::StatementGenerator, TestResult};
use cqrs::{event::Store, in_memory::EventStore, Repository, RepositoryError, VirtualClock};
use std::sync::Arc;

#[tokio::test]
async fn projector_should_produce_monthly_statement() -> TestResult<RepositoryError<String>> {
    // arrange
    let transcoder = domain::transcoder::events();
    let clock = VirtualClock::new();
    let store = Arc::new(EventStore::<String>::new(clock, transcoder));
    let repository: Repository<Account> = (store.clone() as Arc<dyn Store<String>>).into();
    let id = String::from("42");
    let mut account = Account::open(&id);

    account.credit(25.0);
    account.credit(25.0);
    account.credit(50.0);
    account.debit(25.0);
    repository.save(&mut account).await?;

    let mut projector = StatementGenerator::new(store.clone());

    // act
    let statement = projector.run(&id).await?;

    // assert
    assert_eq!(statement.balance, 75.0);
    Ok(())
}
