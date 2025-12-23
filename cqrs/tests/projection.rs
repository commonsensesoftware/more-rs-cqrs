mod common;

use common::{
    TestResult,
    domain::{self, Account},
    projector::StatementGenerator,
};
use cqrs::{Repository, RepositoryError, VirtualClock, event::{Store, StoreOptions}, in_memory::EventStore};
use std::sync::Arc;

#[tokio::test]
async fn projector_should_produce_monthly_statement() -> TestResult<RepositoryError<String>> {
    // arrange
    let options = StoreOptions::builder()
        .clock(VirtualClock::new())
        .transcoder(domain::transcoder::events())
        .build();
    let store = Arc::new(EventStore::<String>::new(options));
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
