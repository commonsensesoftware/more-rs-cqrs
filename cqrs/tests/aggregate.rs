mod common;

use common::{
    TestResult,
    domain::{Account, Credited, Debited},
};
use cqrs::{Aggregate, Version, event::Event, message::Saved};
use futures::stream;
use std::error::Error;

#[inline(always)]
fn yield_item<T: Event + 'static>(
    event: T,
    version: u64,
) -> Result<Saved<Box<dyn Event>>, Box<dyn Error + Send>> {
    Ok(Saved::new(Box::new(event), Version::new(version)))
}

#[test]
fn account_should_record_events() {
    // arrange
    let mut account = Account::open("42");

    // act
    account.credit(25.0);
    account.credit(25.0);
    account.credit(50.0);
    account.debit(25.0);

    // assert
    assert_eq!(account.balance, 75.0);
}

#[test]
fn account_should_replay_events() {
    // arrange
    let mut account = Account::open("42");

    // act
    account.replay(&Credited::new("42", 50.0));
    account.replay(&Debited::new("42", 25.0));
    account.replay(&Credited::new("42", 25.0));
    account.replay(&Credited::new("42", 25.0));

    // assert
    assert_eq!(account.balance, 75.0);
}

#[tokio::test]
async fn account_should_replay_all_history() -> TestResult {
    // arrange
    let mut account = Account::open("42");
    let mut history = stream::iter([
        yield_item(Credited::new("42", 50.0), 1),
        yield_item(Debited::new("42", 25.0), 2),
        yield_item(Credited::new("42", 25.0), 3),
        yield_item(Credited::new("42", 25.0), 4),
    ]);

    // act
    account.replay_all(&mut history).await?;

    // assert
    assert_eq!(account.balance, 75.0);
    assert_eq!(account.version(), Version::new(4));
    Ok(())
}
