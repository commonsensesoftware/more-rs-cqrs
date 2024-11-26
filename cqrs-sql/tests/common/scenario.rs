use super::domain::Account;
use cqrs::{snapshot, Aggregate, Repository};
use std::error::Error;

pub async fn open_new_account(
    repository: &Repository<Account>,
    id: &str,
    amount: f32,
) -> Result<String, Box<dyn Error + 'static>> {
    let mut account = Account::open(id, amount);
    repository.save(&mut account).await?;
    Ok(account.id().into())
}

pub async fn make_deposit(
    repository: &Repository<Account>,
    id: &String,
    amount: f32,
) -> Result<f32, Box<dyn Error + 'static>> {
    let mut account = repository.get(id, None).await?;
    account.credit(amount)?;
    repository.save(&mut account).await?;
    Ok(account.balance())
}

pub async fn make_withdrawal(
    repository: &Repository<Account>,
    id: &String,
    amount: f32,
) -> Result<f32, Box<dyn Error + 'static>> {
    let mut account = repository.get(id, None).await?;
    account.debit(amount)?;
    repository.save(&mut account).await?;
    Ok(account.balance())
}

pub async fn new_monthly_statement<S: snapshot::Store<String>>(
    repository: &Repository<Account>,
    id: &String,
    snapshots: &S,
) -> Result<(), Box<dyn Error + 'static>> {
    let account = repository.get(id, None).await?;
    let statement = account.snapshot().unwrap();
    snapshots.save(id, statement).await?;
    Ok(())
}
