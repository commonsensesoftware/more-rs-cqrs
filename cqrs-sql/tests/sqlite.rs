mod common;

use common::{
    domain::{self, Account},
    scenario, TestResult,
};
use cqrs::{snapshot::Store, Repository, RepositoryError};
use cqrs_sql::{
    sqlite::{EventStore, Migrator, SnapshotStore},
    SqlStoreMigration,
};
use sqlx::sqlite::SqlitePoolOptions;
use std::sync::Arc;

#[tokio::test]
async fn verify_sqlite_integration() -> TestResult {
    // arrange
    let sqlite = SqlitePoolOptions::new().connect("sqlite::memory:").await?;
    let snapshots: Arc<SnapshotStore<String>> = Arc::new(
        SnapshotStore::builder()
            .pool(sqlite.clone())
            .table("TMP_4f244bbd22a54314bc04c048591a1bef")
            .transcoder(domain::transcoder::snapshots())
            .try_into()?,
    );
    let events: EventStore<String> = EventStore::builder()
        .pool(sqlite.clone())
        .table("TMP_e4caf6cc5467403f9b23e6eec7c344eb")
        .transcoder(domain::transcoder::events())
        .with_deletes()
        .snapshots(snapshots.clone() as Arc<dyn cqrs::snapshot::Store<String>>)
        .try_into()?;
    let migrator = Migrator::new();

    migrator.add(SqlStoreMigration::with_pool(&events, sqlite.clone()));
    migrator.add(SqlStoreMigration::with_pool(&*snapshots, sqlite));
    migrator.run().await?;

    let repository = Repository::<Account>::new(events);

    // act / assert
    let id = scenario::open_new_account(&repository, "12345", 50.0).await?;
    assert_eq!(
        scenario::make_deposit(&repository, &id, 200.0).await?,
        250.0,
        "expected balance of 250.0"
    );
    assert_eq!(
        scenario::make_withdrawal(&repository, &id, 100.0).await?,
        150.0,
        "expected balance of 150.0"
    );
    scenario::new_monthly_statement(&repository, &id, &*snapshots).await?;
    assert_eq!(
        scenario::make_deposit(&repository, &id, 150.0).await?,
        300.0,
        "expected balance of 300.0"
    );
    repository.delete(&id).await?;
    assert_eq!(
        repository.get(&id, None).await.unwrap_err(),
        RepositoryError::NotFound(id.clone())
    );
    assert!(snapshots.load(&id, None).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn verify_sqlite_does_not_allow_save_after_delete() -> TestResult {
    // arrange
    let sqlite = SqlitePoolOptions::new().connect("sqlite::memory:").await?;
    let events: EventStore<String> = EventStore::builder()
        .pool(sqlite.clone())
        .table("TMP_b2d85560f008418b9174cc8b0b36b6a4")
        .transcoder(domain::transcoder::events())
        .with_deletes()
        .try_into()?;
    let migrator = Migrator::new();

    migrator.add(SqlStoreMigration::with_pool(&events, sqlite.clone()));
    migrator.run().await?;

    let repository = Repository::<Account>::new(events);

    // act / assert
    let id = scenario::open_new_account(&repository, "12345", 50.0).await?;
    let mut account = repository.get(&id, None).await?;

    repository.delete(&id).await?;
    account.credit(100.0)?;

    assert_eq!(
        repository.save(&mut account).await.unwrap_err(),
        RepositoryError::NotFound(id.clone())
    );
    Ok(())
}
