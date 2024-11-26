mod common;

use common::{
    domain::{self, Account},
    scenario, TestResult,
};
use cqrs::{snapshot::Store, Repository, RepositoryError};
use cqrs_sql::postgres::{EventStore, Migrator, SnapshotStore};
use sqlx::pool::PoolOptions;
use std::sync::Arc;
use testcontainers_modules::{
    postgres::Postgres as PostgresServer, testcontainers::runners::AsyncRunner,
};

#[tokio::test]
async fn verify_postgres_integration() -> TestResult {
    // arrange
    let postgres = PostgresServer::default().start().await?;
    let port = postgres.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);
    let snapshots = Arc::new(
        SnapshotStore::<String>::builder()
            .url(&url)
            .table("TMP_cd725dae49bc4344863488693173469d")
            .transcoder(domain::transcoder::snapshots())
            .build()?,
    );
    let events = EventStore::<String>::builder()
        .url(&url)
        .table("TMP_e476ba563ff64921ab2d1a151c1fdb03")
        .transcoder(domain::transcoder::events())
        .with_deletes()
        .snapshots(snapshots.clone() as Arc<dyn cqrs::snapshot::Store<String>>)
        .build()?;
    let migrator = Migrator::new();

    migrator.configure(&events, &url, PoolOptions::default());
    migrator.configure(&*snapshots, &url, PoolOptions::default());
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
async fn verify_postgres_does_not_allow_save_after_delete() -> TestResult {
    // arrange
    let postgres = PostgresServer::default().start().await?;
    let port = postgres.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);
    let events = EventStore::<String>::builder()
        .url(&url)
        .table("TMP_2b0ccc0d58fb4ceb83bef2c2ab88a73b")
        .transcoder(domain::transcoder::events())
        .with_deletes()
        .build()?;
    let migrator = Migrator::new();

    migrator.configure(&events, &url, PoolOptions::default());
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
