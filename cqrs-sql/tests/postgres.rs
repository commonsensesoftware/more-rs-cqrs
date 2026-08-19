mod common;

use common::{
    TestResult,
    domain::{self, Account},
    scenario,
};
use cqrs::{
    Clock, Repository, RepositoryError, VirtualClock,
    snapshot::{Retention, Store},
};
use cqrs_sql::postgres::{EventStore, Migrator, SnapshotStore};
use sqlx::{AssertSqlSafe, PgPool, pool::PoolOptions};
use std::{sync::Arc, time::Duration};
use testcontainers_modules::{postgres::Postgres as PostgresServer, testcontainers::runners::AsyncRunner};

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

async fn count_snapshots(pool: &PgPool, table: &str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM \"snapshots\".\"{table}\";"
    )))
    .fetch_one(pool)
    .await
}

#[tokio::test]
async fn verify_postgres_prunes_snapshots() -> TestResult {
    // arrange
    const TABLE: &str = "TMP_6b2e9d1a4f8c47e3b0a5d7c9e2f14a80";

    let postgres = PostgresServer::default().start().await?;
    let port = postgres.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);
    let snapshots = SnapshotStore::<String>::builder()
        .url(&url)
        .table(TABLE)
        .transcoder(domain::transcoder::snapshots())
        .build()?;
    let events = EventStore::<String>::builder()
        .url(&url)
        .table("TMP_c4a7f0e2b5d9416a8e3c1b6d0f52a7e9")
        .transcoder(domain::transcoder::events())
        .build()?;
    let migrator = Migrator::new();

    migrator.configure(&events, &url, PoolOptions::default());
    migrator.configure(&snapshots, &url, PoolOptions::default());
    migrator.run().await?;

    let pool = PoolOptions::default().connect(&url).await?;
    let repository = Repository::<Account>::new(events);
    let id = scenario::open_new_account(&repository, "12345", 50.0).await?;

    // one snapshot per version, so three in total
    for _ in 0..3 {
        scenario::new_monthly_statement(&repository, &id, &snapshots).await?;
        scenario::make_deposit(&repository, &id, 10.0).await?;
    }

    assert_eq!(count_snapshots(&pool, TABLE).await?, 3, "expected 3 snapshots");

    // act / assert
    let retention = Retention {
        count: Some(2),
        age: None,
    };

    snapshots.prune(&id, Some(&retention)).await?;
    assert_eq!(count_snapshots(&pool, TABLE).await?, 2, "expected 2 retained by count");

    let retention = Retention {
        count: Some(1),
        age: Some(Duration::from_secs(60)),
    };

    snapshots.prune(&id, Some(&retention)).await?;
    assert_eq!(
        count_snapshots(&pool, TABLE).await?,
        1,
        "expected 1 retained by count and age"
    );

    // nothing is older than the retained age, so the last snapshot survives
    let retention = Retention {
        count: None,
        age: Some(Duration::from_secs(60)),
    };

    snapshots.prune(&id, Some(&retention)).await?;
    assert_eq!(count_snapshots(&pool, TABLE).await?, 1, "expected 1 retained by age");

    snapshots.prune(&id, None).await?;
    assert_eq!(count_snapshots(&pool, TABLE).await?, 0, "expected all snapshots pruned");
    Ok(())
}

#[tokio::test]
async fn verify_postgres_prunes_stale_snapshots() -> TestResult {
    // arrange
    const TABLE: &str = "TMP_7d1f4b0c9a2e46d8b3c5e7a1f0d29b64";

    let postgres = PostgresServer::default().start().await?;
    let port = postgres.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);
    let clock = VirtualClock::new();
    let snapshots = SnapshotStore::<String>::builder()
        .url(&url)
        .table(TABLE)
        .clock(Arc::new(clock.clone()) as Arc<dyn Clock>)
        .transcoder(domain::transcoder::snapshots())
        .build()?;
    let events = EventStore::<String>::builder()
        .url(&url)
        .table("TMP_3a9c6e2b8d0f41a7b5c4d8e2f6a0937b")
        .transcoder(domain::transcoder::events())
        .build()?;
    let migrator = Migrator::new();

    migrator.configure(&events, &url, PoolOptions::default());
    migrator.configure(&snapshots, &url, PoolOptions::default());
    migrator.run().await?;

    let pool = PoolOptions::default().connect(&url).await?;
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

    assert_eq!(count_snapshots(&pool, TABLE).await?, 4, "expected 4 snapshots");

    // act
    let retention = Retention {
        count: Some(2),
        age: Some(Duration::from_secs(60)),
    };

    snapshots.prune(&id, Some(&retention)).await?;

    // assert
    assert_eq!(
        count_snapshots(&pool, TABLE).await?,
        1,
        "expected only the snapshot within the retained age"
    );
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
