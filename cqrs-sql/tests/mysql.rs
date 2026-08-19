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
use cqrs_sql::mysql::{EventStore, Migrator, SnapshotStore};
use sqlx::{AssertSqlSafe, MySqlPool, pool::PoolOptions};
use std::{sync::Arc, time::Duration};
use testcontainers_modules::{
    mysql::Mysql as MysqlServer,
    testcontainers::{ImageExt, runners::AsyncRunner},
};

async fn start_server() -> Result<(impl Sized, String), Box<dyn std::error::Error + 'static>> {
    // MySQL initializes its data directory on first boot, which can comfortably exceed the
    // default startup timeout on a loaded machine
    let server = MysqlServer::default()
        .with_startup_timeout(Duration::from_secs(300))
        .start()
        .await?;
    let port = server.get_host_port_ipv4(3306).await?;
    let url = format!("mysql://root@127.0.0.1:{}/test", port);

    Ok((server, url))
}

#[tokio::test]
async fn verify_mysql_integration() -> TestResult {
    // arrange
    let (_server, url) = start_server().await?;
    let snapshots = Arc::new(
        SnapshotStore::<String>::builder()
            .url(&url)
            .table("TMP_8f3a1c7e5b2d40a9c6e8f1b3d5a7092c")
            .transcoder(domain::transcoder::snapshots())
            .build()?,
    );
    let events = EventStore::<String>::builder()
        .url(&url)
        .table("TMP_2d6b9e4f0a8c41d7b3e5c9a1f7d02b48")
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

async fn count_snapshots(pool: &MySqlPool, table: &str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(AssertSqlSafe(format!("SELECT COUNT(*) FROM `snapshots`.`{table}`;")))
        .fetch_one(pool)
        .await
}

#[tokio::test]
async fn verify_mysql_prunes_snapshots() -> TestResult {
    // arrange
    const TABLE: &str = "TMP_4a9c2e7f1d8b03e5a6c4b9d7f2e10c83";

    let (_server, url) = start_server().await?;
    let snapshots = SnapshotStore::<String>::builder()
        .url(&url)
        .table(TABLE)
        .transcoder(domain::transcoder::snapshots())
        .build()?;
    let events = EventStore::<String>::builder()
        .url(&url)
        .table("TMP_e0b5d3a8c7f24619b2d8e4a0c6f31597")
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
async fn verify_mysql_prunes_stale_snapshots() -> TestResult {
    // arrange
    const TABLE: &str = "TMP_16c8b3f0d95a47e2b7c1a4e6f0d2938b";

    let (_server, url) = start_server().await?;
    let clock = VirtualClock::new();
    let snapshots = SnapshotStore::<String>::builder()
        .url(&url)
        .table(TABLE)
        .clock(Arc::new(clock.clone()) as Arc<dyn Clock>)
        .transcoder(domain::transcoder::snapshots())
        .build()?;
    let events = EventStore::<String>::builder()
        .url(&url)
        .table("TMP_9b2e5d1a7c0f43e8a6d3b8c5f1e07a24")
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
async fn verify_mysql_does_not_allow_save_after_delete() -> TestResult {
    // arrange
    let (_server, url) = start_server().await?;
    let events = EventStore::<String>::builder()
        .url(&url)
        .table("TMP_9c1f6b0d4e7a28c3f5b1d9e0a4c76238")
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
