mod common;

use common::{
    TestResult,
    domain::{self, Account},
    scenario,
};
use cqrs::{
    Repository, RepositoryError,
    snapshot::{Retention, Store},
};
use cqrs_sql::{
    SqlStoreMigration,
    sqlite::{EventStore, Migrator, SnapshotStore},
};
use sqlx::{AssertSqlSafe, SqlitePool, sqlite::SqlitePoolOptions};
use std::{sync::Arc, time::Duration};

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

// SQLite has no schema, so the table is the unqualified, unquoted name 'snapshots_<table>'
async fn count_snapshots(pool: &SqlitePool, table: &str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(AssertSqlSafe(format!("SELECT COUNT(*) FROM snapshots_{table};")))
        .fetch_one(pool)
        .await
}

#[tokio::test]
async fn verify_sqlite_prunes_snapshots() -> TestResult {
    // arrange
    const TABLE: &str = "TMP_1c9a7f4e0b6d47b0a3e5d8c2f1a09b6e";

    let sqlite = SqlitePoolOptions::new().connect("sqlite::memory:").await?;
    let snapshots: SnapshotStore<String> = SnapshotStore::builder()
        .pool(sqlite.clone())
        .table(TABLE)
        .transcoder(domain::transcoder::snapshots())
        .try_into()?;
    let events: EventStore<String> = EventStore::builder()
        .pool(sqlite.clone())
        .table("TMP_0d3b8e6a5c1f42d9b7e4a0c8f2d61b35")
        .transcoder(domain::transcoder::events())
        .try_into()?;
    let migrator = Migrator::new();

    migrator.add(SqlStoreMigration::with_pool(&events, sqlite.clone()));
    migrator.add(SqlStoreMigration::with_pool(&snapshots, sqlite.clone()));
    migrator.run().await?;

    let repository = Repository::<Account>::new(events);
    let id = scenario::open_new_account(&repository, "12345", 50.0).await?;

    // one snapshot per version, so three in total
    for _ in 0..3 {
        scenario::new_monthly_statement(&repository, &id, &snapshots).await?;
        scenario::make_deposit(&repository, &id, 10.0).await?;
    }

    assert_eq!(count_snapshots(&sqlite, TABLE).await?, 3, "expected 3 snapshots");

    // act / assert
    let retention = Retention {
        count: Some(2),
        age: None,
    };

    snapshots.prune(&id, Some(&retention)).await?;
    assert_eq!(
        count_snapshots(&sqlite, TABLE).await?,
        2,
        "expected 2 retained by count"
    );

    let retention = Retention {
        count: Some(1),
        age: Some(Duration::from_secs(60)),
    };

    snapshots.prune(&id, Some(&retention)).await?;
    assert_eq!(
        count_snapshots(&sqlite, TABLE).await?,
        1,
        "expected 1 retained by count and age"
    );

    // nothing is older than the retained age, so the last snapshot survives
    let retention = Retention {
        count: None,
        age: Some(Duration::from_secs(60)),
    };

    snapshots.prune(&id, Some(&retention)).await?;
    assert_eq!(count_snapshots(&sqlite, TABLE).await?, 1, "expected 1 retained by age");

    snapshots.prune(&id, None).await?;
    assert_eq!(
        count_snapshots(&sqlite, TABLE).await?,
        0,
        "expected all snapshots pruned"
    );
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
