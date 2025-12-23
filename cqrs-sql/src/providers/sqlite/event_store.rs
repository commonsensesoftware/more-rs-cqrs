use super::command as cmd;
use crate::{
    BoxErr, SqlStoreBuilder, SqlVersion, SqlVersionPart,
    event::{command, get_snapshot, select_version},
    new_version,
    sql::{self, Context, Ident, IntoRows},
};
use async_stream::try_stream;
use async_trait::async_trait;
use cqrs::{
    Clock, Range, Version,
    event::{Event, EventStream, IdStream, Predicate, Store, StoreError, StoreOptions},
    message::{Saved, Schema},
};
use futures::stream;
use sqlx::Sqlite;
use sqlx::{Connection, Decode, Encode, Pool, Row, Type};
use std::{error::Error, fmt::Debug, ops::Bound::Unbounded, sync::Arc, time::SystemTime};

/// Represents a SQLite [event store](Store).
pub struct EventStore<ID> {
    table: String,
    pub(crate) pool: Pool<Sqlite>,
    options: StoreOptions<ID>,
}

impl<ID> EventStore<ID> {
    /// Initializes a new [EventStore].
    ///
    /// # Arguments
    ///
    /// * `table` - the table identifier
    /// * `pool` - the underlying [connection pool](Pool)
    /// * `options` - the [store options](StoreOptions)
    pub fn new(table: String, pool: Pool<Sqlite>, options: StoreOptions<ID>) -> Self {
        Self {
            table,
            pool,
            options,
        }
    }

    /// Creates and returns a new [SqlStoreBuilder].
    pub fn builder() -> SqlStoreBuilder<ID, dyn Event, Sqlite> {
        SqlStoreBuilder::default()
    }

    pub(crate) fn table(&self) -> Ident<'_> {
        Ident::unqualified(&self.table)
    }
}

#[async_trait]
impl<ID> Store<ID> for EventStore<ID>
where
    ID: Clone
        + Debug
        + for<'db> Encode<'db, Sqlite>
        + for<'db> Decode<'db, Sqlite>
        + Send
        + Sync
        + Type<Sqlite>
        + 'static,
{
    fn clock(&self) -> Arc<dyn Clock> {
        (&self.options).into()
    }

    async fn ids(&self, stored_on: Range<SystemTime>) -> IdStream<ID> {
        let mut db = match self.pool.acquire().await.box_err() {
            Ok(db) => db,
            Err(error) => return Box::pin(stream::iter(vec![Err(StoreError::Unknown(error))])),
        };
        let name = self.table.clone();

        Box::pin(try_stream! {
            let table = Ident::unqualified(&name);
            let mut query = command::select_id(table, stored_on);
            let rows = query.build().fetch(&mut *db);

            for await row in rows {
                yield row.box_err()?.get::<ID, _>(0);
            }
        })
    }

    async fn load<'a>(&self, predicate: Option<&'a Predicate<'a, ID>>) -> EventStream<'a, ID> {
        let mut db = match self.pool.acquire().await.box_err() {
            Ok(db) => db,
            Err(error) => return Box::pin(stream::iter(vec![Err(StoreError::Unknown(error))])),
        };
        let snapshot = match get_snapshot(self.options.snapshots(), predicate).await {
            Ok(snapshot) => snapshot,
            Err(error) => return Box::pin(stream::iter(vec![Err(StoreError::from(error))])),
        };
        let name = self.table.clone();
        let options = self.options.clone();

        Box::pin(try_stream! {
            const TYPE: usize = 0;
            const REVISION: usize = 1;
            const VERSION: usize = 2;
            const SEQUENCE: usize = 3;
            const CONTENT: usize = 4;

            let mut version = Unbounded;

            if let Some(filter) = predicate {
                version = select_version(snapshot.as_ref(), filter, options.mask());

                if let Some(snapshot) = snapshot {
                    let event = options.transcoder().decode(&snapshot.schema, &snapshot.content)?;
                    yield Saved::new(event, snapshot.version);
                }
            }

            let table = Ident::unqualified(&name);
            let mut query = command::select(table, predicate, version);
            let rows = query.build().fetch(&mut *db);

            for await result in rows {
                let row = result.box_err()?;
                let schema = Schema::new(
                    row.get::<&str, _>(TYPE),
                    row.get::<i16, _>(REVISION) as u8,
                );
                let content = row.get::<&[u8], _>(CONTENT);
                let event = options.transcoder().decode(&schema, content)?;
                let mut version = new_version(
                    row.get::<i32, _>(VERSION),
                    row.get::<i16, _>(SEQUENCE),
                );

                if let Some(mask) = options.mask() {
                    version = version.mask(mask);
                }

                yield Saved::new(event, version);
            }
        })
    }

    async fn save(
        &self,
        id: &ID,
        expected_version: Version,
        events: &[Box<dyn Event>],
    ) -> Result<Version, StoreError<ID>> {
        if events.is_empty() {
            return Ok(expected_version);
        }

        let mut version = if expected_version != Version::default()
            && let Some(mask) = self.options.mask()
        {
            expected_version.unmask(mask)
        } else {
            expected_version
        };

        if version.invalid() {
            return Err(StoreError::InvalidVersion);
        }

        let table = self.table();

        loop {
            version = version.increment(SqlVersionPart::Version);

            let context = Context {
                id: id.clone(),
                version,
                clock: self.options.clock(),
                transcoder: self.options.transcoder(),
            };
            let mut rows = events.into_rows(context);
            let first = if let Some(row) = rows.next() {
                row?
            } else {
                return Ok(expected_version);
            };

            let mut db = self.pool.acquire().await.box_err()?;

            if let Some(second) = rows.next() {
                let mut tx = db.begin().await.box_err()?;
                let second = second?;

                if self.options.delete().supported()
                    && let Some(previous) = first.previous()
                {
                    cmd::ensure_not_deleted(&table, &previous, &mut tx).await?;
                }

                let result = cmd::insert_transacted(&table, &first, &mut tx).await;

                // if the first row succeeds or fails, the rest will fail or succeed
                if matches!(result, Err(StoreError::Conflict(_, _))) {
                    if self.options.concurrency().enforced() {
                        result?;
                    } else {
                        continue;
                    }
                }

                cmd::insert_transacted(&table, &second, &mut tx).await?;

                #[allow(clippy::while_let_on_iterator)] // false positive; rows.version() used below
                while let Some(row) = rows.next() {
                    let row = row?;
                    cmd::insert_transacted(&table, &row, &mut tx).await?;
                }

                tx.commit().await.box_err()?;
            } else {
                let mut execute = true;

                if self.options.delete().supported()
                    && let Some(previous) = first.previous()
                {
                    let mut tx = db.begin().await.box_err()?;

                    cmd::ensure_not_deleted(&table, &previous, &mut tx).await?;

                    let result = cmd::insert_transacted(&table, &first, &mut tx).await;

                    if matches!(result, Err(StoreError::Conflict(_, _))) {
                        if self.options.concurrency().enforced() {
                            result?;
                        } else {
                            continue;
                        }
                    }

                    tx.commit().await.box_err()?;
                    execute = false;
                }

                if execute {
                    let mut insert = command::insert(&table, &first);

                    if let Err(error) = insert.build().execute(&mut *db).await {
                        if let sqlx::Error::Database(error) = &error
                            && error.is_unique_violation()
                        {
                            if self.options.concurrency().enforced() {
                                return Err(StoreError::Conflict(id.clone(), first.version as u32));
                            } else {
                                continue;
                            }
                        }
                        return Err(StoreError::Unknown(Box::new(error) as Box<dyn Error + Send>));
                    }
                }
            }

            version = rows.version();
            break;
        }

        if let Some(mask) = self.options.mask() {
            version = version.mask(mask);
        }

        Ok(version)
    }

    async fn delete(&self, id: &ID) -> Result<(), StoreError<ID>> {
        if self.options.delete().unsupported() {
            return Err(StoreError::Unsupported);
        }

        // using a nested transaction, especially in an async context, can cause sqlite to deadlock.
        // sqlx doesn't appear to provide any extra protections. it's unclear if savepoints will make
        // any difference. snapshots are intrinsically volatile so delete them first. if deleting the
        // events somehow fail, things are still in a recoverable state
        if let Some(snapshots) = self.options.snapshots() {
            snapshots.prune(id, None).await?;
        }

        let mut db = self.pool.acquire().await.box_err()?;
        let mut tx = db.begin().await.box_err()?;
        let table = self.table();
        let mut delete = sql::command::delete(&table, id);
        let _ = delete.build().execute(&mut *tx).await.box_err()?;

        tx.commit().await.box_err()?;
        Ok(())
    }
}
