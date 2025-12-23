use super::{command, get_snapshot, select_version};
use crate::{
    BoxErr, SqlStoreBuilder, SqlVersion, SqlVersionPart, new_version,
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
use sqlx::{
    ColumnIndex, Connection, Database, Decode, Encode, Executor, FromRow, IntoArguments, Pool, Row,
    Type,
};
use std::{error::Error, fmt::Debug, ops::Bound, sync::Arc, time::SystemTime};

/// Represents a SQL [event store](Store).
pub struct SqlStore<ID, DB: Database> {
    pub(crate) table: Ident<'static>,
    pub(crate) pool: Pool<DB>,
    options: StoreOptions<ID>,
}

impl<ID, DB: Database> SqlStore<ID, DB> {
    /// Initializes a new [SqlStore].
    ///
    /// # Arguments
    ///
    /// * `table` - the table [identifier](Ident)
    /// * `pool` - the underlying [connection pool](Pool)
    /// * `options` - the [store options](StoreOptions)
    pub fn new(table: Ident<'static>, pool: Pool<DB>, options: StoreOptions<ID>) -> Self {
        Self {
            table,
            pool,
            options,
        }
    }

    /// Creates and returns a new [SqlStoreBuilder].
    pub fn builder() -> SqlStoreBuilder<ID, dyn Event, DB> {
        SqlStoreBuilder::default()
    }
}

#[async_trait]
impl<ID, DB> Store<ID> for SqlStore<ID, DB>
where
    ID: Clone
        + Debug
        + for<'db> Encode<'db, DB>
        + for<'db> Decode<'db, DB>
        + Send
        + Sync
        + Type<DB>
        + 'static,
    DB: Database,
    for<'args, 'db> <DB as Database>::Arguments<'args>: IntoArguments<'db, DB>,
    for<'db> &'db mut <DB as Database>::Connection: Executor<'db, Database = DB>,
    i16: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i32: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i64: for<'db> Encode<'db, DB> + Type<DB>,
    usize: ColumnIndex<<DB as Database>::Row>,
    String: for<'db> Encode<'db, DB> + Type<DB>,
    for<'db> &'db str: Decode<'db, DB> + Type<DB>,
    for<'db> &'db [u8]: Encode<'db, DB> + Decode<'db, DB> + Type<DB>,
    (bool,): for<'db> FromRow<'db, DB::Row>,
{
    fn clock(&self) -> Arc<dyn Clock> {
        (&self.options).into()
    }

    async fn ids(&self, stored_on: Range<SystemTime>) -> IdStream<ID> {
        let mut db = match self.pool.acquire().await.box_err() {
            Ok(db) => db,
            Err(error) => return Box::pin(stream::iter(vec![Err(StoreError::Unknown(error))])),
        };
        let table = self.table.clone();

        Box::pin(try_stream! {
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
        let table = self.table.clone();
        let options = self.options.clone();

        Box::pin(try_stream! {
            const TYPE: usize = 0;
            const REVISION: usize = 1;
            const VERSION: usize = 2;
            const SEQUENCE: usize = 3;
            const CONTENT: usize = 4;

            let mut version = Bound::Unbounded;

            if let Some(filter) = predicate {
                version = select_version(snapshot.as_ref(), filter, options.mask());

                if let Some(snapshot) = snapshot {
                    let event = options.transcoder().decode(&snapshot.schema, &snapshot.content)?;
                    yield Saved::new(event, snapshot.version);
                }
            }

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
                    command::ensure_not_deleted(&self.table, &previous, &mut tx).await?;
                }

                let result = command::insert_transacted(&self.table, &first, &mut tx).await;

                // if the first row succeeds or fails, the rest will fail or succeed
                if matches!(result, Err(StoreError::Conflict(_, _))) {
                    if self.options.concurrency().enforced() {
                        result?;
                    } else {
                        continue;
                    }
                }

                command::insert_transacted(&self.table, &second, &mut tx).await?;

                for row in rows.by_ref() {
                    let row = row?;
                    command::insert_transacted(&self.table, &row, &mut tx).await?;
                }

                tx.commit().await.box_err()?;
            } else {
                let mut execute = true;

                if self.options.delete().supported()
                    && let Some(previous) = first.previous()
                {
                    let mut tx = db.begin().await.box_err()?;

                    command::ensure_not_deleted(&self.table, &previous, &mut tx).await?;

                    let result = command::insert_transacted(&self.table, &first, &mut tx).await;

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
                    let mut insert = command::insert(&self.table, &first);

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

        let mut db = self.pool.acquire().await.box_err()?;
        let mut tx = db.begin().await.box_err()?;
        let mut delete = sql::command::delete(&self.table, id);
        let _ = delete.build().execute(&mut *tx).await.box_err()?;

        if let Some(snapshots) = self.options.snapshots() {
            snapshots.prune(id, None).await?;
        }

        tx.commit().await.box_err()?;
        Ok(())
    }
}
