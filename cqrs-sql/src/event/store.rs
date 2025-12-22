use super::{command, get_snapshot, select_version};
use crate::{
    BoxErr, SqlStoreBuilder, SqlVersion, SqlVersionPart, new_version,
    sql::{self, Context, Ident, IntoRows},
};
use async_stream::try_stream;
use async_trait::async_trait;
use cqrs::{
    Clock, Mask, Range, Version,
    event::{Delete, Event, EventStream, IdStream, Predicate, Store, StoreError},
    message::{Saved, Schema, Transcoder},
    snapshot,
};
use futures::stream;
use sqlx::{
    ColumnIndex, Connection, Database, Decode, Encode, Executor, FromRow, IntoArguments, Pool, Row,
    Type,
};
use std::{error::Error, fmt::Debug, ops::Bound, sync::Arc, time::SystemTime};

/// Represents a SQL [event store](Store).
pub struct SqlStore<ID, DB: Database> {
    delete: Delete,
    pub(crate) table: Ident<'static>,
    pub(crate) pool: Pool<DB>,
    mask: Option<Arc<dyn Mask>>,
    clock: Arc<dyn Clock>,
    transcoder: Arc<Transcoder<dyn Event>>,
    snapshots: Option<Arc<dyn snapshot::Store<ID>>>,
}

impl<ID, DB: Database> SqlStore<ID, DB> {
    /// Initializes a new [`SqlStore`].
    ///
    /// # Arguments
    ///
    /// * `table` - the table [identifier](Ident)
    /// * `pool` - the underlying [connection pool](Pool)
    /// * `mask` - the optional [mask](Mask) used to obfuscate [versions](Version)
    /// * `clock` - the associated [clock](Clock)
    /// * `transcoder` - the associated [transcoder](Transcoder)
    /// * `delete` - indicates whether [deletes](Delete) are supported
    pub fn new(
        table: Ident<'static>,
        pool: Pool<DB>,
        mask: Option<Arc<dyn Mask>>,
        clock: Arc<dyn Clock>,
        transcoder: Arc<Transcoder<dyn Event>>,
        snapshots: Option<Arc<dyn snapshot::Store<ID>>>,
        delete: Delete,
    ) -> Self {
        Self {
            delete,
            table,
            pool,
            mask,
            clock,
            transcoder,
            snapshots,
        }
    }

    /// Creates and returns a new [`SqlStoreBuilder`].
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
        self.clock.clone()
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
        let snapshot = match get_snapshot(self.snapshots.as_deref(), predicate).await {
            Ok(snapshot) => snapshot,
            Err(error) => return Box::pin(stream::iter(vec![Err(StoreError::from(error))])),
        };
        let table = self.table.clone();
        let mask = self.mask.clone();
        let transcoder = self.transcoder.clone();

        Box::pin(try_stream! {
            const TYPE: usize = 0;
            const REVISION: usize = 1;
            const VERSION: usize = 2;
            const SEQUENCE: usize = 3;
            const CONTENT: usize = 4;

            let mut version = Bound::Unbounded;

            if let Some(filter) = predicate {
                version = select_version(snapshot.as_ref(), filter, mask.clone());

                if let Some(snapshot) = snapshot {
                    let event = transcoder.decode(&snapshot.schema, &snapshot.content)?;
                    yield Saved::versioned(event, snapshot.version);
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
                let event = transcoder.decode(&schema, content)?;
                let mut version = new_version(
                    row.get::<i32, _>(VERSION),
                    row.get::<i16, _>(SEQUENCE),
                );

                if let Some(mask) = &mask {
                    version = version.mask(mask);
                }

                yield Saved::versioned(event, version);
            }
        })
    }

    async fn save(
        &self,
        id: &ID,
        events: &[Box<dyn Event>],
        mut expected_version: Version,
    ) -> Result<Option<Version>, StoreError<ID>> {
        if events.is_empty() {
            return Ok(None);
        }

        if expected_version != Version::default() {
            if let Some(mask) = &self.mask {
                expected_version = expected_version.unmask(mask);
            }
        }

        if expected_version.invalid() {
            return Err(StoreError::InvalidVersion);
        }

        let context = Context {
            id: id.clone(),
            version: expected_version.increment(SqlVersionPart::Version),
            clock: &*self.clock,
            transcoder: &self.transcoder,
        };
        let mut rows = events.into_rows(context);
        let first = if let Some(row) = rows.next() {
            row?
        } else {
            return Ok(None);
        };

        let mut db = self.pool.acquire().await.box_err()?;

        if let Some(second) = rows.next() {
            let mut tx = db.begin().await.box_err()?;
            let second = second?;

            if self.delete.supported() {
                if let Some(previous) = first.previous() {
                    command::ensure_not_deleted(&self.table, &previous, &mut tx).await?;
                }
            }

            command::insert_transacted(&self.table, &first, &mut tx).await?;
            command::insert_transacted(&self.table, &second, &mut tx).await?;

            while let Some(row) = rows.next() {
                let row = row?;
                command::insert_transacted(&self.table, &row, &mut tx).await?;
            }

            tx.commit().await.box_err()?;
        } else {
            let mut execute = true;

            if self.delete.supported() {
                if let Some(previous) = first.previous() {
                    let mut tx = db.begin().await.box_err()?;

                    command::ensure_not_deleted(&self.table, &previous, &mut tx).await?;
                    command::insert_transacted(&self.table, &first, &mut tx).await?;
                    tx.commit().await.box_err()?;
                    execute = false;
                }
            }

            if execute {
                let mut insert = command::insert(&self.table, &first);

                if let Err(error) = insert.build().execute(&mut *db).await {
                    if let sqlx::Error::Database(error) = &error {
                        if error.is_unique_violation() {
                            return Err(StoreError::Conflict(id.clone(), first.version as u32));
                        }
                    }
                    return Err(StoreError::Unknown(Box::new(error) as Box<dyn Error + Send>));
                }
            }
        }

        let mut version = rows.version();

        if let Some(mask) = &self.mask {
            version = version.mask(mask);
        }

        Ok(Some(version))
    }

    async fn delete(&self, id: &ID) -> Result<(), StoreError<ID>> {
        if self.delete.unsupported() {
            return Err(StoreError::Unsupported);
        }

        let mut db = self.pool.acquire().await.box_err()?;
        let mut tx = db.begin().await.box_err()?;
        let mut delete = sql::command::delete(&self.table, id);
        let _ = delete.build().execute(&mut *tx).await.box_err()?;

        if let Some(snapshots) = &self.snapshots {
            snapshots.prune(id, None).await?;
        }

        tx.commit().await.box_err()?;
        Ok(())
    }
}
