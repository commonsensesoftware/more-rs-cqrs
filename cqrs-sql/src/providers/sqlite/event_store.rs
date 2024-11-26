use super::command as cmd;
use crate::{
    event::{command, get_snapshot, select_version},
    new_version,
    sql::{self, Context, Ident, IntoRows},
    BoxErr, SqlStoreBuilder, SqlVersion, SqlVersionPart,
};
use async_stream::try_stream;
use async_trait::async_trait;
use cqrs::{
    event::{Delete, Event, EventStream, IdStream, Predicate, Store, StoreError},
    message::{Schema, Transcoder},
    snapshot, Clock, Mask, Range, Version,
};
use futures::stream;
use sqlx::Sqlite;
use sqlx::{Connection, Decode, Encode, Pool, Row, Type};
use std::{error::Error, fmt::Debug, ops::Bound::Unbounded, sync::Arc, time::SystemTime};

/// Represents a SQLite [event store](Store).
pub struct EventStore<ID> {
    delete: Delete,
    table: String,
    pub(crate) pool: Pool<Sqlite>,
    mask: Option<Arc<dyn Mask>>,
    clock: Arc<dyn Clock>,
    transcoder: Arc<Transcoder<dyn Event>>,
    snapshots: Option<Arc<dyn snapshot::Store<ID>>>,
}

impl<ID> EventStore<ID> {
    /// Initializes a new [`EventStore`].
    ///
    /// # Arguments
    ///
    /// * `table` - the table identifier
    /// * `pool` - the underlying [connection pool](Pool)
    /// * `mask` - the optional [mask](Mask) used to obfuscate [versions](Version)
    /// * `clock` - the associated [clock](Clock)
    /// * `transcoder` - the associated [transcoder](Transcoder)
    /// * `delete` - indicates whether [deletes](Delete) are supported
    pub fn new(
        table: String,
        pool: Pool<Sqlite>,
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
        self.clock.clone()
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
        let snapshot = match get_snapshot(self.snapshots.as_deref(), predicate).await {
            Ok(snapshot) => snapshot,
            Err(error) => return Box::pin(stream::iter(vec![Err(StoreError::from(error))])),
        };
        let name = self.table.clone();
        let mask = self.mask.clone();
        let transcoder = self.transcoder.clone();

        Box::pin(try_stream! {
            const TYPE: usize = 0;
            const REVISION: usize = 1;
            const CONTENT: usize = 2;

            let mut version = Unbounded;

            if let Some(filter) = predicate {
                version = select_version(snapshot.as_ref(), filter, mask.clone());

                if let Some(snapshot) = snapshot {
                    let event = transcoder.decode(&snapshot.schema, &snapshot.content)?;
                    yield event;
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
                let mut event = transcoder.decode(&schema, content)?;

                if let Some(mask) = &mask {
                    event.set_version(event.version().mask(mask));
                }

                yield event;
            }
        })
    }

    async fn save(
        &self,
        id: &ID,
        events: &mut [Box<dyn Event>],
        mut expected_version: Version,
    ) -> Result<(), StoreError<ID>> {
        if expected_version != Version::default() {
            if let Some(mask) = self.mask.clone() {
                expected_version = expected_version.unmask(&mask);
            }
        }

        if expected_version.invalid() {
            return Err(StoreError::InvalidVersion);
        }

        let table = self.table();
        let mut versions = Vec::with_capacity(events.len());
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
            return Ok(());
        };

        let mut db = self.pool.acquire().await.box_err()?;

        if let Some(second) = rows.next() {
            let mut tx = db.begin().await.box_err()?;
            let second = second?;

            if self.delete.supported() {
                if let Some(previous) = first.previous() {
                    cmd::ensure_not_deleted(&table, &previous, &mut tx).await?;
                }
            }

            cmd::insert_transacted(&table, &first, &mut tx).await?;
            cmd::insert_transacted(&table, &second, &mut tx).await?;

            versions.push(new_version(first.version, first.sequence));
            versions.push(new_version(second.version, second.sequence));

            for row in rows {
                let row = row?;
                cmd::insert_transacted(&table, &row, &mut tx).await?;
                versions.push(new_version(row.version, row.sequence));
            }

            tx.commit().await.box_err()?;
        } else {
            let mut execute = true;

            if self.delete.supported() {
                if let Some(previous) = first.previous() {
                    let mut tx = db.begin().await.box_err()?;

                    cmd::ensure_not_deleted(&table, &previous, &mut tx).await?;
                    cmd::insert_transacted(&table, &first, &mut tx).await?;
                    tx.commit().await.box_err()?;
                    execute = false;
                }
            }

            if execute {
                let mut insert = command::insert(&table, &first);

                if let Err(error) = insert.build().execute(&mut *db).await {
                    if let sqlx::Error::Database(error) = &error {
                        if error.is_unique_violation() {
                            return Err(StoreError::Conflict(id.clone(), first.version as u32));
                        }
                    }
                    return Err(StoreError::Unknown(Box::new(error) as Box<dyn Error + Send>));
                } else {
                    versions.push(new_version(first.version, first.sequence));
                }
            }
        }

        if let Some(mask) = &self.mask {
            for (i, version) in versions.into_iter().enumerate() {
                events[i].set_version(version.mask(mask));
            }
        } else {
            for (i, version) in versions.into_iter().enumerate() {
                events[i].set_version(version);
            }
        }

        Ok(())
    }

    async fn delete(&self, id: &ID) -> Result<(), StoreError<ID>> {
        if self.delete.unsupported() {
            return Err(StoreError::Unsupported);
        }

        // using a nested transaction, especially in an async context, can cause sqlite to deadlock.
        // sqlx doesn't appear to provide any extra protections. it's unclear if savepoints will make
        // any difference. snapshots are intrinsically volatile so delete them first. if deleting the
        // events somehow fail, things are still in a recoverable state
        if let Some(snapshots) = &self.snapshots {
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
