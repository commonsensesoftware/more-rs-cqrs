use super::command;
use crate::{
    new_version,
    sql::{Context, Ident, IntoRows},
    BoxErr, SqlStoreBuilder, SqlVersion, SqlVersionPart,
};
use async_stream::try_stream;
use async_trait::async_trait;
use cqrs::{
    event::{Event, EventStream, Predicate, Store, StoreError},
    message::{Descriptor, Schema, Transcoder},
    snapshot::{self, SnapshotError},
    Clock, Mask, Version,
};
use futures::{stream, Stream};
use sqlx::{
    pool::PoolConnection, ColumnIndex, Connection, Database, Decode, Encode, Executor,
    IntoArguments, Pool, Row, Type,
};
use std::{error::Error, fmt::Debug, sync::Arc};

fn select_version<T: Debug + Send>(
    snapshot: Option<&Descriptor>,
    predicate: &Predicate<'_, T>,
    mask: Option<Arc<dyn Mask>>,
) -> Option<i32> {
    if let Some(snapshot) = snapshot {
        let version = if let Some(mask) = &mask {
            snapshot.version.unmask((*mask).clone()).number()
        } else {
            snapshot.version.number()
        };

        if let Some(other) = predicate.version {
            let other = if let Some(mask) = &mask {
                other.unmask((*mask).clone()).number()
            } else {
                other.number()
            };

            if version >= other {
                Some(version)
            } else {
                Some(other)
            }
        } else {
            Some(version)
        }
    } else if let Some(mask) = mask {
        predicate.version.as_ref().map(|v| v.unmask(&mask).number())
    } else {
        predicate.version.as_ref().map(SqlVersion::number)
    }
}

fn query<'a, ID, DB>(
    mut db: PoolConnection<DB>,
    ident: Ident<'a>,
    predicate: Option<&'a Predicate<'a, ID>>,
    mask: Option<Arc<dyn Mask>>,
    transcoder: Arc<Transcoder<dyn Event>>,
    snapshot: Option<Descriptor>,
) -> impl Stream<Item = Result<Box<dyn Event>, StoreError<ID>>> + Send + 'a
where
    ID: Debug + for<'db> Decode<'db, DB> + for<'db> Encode<'db, DB> + Send + Sync + Type<DB> + 'a,
    DB: Database,
    for<'args, 'db> <DB as Database>::Arguments<'args>: IntoArguments<'db, DB>,
    for<'db> &'db mut <DB as Database>::Connection: Executor<'db, Database = DB>,
    i16: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i32: for<'db> Encode<'db, DB> + for<'db> Decode<'db, DB> + Type<DB>,
    i64: for<'db> Encode<'db, DB> + Type<DB>,
    usize: ColumnIndex<<DB as Database>::Row>,
    String: for<'db> Encode<'db, DB> + Type<DB>,
    for<'db> &'db str: Decode<'db, DB> + Type<DB>,
    for<'db> &'db [u8]: Decode<'db, DB> + Type<DB>,
{
    const TYPE: usize = 0;
    const REVISION: usize = 1;
    const CONTENT: usize = 2;

    try_stream! {
        let mut version = None;

        if let Some(filter) = predicate {
            version = select_version(snapshot.as_ref(), filter, mask.clone());

            if let Some(snapshot) = snapshot {
                let event = transcoder.decode(&snapshot.schema, &snapshot.content)?;
                yield event;
            }
        }

        let mut query = command::select(ident, predicate, version);
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
    }
}

/// Represents a SQL [event store](Store).
pub struct SqlStore<ID, DB: Database> {
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
    pub fn new(
        table: Ident<'static>,
        pool: Pool<DB>,
        mask: Option<Arc<dyn Mask>>,
        clock: Arc<dyn Clock>,
        transcoder: Arc<Transcoder<dyn Event>>,
        snapshots: Option<Arc<dyn snapshot::Store<ID>>>,
    ) -> Self {
        Self {
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

impl<ID, DB> SqlStore<ID, DB>
where
    ID: Debug + Send,
    DB: Database,
{
    async fn get_snapshot<'a>(
        &self,
        predicate: Option<&Predicate<'a, ID>>,
    ) -> Result<Option<Descriptor>, SnapshotError> {
        if let Some(snapshots) = &self.snapshots {
            if let Some(predicate) = predicate {
                if predicate.load.snapshots {
                    if let Some(id) = predicate.id {
                        let predicate = Some(predicate.into());
                        return snapshots.load_raw(id, predicate.as_ref()).await;
                    }
                }
            }
        }

        Ok(None)
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
        + Type<DB>,
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
{
    async fn load<'a>(&self, predicate: Option<&'a Predicate<'a, ID>>) -> EventStream<'a, ID> {
        let db = match self.pool.acquire().await.box_err() {
            Ok(db) => db,
            Err(error) => return Box::pin(stream::iter(vec![Err(StoreError::Unknown(error))])),
        };
        let snapshot = match self.get_snapshot(predicate).await {
            Ok(snapshot) => snapshot,
            Err(error) => return Box::pin(stream::iter(vec![Err(StoreError::from(error))])),
        };
        let table = self.table.clone();
        let mask = self.mask.clone();
        let transcoder = self.transcoder.clone();

        Box::pin(query(db, table, predicate, mask, transcoder, snapshot))
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
            println!("invalid version: {:?}", expected_version);
            return Err(StoreError::InvalidVersion);
        }

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

            command::insert_transacted(&self.table, &first, &mut tx).await?;
            command::insert_transacted(&self.table, &second, &mut tx).await?;

            versions.push(new_version(first.version, first.sequence));
            versions.push(new_version(second.version, second.sequence));

            for row in rows {
                let row = row?;
                command::insert_transacted(&self.table, &row, &mut tx).await?;
                versions.push(new_version(row.version, row.sequence));
            }

            tx.commit().await.box_err()?;
        } else {
            let mut insert = command::insert(&self.table, &first);

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
}
