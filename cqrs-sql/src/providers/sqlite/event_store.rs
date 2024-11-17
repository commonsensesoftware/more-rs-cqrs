use super::event_command as command;
use crate::{
    new_version,
    sql::{Context, Ident, IntoRows},
    BoxErr, SqlStoreBuilder, SqlVersion, SqlVersionPart,
};
use async_stream::try_stream;
use async_trait::async_trait;
use cqrs::{
    event::{Event, EventStream, IdStream, Predicate, Store, StoreError},
    message::{Descriptor, Schema, Transcoder},
    snapshot::{self, SnapshotError},
    Clock, Mask, Range, Version,
};
use futures::{stream, Stream};
use sqlx::Sqlite;
use sqlx::{pool::PoolConnection, Connection, Decode, Encode, Pool, Row, Type};
use std::{
    error::Error,
    fmt::Debug,
    ops::Bound::{Excluded, Included},
    sync::Arc,
    time::SystemTime,
};

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

        match predicate.version {
            Included(other) => {
                let other = if let Some(mask) = &mask {
                    other.unmask((*mask).clone()).number()
                } else {
                    other.number()
                };

                if other >= version {
                    return Some(other);
                }
            }
            Excluded(other) => {
                let other = if let Some(mask) = &mask {
                    other.unmask((*mask).clone()).number()
                } else {
                    other.number()
                };

                if other > version {
                    return Some(other);
                }
            }
            _ => {}
        }

        Some(version)
    } else if let Some(mask) = mask {
        match predicate.version {
            Included(version) => Some(version.unmask(&mask).number()),
            Excluded(version) => Some(version.unmask(&mask).number()),
            _ => None,
        }
    } else {
        match predicate.version {
            Included(version) => Some(version.number()),
            Excluded(version) => Some(version.number()),
            _ => None,
        }
    }
}

fn distinct_ids<ID>(
    mut db: PoolConnection<Sqlite>,
    ident: Ident,
    stored_on: Range<SystemTime>,
) -> impl Stream<Item = Result<ID, StoreError<ID>>> + Send + '_
where
    ID: Debug + for<'db> Decode<'db, Sqlite> + Send + Sync + Type<Sqlite> + 'static,
{
    try_stream! {
        let mut query = command::select_id(ident, stored_on);
        let rows = query.build().fetch(&mut *db);

        for await row in rows {
            yield row.box_err()?.get::<ID, _>(0);
        }
    }
}

fn query<'a, ID>(
    mut db: PoolConnection<Sqlite>,
    ident: Ident<'a>,
    predicate: Option<&'a Predicate<'a, ID>>,
    mask: Option<Arc<dyn Mask>>,
    transcoder: Arc<Transcoder<dyn Event>>,
    snapshot: Option<Descriptor>,
) -> impl Stream<Item = Result<Box<dyn Event>, StoreError<ID>>> + Send + 'a
where
    ID: Debug
        + for<'db> Decode<'db, Sqlite>
        + for<'db> Encode<'db, Sqlite>
        + Send
        + Sync
        + Type<Sqlite>
        + 'a,
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

/// Represents a SQLite [event store](Store).
pub struct EventStore<ID> {
    pub(crate) table: Ident<'static>,
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
    /// * `table` - the table [identifier](Ident)
    /// * `pool` - the underlying [connection pool](Pool)
    /// * `mask` - the optional [mask](Mask) used to obfuscate [versions](Version)
    /// * `clock` - the associated [clock](Clock)
    /// * `transcoder` - the associated [transcoder](Transcoder)
    pub fn new(
        table: Ident<'static>,
        pool: Pool<Sqlite>,
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
    pub fn builder() -> SqlStoreBuilder<ID, dyn Event, Sqlite> {
        SqlStoreBuilder::default()
    }
}

impl<ID: Debug + Send> EventStore<ID> {
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
    async fn ids(&self, stored_on: Range<SystemTime>) -> IdStream<ID> {
        let db = match self.pool.acquire().await.box_err() {
            Ok(db) => db,
            Err(error) => return Box::pin(stream::iter(vec![Err(StoreError::Unknown(error))])),
        };
        let table = self.table.clone();

        Box::pin(distinct_ids(db, table, stored_on))
    }

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
