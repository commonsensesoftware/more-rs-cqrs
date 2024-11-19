use super::{command, Prune, Upsert};
use crate::{
    new_version,
    sql::{self, Ident},
    BoxErr, SqlStoreBuilder, SqlVersion,
};
use async_trait::async_trait;
use cqrs::{
    message::{Descriptor, Schema, Transcoder},
    snapshot::{Predicate, Retention, Snapshot, SnapshotError, Store},
    Clock, Mask,
};
use futures::StreamExt;
use sqlx::{
    ColumnIndex, Connection, Database, Decode, Encode, Executor, IntoArguments, Pool, Row, Type,
};
use std::{fmt::Debug, marker::PhantomData, sync::Arc};

/// Represents a SQL [snapshot store](Store).
pub struct SqlStore<ID, DB: Database> {
    _id: PhantomData<ID>,
    pub(crate) table: Ident<'static>,
    pub(crate) pool: Pool<DB>,
    mask: Option<Arc<dyn Mask>>,
    clock: Arc<dyn Clock>,
    transcoder: Arc<Transcoder<dyn Snapshot>>,
    retention: Retention,
}

impl<ID, DB: Database> SqlStore<ID, DB> {
    /// Initializes a new [`SqlStore`].
    ///
    /// # Arguments
    ///
    /// * `table` - the table [identifier](Ident)
    /// * `pool` - the underlying [connection pool](Pool)
    /// * `mask` - the [mask](Mask) used to obfuscate [versions](Version)
    /// * `clock` - the associated [clock](Clock)
    /// * `transcoder` - the associated [transcoder](Transcoder)
    /// * `retention` - the [retention](Retention) policy
    pub fn new(
        table: Ident<'static>,
        pool: Pool<DB>,
        mask: Option<Arc<dyn Mask>>,
        clock: Arc<dyn Clock>,
        transcoder: Arc<Transcoder<dyn Snapshot>>,
        retention: Retention,
    ) -> Self {
        Self {
            _id: PhantomData,
            table,
            pool,
            mask,
            clock,
            transcoder,
            retention,
        }
    }

    /// Creates and returns a new [`SqlStoreBuilder`].
    pub fn builder() -> SqlStoreBuilder<ID, dyn Snapshot, DB> {
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
        + Type<DB>,
    DB: Database + for<'db> Prune<'db, ID, DB> + Upsert,
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
    async fn load(
        &self,
        id: &ID,
        predicate: Option<&Predicate>,
    ) -> Result<Option<Box<dyn Snapshot>>, SnapshotError> {
        if let Some(descriptor) = self.load_raw(id, predicate).await? {
            let mut snapshot = self
                .transcoder
                .decode(&descriptor.schema, &descriptor.content)?;

            snapshot.set_version(descriptor.version);

            Ok(Some(snapshot))
        } else {
            Ok(None)
        }
    }

    async fn load_raw(
        &self,
        id: &ID,
        predicate: Option<&Predicate>,
    ) -> Result<Option<Descriptor>, SnapshotError> {
        const VERSION: usize = 0;
        const SEQUENCE: usize = 1;
        const TYPE: usize = 2;
        const REVISION: usize = 3;
        const CONTENT: usize = 4;

        let mut db = self.pool.acquire().await.box_err()?;
        let mut query = command::select(&self.table, id, predicate, self.mask.clone());
        let mut rows = query.build().fetch(&mut *db);

        if let Some(result) = rows.next().await {
            let row = result.box_err()?;
            let schema = Schema::new(row.get::<&str, _>(TYPE), row.get::<i16, _>(REVISION) as u8);
            let mut version = new_version(row.get::<i32, _>(VERSION), row.get::<i16, _>(SEQUENCE));
            let content = row.get::<&[u8], _>(CONTENT);

            if let Some(mask) = &self.mask {
                version = version.mask(mask);
            }

            let snapshot = Descriptor::new(schema, version, content.to_vec());

            Ok(Some(snapshot))
        } else {
            Ok(None)
        }
    }

    async fn save(&self, id: &ID, snapshot: Box<dyn Snapshot>) -> Result<(), SnapshotError> {
        let mut version = snapshot.version();

        if version != Default::default() {
            if let Some(mask) = self.mask.clone() {
                version = version.unmask(&mask);
            }
        }

        if version.invalid() {
            return Err(SnapshotError::InvalidVersion);
        }

        let stored_on = crate::to_secs(self.clock.now());
        let schema = snapshot.schema();
        let content = match self.transcoder.encode(snapshot.as_ref()) {
            Ok(content) => content,
            Err(error) => return Err(SnapshotError::InvalidEncoding(error)),
        };
        let row = sql::Row::<ID> {
            id: id.clone(),
            version: version.number(),
            sequence: Default::default(),
            stored_on,
            kind: schema.kind().into(),
            revision: schema.version() as i16,
            content,
            correlation_id: None,
        };
        let mut db = self.pool.acquire().await.box_err()?;

        if self.retention.all() {
            let mut insert = command::insert(&self.table, &row);
            let _ = insert.build().execute(&mut *db).await.box_err()?;
        } else {
            let mut tx = db.begin().await.box_err()?;
            let mut delete = DB::prune(&self.table, id, &*self.clock, &self.retention);
            let mut insert = command::insert(&self.table, &row);
            let _ = delete.build().execute(&mut *tx).await.box_err()?;
            let _ = insert.build().execute(&mut *tx).await.box_err()?;
            tx.commit().await.box_err()?;
        }

        Ok(())
    }

    async fn delete(&self, id: &ID) -> Result<(), SnapshotError> {
        let mut db = self.pool.acquire().await.box_err()?;
        let mut tx = db.begin().await.box_err()?;
        let mut delete = sql::command::delete(&self.table, id);
        let _ = delete.build().execute(&mut *tx).await.box_err()?;
        tx.commit().await.box_err()?;
        Ok(())
    }
}
