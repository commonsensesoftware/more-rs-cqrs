use super::{Builder, coerce, delete_all, greater_than, less_than};
use crate::{BoxErr, NoSqlVersion, version::new_version};
use async_trait::async_trait;
use aws_sdk_dynamodb::{
    Client,
    primitives::Blob,
    types::AttributeValue::{self, B, N, S},
};
use cqrs::{
    Clock, Mask, Version,
    message::{Descriptor, Saved, Schema, Transcoder},
    snapshot::{Predicate, Retention, Snapshot, SnapshotError, Store},
};
use std::{fmt::Debug, marker::PhantomData, sync::Arc};

/// Represents an Amazon DynamoDB [snapshot store](Store).
pub struct SnapshotStore<T> {
    _id: PhantomData<T>,
    ddb: Client,
    table: String,
    mask: Option<Arc<dyn Mask>>,
    clock: Arc<dyn Clock>,
    transcoder: Arc<Transcoder<dyn Snapshot>>,
}

impl<T> SnapshotStore<T> {
    /// Initializes a new [SnapshotStore].
    ///
    /// # Arguments
    ///
    /// * `client` - the underlying [client](Client)
    /// * `table` - the table identifier
    /// * `mask` - the [mask](Mask) used to obfuscate [versions](cqrs::Version)
    /// * `clock` - the associated [clock](Clock)
    /// * `transcoder` - the associated [transcoder](Transcoder)
    pub fn new(
        client: Client,
        table: String,
        mask: Option<Arc<dyn Mask>>,
        clock: Arc<dyn Clock>,
        transcoder: Arc<Transcoder<dyn Snapshot>>,
    ) -> Self {
        Self {
            _id: PhantomData,
            ddb: client,
            table,
            mask,
            clock,
            transcoder,
        }
    }

    /// Creates and returns a new [Builder].
    pub fn builder() -> Builder<T, dyn Snapshot> {
        Builder::default()
    }
}

#[async_trait]
impl<T> Store<T> for SnapshotStore<T>
where
    T: Clone + Debug + Send + Sync + ToString,
{
    async fn load(
        &self,
        id: &T,
        predicate: Option<&Predicate>,
    ) -> Result<Option<Saved<Box<dyn Snapshot>>>, SnapshotError> {
        if let Some(descriptor) = self.load_raw(id, predicate).await? {
            Ok(Some(Saved::versioned(
                self.transcoder
                    .decode(&descriptor.schema, &descriptor.content)?,
                descriptor.version,
            )))
        } else {
            Ok(None)
        }
    }

    async fn load_raw(
        &self,
        id: &T,
        predicate: Option<&Predicate>,
    ) -> Result<Option<Descriptor>, SnapshotError> {
        let mut condition = String::from("id = :id");
        let mut request = self
            .ddb
            .query()
            .table_name(&self.table)
            .expression_attribute_values(":id", S(id.to_string()))
            .limit(1);

        if let Some(predicate) = predicate {
            if let Some((mut version, op)) = greater_than(&predicate.min_version) {
                if let Some(mask) = &self.mask {
                    version = version.unmask(mask);
                }

                condition.push_str(" AND version ");
                condition.push_str(op);
                condition.push_str(" :version");
                request = request
                    .expression_attribute_values(":version", N(version.number().to_string()));
            }

            // filters are processed after key matches and we're going in reverse so we want the
            // ones less than the last or we'll always end up with the last match
            if let Some((since, op)) = less_than(&predicate.since) {
                let mut filter = String::with_capacity(17);

                filter.push_str("takenOn ");
                filter.push_str(op);
                filter.push_str(" :since");
                request = request
                    .expression_attribute_values(":since", N(crate::to_secs(since).to_string()))
                    .filter_expression(filter)
                    .scan_index_forward(false);
            }
        } else {
            request = request.scan_index_forward(false);
        }

        let query = request.key_condition_expression(condition).into_paginator();
        let mut items = query.items().send();

        if let Some(item) = items.next().await {
            let attributes = item.box_err()?;
            let schema = Schema::new(
                coerce::<String>("kind", &attributes, AttributeValue::as_s),
                coerce("revision", &attributes, AttributeValue::as_n),
            );
            let mut version = new_version(coerce("version", &attributes, AttributeValue::as_n), 0);
            let empty = Blob::default();
            let content = if let Some(attribute) = attributes.get("content") {
                attribute.as_b().unwrap_or(&empty)
            } else {
                &empty
            };

            if let Some(mask) = &self.mask {
                version = version.mask(mask);
            }

            let snapshot = Descriptor::new(schema, version, content.clone().into_inner());

            Ok(Some(snapshot))
        } else {
            Ok(None)
        }
    }

    async fn save(
        &self,
        id: &T,
        mut version: Version,
        snapshot: Box<dyn Snapshot>,
    ) -> Result<(), SnapshotError> {
        if version != Default::default()
            && let Some(mask) = &self.mask
        {
            version = version.unmask(mask);
        }

        if version.invalid() {
            return Err(SnapshotError::InvalidVersion);
        }

        let stored_on = crate::to_secs(self.clock.now());
        let schema = snapshot.schema();
        let content = self.transcoder.encode(snapshot.as_ref())?;
        let request = self
            .ddb
            .put_item()
            .table_name(&self.table)
            .item("id", S(id.to_string()))
            .item("version", N(version.number().to_string()))
            .item("takenOn", N(stored_on.to_string()))
            .item("kind", S(schema.kind().into()))
            .item("revision", N(schema.version().to_string()))
            .item("content", B(Blob::new(content)));

        request.send().await.box_err()?;
        Ok(())
    }

    async fn prune(&self, id: &T, retention: Option<&Retention>) -> Result<(), SnapshotError> {
        delete_all(&self.ddb, &self.table, id.to_string(), retention).await?;
        Ok(())
    }
}
