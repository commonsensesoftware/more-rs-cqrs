use super::{Builder, coerce, delete_all, greater_than, less_than};
use crate::{
    BoxErr, NoSqlVersion,
    NoSqlVersionPart::{Sequence, Version as ByOne},
    version::{from_sort_key, new_version},
};
use async_stream::try_stream;
use async_trait::async_trait;
use aws_sdk_dynamodb::{
    Client,
    operation::{
        query::builders::QueryFluentBuilder,
        transact_write_items::TransactWriteItemsError::TransactionCanceledException,
    },
    primitives::Blob,
    types::{
        AttributeValue::{self as Attr, B, N, S},
        Put, TransactWriteItem, Update,
    },
};
use cqrs::{
    Clock, Mask, Range, Version,
    event::{Delete, Event, EventStream, IdStream, Predicate, PredicateBuilder, Store, StoreError},
    message::{Saved, Schema, Transcoder},
    snapshot,
};
use std::{error::Error, fmt::Debug, str::FromStr, sync::Arc, time::SystemTime};

fn apply_predicate<T>(
    mut request: QueryFluentBuilder,
    predicate: Option<&Predicate<T>>,
    mask: Option<Arc<dyn Mask>>,
) -> QueryFluentBuilder
where
    T: Debug + Send + ToString,
{
    if let Some(predicate) = predicate {
        let mut condition = String::new();
        let mut filter = String::new();

        if let Some(id) = predicate.id {
            condition.push_str("(id = :id)");
            request = request.expression_attribute_values(":id", S(id.to_string()));
        }

        if let Some((mut version, op)) = greater_than(&predicate.version) {
            if let Some(mask) = mask {
                version = version.unmask(mask);
            }

            condition.push_str(" AND (version ");
            condition.push_str(op);
            condition.push_str(" :version)");
            request =
                request.expression_attribute_values(":version", N(version.sort_key().to_string()));
        }

        if let Some((from, op)) = greater_than(&predicate.stored_on.from) {
            filter.push_str("(storedOn ");
            filter.push_str(op);
            filter.push_str(" :from)");
            request =
                request.expression_attribute_values(":from", N(crate::to_secs(from).to_string()));
        }

        if let Some((to, op)) = less_than(&predicate.stored_on.to) {
            if !filter.is_empty() {
                filter.push_str(" AND ");
            }

            filter.push_str("(storedOn ");
            filter.push_str(op);
            filter.push_str(" :to)");
            request = request.expression_attribute_values(":to", N(crate::to_secs(to).to_string()));
        }

        let mut schemas = predicate.types.iter();

        if let Some(schema) = schemas.next() {
            let many = predicate.types.len() > 1;

            if !filter.is_empty() {
                filter.push_str(" AND ");
            }

            if many {
                filter.push('(');
            }

            let mut i = 1usize;
            let mut kind = format!(":kind{i}");
            let mut rev = format!(":rev{i}");

            filter.push_str("(kind = ");
            filter.push_str(&kind);
            filter.push_str(" AND revision = ");
            filter.push_str(&rev);
            filter.push(')');
            request = request
                .expression_attribute_values(kind, S(schema.kind().into()))
                .expression_attribute_values(rev, S(schema.version().to_string()));

            for schema in schemas {
                i += 1;
                kind = format!(":kind{i}");
                rev = format!(":rev{i}");

                filter.push_str(" OR (kind = ");
                filter.push_str(&kind);
                filter.push_str(" AND revision = ");
                filter.push_str(&rev);
                filter.push(')');
                request = request
                    .expression_attribute_values(kind, S(schema.kind().into()))
                    .expression_attribute_values(rev, S(schema.version().to_string()));
            }

            if many {
                filter.push(')');
            }
        }

        if !filter.is_empty() {
            request = request.filter_expression(filter);
        }

        request.key_condition_expression(condition)
    } else {
        request
    }
}

/// Represents an Amazon DynamoDB [event store](Store).
pub struct EventStore<T> {
    ddb: Client,
    table: String,
    delete: Delete,
    mask: Option<Arc<dyn Mask>>,
    clock: Arc<dyn Clock>,
    transcoder: Arc<Transcoder<dyn Event>>,
    snapshots: Option<Arc<dyn snapshot::Store<T>>>,
}

impl<T> EventStore<T> {
    /// Initializes a new [EventStore].
    ///
    /// # Arguments
    ///
    /// * `client` - the underlying [client](Client)
    /// * `table` - the table identifier
    /// * `mask` - the optional [mask](Mask) used to obfuscate [versions](Version)
    /// * `clock` - the associated [clock](Clock)
    /// * `transcoder` - the associated [transcoder](Transcoder)
    /// * `delete` - indicates whether [deletes](Delete) are supported
    pub fn new(
        client: Client,
        table: String,
        mask: Option<Arc<dyn Mask>>,
        clock: Arc<dyn Clock>,
        transcoder: Arc<Transcoder<dyn Event>>,
        snapshots: Option<Arc<dyn snapshot::Store<T>>>,
        delete: Delete,
    ) -> Self {
        Self {
            ddb: client,
            table,
            delete,
            mask,
            clock,
            transcoder,
            snapshots,
        }
    }

    /// Creates and returns a new [Builder].
    pub fn builder() -> Builder<T, dyn Event> {
        Builder::default()
    }
}

impl<T> EventStore<T>
where
    T: Clone + Debug + Send + Sync + ToString + 'static,
{
    #[allow(clippy::borrowed_box)]
    async fn write_one(
        &self,
        id: &T,
        version: Version,
        event: &Box<dyn Event>,
    ) -> Result<Version, StoreError<T>> {
        let stored_on = crate::to_secs(self.clock.now());
        let schema = event.schema();
        let content = self.transcoder.encode(event.as_ref())?;

        if self.delete.supported()
            && let Some(previous) = version.previous()
        {
            let mut request = self.ddb.transact_write_items();

            // the following update doesn't change anything, but it ensures the previous
            // version still exists and hasn't been deleted
            let update = Update::builder()
                .table_name(&self.table)
                .key("id", S(id.to_string()))
                .key("version", N(previous.sort_key().to_string()))
                .update_expression("SET version = :version")
                .condition_expression("attribute_exists(id) AND attribute_exists(version)")
                .expression_attribute_values(":version", N(previous.sort_key().to_string()));

            request = request.transact_items(
                TransactWriteItem::builder()
                    .update(update.build().unwrap())
                    .build(),
            );

            let mut put = Put::builder()
                .table_name(&self.table)
                .item("id", S(id.to_string()))
                .item("version", N(version.sort_key().to_string()))
                .item("storedOn", N(stored_on.to_string()))
                .item("kind", S(schema.kind().into()))
                .item("revision", N(schema.version().to_string()))
                .item("content", B(Blob::new(content)))
                .condition_expression("attribute_not_exists(id) AND attribute_not_exists(version)");

            if let Some(cid) = event.correlation_id() {
                put = put.item("correlationId", S(cid.into()));
            }

            request = request.transact_items(
                TransactWriteItem::builder()
                    .put(put.build().unwrap())
                    .build(),
            );

            if let Err(failure) = request.send().await {
                let error = failure.into_service_error();

                if let TransactionCanceledException(canceled) = &error
                    && let Some(reasons) = &canceled.cancellation_reasons
                {
                    for reason in reasons {
                        if let Some(code) = &reason.code
                            && code == "ConditionalCheckFailed"
                        {
                            if let Some(item) = &reason.item {
                                let existing = from_sort_key(coerce("version", item, Attr::as_n));

                                if version.number() == existing.number() {
                                    return Err(StoreError::Conflict(id.clone(), version.number()));
                                }
                            }

                            return Err(StoreError::Deleted(id.clone()));
                        }
                    }
                }

                return Err(StoreError::Unknown(Box::new(error) as Box<dyn Error + Send>));
            } else {
                return Ok(version);
            }
        }

        let mut request = self
            .ddb
            .put_item()
            .table_name(&self.table)
            .item("id", S(id.to_string()))
            .item("version", N(version.sort_key().to_string()))
            .item("storedOn", N(stored_on.to_string()))
            .item("kind", S(schema.kind().into()))
            .item("revision", N(schema.version().to_string()))
            .item("content", B(Blob::new(content)))
            .condition_expression("attribute_not_exists(id) AND attribute_not_exists(version)");

        if let Some(cid) = event.correlation_id() {
            request = request.item("correlationId", S(cid.into()));
        }

        if let Err(failure) = request.send().await {
            let error = failure.into_service_error();

            if error.is_conditional_check_failed_exception()
                || error.is_transaction_conflict_exception()
            {
                Err(StoreError::Conflict(id.clone(), version.number()))
            } else {
                Err(StoreError::Unknown(Box::new(error) as Box<dyn Error + Send>))
            }
        } else {
            Ok(version)
        }
    }

    async fn write_all(
        &self,
        id: &T,
        mut version: Version,
        events: &[Box<dyn Event>],
    ) -> Result<Version, StoreError<T>> {
        let stored_on = crate::to_secs(self.clock.now());
        let mut request = self.ddb.transact_write_items();

        if self.delete.supported()
            && let Some(previous) = version.previous()
        {
            // the following update doesn't change anything, but it ensures the previous
            // version still exists and hasn't been deleted
            let update = Update::builder()
                .table_name(&self.table)
                .key("id", S(id.to_string()))
                .key("version", N(previous.sort_key().to_string()))
                .update_expression("SET version = :version")
                .condition_expression("attribute_exists(id) AND attribute_exists(version)")
                .expression_attribute_values(":version", N(previous.sort_key().to_string()));

            request = request.transact_items(
                TransactWriteItem::builder()
                    .update(update.build().unwrap())
                    .build(),
            );
        }

        let mut current_version = version;

        for event in events {
            let schema = event.schema();
            let content = self.transcoder.encode(event.as_ref())?;
            let mut put = Put::builder()
                .table_name(&self.table)
                .item("id", S(id.to_string()))
                .item("version", N(version.sort_key().to_string()))
                .item("storedOn", N(stored_on.to_string()))
                .item("kind", S(schema.kind().into()))
                .item("revision", N(schema.version().to_string()))
                .item("content", B(Blob::new(content)))
                .condition_expression("attribute_not_exists(id) AND attribute_not_exists(version)");

            if let Some(cid) = event.correlation_id() {
                put = put.item("correlationId", S(cid.into()));
            }

            request = request.transact_items(
                TransactWriteItem::builder()
                    .put(put.build().unwrap())
                    .build(),
            );
            current_version = version;
            version = current_version.increment(Sequence);
        }

        version = current_version;

        if let Err(failure) = request.send().await {
            let error = failure.into_service_error();

            if let TransactionCanceledException(canceled) = &error
                && let Some(reasons) = &canceled.cancellation_reasons
            {
                for reason in reasons {
                    if let Some(code) = &reason.code
                        && code == "ConditionalCheckFailed"
                    {
                        if let Some(item) = &reason.item {
                            let existing = from_sort_key(coerce("version", item, Attr::as_n));

                            if version.number() == existing.number() {
                                return Err(StoreError::Conflict(id.clone(), version.number()));
                            }
                        }

                        return Err(StoreError::Deleted(id.clone()));
                    }
                }
            }

            Err(StoreError::Unknown(Box::new(error) as Box<dyn Error + Send>))
        } else {
            Ok(version)
        }
    }
}

#[async_trait]
impl<T> Store<T> for EventStore<T>
where
    T: Clone + Debug + Default + FromStr + Send + Sync + ToString + 'static,
{
    fn clock(&self) -> Arc<dyn Clock> {
        self.clock.clone()
    }

    async fn ids(&self, stored_on: Range<SystemTime>) -> IdStream<T> {
        let request = self
            .ddb
            .query()
            .table_name(&self.table)
            .key_condition_expression("version = :version")
            .expression_attribute_values(":version", N(new_version(1, 0).sort_key().to_string()));
        let mask = self.mask.clone();
        let predicate = PredicateBuilder::<T>::new(None)
            .stored_on(stored_on)
            .build();
        let query = apply_predicate(request, Some(&predicate), mask.clone()).into_paginator();
        let mut items = query.items().send();

        Box::pin(try_stream! {
            while let Some(item) = items.next().await {
                let attributes = item.box_err()?;
                yield coerce::<T>("id", &attributes, Attr::as_s);
            }
        })
    }

    async fn load<'a>(&self, predicate: Option<&'a Predicate<'a, T>>) -> EventStream<'a, T> {
        let request = self.ddb.query().table_name(&self.table);
        let mask = self.mask.clone();
        let query = apply_predicate(request, predicate, mask.clone()).into_paginator();
        let mut items = query.items().send();
        let transcoder = self.transcoder.clone();

        Box::pin(try_stream! {
            while let Some(item) = items.next().await {
                let attributes = item.box_err()?;
                let mut version = from_sort_key(coerce("version", &attributes, Attr::as_n));
                let schema = Schema::new(
                    coerce::<String>("kind", &attributes, Attr::as_s),
                    coerce("revision", &attributes, Attr::as_n));
                let empty = Blob::default();
                let content = if let Some(attribute) = attributes.get("content") {
                    attribute.as_b().unwrap_or(&empty)
                } else {
                    &empty
                };
                let event = transcoder.decode(&schema, content.as_ref())?;

                if let Some(mask) = &mask {
                    version = version.mask(mask);
                }

                yield Saved::versioned(event, version);
            }
        })
    }

    async fn save(
        &self,
        id: &T,
        mut expected_version: Version,
        events: &[Box<dyn Event>],
    ) -> Result<Option<Version>, StoreError<T>> {
        if events.is_empty() {
            return Ok(None);
        }

        if expected_version != Version::default()
            && let Some(mask) = &self.mask
        {
            expected_version = expected_version.unmask(mask);
        }

        if expected_version.invalid() {
            return Err(StoreError::InvalidVersion);
        }

        expected_version = expected_version.increment(ByOne);

        let mut version = if events.len() == 1 {
            self.write_one(id, expected_version, &events[0]).await?
        } else {
            self.write_all(id, expected_version, events).await?
        };

        if let Some(mask) = &self.mask {
            version = version.mask(mask);
        }

        Ok(Some(version))
    }

    async fn delete(&self, id: &T) -> Result<(), StoreError<T>> {
        if self.delete.unsupported() {
            return Err(StoreError::Unsupported);
        }

        if let Some(snapshots) = &self.snapshots {
            snapshots.prune(id, None).await?;
        }

        delete_all(&self.ddb, &self.table, id.to_string(), None).await?;
        Ok(())
    }
}
