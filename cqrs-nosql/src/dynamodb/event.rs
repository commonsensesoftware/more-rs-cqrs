use super::{Builder, MAX_TRANSACTION_SIZE, coerce, delete_all, greater_than, less_than};
use crate::{
    BoxErr, NoSqlVersion,
    NoSqlVersionPart::Sequence,
    append::Append,
    snapshot::{get_snapshot, select_version},
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
        Put,
        ReturnValuesOnConditionCheckFailure::AllOld,
        TransactWriteItem, Update,
    },
};
use cqrs::{
    Clock, Range, Version,
    event::{Event, EventStream, IdStream, Predicate, Store, StoreError, StoreOptions, filter_types},
    message::{Saved, Schema},
};
use futures::stream;
use std::{
    convert::Infallible, error::Error, fmt::Debug, num::NonZeroU8, ops::Bound, str::FromStr, sync::Arc,
    time::SystemTime,
};

/// Translates the message types of a predicate into a DynamoDB filter expression.
struct TypeFilter<'a> {
    text: &'a mut String,

    // the request is built by value, so it is taken and replaced as each value is added
    request: Option<QueryFluentBuilder>,
}

impl<'a> cqrs::event::TypeFilter for TypeFilter<'a> {
    type Error = Infallible;

    fn begin(&mut self, many: bool) {
        if many {
            self.text.push('(');
        }
    }

    // the attribute values are 1-based to match the other placeholders in the expression
    fn condition(&mut self, index: usize, kind: &str, revision: Option<NonZeroU8>) -> Result<(), Self::Error> {
        let index = index + 1;
        let mut request = self.request.take().unwrap();

        self.text.push_str("(kind = :kind");
        self.text.push_str(&index.to_string());
        request = request.expression_attribute_values(format!(":kind{index}"), S(kind.into()));

        if let Some(revision) = revision {
            self.text.push_str(" AND revision = :rev");
            self.text.push_str(&index.to_string());
            request = request.expression_attribute_values(format!(":rev{index}"), N(revision.to_string()));
        }

        self.text.push(')');
        self.request = Some(request);
        Ok(())
    }

    fn or(&mut self) {
        self.text.push_str(" OR ");
    }

    fn end(&mut self, many: bool) {
        if many {
            self.text.push(')');
        }
    }
}

fn apply_predicate<T>(
    mut request: QueryFluentBuilder,
    predicate: Option<&Predicate<T>>,
    version: Bound<Version>,
) -> QueryFluentBuilder
where
    T: Debug + Send + ToString,
{
    let mut condition = String::new();

    if let Some(id) = predicate.and_then(|predicate| predicate.id) {
        condition.push_str("(id = :id)");
        request = request.expression_attribute_values(":id", S(id.to_string()));
    }

    // the version bound has already been resolved against the snapshot a load is seeded
    // from, if any, so it is never masked
    if let Some((version, op)) = greater_than(&version) {
        if !condition.is_empty() {
            condition.push_str(" AND ");
        }

        condition.push_str("(version ");
        condition.push_str(op);
        condition.push_str(" :version)");
        request = request.expression_attribute_values(":version", N(version.sort_key().to_string()));
    }

    // a key condition that is empty would replace the one the caller has already configured
    if !condition.is_empty() {
        request = request.key_condition_expression(condition);
    }

    if let Some(predicate) = predicate {
        let mut filter = String::new();

        if let Some((from, op)) = greater_than(&predicate.stored_on.from) {
            filter.push_str("(storedOn ");
            filter.push_str(op);
            filter.push_str(" :from)");
            request = request.expression_attribute_values(":from", N(crate::to_secs(from).to_string()));
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

        if !predicate.types.is_empty() && !filter.is_empty() {
            filter.push_str(" AND ");
        }

        let mut types = TypeFilter {
            text: &mut filter,
            request: Some(request),
        };

        filter_types(&predicate.types, &mut types).unwrap();
        request = types.request.unwrap();

        if !filter.is_empty() {
            request = request.filter_expression(filter);
        }
    }

    request
}

/// Represents an Amazon DynamoDB [event store](Store).
pub struct EventStore<ID> {
    ddb: Client,
    table: String,
    options: StoreOptions<ID>,
}

impl<ID> EventStore<ID> {
    /// Initializes a new [EventStore].
    ///
    /// # Arguments
    ///
    /// * `client` - the underlying [client](Client)
    /// * `table` - the table identifier
    /// * `options` - the [store options](StoreOptions)
    pub fn new(client: Client, table: String, options: StoreOptions<ID>) -> Self {
        Self {
            ddb: client,
            table,
            options,
        }
    }

    /// Creates and returns a new [Builder].
    pub fn builder() -> Builder<ID, dyn Event> {
        Builder::default()
    }
}

#[async_trait]
impl<ID> Append<ID> for EventStore<ID>
where
    ID: Clone + Debug + Send + Sync + ToString + 'static,
{
    const MAX_BATCH_SIZE: usize = MAX_TRANSACTION_SIZE;

    fn options(&self) -> &StoreOptions<ID> {
        &self.options
    }

    async fn write_one(
        &self,
        id: &ID,
        version: Version,
        event: &(dyn Event + 'static),
    ) -> Result<Version, StoreError<ID>> {
        let stored_on = crate::to_secs(self.options.clock().now());
        let schema = event.schema();
        let content = self.options.transcoder().encode(event)?;

        if self.options.delete().supported()
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

            request = request.transact_items(TransactWriteItem::builder().update(update.build().unwrap()).build());

            let mut put = Put::builder()
                .table_name(&self.table)
                .item("id", S(id.to_string()))
                .item("version", N(version.sort_key().to_string()))
                .item("storedOn", N(stored_on.to_string()))
                .item("kind", S(schema.kind().into()))
                .item("revision", N(schema.revision().to_string()))
                .item("content", B(Blob::new(content)))
                .condition_expression("attribute_not_exists(id) AND attribute_not_exists(version)")
                .return_values_on_condition_check_failure(AllOld);

            if let Some(cid) = event.correlation_id() {
                put = put.item("correlationId", S(cid.into()));
            }

            request = request.transact_items(TransactWriteItem::builder().put(put.build().unwrap()).build());

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
            .item("revision", N(schema.revision().to_string()))
            .item("content", B(Blob::new(content)))
            .condition_expression("attribute_not_exists(id) AND attribute_not_exists(version)")
            // the conflicting item identifies whether the version was taken or the aggregate was deleted
            .return_values_on_condition_check_failure(AllOld);

        if let Some(cid) = event.correlation_id() {
            request = request.item("correlationId", S(cid.into()));
        }

        if let Err(failure) = request.send().await {
            let error = failure.into_service_error();

            if error.is_conditional_check_failed_exception() || error.is_transaction_conflict_exception() {
                Err(StoreError::Conflict(id.clone(), version.number()))
            } else {
                Err(StoreError::Unknown(Box::new(error) as Box<dyn Error + Send>))
            }
        } else {
            Ok(version)
        }
    }

    async fn written(&self, id: &ID, version: Version, event: &(dyn Event + 'static)) -> Result<bool, StoreError<ID>> {
        let output = self
            .ddb
            .get_item()
            .table_name(&self.table)
            .key("id", S(id.to_string()))
            .key("version", N(version.sort_key().to_string()))
            .consistent_read(true)
            .send()
            .await
            .box_err()?;
        let Some(existing) = output.item else {
            return Ok(false);
        };
        let schema = event.schema();
        let content = self.options.transcoder().encode(event)?;

        Ok(existing
            .get("kind")
            .and_then(|value| value.as_s().ok())
            .map(String::as_str)
            == Some(schema.kind())
            && coerce::<u8>("revision", &existing, Attr::as_n) == schema.revision().get()
            && existing
                .get("correlationId")
                .and_then(|value| value.as_s().ok())
                .map(String::as_str)
                == event.correlation_id()
            && existing
                .get("content")
                .and_then(|value| value.as_b().ok())
                .map(Blob::as_ref)
                == Some(&content[..]))
    }

    async fn write_all(
        &self,
        id: &ID,
        mut version: Version,
        events: &[Box<dyn Event>],
    ) -> Result<Version, StoreError<ID>> {
        let stored_on = crate::to_secs(self.options.clock().now());
        let mut request = self.ddb.transact_write_items();

        if self.options.delete().supported()
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

            request = request.transact_items(TransactWriteItem::builder().update(update.build().unwrap()).build());
        }

        let mut current_version = version;

        for event in events {
            let schema = event.schema();
            let content = self.options.transcoder().encode(event.as_ref())?;
            let mut put = Put::builder()
                .table_name(&self.table)
                .item("id", S(id.to_string()))
                .item("version", N(version.sort_key().to_string()))
                .item("storedOn", N(stored_on.to_string()))
                .item("kind", S(schema.kind().into()))
                .item("revision", N(schema.revision().to_string()))
                .item("content", B(Blob::new(content)))
                .condition_expression("attribute_not_exists(id) AND attribute_not_exists(version)")
                .return_values_on_condition_check_failure(AllOld);

            if let Some(cid) = event.correlation_id() {
                put = put.item("correlationId", S(cid.into()));
            }

            request = request.transact_items(TransactWriteItem::builder().put(put.build().unwrap()).build());
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
        (&self.options).into()
    }

    /// Streams the unique sets of all identifiers in the store.
    ///
    /// # Arguments
    ///
    /// * `stored_on` - the [date](SystemTime) [range](Range) used to filter results
    ///
    /// # Remarks
    ///
    /// The first event of an aggregate identifies it, but a query requires the partition key, which is the very
    /// identifier being looked up, so the table is scanned instead. A global secondary index on the version would allow
    /// a query at the expense of additional storage.
    async fn ids(&self, stored_on: Range<SystemTime>) -> IdStream<T> {
        let mut filter = String::from("version = :version");
        let mut request = self
            .ddb
            .scan()
            .table_name(&self.table)
            .projection_expression("id")
            .expression_attribute_values(":version", N(new_version(1, 0).sort_key().to_string()));

        if let Some((from, op)) = greater_than(&stored_on.from) {
            filter.push_str(" AND storedOn ");
            filter.push_str(op);
            filter.push_str(" :from");
            request = request.expression_attribute_values(":from", N(crate::to_secs(from).to_string()));
        }

        if let Some((to, op)) = less_than(&stored_on.to) {
            filter.push_str(" AND storedOn ");
            filter.push_str(op);
            filter.push_str(" :to");
            request = request.expression_attribute_values(":to", N(crate::to_secs(to).to_string()));
        }

        let query = request.filter_expression(filter).into_paginator();
        let mut items = query.items().send();

        Box::pin(try_stream! {
            while let Some(item) = items.next().await {
                let attributes = item.box_err()?;
                yield coerce::<T>("id", &attributes, Attr::as_s);
            }
        })
    }

    /// Loads a sequence of [events](Event).
    ///
    /// # Arguments
    ///
    /// * `predicate` - the optional [predicate](Predicate) used to filter events
    ///
    /// # Remarks
    ///
    /// If a [snapshot store](cqrs::snapshot::Store) is configured, the stream is seeded with the most recent
    /// [snapshot](cqrs::snapshot::Snapshot) and only the events which are not already summarized by it are loaded.
    async fn load<'a>(&self, predicate: Option<&'a Predicate<'a, T>>) -> EventStream<'a, T> {
        let snapshot = match get_snapshot(self.options.snapshots(), predicate).await {
            Ok(snapshot) => snapshot,
            Err(error) => return Box::pin(stream::iter(vec![Err(StoreError::from(error))])),
        };
        let mut version = Bound::Unbounded;

        if let Some(filter) = predicate {
            version = select_version(snapshot.as_ref(), filter, self.options.mask());
        }

        let request = self.ddb.query().table_name(&self.table);
        let query = apply_predicate(request, predicate, version).into_paginator();
        let mut items = query.items().send();
        let options = self.options.clone();

        Box::pin(try_stream! {
            if predicate.is_some()
                && let Some(snapshot) = snapshot
            {
                let event = options.transcoder().decode(&snapshot.schema, &snapshot.content)?;
                yield Saved::new(event, snapshot.version);
            }

            while let Some(item) = items.next().await {
                let attributes = item.box_err()?;
                let mut version = from_sort_key(coerce("version", &attributes, Attr::as_n));
                let kind = coerce::<String>("kind", &attributes, Attr::as_s);
                let revision = coerce::<u8>("revision", &attributes, Attr::as_n);
                let schema = Schema::new(
                    &kind,
                    NonZeroU8::new(revision).ok_or_else(|| StoreError::InvalidSchema(kind.clone()))?,
                );
                let empty = Blob::default();
                let content = if let Some(attribute) = attributes.get("content") {
                    attribute.as_b().unwrap_or(&empty)
                } else {
                    &empty
                };
                let event = options.transcoder().decode(&schema, content.as_ref())?;

                if let Some(mask) = options.mask() {
                    version = version.mask(mask);
                }

                yield Saved::new(event, version);
            }
        })
    }

    async fn save(
        &self,
        id: &T,
        expected_version: Version,
        events: &[Box<dyn Event>],
    ) -> Result<Version, StoreError<T>> {
        self.append(id, expected_version, events).await
    }

    async fn delete(&self, id: &T) -> Result<(), StoreError<T>> {
        if self.options.delete().unsupported() {
            return Err(StoreError::Unsupported);
        }

        if let Some(snapshots) = self.options.snapshots() {
            snapshots.prune(id, None).await?;
        }

        delete_all(&self.ddb, &self.table, id.to_string(), None, self.options.clock().now()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::config::{BehaviorVersion, Config, Credentials, Region};
    use cqrs::event::PredicateBuilder;
    use cqrs::message::Type;
    use uuid::Uuid;

    fn request() -> QueryFluentBuilder {
        let config = Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::for_tests())
            .build();

        Client::from_conf(config).query().table_name("events")
    }

    fn filter(predicate: &Predicate<'_, Uuid>) -> (String, Vec<(String, Attr)>) {
        let request = apply_predicate(request(), Some(predicate), Bound::Unbounded);
        let text = request.get_filter_expression().clone().unwrap_or_default();
        let mut values: Vec<_> = request
            .get_expression_attribute_values()
            .clone()
            .unwrap_or_default()
            .into_iter()
            .collect();

        values.sort_by(|(left, _), (right, _)| left.cmp(right));
        (text, values)
    }

    #[test]
    fn apply_predicate_should_match_revision_as_a_number() {
        // arrange
        let predicate = PredicateBuilder::<Uuid>::new(None)
            .add_type(Schema::version::<1>("created"))
            .build();

        // act
        let (text, values) = filter(&predicate);

        // assert
        assert_eq!(text, "(kind = :kind1 AND revision = :rev1)");
        assert_eq!(
            values,
            vec![
                (":kind1".to_string(), S("created".into())),
                (":rev1".to_string(), N("1".into())),
            ]
        );
    }

    #[test]
    fn apply_predicate_should_not_constrain_revision_for_versionless_type() {
        // arrange
        let predicate = PredicateBuilder::<Uuid>::new(None)
            .add_type(Type::any("created"))
            .build();

        // act
        let (text, values) = filter(&predicate);

        // assert
        assert_eq!(text, "(kind = :kind1)");
        assert_eq!(values, vec![(":kind1".to_string(), S("created".into()))]);
    }

    #[test]
    fn apply_predicate_should_mix_versionless_and_versioned_types() {
        // arrange
        let predicate = PredicateBuilder::<Uuid>::new(None)
            .add_type(Type::any("created"))
            .add_type(Schema::version::<2>("shipped"))
            .build();

        // act
        let (text, _) = filter(&predicate);

        // assert
        assert_eq!(text, "((kind = :kind1) OR (kind = :kind2 AND revision = :rev2))");
    }
}
