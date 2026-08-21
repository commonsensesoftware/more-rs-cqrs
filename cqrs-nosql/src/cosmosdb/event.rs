use super::{Builder, CONFLICT, Container, MAX_BATCH_SIZE, NOT_FOUND, delete_all, execute, key};
use crate::{
    BoxErr, NoSqlVersion,
    NoSqlVersionPart::Sequence,
    append::Append,
    bound::{greater_than, less_than},
    snapshot::{get_snapshot, select_version},
    version::{from_sort_key, new_version},
};
use async_stream::try_stream;
use async_trait::async_trait;
use azure_data_cosmos::{CosmosError, Query, feed::FeedScope, models::TransactionalBatch};
use cqrs::{
    Clock, Range, Version,
    event::{Event, EventStream, IdStream, Predicate, Store, StoreError, StoreOptions, filter_types},
    message::{Saved, Schema},
};
use futures::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt::Debug, num::NonZeroU8, ops::Bound, str::FromStr, sync::Arc, time::SystemTime};

/// Represents the item of a stored [event](Event).
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Document {
    id: String,
    aggregate_id: String,
    version: u32,
    stored_on: u64,
    kind: String,
    revision: u8,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    correlation_id: Option<String>,

    content: String,
}

/// Represents the projection of an aggregate identifier.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Identifier {
    aggregate_id: String,
}

fn select<T>(predicate: Option<&Predicate<T>>, version: Bound<Version>) -> Result<(Query, FeedScope), CosmosError>
where
    T: Debug + Send + ToString,
{
    let mut text = String::from("SELECT * FROM c");
    let mut query = Query::from("");
    let mut and = " WHERE ";

    // the version bound is resolved against the snapshot a load is seeded from, if any, so it's never masked
    if let Some((version, op)) = greater_than(&version) {
        text.push_str(and);
        text.push_str("c.version ");
        text.push_str(op);
        text.push_str(" @version");
        and = " AND ";
        query = query.with_parameter("@version", version.sort_key())?;
    }

    let Some(predicate) = predicate else {
        return Ok((query.with_text(text), FeedScope::full_container()));
    };

    if let Some((from, op)) = greater_than(&predicate.stored_on.from) {
        text.push_str(and);
        text.push_str("c.storedOn ");
        text.push_str(op);
        text.push_str(" @from");
        and = " AND ";
        query = query.with_parameter("@from", crate::to_secs(from))?;
    }

    if let Some((to, op)) = less_than(&predicate.stored_on.to) {
        text.push_str(and);
        text.push_str("c.storedOn ");
        text.push_str(op);
        text.push_str(" @to");
        and = " AND ";
        query = query.with_parameter("@to", crate::to_secs(to))?;
    }

    if !predicate.types.is_empty() {
        text.push_str(and);
    }

    let mut filter = TypeFilter {
        text: &mut text,
        query: Some(query),
    };

    filter_types(&predicate.types, &mut filter)?;
    query = filter.query.unwrap();

    // ORDER BY is unsupported in a cross-partition query. an aggregate is a partition, so events can only be ordered by
    // version when the query is scoped to a single aggregate
    if let Some(id) = predicate.id {
        text.push_str(" ORDER BY c.version");
        Ok((query.with_text(text), FeedScope::partition(id.to_string())))
    } else {
        Ok((query.with_text(text), FeedScope::full_container()))
    }
}

/// Translates the message types of a predicate into a Cosmos DB filter.
struct TypeFilter<'a> {
    text: &'a mut String,

    // the query is built by value, so it is taken and replaced as each parameter is added
    query: Option<Query>,
}

impl<'a> cqrs::event::TypeFilter for TypeFilter<'a> {
    type Error = CosmosError;

    fn begin(&mut self, many: bool) {
        if many {
            self.text.push('(');
        }
    }

    fn condition(&mut self, index: usize, kind: &str, revision: Option<NonZeroU8>) -> Result<(), Self::Error> {
        let mut query = self.query.take().unwrap();

        self.text.push_str("(c.kind = @kind");
        self.text.push_str(&index.to_string());
        query = query.with_parameter(format!("@kind{index}"), kind)?;

        if let Some(revision) = revision {
            self.text.push_str(" AND c.revision = @revision");
            self.text.push_str(&index.to_string());
            query = query.with_parameter(format!("@revision{index}"), revision.get())?;
        }

        self.text.push(')');
        self.query = Some(query);
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

/// Represents an Azure Cosmos DB [event store](Store).
pub struct EventStore<ID> {
    container: Container,
    options: StoreOptions<ID>,
}

impl<ID> EventStore<ID> {
    /// Initializes a new [EventStore].
    ///
    /// # Arguments
    ///
    /// * `container` - the underlying [container](Container)
    /// * `options` - the [store options](StoreOptions)
    pub(crate) fn new(container: Container, options: StoreOptions<ID>) -> Self {
        Self { container, options }
    }

    /// Creates and returns a new [Builder].
    pub fn builder() -> Builder<ID, dyn Event> {
        Builder::default()
    }
}

impl<ID> EventStore<ID>
where
    ID: Clone + Debug + Send + Sync + ToString + 'static,
{
    fn document(
        &self,
        id: &ID,
        version: Version,
        stored_on: u64,
        event: &(dyn Event + 'static),
    ) -> Result<Document, StoreError<ID>> {
        let schema = event.schema();
        let content = self.options.transcoder().encode(event)?;

        Ok(Document {
            id: key(version.sort_key()),
            aggregate_id: id.to_string(),
            version: version.sort_key(),
            stored_on,
            kind: schema.kind().into(),
            revision: schema.revision().get(),
            correlation_id: event.correlation_id().map(Into::into),
            content: super::encode(&content),
        })
    }
}

#[async_trait]
impl<ID> Append<ID> for EventStore<ID>
where
    ID: Clone + Debug + Send + Sync + ToString + 'static,
{
    const MAX_BATCH_SIZE: usize = MAX_BATCH_SIZE;

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
        let document = self.document(id, version, stored_on, event)?;
        let client = self.container.resolve().await.box_err()?;
        let partition = id.to_string();

        if self.options.delete().supported()
            && let Some(previous) = version.previous()
        {
            // the following read doesn't change anything, but it ensures the previous version still exists and hasn't
            // been deleted. an operation that fails rolls back the entire batch
            let batch = TransactionalBatch::new(partition)
                .read_item(key(previous.sort_key()), None)
                .create_item(&document)
                .box_err()?;

            return match execute(&client, batch).await.box_err()? {
                None => Ok(version),
                Some(CONFLICT) => Err(StoreError::Conflict(id.clone(), version.number())),
                Some(NOT_FOUND) => Err(StoreError::Deleted(id.clone())),
                Some(status) => Err(StoreError::Unknown(
                    Box::new(super::BatchError(status)) as Box<dyn Error + Send>
                )),
            };
        }

        if let Err(error) = client.create_item(partition, &document.id, &document, None).await {
            if error.status().is_conflict() {
                Err(StoreError::Conflict(id.clone(), version.number()))
            } else {
                Err(StoreError::Unknown(Box::new(error) as Box<dyn Error + Send>))
            }
        } else {
            Ok(version)
        }
    }

    async fn written(&self, id: &ID, version: Version, event: &(dyn Event + 'static)) -> Result<bool, StoreError<ID>> {
        let client = self.container.resolve().await.box_err()?;
        let item = key(version.sort_key());
        let existing = match client.read_item(id.to_string(), &item, None).await {
            Ok(response) => response.into_model::<Document>().box_err()?,
            Err(error) => {
                return if error.status().is_not_found() {
                    Ok(false)
                } else {
                    Err(StoreError::Unknown(Box::new(error) as Box<dyn Error + Send>))
                };
            }
        };

        let expected = self.document(id, version, existing.stored_on, event)?;

        Ok(existing.kind == expected.kind
            && existing.revision == expected.revision
            && existing.correlation_id == expected.correlation_id
            && existing.content == expected.content)
    }

    async fn write_all(
        &self,
        id: &ID,
        mut version: Version,
        events: &[Box<dyn Event>],
    ) -> Result<Version, StoreError<ID>> {
        let previous = if self.options.delete().supported() {
            version.previous()
        } else {
            None
        };
        let stored_on = crate::to_secs(self.options.clock().now());
        let partition = id.to_string();
        let mut batch = TransactionalBatch::new(partition);

        if let Some(previous) = previous {
            // the following read doesn't change anything, but it ensures the previous version still exists and hasn't
            // been deleted. an operation that fails rolls back the entire batch
            batch = batch.read_item(key(previous.sort_key()), None);
        }

        let mut current_version = version;

        for event in events {
            let document = self.document(id, version, stored_on, event.as_ref())?;

            batch = batch.create_item(&document).box_err()?;
            current_version = version;
            version = current_version.increment(Sequence);
        }

        version = current_version;

        let client = self.container.resolve().await.box_err()?;

        match execute(&client, batch).await.box_err()? {
            None => Ok(version),
            Some(CONFLICT) => Err(StoreError::Conflict(id.clone(), version.number())),
            Some(NOT_FOUND) => Err(StoreError::Deleted(id.clone())),
            Some(status) => Err(StoreError::Unknown(
                Box::new(super::BatchError(status)) as Box<dyn Error + Send>
            )),
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

    async fn ids(&self, stored_on: Range<SystemTime>) -> IdStream<T> {
        let container = self.container.clone();

        Box::pin(try_stream! {
            let mut text = String::from("SELECT c.aggregateId FROM c WHERE c.version = @version");
            let mut query = Query::from("").with_parameter("@version", new_version(1, 0).sort_key()).box_err()?;

            if let Some((from, op)) = greater_than(&stored_on.from) {
                text.push_str(" AND c.storedOn ");
                text.push_str(op);
                text.push_str(" @from");
                query = query.with_parameter("@from", crate::to_secs(from)).box_err()?;
            }

            if let Some((to, op)) = less_than(&stored_on.to) {
                text.push_str(" AND c.storedOn ");
                text.push_str(op);
                text.push_str(" @to");
                query = query.with_parameter("@to", crate::to_secs(to)).box_err()?;
            }

            let client = container.resolve().await.box_err()?;
            let mut items = client
                .query_items::<Identifier>(query.with_text(text), FeedScope::full_container(), None)
                .await
                .box_err()?;

            while let Some(item) = items.next().await {
                let id = item.box_err()?.aggregate_id;
                yield id.parse::<T>().unwrap_or_default();
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
    /// Events are only ordered by [version](Version) when the [predicate](Predicate) identifies the aggregate to load
    /// because Azure Cosmos DB cannot order the results of a query that spans multiple partitions.
    ///
    /// If a [snapshot store](cqrs::snapshot::Store) is configured, the stream is seeded with the most recent
    /// [snapshot](cqrs::snapshot::Snapshot) and only the events which are not already summarized by it are loaded.
    async fn load<'a>(&self, predicate: Option<&'a Predicate<'a, T>>) -> EventStream<'a, T> {
        let container = self.container.clone();
        let options = self.options.clone();
        let snapshot = match get_snapshot(self.options.snapshots(), predicate).await {
            Ok(snapshot) => snapshot,
            Err(error) => return Box::pin(stream::iter(vec![Err(StoreError::from(error))])),
        };

        Box::pin(try_stream! {
            let mut version = Bound::Unbounded;

            if let Some(filter) = predicate {
                version = select_version(snapshot.as_ref(), filter, options.mask());

                if let Some(snapshot) = snapshot {
                    let event = options.transcoder().decode(&snapshot.schema, &snapshot.content)?;
                    yield Saved::new(event, snapshot.version);
                }
            }

            let (query, scope) = select(predicate, version).box_err()?;
            let client = container.resolve().await.box_err()?;
            let mut items = client.query_items::<Document>(query, scope, None).await.box_err()?;

            while let Some(item) = items.next().await {
                let document = item.box_err()?;
                let mut version = from_sort_key(document.version);
                let revision = NonZeroU8::new(document.revision)
                    .ok_or_else(|| StoreError::InvalidSchema(document.kind.clone()))?;
                let schema = Schema::new(document.kind, revision);
                let content = super::decode(&document.content)?;
                let event = options.transcoder().decode(&schema, &content)?;

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

        delete_all(&self.container, id.to_string(), None, self.options.clock().now()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use cqrs::event::PredicateBuilder;
    use cqrs::message::Type;
    use std::ops::Bound::{Excluded, Included, Unbounded};
    use std::time::{Duration, UNIX_EPOCH};
    use uuid::Uuid;

    fn text(query: &Query) -> String {
        let json = serde_json::to_value(query).unwrap();
        json["query"].as_str().unwrap().into()
    }

    fn parameters(query: &Query) -> Vec<(String, serde_json::Value)> {
        let json = serde_json::to_value(query).unwrap();

        if let Some(parameters) = json["parameters"].as_array() {
            parameters
                .iter()
                .map(|parameter| (parameter["name"].as_str().unwrap().into(), parameter["value"].clone()))
                .collect()
        } else {
            Vec::new()
        }
    }

    #[test]
    fn select_should_span_all_partitions_without_predicate() {
        // arrange
        let predicate: Option<&Predicate<Uuid>> = None;

        // act
        let (query, scope) = select(predicate, Unbounded).unwrap();

        // assert
        assert_eq!(text(&query), "SELECT * FROM c");
        assert!(parameters(&query).is_empty());
        assert!(matches!(scope, FeedScope::Range(_)));
    }

    #[test]
    fn select_should_order_events_within_a_partition() {
        // arrange
        let id = Uuid::nil();
        let predicate = PredicateBuilder::new(Some(&id)).build();

        // act
        let (query, scope) = select(Some(&predicate), Unbounded).unwrap();

        // assert
        assert_eq!(text(&query), "SELECT * FROM c ORDER BY c.version");
        assert!(matches!(scope, FeedScope::Partition(_)));
    }

    #[test]
    fn select_should_not_order_events_across_partitions() {
        // arrange
        let predicate = PredicateBuilder::<Uuid>::new(None).build();

        // act
        let (query, scope) = select(Some(&predicate), Included(new_version(2, 0))).unwrap();

        // assert
        assert_eq!(text(&query), "SELECT * FROM c WHERE c.version >= @version");
        assert!(matches!(scope, FeedScope::Range(_)));
    }

    #[test]
    fn select_should_not_constrain_revision_for_versionless_type() {
        // arrange
        let id = Uuid::nil();
        let predicate = PredicateBuilder::new(Some(&id)).add_type(Type::any("created")).build();

        // act
        let (query, _) = select(Some(&predicate), Unbounded).unwrap();

        // assert
        assert_eq!(
            text(&query),
            "SELECT * FROM c WHERE (c.kind = @kind0) ORDER BY c.version"
        );
        assert_eq!(parameters(&query), vec![("@kind0".into(), "created".into())]);
    }

    #[test]
    fn select_should_mix_versionless_and_versioned_types() {
        // arrange
        let id = Uuid::nil();
        let predicate = PredicateBuilder::new(Some(&id))
            .add_type(Type::any("created"))
            .add_type(Schema::version::<2>("shipped"))
            .build();

        // act
        let (query, _) = select(Some(&predicate), Unbounded).unwrap();

        // assert
        assert_eq!(
            text(&query),
            "SELECT * FROM c \
             WHERE ((c.kind = @kind0) \
             OR (c.kind = @kind1 AND c.revision = @revision1)) \
             ORDER BY c.version"
        );
        assert_eq!(
            parameters(&query),
            vec![
                ("@kind0".into(), "created".into()),
                ("@kind1".into(), "shipped".into()),
                ("@revision1".into(), 2.into()),
            ]
        );
    }

    #[test]
    fn select_should_apply_predicate() {
        // arrange
        let id = Uuid::nil();
        let predicate = PredicateBuilder::new(Some(&id))
            .version(Excluded(new_version(2, 1)))
            .stored_on(UNIX_EPOCH + Duration::from_secs(60)..UNIX_EPOCH + Duration::from_secs(120))
            .add_type(Schema::version::<1>("created"))
            .add_type(Schema::version::<2>("shipped"))
            .build();

        // act
        let (query, _) = select(Some(&predicate), Excluded(new_version(2, 1))).unwrap();

        // assert
        assert_eq!(
            text(&query),
            "SELECT * FROM c \
             WHERE c.version > @version \
             AND c.storedOn >= @from \
             AND c.storedOn <= @to \
             AND ((c.kind = @kind0 AND c.revision = @revision0) \
             OR (c.kind = @kind1 AND c.revision = @revision1)) \
             ORDER BY c.version"
        );
        assert_eq!(
            parameters(&query),
            vec![
                ("@version".into(), new_version(2, 1).sort_key().into()),
                ("@from".into(), 60.into()),
                ("@to".into(), 120.into()),
                ("@kind0".into(), "created".into()),
                ("@revision0".into(), 1.into()),
                ("@kind1".into(), "shipped".into()),
                ("@revision1".into(), 2.into()),
            ]
        );
    }

    #[test]
    fn document_should_be_stored_as_camel_case() {
        // arrange
        let document = Document {
            id: key(new_version(1, 0).sort_key()),
            aggregate_id: "42".into(),
            version: new_version(1, 0).sort_key(),
            stored_on: 60,
            kind: "created".into(),
            revision: 1,
            correlation_id: None,
            content: super::super::encode(&[1, 2, 3]),
        };

        // act
        let json = serde_json::to_string(&document).unwrap();

        // assert
        assert_eq!(
            json,
            r#"{"id":"0000000256","aggregateId":"42","version":256,"storedOn":60,"kind":"created","revision":1,"content":"AQID"}"#
        );
    }
}
