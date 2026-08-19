use super::{Builder, Container, delete_all, key};
use crate::{
    BoxErr, NoSqlVersion,
    bound::{greater_than, less_than},
    version::new_version,
};
use async_trait::async_trait;
use azure_data_cosmos::{CosmosError, Query, feed::FeedScope};
use cqrs::{
    Mask, Version,
    message::{Descriptor, Saved, Schema},
    snapshot::{Predicate, Retention, Snapshot, SnapshotError, Store, StoreOptions},
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, marker::PhantomData};

/// Represents the item of a stored [snapshot](Snapshot).
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Document {
    id: String,
    aggregate_id: String,
    version: u32,
    taken_on: u64,
    kind: String,
    revision: u8,
    content: String,
}

fn select(predicate: Option<&Predicate>, mask: Option<&(dyn Mask + 'static)>) -> Result<Query, CosmosError> {
    let mut text = String::from("SELECT * FROM c");
    let mut query = Query::from("");
    let mut and = " WHERE ";

    if let Some(predicate) = predicate {
        if let Some((mut version, op)) = greater_than(&predicate.min_version) {
            if let Some(mask) = mask {
                version = version.unmask(mask);
            }

            text.push_str(and);
            text.push_str("c.version ");
            text.push_str(op);
            text.push_str(" @version");
            and = " AND ";
            query = query.with_parameter("@version", version.number())?;
        }

        // the most recent snapshot taken as of the specified date and time
        if let Some((since, op)) = less_than(&predicate.since) {
            text.push_str(and);
            text.push_str("c.takenOn ");
            text.push_str(op);
            text.push_str(" @since");
            query = query.with_parameter("@since", crate::to_secs(since))?;
        }
    }

    text.push_str(" ORDER BY c.version DESC OFFSET 0 LIMIT 1");

    Ok(query.with_text(text))
}

/// Represents an Azure Cosmos DB [snapshot store](Store).
pub struct SnapshotStore<T> {
    _id: PhantomData<T>,
    container: Container,
    options: StoreOptions,
}

impl<T> SnapshotStore<T> {
    /// Initializes a new [SnapshotStore].
    ///
    /// # Arguments
    ///
    /// * `container` - the underlying [container](Container)
    /// * `options` - the [store options](StoreOptions)
    pub(crate) fn new(container: Container, options: StoreOptions) -> Self {
        Self {
            _id: PhantomData,
            container,
            options,
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
            Ok(Some(Saved::new(
                self.options
                    .transcoder()
                    .decode(&descriptor.schema, &descriptor.content)?,
                descriptor.version,
            )))
        } else {
            Ok(None)
        }
    }

    async fn load_raw(&self, id: &T, predicate: Option<&Predicate>) -> Result<Option<Descriptor>, SnapshotError> {
        let query = select(predicate, self.options.mask()).box_err()?;
        let client = self.container.resolve().await.box_err()?;
        let mut items = client
            .query_items::<Document>(query, FeedScope::partition(id.to_string()), None)
            .await
            .box_err()?;

        if let Some(item) = items.next().await {
            let document = item.box_err()?;
            let schema = Schema::new(document.kind, document.revision);
            let mut version = new_version(document.version, 0);
            let content = super::decode(&document.content)?;

            if let Some(mask) = self.options.mask() {
                version = version.mask(mask);
            }

            Ok(Some(Descriptor::new(schema, version, content)))
        } else {
            Ok(None)
        }
    }

    async fn save(&self, id: &T, mut version: Version, snapshot: Box<dyn Snapshot>) -> Result<(), SnapshotError> {
        if version != Default::default()
            && let Some(mask) = self.options.mask()
        {
            version = version.unmask(mask);
        }

        if version.invalid() {
            return Err(SnapshotError::InvalidVersion);
        }

        let taken_on = crate::to_secs(self.options.clock().now());
        let schema = snapshot.schema();
        let content = self.options.transcoder().encode(snapshot.as_ref())?;
        let document = Document {
            id: key(version.number()),
            aggregate_id: id.to_string(),
            version: version.number(),
            taken_on,
            kind: schema.kind().into(),
            revision: schema.version(),
            content: super::encode(&content),
        };
        let client = self.container.resolve().await.box_err()?;

        client
            .upsert_item(id.to_string(), &document.id, &document, None)
            .await
            .box_err()?;

        Ok(())
    }

    async fn prune(&self, id: &T, retention: Option<&Retention>) -> Result<(), SnapshotError> {
        delete_all(&self.container, id.to_string(), retention, self.options.clock().now()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use cqrs::snapshot::PredicateBuilder;
    use std::ops::Bound::{Included, Unbounded};
    use std::time::{Duration, UNIX_EPOCH};

    fn text(query: &Query) -> String {
        let json = serde_json::to_value(query).unwrap();
        json["query"].as_str().unwrap().into()
    }

    #[test]
    fn select_should_return_most_recent_snapshot() {
        // act
        let query = select(None, None).unwrap();

        // assert
        assert_eq!(text(&query), "SELECT * FROM c ORDER BY c.version DESC OFFSET 0 LIMIT 1");
    }

    #[test]
    fn select_should_apply_predicate() {
        // arrange
        let predicate = PredicateBuilder::new()
            .min_version(Included(new_version(2, 0)))
            .since(Included(UNIX_EPOCH + Duration::from_secs(60)))
            .build();

        // act
        let query = select(Some(&predicate), None).unwrap();

        // assert
        assert_eq!(
            text(&query),
            "SELECT * FROM c \
             WHERE c.version >= @version \
             AND c.takenOn <= @since \
             ORDER BY c.version DESC OFFSET 0 LIMIT 1"
        );
    }

    #[test]
    fn select_should_ignore_unbounded_predicate() {
        // arrange
        let predicate = PredicateBuilder::new().min_version(Unbounded).since(Unbounded).build();

        // act
        let query = select(Some(&predicate), None).unwrap();

        // assert
        assert_eq!(text(&query), "SELECT * FROM c ORDER BY c.version DESC OFFSET 0 LIMIT 1");
    }
}
