use async_trait::async_trait;
use aws_sdk_dynamodb::{
    types::{AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType},
    Client,
};
use cqrs::StoreMigration;
use std::error::Error;

/// Represents the migrations for an Amazon DynamoDB [event store](super::EventStore).
pub struct EventStoreMigration {
    client: Client,
    table: String,
}

impl EventStoreMigration {
    /// Initializes a new [EventStoreMigration].
    /// 
    /// # Arguments
    /// 
    /// * `client` - the [client](Client) to perform the migration with
    pub fn new<S: Into<String>>(client: Client, table: S) -> Self {
        Self {
            client,
            table: table.into(),
        }
    }
}

#[async_trait]
impl StoreMigration for EventStoreMigration {
    async fn run(&self) -> Result<(), Box<dyn Error + 'static>> {
        let request = self
            .client
            .create_table()
            .table_name(&self.table)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("id")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("version")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("id")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("version")
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            )
            .billing_mode(BillingMode::PayPerRequest);

        let _ = request.send().await?;
        Ok(())
    }
}

/// Represents the migrations for an Amazon DynamoDB [snapshot store](super::SnapshotStore).
pub struct SnapshotStoreMigration {
    client: Client,
    table: String,
}

impl SnapshotStoreMigration {
    /// Initializes a new [EventStoreMigration].
    /// 
    /// # Arguments
    /// 
    /// * `client` - the [client](Client) to perform the migration with
    pub fn new<S: Into<String>>(client: Client, table: S) -> Self {
        Self {
            client,
            table: table.into(),
        }
    }
}

#[async_trait]
impl StoreMigration for SnapshotStoreMigration {
    async fn run(&self) -> Result<(), Box<dyn Error + 'static>> {
        let request = self
            .client
            .create_table()
            .table_name(&self.table)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("id")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("version")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("id")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("version")
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            )
            .billing_mode(BillingMode::PayPerRequest);

        let _ = request.send().await?;
        Ok(())
    }
}
