# More CQRS with NoSQL

`more-cqrs-nosql` provides everything you need to enable Command Query Responsibility Segregation (CQRS) in Rust using
storage backed by a NoSQL database.

## Design Tenets

- Use lock-free, append-only write operations
- Support storage migration to create or update the backing database
- Support dependency injection (DI)

## Features

This crate provides the following features:

- **cosmosdb** - Provides storage using Azure Cosmos DB
- **di** - Enables dependency injection (DI)
- **dynamodb** - Provides storage using Amazon DynamoDB
- **migrate** - Provides NoSQL storage migrations

## Azure Cosmos DB

An aggregate is a partition; specifically, the `/aggregateId` partition key of the container it is stored in.
Collocating the items an aggregate owns is what allows the events appended by a single command to be saved
atomically using a transactional batch, which is limited to 100 operations.

A [client](https://docs.rs/azure_data_cosmos/latest/azure_data_cosmos/struct.CosmosClient.html) is always
required and must be provided when a store is configured. Unlike other providers, there is no ambient
configuration a client can be created from and a client is created asynchronously, which a store cannot do
on behalf of a consumer while it is being built.

```rust
let credential = DeveloperToolsCredential::new(None)?;
let endpoint: AccountEndpoint = "https://myaccount.documents.azure.com/".parse()?;
let account = AccountReference::with_credential(endpoint, credential);
let client = CosmosClient::builder()
    .build(account, RoutingStrategy::ProximityTo(Region::WEST_US))
    .await?;
let provider = ServiceCollection::new()
    .add_cqrs(|options| {
        options
            .store::<Order>()
            .in_cosmosdb()
            .with()
            .database("orders")
            .client(client)
            .enforce_concurrency()
            .migrations();
    })
    .build_provider()?;
```

The integration tests run against the
[Azure Cosmos DB emulator](https://learn.microsoft.com/azure/cosmos-db/how-to-develop-emulator),
which requires the Docker engine to be running. They are excluded from the default test run
because starting a database engine dominates the time it takes to run them:

```bash
cargo nextest run -P ci -E 'test(/^verify_cosmosdb_/)'
```

## Example

Coming soon. In the meantime, see the
[DynamoDB orders](https://github.com/commonsensesoftware/more-rs-cqrs/tree/main/examples/dynamodb/orders)
example.