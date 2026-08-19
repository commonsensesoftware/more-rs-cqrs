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

## Examples

The following solutions for a simple _Orders_ service are implemented for:

- [DynamoDB]

>Switching between storage implementations is pure configuration.

[DynamoDB orders]: https://github.com/commonsensesoftware/more-rs-cqrs/tree/main/examples/dynamodb/orders