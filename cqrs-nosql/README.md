# More CQRS with NoSQL

`more-cqrs-nosql` provides everything you need to enable Command Query Responsibility Segregation (CQRS) in Rust using
storage backed by a NoSQL database.

## Design Tenets

- Use lock-free, append-only write operations
- Support storage migration to create or update the backing database
- Support dependency injection (DI)

## Features

This crate provides the following features:

- **di** - Enables dependency injection (DI)
- **dynamodb** - Provides storage using Amazon DynamoDB
- **migrate** - Provides NoSQL storage migrations

## Example

Coming soon. In the meantime, see the
[DynamoDB orders](https://github.com/commonsensesoftware/more-rs-cqrs/tree/main/examples/dynamodb/orders)
example.