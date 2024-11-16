# More CQRS with SQL

`more-cqrs-sql` provides everything you need to enable Command Query Responsibility Segregation (CQRS) in Rust using
storage backed by a SQL database.

## Design Tenets

- Use lock-free, append-only write operations
- Support storage migration to create or update the backing database
- Support dependency injection (DI)

## Features

This crate provides the following features:

- **di** - Enables dependency injection (DI)
- **mysql** - Provides storage using MySql
- **postgres** - Provides storage using PostgreSQL
- **sqlite** - Provides storage using SQLite
- **migrate** - Provides SQL storage migrations

## Example

Coming soon. In the meantime, see the
[Postgres orders](https://github.com/commonsensesoftware/more-rs-cqrs/tree/main/examples/postgres/orders)
example.