# Introduction

`more-cqrs` is a collection of crates that contain all of the fundamental abstractions for Command Query Responsibility
Segregation (CQRS) in Rust.

## Design Tenets

- Use [event sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) as the foundation of state change
- Promote [domain-driven design](https://en.wikipedia.org/wiki/Domain-driven_design)(DDD) using a set of events
- Support message passing with a command that produces an event
- Mitigate the [lost update](https://en.wikipedia.org/wiki/Write%E2%80%93write_conflict) problem
- Support any type of message encoding (ex: JSON, Protocol Buffers, etc)
- Provide message versioning
- Support snapshots of aggregate event streams at a point in time
- Support projections of event streams into materialized views
- Support storage migration
- Support dependency injection (DI)

## Features

### more-cqrs

This crate provides the following features:

- **mem** - Provides in-memory storage for testing and rapid prototyping
- **di** - Enables dependency injection (DI)
- **json** - Enables Java Script Object Notation (JSON) message encoding
- **protobuf** - Enables Protocol Buffers (ProtoBuf) message encoding
- **message-pack** - Enables Message Pack (MP) message encoding
- **cbor** - Enables Concise Binary Object Representation (CBOR) message encoding

### more-cqrs-sql

This crate provides the following features:

- **di** - Enables dependency injection (DI)
- **mysql** - Provides storage using MySql
- **postgres** - Provides storage using PostgreSQL
- **sqlite** - Provides storage using SQLite
- **migrate** - Provides SQL storage migrations

## Contributing

`more-cqrs` is free and open source. You can find the source code on [GitHub](https://github.com/commonsensesoftware/more-rs-cqrs)
and issues and feature requests can be posted on the [GitHub issue tracker](https://github.com/commonsensesoftware/more-rs-cqrs/issues).
`more-cqrs` relies on the community to fix bugs and add features: if you'd like to contribute, please read the
[CONTRIBUTING](https://github.com/commonsensesoftware/more-rs-cqrs/blob/main/CONTRIBUTING.md) guide and consider opening
a [pull request](https://github.com/commonsensesoftware/more-rs-cqrs/pulls).

## License

This project is licensed under the [MIT](https://github.com/commonsensesoftware/more-rs-cqrs/blob/main/LICENSE) license.