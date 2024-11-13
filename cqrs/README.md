# More CQRS

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