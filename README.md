# More CQRS &emsp; ![CI][ci-badge] [![Crates.io][crates-badge]][crates-url] [![MIT licensed][mit-badge]][mit-url] 

[crates-badge]: https://img.shields.io/crates/v/more-cqrs.svg
[crates-url]: https://crates.io/crates/more-cqrs
[mit-badge]: https://img.shields.io/badge/license-MIT-blueviolet.svg
[mit-url]: https://github.com/commonsensesoftware/more-rs-cqrs/blob/main/LICENSE
[ci-badge]: https://github.com/commonsensesoftware/more-rs-cqrs/actions/workflows/ci.yml/badge.svg

More CQRS is a collection of crates that contain all of the fundamental libraries to implement Command Query
Responsibility Segregation (CQRS) in Rust.

You may be looking for:

- [User Guide](https://commonsensesoftware.github.io/more-rs-cqrs)
- [API Documentation](https://docs.rs/more-cqrs)
- [Release Notes](https://github.com/commonsensesoftware/more-rs-cqrs/releases)

## Goals

- [x] Provide fundamental core abstractions
- [x] Provide support for SQL-based storage
- [ ] Provide support for NoSQL-based storage
- [ ] Provide tooling for snapshots
- [ ] Provide tooling for projections into materialized views

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

## CQRS in Action

Coming soon. Refer to the [examples](https://github.com/commonsensesoftware/more-rs-cqrs/examples) in the meantime.