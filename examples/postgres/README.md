# PostgreSQL Examples

This directory contains examples using PostgreSQL as a backing store.

## Orders

This example illustrates using:

- A contrived _order_
- PostgreSQL as a backing store
- State change
- Persistence
- Replaying events
- Message encoding with Protocol Buffers
- Concurrency enforcement that prevents the _lost update_ problem

Run it with:

```bash
cargo run --package postgres-examples --example postgres-orders
```

>**Note**: In order to run this example, the Docker engine must be running.