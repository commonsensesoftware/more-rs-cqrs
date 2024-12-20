# SQLite Examples

This directory contains examples using SQLite as a backing store.

## Orders

This example illustrates using:

- A contrived _order_
- SQLite as a backing store
- State change
- Persistence
- Replaying events
- Message encoding with Protocol Buffers
- Concurrency enforcement that prevents the _lost update_ problem

Run it with:

```bash
cargo run --package sqlite-examples --example sqlite-orders
```