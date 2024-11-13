use super::SqlStoreBuilder;
use cqrs::{Aggregate, di::AggregateBuilder};
use sqlx::{ColumnIndex, Database, Decode, Encode, Postgres, Type};

/// Represents the Postgres storage configuration extensions.
pub trait PostgresExt<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone
        + for<'db> Encode<'db, Postgres>
        + for<'db> Decode<'db, Postgres>
        + Sync
        + Type<Postgres>,
    i16: for<'db> Encode<'db, Postgres> + for<'db> Decode<'db, Postgres> + Type<Postgres>,
    i32: for<'db> Encode<'db, Postgres> + Type<Postgres>,
    i64: for<'db> Encode<'db, Postgres> + Type<Postgres>,
    usize: ColumnIndex<<Postgres as Database>::Row>,
    String: for<'db> Encode<'db, Postgres> + Type<Postgres>,
    for<'db> &'db str: Decode<'db, Postgres> + Type<Postgres>,
    for<'db> &'db [u8]: Encode<'db, Postgres> + Decode<'db, Postgres> + Type<Postgres>,
{
    /// Configures an [aggregate](Aggregate) with Postgres storage.
    fn in_postgres(self) -> SqlStoreBuilder<'a, A, Postgres>;
}

impl<'a, A> PostgresExt<'a, A> for AggregateBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone
        + for<'db> Encode<'db, Postgres>
        + for<'db> Decode<'db, Postgres>
        + Sync
        + Type<Postgres>,
    i16: for<'db> Encode<'db, Postgres> + for<'db> Decode<'db, Postgres> + Type<Postgres>,
    i32: for<'db> Encode<'db, Postgres> + Type<Postgres>,
    i64: for<'db> Encode<'db, Postgres> + Type<Postgres>,
    usize: ColumnIndex<<Postgres as Database>::Row>,
    String: for<'db> Encode<'db, Postgres> + Type<Postgres>,
    for<'db> &'db str: Decode<'db, Postgres> + Type<Postgres>,
    for<'db> &'db [u8]: Encode<'db, Postgres> + Decode<'db, Postgres> + Type<Postgres>,
{
    fn in_postgres(self) -> SqlStoreBuilder<'a, A, Postgres> {
        SqlStoreBuilder::new(self.services)
    }
}
