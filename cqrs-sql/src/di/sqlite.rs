use super::SqlStoreBuilder;
use cqrs::{Aggregate, di::AggregateBuilder};
use sqlx::{ColumnIndex, Database, Decode, Encode, Sqlite, Type};

/// Represents the SQLite storage configuration extensions.
pub trait SqliteExt<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone
        + for<'db> Encode<'db, Sqlite>
        + for<'db> Decode<'db, Sqlite>
        + Sync
        + Type<Sqlite>,
    i16: for<'db> Encode<'db, Sqlite> + for<'db> Decode<'db, Sqlite> + Type<Sqlite>,
    i32: for<'db> Encode<'db, Sqlite> + Type<Sqlite>,
    i64: for<'db> Encode<'db, Sqlite> + Type<Sqlite>,
    usize: ColumnIndex<<Sqlite as Database>::Row>,
    String: for<'db> Encode<'db, Sqlite> + Type<Sqlite>,
    for<'db> &'db str: Decode<'db, Sqlite> + Type<Sqlite>,
    for<'db> &'db [u8]: Encode<'db, Sqlite> + Decode<'db, Sqlite> + Type<Sqlite>,
{
    /// Configures an [aggregate](Aggregate) with Sqlite storage.
    fn in_sqlite(self) -> SqlStoreBuilder<'a, A, Sqlite>;
}

impl<'a, A> SqliteExt<'a, A> for AggregateBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone
        + for<'db> Encode<'db, Sqlite>
        + for<'db> Decode<'db, Sqlite>
        + Sync
        + Type<Sqlite>,
    i16: for<'db> Encode<'db, Sqlite> + for<'db> Decode<'db, Sqlite> + Type<Sqlite>,
    i32: for<'db> Encode<'db, Sqlite> + Type<Sqlite>,
    i64: for<'db> Encode<'db, Sqlite> + Type<Sqlite>,
    usize: ColumnIndex<<Sqlite as Database>::Row>,
    String: for<'db> Encode<'db, Sqlite> + Type<Sqlite>,
    for<'db> &'db str: Decode<'db, Sqlite> + Type<Sqlite>,
    for<'db> &'db [u8]: Encode<'db, Sqlite> + Decode<'db, Sqlite> + Type<Sqlite>,
{
    fn in_sqlite(self) -> SqlStoreBuilder<'a, A, Sqlite> {
        SqlStoreBuilder::new(self.services)
    }
}
