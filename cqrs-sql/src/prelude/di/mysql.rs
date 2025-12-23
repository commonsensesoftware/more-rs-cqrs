use super::SqlStoreBuilder;
use cqrs::{Aggregate, prelude::AggregateBuilder};
use sqlx::{ColumnIndex, Database, Decode, Encode, MySql, Type};

/// Represents the MySql storage configuration extensions.
pub trait MySqlExt<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + for<'db> Encode<'db, MySql> + for<'db> Decode<'db, MySql> + Sync + Type<MySql>,
    i16: for<'db> Encode<'db, MySql> + for<'db> Decode<'db, MySql> + Type<MySql>,
    i32: for<'db> Encode<'db, MySql> + Type<MySql>,
    i64: for<'db> Encode<'db, MySql> + Type<MySql>,
    usize: ColumnIndex<<MySql as Database>::Row>,
    String: for<'db> Encode<'db, MySql> + Type<MySql>,
    for<'db> &'db str: Decode<'db, MySql> + Type<MySql>,
    for<'db> &'db [u8]: Encode<'db, MySql> + Decode<'db, MySql> + Type<MySql>,
{
    /// Configures an [aggregate](Aggregate) with MySql storage.
    fn in_mysql(self) -> SqlStoreBuilder<'a, A, MySql>;
}

impl<'a, A> MySqlExt<'a, A> for AggregateBuilder<'a, A>
where
    A: Aggregate + Default + Sync + 'static,
    A::ID: Clone + for<'db> Encode<'db, MySql> + for<'db> Decode<'db, MySql> + Sync + Type<MySql>,
    i16: for<'db> Encode<'db, MySql> + for<'db> Decode<'db, MySql> + Type<MySql>,
    i32: for<'db> Encode<'db, MySql> + Type<MySql>,
    i64: for<'db> Encode<'db, MySql> + Type<MySql>,
    usize: ColumnIndex<<MySql as Database>::Row>,
    String: for<'db> Encode<'db, MySql> + Type<MySql>,
    for<'db> &'db str: Decode<'db, MySql> + Type<MySql>,
    for<'db> &'db [u8]: Encode<'db, MySql> + Decode<'db, MySql> + Type<MySql>,
{
    fn in_mysql(self) -> SqlStoreBuilder<'a, A, MySql> {
        SqlStoreBuilder::new(self.services)
    }
}
