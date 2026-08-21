use crate::sql::{self, Provider, greater_than, less_than};
use cqrs::{
    Range,
    event::{Predicate, StoreError, filter_types},
};
use sqlx::{Database, Encode, Executor, FromRow, IntoArguments, QueryBuilder, Transaction, Type};
use std::{
    convert::Infallible, error::Error, fmt::Debug, marker::PhantomData, num::NonZeroU8, ops::Bound, time::SystemTime,
};

/// Translates the message types of a predicate into a SQL filter.
struct TypeFilter<'a, 'b, DB: Database>(&'a mut QueryBuilder<DB>, PhantomData<&'b ()>);

impl<'a, 'b, DB> cqrs::event::TypeFilter for TypeFilter<'a, 'b, DB>
where
    DB: Database,
    i16: Encode<'b, DB> + Type<DB>,
    String: Encode<'b, DB> + Type<DB>,
{
    type Error = Infallible;

    fn begin(&mut self, many: bool) {
        if many {
            self.0.push('(');
        }
    }

    fn condition(&mut self, _index: usize, kind: &str, revision: Option<NonZeroU8>) -> Result<(), Self::Error> {
        self.0.push("(type = ").push_bind(kind.to_string());

        if let Some(revision) = revision {
            self.0.push(" AND revision = ").push_bind(revision.get() as i16);
        }

        self.0.push(')');
        Ok(())
    }

    fn or(&mut self) {
        self.0.push(" OR ");
    }

    fn end(&mut self, many: bool) {
        if many {
            self.0.push(')');
        }
    }
}

fn and_stored_on<'a, D>(builder: &mut QueryBuilder<D>, (time, op): (SystemTime, &str))
where
    D: Database,
    i64: Encode<'a, D> + Type<D>,
{
    builder
        .push("stored_on ")
        .push(op)
        .push(" ")
        .push_bind(crate::to_secs(time));
}

pub fn select_id<'a, DB>(table: sql::Ident<'a>, stored_on: Range<SystemTime>) -> QueryBuilder<DB>
where
    DB: Database + Provider,
    i64: Encode<'a, DB> + Type<DB>,
{
    let mut select = QueryBuilder::new("SELECT id FROM ");

    select
        .push(DB::quote(&table))
        .push(" WHERE version = 1 AND sequence = 0");

    if let Some(lower) = greater_than(&stored_on.from) {
        select.push(" AND ");

        if let Some(upper) = less_than(&stored_on.to) {
            select.push('(');
            and_stored_on(&mut select, lower);
            select.push(" AND ");
            and_stored_on(&mut select, upper);
            select.push(')');
        } else {
            and_stored_on(&mut select, lower);
        }
    } else if let Some(upper) = less_than(&stored_on.to) {
        select.push(" AND ");
        and_stored_on(&mut select, upper);
    }

    select.push(';');
    select
}

pub fn select<'a, ID, DB>(
    table: sql::Ident<'a>,
    predicate: Option<&'a Predicate<'a, ID>>,
    version: Bound<i32>,
) -> QueryBuilder<DB>
where
    ID: Debug + Encode<'a, DB> + Send + Type<DB> + 'a,
    DB: Database + Provider,
    i16: Encode<'a, DB> + Type<DB>,
    i32: Encode<'a, DB> + Type<DB>,
    i64: Encode<'a, DB> + Type<DB>,
    String: Encode<'a, DB> + Type<DB>,
{
    fn add_where<D: Database>(builder: &mut QueryBuilder<D>, added: &mut bool) {
        if *added {
            builder.push(" AND ");
        } else {
            *added = true;
            builder.push(" WHERE ");
        }
    }

    const INIT: &str = "SELECT type, revision, version, sequence, content FROM ";
    let mut select = QueryBuilder::new(INIT);

    select.push(DB::quote(&table));

    if let Some(predicate) = predicate {
        let mut added = false;

        if let Some(id) = predicate.id {
            select.push(" WHERE id = ").push_bind(id);
            added = true;
        }

        if let Some((version, op)) = greater_than(&version) {
            add_where(&mut select, &mut added);
            select.push("version ").push(op).push(" ").push_bind(version);
        }

        if let Some(lower) = greater_than(&predicate.stored_on.from) {
            add_where(&mut select, &mut added);

            if let Some(upper) = less_than(&predicate.stored_on.to) {
                select.push('(');
                and_stored_on(&mut select, lower);
                select.push(" AND ");
                and_stored_on(&mut select, upper);
                select.push(')');
            } else {
                and_stored_on(&mut select, lower);
            }
        } else if let Some(upper) = less_than(&predicate.stored_on.to) {
            add_where(&mut select, &mut added);
            and_stored_on(&mut select, upper);
        }

        if !predicate.types.is_empty() {
            add_where(&mut select, &mut added);
        }

        let mut filter = TypeFilter::<'_, 'a, DB>(&mut select, PhantomData);

        filter_types(&predicate.types, &mut filter).unwrap();
    }

    select.push(';');
    select
}

pub fn exists<'a, ID, DB>(table: &'a sql::Ident<'a>, previous: &'a sql::Row<ID>) -> QueryBuilder<DB>
where
    DB: Database + Provider,
    ID: Clone + Debug + for<'db> Encode<'db, DB> + Send + Type<DB>,
    i32: for<'db> Encode<'db, DB> + Type<DB>,
{
    let mut select = QueryBuilder::new("SELECT EXISTS(SELECT 1 FROM ");

    select
        .push(DB::quote(table))
        .push(' ')
        .push(" WHERE id = ")
        .push_bind(&previous.id)
        .push(" AND version = ")
        .push_bind(previous.version)
        .push(" AND sequence = 0")
        .push(");");

    select
}

pub fn insert<'a, ID, DB>(table: &'a sql::Ident<'a>, row: &'a sql::Row<ID>) -> QueryBuilder<DB>
where
    DB: Database + Provider,
    ID: Encode<'a, DB> + Send + Type<DB> + 'a,
    i16: for<'db> Encode<'db, DB> + Type<DB>,
    i32: for<'db> Encode<'db, DB> + Type<DB>,
    i64: for<'db> Encode<'db, DB> + Type<DB>,
    String: for<'db> Encode<'db, DB> + Type<DB>,
    for<'db> &'db [u8]: Encode<'db, DB> + Type<DB>,
{
    let mut insert = QueryBuilder::new("INSERT INTO ");

    insert
        .push(DB::quote(table))
        .push(' ')
        .push(" VALUES (")
        .push_bind(&row.id)
        .push(", ")
        .push_bind(row.version)
        .push(", ")
        .push_bind(row.sequence)
        .push(", ")
        .push_bind(row.revision)
        .push(", ")
        .push_bind(row.stored_on)
        .push(", ")
        .push_bind(&row.kind)
        .push(", ")
        .push_bind(row.content.as_slice())
        .push(", ");

    if let Some(cid) = &row.correlation_id {
        insert.push_bind(cid);
    } else {
        insert.push("NULL");
    }

    insert.push(");");
    insert
}

pub async fn insert_transacted<'a, ID, DB>(
    table: &'a sql::Ident<'a>,
    row: &'a sql::Row<ID>,
    tx: &'a mut Transaction<'_, DB>,
) -> Result<(), StoreError<ID>>
where
    DB: Database + Provider,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'db> &'db mut <DB as Database>::Connection: Executor<'db, Database = DB>,
    ID: Clone + Debug + for<'db> Encode<'db, DB> + Send + Type<DB>,
    i16: for<'db> Encode<'db, DB> + Type<DB>,
    i32: for<'db> Encode<'db, DB> + Type<DB>,
    i64: for<'db> Encode<'db, DB> + Type<DB>,
    String: for<'db> Encode<'db, DB> + Type<DB>,
    for<'db> &'db [u8]: Encode<'db, DB> + Type<DB>,
{
    let mut insert = insert::<ID, DB>(table, row);

    if let Err(error) = insert.build().execute(&mut **tx).await {
        if let sqlx::Error::Database(error) = &error
            && error.is_unique_violation()
        {
            Err(StoreError::Conflict(row.id.clone(), row.version as u32))
        } else {
            Err(StoreError::Unknown(Box::new(error) as Box<dyn Error + Send>))
        }
    } else {
        Ok(())
    }
}

pub async fn ensure_not_deleted<'a, ID, DB>(
    table: &'a sql::Ident<'a>,
    previous: &'a sql::Row<ID>,
    tx: &'a mut Transaction<'_, DB>,
) -> Result<(), StoreError<ID>>
where
    DB: Database + Provider,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'db> &'db mut <DB as Database>::Connection: Executor<'db, Database = DB>,
    ID: Clone + Debug + for<'db> Encode<'db, DB> + Send + Type<DB>,
    i32: for<'db> Encode<'db, DB> + Type<DB>,
    (bool,): for<'db> FromRow<'db, DB::Row>,
{
    let exists: bool = exists(table, previous)
        .build_query_scalar()
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| StoreError::Unknown(Box::new(e) as Box<dyn Error + Send>))?;

    if exists {
        Ok(())
    } else {
        Err(StoreError::Deleted(previous.id.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrs::event::PredicateBuilder;
    use cqrs::message::{Schema, Type};
    use sqlx::Sqlite;
    use uuid::Uuid;

    fn sql(predicate: &Predicate<'_, Uuid>) -> String {
        select::<Uuid, Sqlite>(sql::Ident::unqualified("events"), Some(predicate), Bound::Unbounded)
            .sql()
            .as_str()
            .into()
    }

    #[test]
    fn select_should_constrain_revision_for_versioned_type() {
        // arrange
        let predicate = PredicateBuilder::<Uuid>::new(None)
            .add_type(Schema::version::<1>("created"))
            .build();

        // act
        let sql = sql(&predicate);

        // assert
        assert!(sql.ends_with(" WHERE (type = ? AND revision = ?);"), "{sql}");
    }

    #[test]
    fn select_should_not_constrain_revision_for_versionless_type() {
        // arrange
        let predicate = PredicateBuilder::<Uuid>::new(None)
            .add_type(Type::any("created"))
            .build();

        // act
        let sql = sql(&predicate);

        // assert
        assert!(sql.ends_with(" WHERE (type = ?);"), "{sql}");
    }

    #[test]
    fn select_should_mix_versionless_and_versioned_types() {
        // arrange
        let predicate = PredicateBuilder::<Uuid>::new(None)
            .add_type(Type::any("created"))
            .add_type(Schema::version::<2>("shipped"))
            .build();

        // act
        let sql = sql(&predicate);

        // assert
        assert!(
            sql.ends_with(" WHERE ((type = ?) OR (type = ? AND revision = ?));"),
            "{sql}"
        );
    }
}
