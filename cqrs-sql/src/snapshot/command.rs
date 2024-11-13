use super::Upsert;
use crate::{sql, SqlVersion};
use cqrs::snapshot::Predicate;
use sqlx::{Database, Encode, Executor, IntoArguments, QueryBuilder, Type};
use std::fmt::Debug;

fn _select<'a, ID, DB>(
    columns: &'static str,
    table: &sql::Ident<'a>,
    id: &'a ID,
    predicate: Option<&Predicate>,
) -> QueryBuilder<'a, DB>
where
    ID: Debug + Encode<'a, DB> + Send + Type<DB> + 'a,
    DB: Database,
    i32: Debug + for<'db> Encode<'db, DB> + Send + Type<DB>,
    i64: Debug + for<'db> Encode<'db, DB> + Send + Type<DB>,
{
    let mut select = QueryBuilder::new(columns);

    select
        .push(" FROM ")
        .push(table.quote())
        .push(" WHERE id = ")
        .push_bind(id);

    if let Some(predicate) = predicate {
        if let Some(version) = predicate.min_version {
            select.push(" AND version >= ").push_bind(version.number());
        }

        if let Some(since) = predicate.since {
            select
                .push(" AND taken_on >= ")
                .push_bind(crate::to_secs(since))
                .push(" ORDER BY taken_on");
        }
    } else {
        select.push(" ORDER BY version DESC");
    }

    select.push(" LIMIT 1;");
    select
}

#[inline]
pub fn select<'a, ID, DB>(
    table: &sql::Ident<'a>,
    id: &'a ID,
    predicate: Option<&Predicate>,
) -> QueryBuilder<'a, DB>
where
    ID: Debug + Encode<'a, DB> + Send + Type<DB> + 'a,
    DB: Database,
    i32: Debug + for<'db> Encode<'db, DB> + Send + Type<DB>,
    i64: Debug + for<'db> Encode<'db, DB> + Send + Type<DB>,
{
    _select("SELECT type, revision, content ", table, id, predicate)
}

#[inline]
pub fn select_raw<'a, ID, DB>(
    table: &sql::Ident<'a>,
    id: &'a ID,
    predicate: Option<&Predicate>,
) -> QueryBuilder<'a, DB>
where
    ID: Debug + Encode<'a, DB> + Send + Type<DB> + 'a,
    DB: Database,
    i32: Debug + for<'db> Encode<'db, DB> + Send + Type<DB>,
    i64: Debug + for<'db> Encode<'db, DB> + Send + Type<DB>,
{
    _select(
        "SELECT version, sequence, type, revision, content ",
        table,
        id,
        predicate,
    )
}

pub fn insert<'a, ID, DB>(table: &'a sql::Ident<'a>, row: &'a sql::Row<ID>) -> QueryBuilder<'a, DB>
where
    DB: Database + Upsert,
    for<'args, 'db> <DB as Database>::Arguments<'args>: IntoArguments<'db, DB>,
    for<'db> &'db mut <DB as Database>::Connection: Executor<'db, Database = DB>,
    ID: Encode<'a, DB> + Send + Type<DB> + 'a,
    i16: for<'db> Encode<'db, DB> + Type<DB>,
    i32: for<'db> Encode<'db, DB> + Type<DB>,
    i64: for<'db> Encode<'db, DB> + Type<DB>,
    String: for<'db> Encode<'db, DB> + Type<DB>,
    for<'db> &'db [u8]: Encode<'db, DB> + Type<DB>,
{
    let mut insert = QueryBuilder::new("INSERT INTO ");

    insert
        .push(table.quote())
        .push(' ')
        .push(" VALUES (")
        .push_bind(&row.id)
        .push(", ")
        .push_bind(row.version)
        .push(", ")
        .push_bind(row.revision)
        .push(", ")
        .push_bind(row.stored_on)
        .push(", ")
        .push_bind(&row.kind)
        .push(", ")
        .push_bind(row.content.as_slice())
        .push(") ")
        .push(DB::on_conflict())
        .push(';');

    insert
}
