use super::Upsert;
use crate::{sql, SqlVersion};
use cqrs::{snapshot::Predicate, Mask};
use sqlx::{Database, Encode, Executor, IntoArguments, QueryBuilder, Type};
use std::{
    fmt::Debug,
    ops::Bound::{self, *},
    sync::Arc,
};

#[inline]
fn ge<T: Copy>(bound: &Bound<T>) -> (&'static str, T) {
    match bound {
        Included(value) => (">=", *value),
        Excluded(value) => (">", *value),
        _ => unreachable!(),
    }
}

pub fn select<'a, ID, DB>(
    table: &sql::Ident<'a>,
    id: &'a ID,
    predicate: Option<&Predicate>,
    mask: Option<Arc<dyn Mask>>,
) -> QueryBuilder<'a, DB>
where
    ID: Debug + Encode<'a, DB> + Send + Type<DB> + 'a,
    DB: Database,
    i32: Debug + for<'db> Encode<'db, DB> + Send + Type<DB>,
    i64: Debug + for<'db> Encode<'db, DB> + Send + Type<DB>,
{
    let mut select = QueryBuilder::new("SELECT version, sequence, type, revision, content ");

    select
        .push(" FROM ")
        .push(table.quote())
        .push(" WHERE id = ")
        .push_bind(id);

    if let Some(predicate) = predicate {
        if !matches!(predicate.min_version, Unbounded) {
            let (op, mut version) = ge(&predicate.min_version);

            if let Some(mask) = &mask {
                version = version.unmask(mask);
            }

            select
                .push(" AND version ")
                .push(op)
                .push(" ")
                .push_bind(version.number());
        }

        if !matches!(predicate.since, Unbounded) {
            let (op, since) = ge(&predicate.since);
            select
                .push(" AND ")
                .push(op)
                .push(" ")
                .push_bind(crate::to_secs(since))
                .push(" ORDER BY taken_on");
        }
    } else {
        select.push(" ORDER BY version DESC");
    }

    select.push(" LIMIT 1;");
    select
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
        .push(", ");

    if let Some(cid) = &row.correlation_id {
        insert.push_bind(cid);
    } else {
        insert.push("NULL");
    }

    insert.push(") ").push(DB::on_conflict()).push(';');
    insert
}
