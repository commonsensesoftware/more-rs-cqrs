use crate::{snapshot::Upsert, sql, SqlVersion};
use cqrs::{snapshot::Predicate, Mask};
use sqlx::{Encode, QueryBuilder, Sqlite, Type};
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

pub fn select<'a, ID>(
    table: &sql::Ident<'a>,
    id: &'a ID,
    predicate: Option<&Predicate>,
    mask: Option<Arc<dyn Mask>>,
) -> QueryBuilder<'a, Sqlite>
where
    ID: Debug + Encode<'a, Sqlite> + Send + Type<Sqlite> + 'a,
{
    let mut select = QueryBuilder::new("SELECT version, sequence, type, revision, content ");

    select
        .push(" FROM ")
        .push(table.as_object_name())
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

pub fn insert<'a, ID>(table: &'a sql::Ident<'a>, row: &'a sql::Row<ID>) -> QueryBuilder<'a, Sqlite>
where
    ID: Encode<'a, Sqlite> + Send + Type<Sqlite> + 'a,
{
    let mut insert = QueryBuilder::new("INSERT INTO ");

    insert
        .push(table.as_object_name())
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

    insert.push(") ").push(Sqlite::on_conflict()).push(';');
    insert
}
