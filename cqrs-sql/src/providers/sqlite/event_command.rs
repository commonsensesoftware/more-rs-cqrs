use crate::sql::{self, greater_than, less_than};
use cqrs::{
    event::{Predicate, StoreError},
    Range,
};
use sqlx::{Encode, QueryBuilder, Sqlite, Transaction, Type};
use std::{error::Error, fmt::Debug, time::SystemTime};

#[inline]
fn and_stored_on(builder: &mut QueryBuilder<Sqlite>, (time, op): (SystemTime, &str)) {
    builder
        .push("stored_on ")
        .push(op)
        .push(" ")
        .push_bind(crate::to_secs(time));
}

pub fn select_id(table: sql::Ident, stored_on: Range<SystemTime>) -> QueryBuilder<Sqlite> {
    let mut select = QueryBuilder::new("SELECT id FROM ");

    select
        .push(table.as_object_name())
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

pub fn select<'a, ID>(
    table: sql::Ident<'a>,
    predicate: Option<&'a Predicate<'a, ID>>,
    version: Option<i32>,
) -> QueryBuilder<'a, Sqlite>
where
    ID: Debug + Encode<'a, Sqlite> + Send + Type<Sqlite> + 'a,
{
    fn add_where(builder: &mut QueryBuilder<'_, Sqlite>, added: &mut bool) {
        if *added {
            builder.push(" AND ");
        } else {
            *added = true;
            builder.push(" WHERE ");
        }
    }

    let mut select = QueryBuilder::new("SELECT type, revision, content FROM ");

    select.push(table.as_object_name());

    if let Some(predicate) = predicate {
        let mut added = false;

        if let Some(id) = predicate.id {
            select.push(" WHERE id = ").push_bind(id);
            added = true;
        }

        if let Some(version) = version {
            add_where(&mut select, &mut added);
            select.push("version = ").push_bind(version);
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

        let mut schemas = predicate.types.iter();
        
        if let Some(schema) = schemas.next() {
            add_where(&mut select, &mut added);

            let many = predicate.types.len() > 1;

            if many {
                select.push('(');
            }

            select.push("(type = ").push_bind(schema.kind().to_string());

            if schema.version() > 0 {
                select
                    .push(" AND revision = ")
                    .push_bind(schema.version() as i16);
            }

            select.push(")");

            for schema in schemas {
                select
                    .push(" OR (type = ")
                    .push_bind(schema.kind().to_string());

                if schema.version() > 0 {
                    select
                        .push(" AND revision = ")
                        .push_bind(schema.version() as i16);
                }

                select.push(")");
            }

            if many {
                select.push(')');
            }
        }
    }

    select.push(';');
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

pub async fn insert_transacted<'a, ID>(
    table: &'a sql::Ident<'a>,
    row: &'a sql::Row<ID>,
    tx: &'a mut Transaction<'_, Sqlite>,
) -> Result<(), StoreError<ID>>
where
    ID: Clone + Debug + for<'db> Encode<'db, Sqlite> + Send + Type<Sqlite>,
{
    let mut insert = insert::<ID>(table, row);

    if let Err(error) = insert.build().execute(&mut **tx).await {
        if let sqlx::Error::Database(error) = &error {
            if error.is_unique_violation() {
                return Err(StoreError::Conflict(row.id.clone(), row.version as u32));
            }
        }
        return Err(StoreError::Unknown(Box::new(error) as Box<dyn Error + Send>));
    }

    Ok(())
}

pub async fn ensure_not_deleted<'a, ID>(
    table: &'a sql::Ident<'a>,
    previous: &'a sql::Row<ID>,
    tx: &'a mut Transaction<'_, Sqlite>,
) -> Result<(), StoreError<ID>>
where
    ID: Clone + Debug + for<'db> Encode<'db, Sqlite> + Send + Type<Sqlite>,
{
    let mut insert = insert::<ID>(table, previous);

    if let Err(error) = insert.build().execute(&mut **tx).await {
        if let sqlx::Error::Database(error) = &error {
            if error.is_unique_violation() {
                // the events before now still exist; e.g. not deleted
                return Ok(());
            }
        }
        return Err(StoreError::Unknown(Box::new(error) as Box<dyn Error + Send>));
    }

    // if the insert succeeds, then this means all of the events must have been
    // deleted some time before the current save
    Err(StoreError::Deleted(previous.id.clone()))
}

pub fn delete<'a, ID>(table: &'a sql::Ident<'a>, id: &'a ID) -> QueryBuilder<'a, Sqlite>
where
    ID: Encode<'a, Sqlite> + Send + Type<Sqlite> + 'a,
{
    let mut delete = QueryBuilder::new("DELETE FROM ");

    delete
        .push(table.quote())
        .push(" WHERE id = ")
        .push_bind(id)
        .push(';');

    delete
}
