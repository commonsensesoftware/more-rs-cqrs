use crate::{event, sql};
use cqrs::event::StoreError;
use sqlx::{Encode, Sqlite, Transaction, Type};
use std::{error::Error, fmt::Debug};

pub async fn insert_transacted<'a, ID>(
    table: &'a sql::Ident<'a>,
    row: &'a sql::Row<ID>,
    tx: &'a mut Transaction<'_, Sqlite>,
) -> Result<(), StoreError<ID>>
where
    ID: Clone + Debug + for<'db> Encode<'db, Sqlite> + Send + Type<Sqlite>,
{
    let mut insert = event::command::insert::<ID, Sqlite>(table, row);

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
    let exists: bool = event::command::exists(table, previous)
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
