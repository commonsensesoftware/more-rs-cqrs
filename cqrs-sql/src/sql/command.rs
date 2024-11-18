use super::Ident;
use sqlx::{Database, Encode, Executor, IntoArguments, QueryBuilder, Type};

pub fn delete<'a, ID, DB>(table: &'a Ident<'a>, id: &'a ID) -> QueryBuilder<'a, DB>
where
    DB: Database,
    for<'args, 'db> <DB as Database>::Arguments<'args>: IntoArguments<'db, DB>,
    for<'db> &'db mut <DB as Database>::Connection: Executor<'db, Database = DB>,
    ID: Encode<'a, DB> + Send + Type<DB> + 'a,
{
    let mut delete = QueryBuilder::new("DELETE FROM ");

    delete
        .push(table.quote())
        .push(" WHERE id = ")
        .push_bind(id)
        .push(';');

    delete
}
