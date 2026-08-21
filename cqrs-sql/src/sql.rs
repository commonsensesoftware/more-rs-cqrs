pub(crate) mod command;
mod ident;
mod row;

pub use ident::{Delimiters, Ident, IdentPart};
pub(crate) use row::{Context, IntoRows, Row};

use std::borrow::Cow;
use std::ops::Bound::{self, Excluded, Included};

/// Defines the behavior of a SQL database provider.
pub trait Provider {
    /// Gets a value indicating whether the provider supports schemas.
    ///
    /// # Remarks
    ///
    /// The default value is `true`. Providers which return `false` have the schema
    /// folded into the object name; for example, `"events"."orders"` is represented
    /// as the unqualified name `events_orders`.
    fn supports_schemas() -> bool {
        true
    }

    /// Creates and returns the [identifier](Ident) of a storage table.
    ///
    /// # Arguments
    ///
    /// * `schema` - the schema name, which may be empty
    /// * `table` - the table name
    fn table(schema: &'static str, table: &'static str) -> Ident<'static> {
        if schema.is_empty() {
            Ident::unqualified(table)
        } else if Self::supports_schemas() {
            Ident::qualified(schema, table)
        } else {
            Ident::unqualified(Ident::qualified(schema, table).as_object_name().into_owned())
        }
    }

    /// Gets the [delimiters](Delimiters) which enclose a quoted identifier.
    ///
    /// # Remarks
    ///
    /// The default value is [`Delimiters::ANSI`]. A provider which does not treat a double
    /// quote as an identifier delimiter must override this; for example, MySQL uses
    /// [`Delimiters::BACKTICK`] and SQL Server uses [`Delimiters::BRACKET`].
    fn delimiters() -> Delimiters {
        Delimiters::ANSI
    }

    /// Returns the full identifier name, including quotes if necessary.
    ///
    /// # Arguments
    ///
    /// * `ident` - the [identifier](Ident) to quote
    fn quote<'a>(ident: &'a Ident<'_>) -> Cow<'a, str> {
        ident.quote_with(Self::delimiters())
    }

    /// Returns an identifier part, including quotes if necessary.
    ///
    /// # Arguments
    ///
    /// * `ident` - the [identifier](Ident) to quote
    /// * `part` - the [part](IdentPart) to quote
    fn quote_part<'a>(ident: &'a Ident<'_>, part: IdentPart) -> Option<Cow<'a, str>> {
        ident.quote_part_with(part, Self::delimiters())
    }
}

const fn op<T: Copy>(bound: &Bound<T>, op1: &'static str, op2: &'static str) -> Option<(T, &'static str)> {
    match bound {
        Included(value) => Some((*value, op1)),
        Excluded(value) => Some((*value, op2)),
        _ => None,
    }
}

pub(crate) const fn greater_than<T: Copy>(bound: &Bound<T>) -> Option<(T, &'static str)> {
    op(bound, ">=", ">")
}
pub(crate) const fn less_than<T: Copy>(bound: &Bound<T>) -> Option<(T, &'static str)> {
    op(bound, "<=", "<")
}
