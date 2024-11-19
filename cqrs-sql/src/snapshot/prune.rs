use crate::sql;
use cqrs::{snapshot::Retention, Clock};
use sqlx::{Database, QueryBuilder};

/// Defines the behavior of a pruning SQL statement for snapshots.
pub trait Prune<'a, ID, DB: Database> {
    /// Gets the appropriate SQL `DELETE` statement to prune snapshots.
    ///
    /// # Arguments
    ///
    /// * `table` - the table [identifier](sql::Ident)
    /// * `id` - the identifier of the snapshots to prune
    /// * `clock` - the current [clock](Clock)
    /// * `retention` - the [retention](Retention) policy to apply
    fn prune(
        table: &'a sql::Ident<'a>,
        id: &'a ID,
        clock: &'a dyn Clock,
        retention: &'a Retention,
    ) -> QueryBuilder<'a, DB>;
}
