use crate::sql;
use cqrs::{Clock, snapshot::Retention};
use sqlx::{Database, Encode, QueryBuilder, Type};

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
    fn prune(table: &'a sql::Ident<'a>, id: &'a ID, clock: &'a dyn Clock, retention: &'a Retention)
    -> QueryBuilder<DB>;
}

/// Appends the columns a [retention](Retention) policy is applied to.
///
/// # Arguments
///
/// * `select` - the [statement](QueryBuilder) to append to
/// * `retention` - the [retention](Retention) policy to apply
///
/// # Remarks
///
/// A retained count is relative to the other snapshots, which requires ranking them from newest to oldest.
#[allow(dead_code)]
pub(crate) fn columns<DB: Database>(select: &mut QueryBuilder<DB>, retention: &Retention) {
    if retention.count.is_some() {
        select.push(", ROW_NUMBER() OVER (ORDER BY taken_on DESC) AS ordinal");
    }

    if retention.age.is_some() {
        select.push(", taken_on");
    }
}

/// Appends the predicate which matches the snapshots that a [retention](Retention) policy does not retain.
///
/// # Arguments
///
/// * `delete` - the [statement](QueryBuilder) to append to
/// * `alias` - the alias qualifying the columns, if any
/// * `clock` - the current [clock](Clock)
/// * `retention` - the [retention](Retention) policy to apply
///
/// # Remarks
///
/// A [retention](Retention) is a set of constraints on what is kept, so a snapshot is pruned
/// when it violates any of them; specifically, it is beyond the number of snapshots to retain
/// or it is older than the age to retain. A policy without any constraints prunes everything.
#[allow(dead_code)]
pub(crate) fn stale<DB: Database>(delete: &mut QueryBuilder<DB>, alias: &str, clock: &dyn Clock, retention: &Retention)
where
    i16: for<'db> Encode<'db, DB> + Type<DB>,
    i64: for<'db> Encode<'db, DB> + Type<DB>,
{
    let cutoff = retention.age.map(|age| crate::cutoff(clock.now(), age));

    match (retention.count, cutoff) {
        (Some(count), Some(cutoff)) => {
            delete
                .push(" AND (")
                .push(alias)
                .push("ordinal > ")
                .push_bind(count as i16)
                .push(" OR ")
                .push(alias)
                .push("taken_on <= ")
                .push_bind(cutoff)
                .push(')');
        }
        (Some(count), None) => {
            delete
                .push(" AND ")
                .push(alias)
                .push("ordinal > ")
                .push_bind(count as i16);
        }
        (None, Some(cutoff)) => {
            delete.push(" AND ").push(alias).push("taken_on <= ").push_bind(cutoff);
        }
        (None, None) => {}
    }
}

#[cfg(all(test, any(feature = "mysql", feature = "postgres", feature = "sqlite")))]
mod test {
    use super::*;
    use cqrs::VirtualClock;
    use std::time::Duration;

    const AGE: Duration = Duration::from_secs(60);

    fn retention(count: Option<u8>, age: Option<Duration>) -> Retention {
        Retention { count, age }
    }

    fn prune<DB>(retention: Retention) -> String
    where
        DB: Database + for<'a> Prune<'a, String, DB>,
    {
        let table = sql::Ident::unqualified("snapshots");
        let id = String::new();
        let clock = VirtualClock::new();

        DB::prune(&table, &id, &clock, &retention).into_string()
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_should_prune_stale_snapshots() {
        use sqlx::Postgres;

        // act
        let count = prune::<Postgres>(retention(Some(2), None));
        let age = prune::<Postgres>(retention(None, Some(AGE)));
        let both = prune::<Postgres>(retention(Some(2), Some(AGE)));
        let all = prune::<Postgres>(retention(None, None));

        // assert
        assert_eq!(
            count,
            concat!(
                "WITH s2 AS (SELECT id, version, ROW_NUMBER() OVER (ORDER BY taken_on DESC) AS ordinal",
                " FROM snapshots WHERE id = $1) DELETE FROM snapshots s1 USING s2",
                " WHERE s1.id = s2.id AND s1.version = s2.version AND s2.ordinal > $2;"
            )
        );
        assert_eq!(
            age,
            concat!(
                "WITH s2 AS (SELECT id, version, taken_on",
                " FROM snapshots WHERE id = $1) DELETE FROM snapshots s1 USING s2",
                " WHERE s1.id = s2.id AND s1.version = s2.version AND s2.taken_on <= $2;"
            )
        );
        assert_eq!(
            both,
            concat!(
                "WITH s2 AS (SELECT id, version, ROW_NUMBER() OVER (ORDER BY taken_on DESC) AS ordinal, taken_on",
                " FROM snapshots WHERE id = $1) DELETE FROM snapshots s1 USING s2",
                " WHERE s1.id = s2.id AND s1.version = s2.version",
                " AND (s2.ordinal > $2 OR s2.taken_on <= $3);"
            )
        );
        assert_eq!(
            all,
            concat!(
                "WITH s2 AS (SELECT id, version FROM snapshots WHERE id = $1)",
                " DELETE FROM snapshots s1 USING s2 WHERE s1.id = s2.id AND s1.version = s2.version;"
            )
        );
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn mysql_should_prune_stale_snapshots() {
        use sqlx::MySql;

        // act
        let count = prune::<MySql>(retention(Some(2), None));
        let both = prune::<MySql>(retention(Some(2), Some(AGE)));
        let all = prune::<MySql>(retention(None, None));

        // assert
        assert_eq!(
            count,
            concat!(
                "WITH s2 AS (SELECT id, version, ROW_NUMBER() OVER (ORDER BY taken_on DESC) AS ordinal",
                " FROM snapshots WHERE id = ?) DELETE s1 FROM snapshots s1 INNER JOIN s2",
                " WHERE s1.id = s2.id AND s1.version = s2.version AND s2.ordinal > ?;"
            )
        );
        assert_eq!(
            both,
            concat!(
                "WITH s2 AS (SELECT id, version, ROW_NUMBER() OVER (ORDER BY taken_on DESC) AS ordinal, taken_on",
                " FROM snapshots WHERE id = ?) DELETE s1 FROM snapshots s1 INNER JOIN s2",
                " WHERE s1.id = s2.id AND s1.version = s2.version AND (s2.ordinal > ? OR s2.taken_on <= ?);"
            )
        );
        assert_eq!(
            all,
            concat!(
                "WITH s2 AS (SELECT id, version FROM snapshots WHERE id = ?)",
                " DELETE s1 FROM snapshots s1 INNER JOIN s2 WHERE s1.id = s2.id AND s1.version = s2.version;"
            )
        );
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_should_prune_stale_snapshots() {
        use sqlx::Sqlite;

        // act
        let count = prune::<Sqlite>(retention(Some(2), None));
        let both = prune::<Sqlite>(retention(Some(2), Some(AGE)));
        let all = prune::<Sqlite>(retention(None, None));

        // assert
        assert_eq!(
            count,
            concat!(
                "DELETE FROM snapshots WHERE (id, version) IN (SELECT id, version FROM",
                " (SELECT id, version, ROW_NUMBER() OVER (ORDER BY taken_on DESC) AS ordinal",
                " FROM snapshots WHERE id = ?) WHERE id = ? AND ordinal > ?);"
            )
        );
        assert_eq!(
            both,
            concat!(
                "DELETE FROM snapshots WHERE (id, version) IN (SELECT id, version FROM",
                " (SELECT id, version, ROW_NUMBER() OVER (ORDER BY taken_on DESC) AS ordinal, taken_on",
                " FROM snapshots WHERE id = ?) WHERE id = ? AND (ordinal > ? OR taken_on <= ?));"
            )
        );
        assert_eq!(
            all,
            concat!(
                "DELETE FROM snapshots WHERE (id, version) IN (SELECT id, version FROM",
                " (SELECT id, version FROM snapshots WHERE id = ?) WHERE id = ?);"
            )
        );
    }
}
