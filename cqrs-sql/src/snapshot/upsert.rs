/// Defines the behavior of a SQL upsert for snapshots.
pub trait Upsert {
    /// Gets the appropriate SQL `ON CONFLICT` upsert clause.
    fn on_conflict() -> &'static str;
}