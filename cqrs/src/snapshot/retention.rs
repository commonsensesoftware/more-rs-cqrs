use std::time::Duration;

/// Represents the retention for a [snapshot](super::Snapshot).
#[derive(Clone, Debug, Default)]
pub struct Retention {
    /// Gets or sets the optional number of snapshots to retain.
    pub count: Option<u8>,

    /// Gets or sets the optional age of snapshots to retain.
    pub age: Option<Duration>,
}

impl Retention {
    /// Gets a value indicating whether all snapshots are retained.
    pub fn all(&self) -> bool {
        self.count.is_none() && self.age.is_none()
    }

    /// Initializes a new [`Retention`] that retains a fixed number of snapshots.
    ///
    /// # Arguments
    ///
    /// * `count` - the number of snapshots to retain
    pub fn count(count: u8) -> Self {
        Self {
            count: Some(count),
            age: None,
        }
    }

    /// Initializes a new [`Retention`] that retains a fixed age of snapshots.
    ///
    /// # Arguments
    ///
    /// * `age` - the [age](Duration) of snapshots to retain
    pub fn age(age: Duration) -> Self {
        Self {
            count: None,
            age: Some(age),
        }
    }

    /// Initializes a new [`Retention`] that retains snapshots for a fixed number of days.
    ///
    /// # Arguments
    ///
    /// * `days` - the number of days to retain snapshots for
    pub fn days(days: u16) -> Self {
        Self::age(Duration::from_secs(60u64 * 60u64 * 24u64 * days as u64))
    }
}
