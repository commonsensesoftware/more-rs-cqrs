use cqrs::snapshot::Retention;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Represents the state of applying a [retention](Retention) policy while pruning snapshots.
///
/// # Remarks
///
/// A [retention](Retention) is a set of constraints on what is kept, so a snapshot is pruned when it violates any of
/// them; specifically, it is beyond the number of snapshots to retain or it is older than the age to retain. A policy
/// without any constraints prunes everything.
///
/// The policy is stateful because a retained count is relative to the snapshots that have already been visited.
/// Snapshots are expected to be visited from newest to oldest.
#[allow(dead_code)]
pub(crate) struct Prune {
    count: Option<u8>,
    cutoff: Option<Duration>,
    kept: u8,
}

#[allow(dead_code)]
impl Prune {
    /// Initializes a new [Prune] policy.
    ///
    /// # Arguments
    ///
    /// * `retention` - the [retention](Retention) policy to apply, if any
    /// * `now` - the current [date and time](SystemTime)
    ///
    /// # Remarks
    ///
    /// A policy without a [retention](Retention) prunes everything. An age that predates the epoch retains everything.
    pub fn new(retention: Option<&Retention>, now: SystemTime) -> Self {
        let (count, cutoff) = if let Some(retention) = retention {
            let cutoff = retention.age.map(|age| {
                now.checked_sub(age)
                    .and_then(|cutoff| cutoff.duration_since(UNIX_EPOCH).ok())
                    .unwrap_or_default()
            });

            (retention.count, cutoff)
        } else {
            (None, None)
        };

        Self { count, cutoff, kept: 0 }
    }

    /// Gets a value indicating whether the snapshot has expired and should be pruned.
    ///
    /// # Arguments
    ///
    /// * `taken_on` - the [time](Duration) since the epoch the snapshot was taken on
    ///
    /// # Remarks
    ///
    /// The count is always evaluated so the number of snapshots visited so far is tracked, even when the snapshot is
    /// already known to be stale.
    pub fn expired(&mut self, taken_on: Duration) -> bool {
        if self.count.is_none() && self.cutoff.is_none() {
            return true;
        }

        let beyond_count = match self.count {
            Some(count) => !self.keep(count),
            None => false,
        };
        let stale = match self.cutoff {
            Some(cutoff) => taken_on <= cutoff,
            None => false,
        };

        beyond_count || stale
    }

    const fn keep(&mut self, count: u8) -> bool {
        if self.kept < count {
            self.kept += 1;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    const HOUR: Duration = Duration::from_secs(60 * 60);

    fn now() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(100 * 60 * 60)
    }

    fn hours_ago(hours: u64) -> Duration {
        (now() - Duration::from_secs(hours * 60 * 60))
            .duration_since(UNIX_EPOCH)
            .unwrap()
    }

    #[test]
    fn expired_should_be_true_without_retention() {
        // arrange
        let mut prune = Prune::new(None, now());

        // act
        let expired: Vec<_> = (1..=3).map(|hour| prune.expired(hours_ago(hour))).collect();

        // assert
        assert_eq!(expired, vec![true, true, true]);
    }

    #[test]
    fn expired_should_retain_count() {
        // arrange
        let retention = Retention::count(2);
        let mut prune = Prune::new(Some(&retention), now());

        // act
        let expired: Vec<_> = (1..=4).map(|hour| prune.expired(hours_ago(hour))).collect();

        // assert
        assert_eq!(expired, vec![false, false, true, true]);
    }

    #[test]
    fn expired_should_retain_age() {
        // arrange
        let retention = Retention::age(HOUR * 2);
        let mut prune = Prune::new(Some(&retention), now());

        // act
        let expired: Vec<_> = (1..=4).map(|hour| prune.expired(hours_ago(hour))).collect();

        // assert
        assert_eq!(expired, vec![false, true, true, true]);
    }

    #[test]
    fn expired_should_retain_count_and_age() {
        // arrange
        let retention = Retention {
            count: Some(2),
            age: Some(HOUR * 3),
        };
        let mut prune = Prune::new(Some(&retention), now());

        // act
        let expired: Vec<_> = (1..=4).map(|hour| prune.expired(hours_ago(hour))).collect();

        // assert
        assert_eq!(expired, vec![false, false, true, true]);
    }

    #[test]
    fn expired_should_prune_stale_snapshot_within_count() {
        // arrange
        let retention = Retention {
            count: Some(3),
            age: Some(HOUR * 2),
        };
        let mut prune = Prune::new(Some(&retention), now());

        // act
        let expired: Vec<_> = (1..=3).map(|hour| prune.expired(hours_ago(hour))).collect();

        // assert
        assert_eq!(expired, vec![false, true, true]);
    }

    #[test]
    fn expired_should_be_false_when_age_predates_epoch() {
        // arrange
        let retention = Retention::days(u16::MAX);
        let mut prune = Prune::new(Some(&retention), now());

        // act
        let expired = prune.expired(hours_ago(1));

        // assert
        assert!(!expired);
    }
}
