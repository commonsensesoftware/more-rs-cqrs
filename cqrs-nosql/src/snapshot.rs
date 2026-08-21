use crate::{NoSqlVersion, version::new_version};
use cqrs::{
    Mask, Version,
    event::Predicate,
    message::Descriptor,
    snapshot::{SnapshotError, Store},
};
use std::{
    fmt::Debug,
    ops::Bound::{self, *},
};

#[inline]
fn unmask(version: Version, mask: Option<&(dyn Mask + 'static)>) -> Version {
    if let Some(mask) = mask {
        version.unmask(mask)
    } else {
        version
    }
}

/// Selects the [version](Version) [bound](Bound) of the events to load.
///
/// # Arguments
///
/// * `snapshot` - the [snapshot](Descriptor) the load is seeded from, if any
/// * `predicate` - the [predicate](Predicate) the load is filtered by
/// * `mask` - the [mask](Mask) used to obfuscate [versions](Version), if any
///
/// # Remarks
///
/// The returned [bound](Bound) is never masked because it is only ever used to build a query.
/// A [predicate](Predicate) that asks for events beyond the snapshot is honored as-is; otherwise,
/// only the events which are not already summarized by the snapshot are loaded.
pub(crate) fn select_version<T: Debug + Send>(
    snapshot: Option<&Descriptor>,
    predicate: &Predicate<'_, T>,
    mask: Option<&(dyn Mask + 'static)>,
) -> Bound<Version> {
    if let Some(snapshot) = snapshot {
        let version = unmask(snapshot.version, mask).number();

        match predicate.version {
            Included(other) => {
                let other = unmask(other, mask);

                if other.number() >= version {
                    return Included(other);
                }
            }
            Excluded(other) => {
                let other = unmask(other, mask);

                if other.number() > version {
                    return Excluded(other);
                }
            }
            _ => {}
        }

        // a snapshot summarizes every event up to and including its version, which is
        // encoded without a sequence, so a replay resumes at the next version
        Included(new_version(version.saturating_add(1), 0))
    } else {
        match predicate.version {
            Included(version) => Included(unmask(version, mask)),
            Excluded(version) => Excluded(unmask(version, mask)),
            _ => Unbounded,
        }
    }
}

/// Gets the [snapshot](Descriptor) a load is seeded from, if any.
///
/// # Arguments
///
/// * `snapshots` - the [snapshot store](Store) to load from, if any
/// * `predicate` - the [predicate](Predicate) the load is filtered by, if any
///
/// # Remarks
///
/// A snapshot can only seed a load for a single, identified aggregate.
pub(crate) async fn get_snapshot<'a, ID: Debug + Send>(
    snapshots: Option<&'a dyn Store<ID>>,
    predicate: Option<&Predicate<'a, ID>>,
) -> Result<Option<Descriptor>, SnapshotError> {
    if let Some(snapshots) = snapshots
        && let Some(predicate) = predicate
        && predicate.load.snapshots
        && let Some(id) = predicate.id
    {
        let predicate = Some(predicate.into());
        return snapshots.load_raw(id, predicate.as_ref()).await;
    }

    Ok(None)
}

#[cfg(test)]
mod test {
    use super::*;
    use cqrs::{SecureMask, event::PredicateBuilder, message::Schema};
    use std::sync::Arc;
    use uuid::Uuid;

    fn descriptor(version: Version) -> Descriptor {
        Descriptor::new(Schema::version::<1>("statement"), version, Vec::new())
    }

    #[test]
    fn select_version_should_resume_after_snapshot() {
        // arrange
        let snapshot = descriptor(new_version(3, 0));
        let predicate = PredicateBuilder::<Uuid>::new(None).build();

        // act
        let version = select_version(Some(&snapshot), &predicate, None);

        // assert
        assert_eq!(version, Included(new_version(4, 0)));
    }

    #[test]
    fn select_version_should_ignore_sequence_of_snapshot() {
        // arrange
        let snapshot = descriptor(new_version(3, 2));
        let predicate = PredicateBuilder::<Uuid>::new(None).build();

        // act
        let version = select_version(Some(&snapshot), &predicate, None);

        // assert
        assert_eq!(version, Included(new_version(4, 0)));
    }

    #[test]
    fn select_version_should_use_predicate_beyond_snapshot() {
        // arrange
        let snapshot = descriptor(new_version(3, 0));
        let predicate = PredicateBuilder::<Uuid>::new(None)
            .version(Included(new_version(5, 0)))
            .build();

        // act
        let version = select_version(Some(&snapshot), &predicate, None);

        // assert
        assert_eq!(version, Included(new_version(5, 0)));
    }

    #[test]
    fn select_version_should_resume_after_snapshot_beyond_predicate() {
        // arrange
        let snapshot = descriptor(new_version(7, 0));
        let predicate = PredicateBuilder::<Uuid>::new(None)
            .version(Included(new_version(2, 0)))
            .build();

        // act
        let version = select_version(Some(&snapshot), &predicate, None);

        // assert
        assert_eq!(version, Included(new_version(8, 0)));
    }

    #[test]
    fn select_version_should_unmask_snapshot_and_predicate() {
        // arrange
        let mask: Arc<dyn Mask> = Arc::new(SecureMask::ephemeral());
        let snapshot = descriptor(new_version(3, 0).mask(&*mask));
        let predicate = PredicateBuilder::<Uuid>::new(None)
            .version(Included(new_version(5, 0).mask(&*mask)))
            .build();

        // act
        let version = select_version(Some(&snapshot), &predicate, Some(&*mask));

        // assert
        assert_eq!(version, Included(new_version(5, 0)));
    }

    #[test]
    fn select_version_should_use_predicate_without_snapshot() {
        // arrange
        let predicate = PredicateBuilder::<Uuid>::new(None)
            .version(Excluded(new_version(2, 1)))
            .build();

        // act
        let version = select_version(None, &predicate, None);

        // assert
        assert_eq!(version, Excluded(new_version(2, 1)));
    }

    #[test]
    fn select_version_should_be_unbounded_without_snapshot_or_predicate() {
        // arrange
        let predicate = PredicateBuilder::<Uuid>::new(None).build();

        // act
        let version = select_version(None, &predicate, None);

        // assert
        assert_eq!(version, Unbounded);
    }
}
