pub(crate) mod command;
mod store;

pub use store::SqlStore;

use crate::SqlVersion;
use cqrs::{
    message::Descriptor,
    event::Predicate,
    snapshot::{SnapshotError, Store},
    Mask,
};
use std::{
    fmt::Debug,
    ops::Bound::{self, *},
    sync::Arc,
};

pub(crate) fn select_version<T: Debug + Send>(
    snapshot: Option<&Descriptor>,
    predicate: &Predicate<'_, T>,
    mask: Option<Arc<dyn Mask>>,
) -> Bound<i32> {
    if let Some(snapshot) = snapshot {
        let version = if let Some(mask) = &mask {
            snapshot.version.unmask(mask).number()
        } else {
            snapshot.version.number()
        };

        match predicate.version {
            Included(other) => {
                let other = if let Some(mask) = &mask {
                    other.unmask(mask).number()
                } else {
                    other.number()
                };

                if other >= version {
                    return Included(other);
                }
            }
            Excluded(other) => {
                let other = if let Some(mask) = &mask {
                    other.unmask(mask).number()
                } else {
                    other.number()
                };

                if other > version {
                    return Excluded(other);
                }
            }
            _ => {}
        }

        Excluded(version)
    } else if let Some(mask) = mask {
        match predicate.version {
            Included(version) => Included(version.unmask(&mask).number()),
            Excluded(version) => Excluded(version.unmask(&mask).number()),
            _ => Unbounded,
        }
    } else {
        match predicate.version {
            Included(version) => Included(version.number()),
            Excluded(version) => Excluded(version.number()),
            _ => Unbounded,
        }
    }
}

pub(crate) async fn get_snapshot<'a, ID: Debug + Send>(
    snapshots: Option<&'a dyn Store<ID>>,
    predicate: Option<&Predicate<'a, ID>>,
) -> Result<Option<Descriptor>, SnapshotError> {
    if let Some(snapshots) = snapshots {
        if let Some(predicate) = predicate {
            if predicate.load.snapshots {
                if let Some(id) = predicate.id {
                    let predicate = Some(predicate.into());
                    return snapshots.load_raw(id, predicate.as_ref()).await;
                }
            }
        }
    }

    Ok(None)
}
