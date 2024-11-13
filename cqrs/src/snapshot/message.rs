use crate::{message::Message, Version};
use std::any::Any;

/// Represents the snapshot of state.
pub trait Snapshot: Message {
    /// Gets the [version](Version) of the snapshot.
    fn version(&self) -> Version;

    /// Gets the snapshot as a dynamic type.
    fn as_any(&self) -> &dyn Any;
}
