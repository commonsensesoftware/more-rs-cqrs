use crate::message::Message;
use std::any::Any;

/// Represents the snapshot of state.
pub trait Snapshot: Message {
    /// Gets the snapshot as a dynamic type.
    fn as_any(&self) -> &dyn Any;
}
