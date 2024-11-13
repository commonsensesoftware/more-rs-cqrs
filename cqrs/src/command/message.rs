use crate::{message::Message, Version};
use std::any::Any;

/// Defines the behavior of a command.
pub trait Command: Message {
    /// Gets the expected [version](Version) aggregate version the command corresponds to.
    fn expected_version(&self) -> Version;

    /// Gets the command as a dynamic type.
    fn as_any(&self) -> &dyn Any;
}
