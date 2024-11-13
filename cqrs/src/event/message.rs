use crate::{message::Message, Version};
use std::any::Any;

/// Defines the behavior of an event.
pub trait Event: Message {
    /// Gets the name of the event.
    fn name(&self) -> &str;

    /// Gets the [version](Version) of the event.
    fn version(&self) -> Version;

    /// Sets the [version](Version) of the event.
    ///
    /// # Arguments
    ///
    /// * `version` - the new event [version](Version)
    fn set_version(&mut self, version: Version);

    /// Gets the event as a dynamic type.
    fn as_any(&self) -> &dyn Any;
}
