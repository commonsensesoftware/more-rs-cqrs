use crate::message::Message;
use std::any::Any;

/// Defines the behavior of an event.
pub trait Event: Message {
    /// Gets the name of the event.
    fn name(&self) -> &str;

    /// Gets the event as a dynamic type.
    fn as_any(&self) -> &dyn Any;
}
