use super::Schema;
use std::any::type_name;

/// Defines the behavior of a message.
pub trait Message: Send + Sync {
    /// Get the message [schema](Schema).
    fn schema(&self) -> Schema {
        Schema::new(type_name::<Self>(), 1)
    }

    /// Gets the associated message correlation identifier, if any.
    fn correlation_id(&self) -> Option<&str> {
        None
    }
}