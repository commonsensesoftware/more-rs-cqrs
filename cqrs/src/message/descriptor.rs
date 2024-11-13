use super::Schema;
use crate::Version;

/// Represents a [message][super::Message] descriptor.
pub struct Descriptor {
    /// Gets or sets the message [schema](Schema).
    pub schema: Schema,

    // Gets or sets the message [version](Version).
    pub version: Version,

    /// Gets or sets the message content.
    pub content: Vec<u8>,
}

impl Descriptor {
    /// Initializes a new [`Descriptor`].
    pub fn new(
        schema: Schema,
        version: Version,
        content: Vec<u8>,
    ) -> Self {
        Self {
            schema,
            version,
            content
        }
    }
}
