use crate::Version;

/// Represents a saved [message](super::Message).
pub struct Saved<T> {
    message: T,
    version: Version,
}

impl<T> Saved<T> {
    /// Initializes new [Saved] message.
    ///
    /// # Arguments
    ///
    /// * `message` - The versioned [message](super::Message)
    /// * `version` - The message [version](Version)
    pub fn new(message: T, version: Version) -> Self {
        Self {
            message,
            version,
        }
    }

    /// Gets the [message](super::Message).
    pub fn message(&self) -> &T {
        &self.message
    }

    /// Gets the message [version](Version), if any.
    pub fn version(&self) -> Version {
        self.version
    }
}

impl<T> From<Saved<T>> for (T, Version) {
    fn from(saved: Saved<T>) -> Self {
        (saved.message, saved.version)
    }
}
