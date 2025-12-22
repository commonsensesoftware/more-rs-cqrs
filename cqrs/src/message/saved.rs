use crate::Version;

/// Represents a saved [message](super::Message).
pub struct Saved<T> {
    message: T,
    version: Option<Version>,
}

impl<T> Saved<T> {
    /// Initializes new [Saved] message.
    ///
    /// # Arguments
    ///
    /// * `message` - The versioned [message](super::Message)
    /// * `version` - The message [version](Version)
    pub fn versioned(message: T, version: Version) -> Self {
        Self {
            message,
            version: Some(version),
        }
    }

    /// Initializes new unversioned [Saved] message.
    ///
    /// # Arguments
    ///
    /// * `message` - The unversioned [message](super::Message)
    pub fn unversioned(message: T) -> Self {
        Self {
            message,
            version: None,
        }
    }

    /// Gets the [message](super::Message).
    pub fn message(&self) -> &T {
        &self.message
    }

    /// Gets the message [version](Version), if any.
    pub fn version(&self) -> Option<Version> {
        self.version.clone()
    }
}

impl<T> From<(T, Option<Version>)> for Saved<T> {
    fn from((message, version): (T, Option<Version>)) -> Self {
        Self { message, version }
    }
}

impl<T> Into<(T, Option<Version>)> for Saved<T> {
    fn into(self) -> (T, Option<Version>) {
        (self.message, self.version)
    }
}
