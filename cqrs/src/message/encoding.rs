use super::Schema;
use std::error::Error;
use thiserror::Error;

/// Defines the behavior of a message encoding.
pub trait Encoding<T: ?Sized + Sync>: Send + Sync {
    /// Get the message [schema](Schema) the encoding applies to.
    fn schema(&self) -> &Schema;

    /// Encodes the specified message.
    ///
    /// # Arguments
    ///
    /// * `message` - the message to encode
    ///
    /// # Returns
    ///
    /// The encoded message as binary.
    fn encode(&self, message: &T) -> Result<Vec<u8>, Box<dyn Error + Send>>;

    /// Decodes the specified message
    ///
    /// # Arguments
    ///
    /// * `message` - the message to decode
    ///
    /// # Returns
    ///
    /// The decoded message is successful; otherwise, an [error](Error).
    fn decode(&self, message: &[u8]) -> Result<Box<T>, Box<dyn Error + Send>>;
}

/// Represents the possible encoding errors.
#[derive(Error, Debug)]
pub enum EncodingError {
    /// Indicates a [schema](Schema) for an encoding has already been registered.
    #[error("type {} for revision {} has already been registered", (.0).kind(), (.0).version())]
    DuplicateSchema(Schema),

    /// Indicates an encoding [schema](Schema) has not be registered.
    #[error("type {} for revision {} has not been registered", (.0).kind(), (.0).version())]
    Unregistered(Schema),

    /// Indicates that an [encoding](Encoding) failed to [Encoding::encode] or [Encoding::decode].
    #[error(transparent)]
    Failed(#[from] Box<dyn Error + Send>),
}

impl PartialEq for EncodingError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::DuplicateSchema(l0), Self::DuplicateSchema(r0)) => l0 == r0,
            (Self::Unregistered(l0), Self::Unregistered(r0)) => l0 == r0,
            _ => false,
        }
    }
}
