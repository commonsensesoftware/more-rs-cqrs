use super::{Encoding, EncodingError, Message, Schema};
use std::collections::HashMap;

/// Represents a message transcoder.
pub struct Transcoder<T: ?Sized + Sync> {
    encodings: HashMap<Schema, Box<dyn Encoding<T>>>,
}

impl<T: ?Sized + Sync> Default for Transcoder<T> {
    fn default() -> Self {
        Self {
            encodings: Default::default(),
        }
    }
}

impl<T: ?Sized + Sync> Transcoder<T> {
    /// Initializes a new [Transcoder].
    pub fn new() -> Self {
        Self::default()
    }
}

impl<T: ?Sized + Message> Transcoder<T> {
    /// Encodes the specified message.
    ///
    /// # Arguments
    ///
    /// * `message` - the [message](Message) to encode
    ///
    /// # Returns
    ///
    /// The encoded message is successful; otherwise an [error](EncodingError).
    pub fn encode(&self, message: &T) -> Result<Vec<u8>, EncodingError> {
        let schema = message.schema();

        if let Some(encoding) = self.encodings.get(&schema) {
            Ok(encoding.encode(message)?)
        } else {
            Err(EncodingError::Unregistered(schema))
        }
    }

    /// Decodes the specified message.
    ///
    /// # Arguments
    ///
    /// * `schema` - the [schema](Schema) to decode
    /// * `message` - the message to decode
    ///
    /// # Returns
    ///
    /// The decoded [Message] is successful; otherwise an [error](EncodingError).
    pub fn decode(&self, schema: &Schema, message: &[u8]) -> Result<Box<T>, EncodingError> {
        if let Some(encoding) = self.encodings.get(schema) {
            Ok(encoding.decode(message)?)
        } else {
            Err(EncodingError::Unregistered(schema.clone()))
        }
    }

    /// Registers an [encoding](Encoding) for a [message](Message).
    ///
    /// # Arguments
    ///
    /// * `encoding` - the [encoding](Encoding) to register
    ///
    /// # Returns
    ///
    /// An [error](EncodingError) if an [encoding](Encoding) has already been registered for the
    /// specified [message](Message).
    pub fn register<E>(&mut self, encoding: E) -> Result<(), EncodingError>
    where
        E: Encoding<T> + 'static,
    {
        let schema = encoding.schema();

        if self.encodings.contains_key(schema) {
            Err(EncodingError::DuplicateSchema(schema.clone()))
        } else {
            self.encodings.insert(schema.clone(), Box::new(encoding));
            Ok(())
        }
    }

    /// Merges another transcoder into the current instance.
    ///
    /// # Arguments
    ///
    /// * `other` - the [transcoder](Transcoder) to merge into the current instance
    pub fn merge(&mut self, other: Transcoder<T>) {
        for (key, encoding) in other.encodings {
            self.encodings.entry(key).or_insert(encoding);
        }
    }
}
