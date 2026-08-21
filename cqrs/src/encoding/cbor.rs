use crate::event::Event;
use crate::message::{Encoded, Encoding, Schema};
use crate::snapshot::Snapshot;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::marker::PhantomData;

/// Represents a message encoding using Concise Binary Object Representation (CBOR) as defined in
/// [RFC 7049](https://tools.ietf.org/html/rfc7049).
pub struct Cbor<T> {
    schema: Schema,
    _marker: PhantomData<T>,
}

impl<T> Cbor<T> {
    /// Initializes a new [CBOR](Cbor) message encoding for the specified [schema](Schema).
    ///
    /// # Arguments
    ///
    /// * `schema` - the [schema](Schema) the encoding applies to
    ///
    /// # Remarks
    ///
    /// This is meant for a scenario where the [schema](Schema) of a message cannot be derived from the message itself,
    /// such as decoding messages that were stored before the message type was moved or renamed.
    pub fn with_schema(schema: Schema) -> Self {
        Self {
            schema,
            _marker: Default::default(),
        }
    }
}

impl<T: Encoded> Default for Cbor<T> {
    fn default() -> Self {
        Self {
            schema: T::schema(),
            _marker: Default::default(),
        }
    }
}

impl<T: Encoded> Cbor<T> {
    /// Initializes a new [CBOR](Cbor) message encoding for the specified [encoded](Encoded) message type.
    pub fn new() -> Self {
        Self::default()
    }

    /// Initializes a new [CBOR](Cbor) message encoding for a specific revision of the message type.
    ///
    /// # Remarks
    ///
    /// [Self::new] registers a message type for its own [schema](Encoded::schema). This registers the same type for
    /// another revision of the same [kind](Schema::kind), which allows one type to decode more than one
    /// revision; for example, when a revision only added fields the type is able to default. The result is effectively
    /// read-only, because encoding always uses the [schema](Encoded::schema) of the message itself and, therefore,
    /// always writes the current revision.
    ///
    /// Use [Self::with_schema] instead when the [kind](Schema::kind) changed as well, such as when the message type was
    /// moved or renamed. A `VERSION` of `0` fails to compile.
    pub fn version<const VERSION: u8>() -> Self {
        Self::with_schema(Schema::version::<VERSION>(T::schema().kind()))
    }
}

impl<T> Encoding<dyn Event> for Cbor<T>
where
    T: Default + for<'de> Deserialize<'de> + Serialize + Event + 'static,
{
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn encode(&self, message: &dyn Event) -> Result<Vec<u8>, Box<dyn Error + Send>> {
        let mut bytes = Vec::new();

        ciborium::into_writer(message.as_any().downcast_ref::<T>().unwrap(), &mut bytes)
            .map_err(|err| Box::new(err) as Box<dyn Error + Send>)?;

        Ok(bytes)
    }

    fn decode(&self, message: &[u8]) -> Result<Box<dyn Event>, Box<dyn Error + Send>> {
        Ok(ciborium::from_reader::<T, _>(message)
            .map(|event| Box::new(event) as Box<dyn Event + Send>)
            .map_err(|err| Box::new(err) as Box<dyn Error + Send>)?)
    }
}

impl<T> Encoding<dyn Snapshot> for Cbor<T>
where
    T: Default + for<'de> Deserialize<'de> + Serialize + Snapshot + 'static,
{
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn encode(&self, message: &dyn Snapshot) -> Result<Vec<u8>, Box<dyn Error + Send>> {
        let mut bytes = Vec::new();

        ciborium::into_writer(message.as_any().downcast_ref::<T>().unwrap(), &mut bytes)
            .map_err(|err| Box::new(err) as Box<dyn Error + Send>)?;

        Ok(bytes)
    }

    fn decode(&self, message: &[u8]) -> Result<Box<dyn Snapshot>, Box<dyn Error + Send>> {
        Ok(ciborium::from_reader::<T, _>(message)
            .map(|event| Box::new(event) as Box<dyn Snapshot + Send>)
            .map_err(|err| Box::new(err) as Box<dyn Error + Send>)?)
    }
}

#[cfg(test)]
mod tests {
    use crate::Version;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct Example {
        id: Uuid,
        version: Version,
    }

    #[test]
    fn fields_should_roundtrip_with_cbor() {
        // arrange
        let expected = Example {
            id: uuid::Uuid::new_v4(),
            version: Version::new(42),
        };
        let mut binary = Vec::new();

        // act
        ciborium::into_writer(&expected, &mut binary).unwrap();
        let actual = ciborium::from_reader::<Example, _>(binary.as_slice()).unwrap();

        // assert
        assert_eq!(actual, expected);
    }
}
