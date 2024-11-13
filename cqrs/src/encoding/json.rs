use crate::event::Event;
use crate::message::{Encoded, Encoding, Schema};
use crate::snapshot::Snapshot;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::marker::PhantomData;

/// Represents a message encoding using Java Script Object Notion (JSON).
pub struct Json<T> {
    schema: Schema,
    _marker: PhantomData<T>,
}

impl<T> Json<T> {
    /// Initializes a new [JSON](Json) message encoding for the specified message type and version.
    ///
    /// # Arguments
    ///
    /// * `version` - the supported message version
    pub fn version(version: u8) -> Self {
        Self {
            schema: Schema::new(std::any::type_name::<T>(), version),
            _marker: Default::default(),
        }
    }
}

impl<T: Encoded> Default for Json<T> {
    fn default() -> Self {
        Self {
            schema: T::schema(),
            _marker: Default::default(),
        }
    }
}

impl<T: Encoded> Json<T> {
    /// Initializes a new [JSON](Json) message encoding for the specified [encoded](Encoded) message type.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<T> Encoding<dyn Event> for Json<T>
where
    T: Default + for<'de> Deserialize<'de> + Serialize + Event + 'static,
{
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn encode(&self, message: &dyn Event) -> Result<Vec<u8>, Box<dyn Error + Send>> {
        serde_json::to_vec(message.as_any().downcast_ref::<T>().unwrap())
            .map_err(|err| Box::new(err) as Box<dyn Error + Send>)
    }

    fn decode(&self, message: &[u8]) -> Result<Box<dyn Event>, Box<dyn Error + Send>> {
        Ok(serde_json::from_slice::<T>(message)
            .map(|event| Box::new(event) as Box<dyn Event + Send>)
            .map_err(|err| Box::new(err) as Box<dyn Error + Send>)?)
    }
}

impl<T> Encoding<dyn Snapshot> for Json<T>
where
    T: Default + for<'de> Deserialize<'de> + Serialize + Snapshot + 'static,
{
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn encode(&self, message: &dyn Snapshot) -> Result<Vec<u8>, Box<dyn Error + Send>> {
        serde_json::to_vec(message.as_any().downcast_ref::<T>().unwrap())
            .map_err(|err| Box::new(err) as Box<dyn Error + Send>)
    }

    fn decode(&self, message: &[u8]) -> Result<Box<dyn Snapshot>, Box<dyn Error + Send>> {
        Ok(serde_json::from_slice::<T>(message)
            .map(|snapshot| Box::new(snapshot) as Box<dyn Snapshot + Send>)
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
    fn fields_should_roundtrip_with_json() {
        // arrange
        let expected = Example {
            id: uuid::Uuid::new_v4().into(),
            version: Version::new(42),
        };

        // act
        let binary = serde_json::to_vec(&expected).unwrap();
        let actual = serde_json::from_slice::<Example>(binary.as_slice()).unwrap();

        // assert
        assert_eq!(actual, expected);
    }
}
