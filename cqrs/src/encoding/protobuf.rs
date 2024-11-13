use crate::event::Event;
use crate::message::{Encoded, Encoding, Schema};
use crate::snapshot::Snapshot;
use prost::{
    bytes::{Buf, BufMut},
    encoding::{
        check_wire_type, encode_key, encoded_len_varint, key_len, skip_field, DecodeContext,
        WireType,
    },
    DecodeError, Message,
};
use std::error::Error;
use std::fmt::{self, Debug, Formatter, Result as FormatResult};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

/// Represents a message encoding using Protocol Buffers with [prost](https://crates.io/crates/prost).
pub struct ProtoBuf<T> {
    schema: Schema,
    _marker: PhantomData<T>,
}

impl<T> ProtoBuf<T> {
    /// Initializes a new [Protocol Buffers](ProtoBuf) message encoding for the specified message type and version.
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

impl<T: Encoded> Default for ProtoBuf<T> {
    fn default() -> Self {
        Self {
            schema: T::schema(),
            _marker: Default::default(),
        }
    }
}

impl<T: Encoded> ProtoBuf<T> {
    /// Initializes a new [Protocol Buffers](ProtoBuf) message encoding for the specified [encoded](Encoded) message type.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<T: Default + Message + Event + 'static> Encoding<dyn Event> for ProtoBuf<T> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn encode(&self, message: &dyn Event) -> Result<Vec<u8>, Box<dyn Error + Send>> {
        Ok(message
            .as_any()
            .downcast_ref::<T>()
            .unwrap()
            .encode_to_vec())
    }

    fn decode(&self, message: &[u8]) -> Result<Box<dyn Event>, Box<dyn Error + Send>> {
        Ok(T::decode(message)
            .map(|evt| Box::new(evt) as Box<dyn Event + Send>)
            .map_err(|err| Box::new(err) as Box<dyn Error + Send>)?)
    }
}

impl<T: Default + Message + Snapshot + 'static> Encoding<dyn Snapshot> for ProtoBuf<T> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn encode(&self, message: &dyn Snapshot) -> Result<Vec<u8>, Box<dyn Error + Send>> {
        Ok(message
            .as_any()
            .downcast_ref::<T>()
            .unwrap()
            .encode_to_vec())
    }

    fn decode(&self, message: &[u8]) -> Result<Box<dyn Snapshot>, Box<dyn Error + Send>> {
        Ok(T::decode(message)
            .map(|evt| Box::new(evt) as Box<dyn Snapshot + Send>)
            .map_err(|err| Box::new(err) as Box<dyn Error + Send>)?)
    }
}

// REF: https://github.com/uuid-rs/uuid/pull/716

#[derive(Default, Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Uuid(uuid::Uuid);

impl Message for Uuid {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        encode_key(1, WireType::LengthDelimited, buf);
        buf.put_u8(16);
        buf.put_slice(self.as_bytes());
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        if tag == 1 {
            check_wire_type(WireType::LengthDelimited, wire_type)?;

            if buf.remaining() < 17 {
                return Err(DecodeError::new("buffer underflow"));
            }

            if buf.get_u8() != 16 {
                return Err(DecodeError::new("invalid UUID length"));
            }

            let mut bytes = self.0.into_bytes();
            buf.copy_to_slice(&mut bytes);
            self.0 = uuid::Uuid::from_bytes(bytes);

            Ok(())
        } else {
            skip_field(wire_type, tag, buf, ctx)
        }
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        key_len(1) + encoded_len_varint(16u64) + 16
    }

    fn clear(&mut self) {
        let mut bytes = self.0.into_bytes();

        for byte in &mut bytes {
            *byte = 0
        }

        self.0 = uuid::Uuid::from_bytes(bytes);
    }
}

impl Debug for Uuid {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        self.0.fmt(f)
    }
}

impl fmt::Display for Uuid {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        fmt::Display::fmt(&self.0, f)
    }
}

impl AsMut<uuid::Uuid> for Uuid {
    fn as_mut(&mut self) -> &mut uuid::Uuid {
        &mut self.0
    }
}

impl AsRef<uuid::Uuid> for Uuid {
    fn as_ref(&self) -> &uuid::Uuid {
        &self.0
    }
}

impl Deref for Uuid {
    type Target = uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Uuid {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<uuid::Uuid> for Uuid {
    fn from(value: uuid::Uuid) -> Self {
        Self(value)
    }
}

impl From<Uuid> for uuid::Uuid {
    fn from(value: Uuid) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Version;

    #[derive(Message, PartialEq)]
    struct Example {
        #[prost(message)]
        id: Option<Uuid>,

        #[prost(message)]
        version: Option<Version>,
    }

    #[test]
    fn uuid_should_roundtrip_with_protobuf() {
        // arrange
        const UUID_BUF: [u8; 16] = [
            0x0f, 0x10, 0x8c, 0x6e, 0xb2, 0xce, 0xaa, 0x4f, 0xb6, 0xbf, 0x32, 0x9b, 0xf3, 0x9f,
            0xa1, 0xe4,
        ];
        let expected_uuid: super::Uuid = uuid::Uuid::from_bytes(UUID_BUF).into();
        let encoded_uuid = expected_uuid.encode_to_vec();

        // act
        let decoded_uuid = Uuid::decode(&encoded_uuid[..]).unwrap();
        let actual_uuid = decoded_uuid.as_bytes();

        // assert
        assert_eq!(&UUID_BUF, actual_uuid);
    }

    #[test]
    fn fields_should_roundtrip_with_protobuf() {
        // arrange
        let expected = Example {
            id: Some(uuid::Uuid::new_v4().into()),
            version: Some(Version::new(42)),
        };

        // act
        let binary = expected.encode_to_vec();
        let actual = Example::decode(binary.as_slice()).unwrap();

        // assert
        assert_eq!(actual, expected);
    }
}
