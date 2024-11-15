use crate::Mask;
use cfg_if::cfg_if;
use std::{array::TryFromSliceError, fmt::{self, Debug, Formatter, Result as FormatResult}};

/// Represents an entity version.
///
/// # Remarks
///
/// A version is opaque to consumers. The internal representation should be considered encoded binary.
#[repr(transparent)]
#[cfg_attr(
    any(feature = "cbor", feature = "json", feature = "message-pack"),
    derive(serde::Deserialize, serde::Serialize),
    serde(transparent)
)]
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version(u64);

impl Debug for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        self.0.fmt(f)
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        fmt::Display::fmt(&self.0, f)
    }
}

impl Version {
    /// Initializes a new [`Version`].
    ///
    /// # Arguments
    ///
    /// * `value` - the implementation-specific, encoded value
    #[inline(always)]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Obfuscates the version using the provided mask.
    ///
    /// # Arguments
    ///
    /// * `mask` - the [mask](Mask) used to obfuscate the [version](Version)
    pub fn mask<M: AsRef<dyn Mask>>(&self, mask: M) -> Self {
        Self::from(mask.as_ref().mask(self.into()))
    }

    /// Deobfuscates the version using the provided mask.
    ///
    /// # Arguments
    ///
    /// * `mask` - the [mask](Mask) used to deobfuscate the [version](Version)
    pub fn unmask<M: AsRef<dyn Mask>>(&self, mask: M) -> Self {
        Self::from(mask.as_ref().unmask(self.into()))
    }
}

impl From<u64> for Version {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<[u8; 8]> for Version {
    fn from(value: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(value))
    }
}

impl From<Version> for u64 {
    fn from(value: Version) -> Self {
        value.0
    }
}

impl From<&Version> for u64 {
    fn from(value: &Version) -> Self {
        value.0
    }
}

impl From<Version> for [u8; 8] {
    fn from(value: Version) -> Self {
        value.0.to_be_bytes()
    }
}

impl From<&Version> for [u8; 8] {
    fn from(value: &Version) -> Self {
        value.0.to_be_bytes()
    }
}

impl TryFrom<&[u8]> for Version {
    type Error = TryFromSliceError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(<Self as From<[u8; 8]>>::from(value.try_into()?))
    }
}

impl TryFrom<&Vec<u8>> for Version {
    type Error = TryFromSliceError;

    fn try_from(value: &Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(value.as_slice())
    }
}

impl TryFrom<Vec<u8>> for Version {
    type Error = TryFromSliceError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(value.as_slice())
    }
}

cfg_if! {
    if #[cfg(feature = "protobuf")] {
        use prost::{
            bytes::{Buf, BufMut},
            encoding::{key_len, check_wire_type, encode_key, encoded_len_varint, skip_field, DecodeContext, WireType},
            DecodeError, Message,
        };

        impl Message for Version {
            fn encode_raw(&self, buf: &mut impl BufMut) {
                encode_key(1, WireType::LengthDelimited, buf);
                buf.put_u8(8);
                buf.put_u64(self.0);
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

                    if buf.remaining() < 9 {
                        return Err(DecodeError::new("buffer underflow"));
                    }

                    if buf.get_u8() != 8 {
                        return Err(DecodeError::new("invalid version length"));
                    }

                    self.0 = buf.get_u64();

                    Ok(())
                } else {
                    skip_field(wire_type, tag, buf, ctx)
                }
            }

            #[inline]
            fn encoded_len(&self) -> usize {
                key_len(1) + encoded_len_varint(8u64) + 8
            }

            fn clear(&mut self) {
                self.0 = 0;
            }
        }
    }
}
