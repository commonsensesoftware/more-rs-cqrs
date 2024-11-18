use cqrs::Version;
use std::fmt::{Debug, Display, Formatter, Result as FormatResult, Write};

// VERSION ENCODING
//
// max bit = position of the highest set bit
// set bits = count of all set bits
// version = entity version number
// sequence = message sequence in a batch
//
// max and set bits are used as a naive integrity check to detect tampering.
// when combined with a secure Mask, it should be difficult to spoof.
//
// | 20-bits | 6-bits  | 6-bits   | 24-bits | 8-bits   |
// | ------- | ------- | -------- | ------- | -------- |
// | unused  | max bit | set bits | version | sequence |

#[inline]
fn max_bit(bits: u32) -> u8 {
    let mut mask = 0b100000000000000000000000;

    for max in (1..=24).rev() {
        if (bits & mask) != 0 {
            return max;
        }

        mask >>= 1;
    }

    0
}

fn encode(version: i32, sequence: i16) -> u64 {
    let version = version as u32;
    let value = version << 8 | sequence as u32;
    let set_bits = (value.count_ones() as u64) << 32;
    let max_bit = (max_bit(version) as u64) << 38;

    max_bit | set_bits | value as u64
}

#[inline]
pub fn new_version(version: i32, sequence: i16) -> Version {
    Version::new(encode(version, sequence))
}

#[derive(Copy, Clone, Debug)]
pub enum SqlVersionPart {
    /// Indicates the version part.
    Version,

    /// Indicates the sequence part.
    Sequence,
}

/// Defines the behavior of a [version](Version) used by SQL storage.
///
/// # Remarks
///
/// This trait is only intended to be used by SQL storage implementors.
pub trait SqlVersion: Sized {
    /// Gets the maximum [version](Version) allowed.
    fn max() -> Self;

    /// Gets the version number.
    fn number(&self) -> i32;

    /// Gets the sequence number.
    fn sequence(&self) -> i16;

    /// Increments the current version part by one.
    ///
    /// # Arguments
    ///
    /// * `part` - the [part](SqlVersionPart) to increment
    ///
    /// # Remarks
    ///
    /// Incrementing the [version](SqlVersionPart::Version) resets the sequence to `0`.
    fn increment(&self, part: SqlVersionPart) -> Self;

    /// Gets a value that can [display](Display) the encoded version.
    fn display(&self) -> SqlVersionDisplay;

    /// Gets a value indicating whether the encoded version is invalid.
    ///
    /// # Remarks
    ///
    /// A version is opaque to a consumer, but a version can be invalid because:
    ///
    /// 1. A user tampered with or tried to generate it
    /// 2. The value came from some other store
    fn invalid(&self) -> bool;
}

/// Represents the display for a SQL-encoded [version](Version).
#[derive(Debug)]
pub struct SqlVersionDisplay {
    version: i32,
    sequence: i16,
}

impl SqlVersion for Version {
    fn max() -> Self {
        Self::new((u32::MAX as u64) >> 8)
    }

    fn number(&self) -> i32 {
        ((u64::from(self) & 0x00000000_FFFFFF00) >> 8) as i32
    }

    fn sequence(&self) -> i16 {
        (u64::from(self) & 0x00000000_000000FF) as i16
    }

    fn increment(&self, part: SqlVersionPart) -> Self {
        match part {
            SqlVersionPart::Version => {
                Self::new(encode(self.number().saturating_add(1), 0))
            }
            SqlVersionPart::Sequence => {
                Self::new(encode(self.number(), self.sequence().saturating_add(1)))
            }
        }
    }

    fn display(&self) -> SqlVersionDisplay {
        SqlVersionDisplay {
            version: self.number(),
            sequence: self.sequence(),
        }
    }

    #[allow(clippy::unusual_byte_groupings)]
    fn invalid(&self) -> bool {
        let value = u64::from(self);

        if value == 0 {
            return false;
        }

        let unused = value & 0b11111111111111111111_000000_000000_000000000000000000000000_00000000;

        if unused != 0 {
            return true;
        }

        let max = ((value & 0b00000000000000000000_111111_000000_000000000000000000000000_00000000)
            >> 38) as u8;
        let set = ((value & 0b00000000000000000000_000000_111111_000000000000000000000000_00000000)
            >> 32) as u8;
        let bits =
            (value & 0b00000000000000000000_000000_000000_111111111111111111111111_11111111) as u32;

        max != max_bit(bits >> 8) || set != (bits.count_ones() as u8)
    }
}

impl Display for SqlVersionDisplay {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        Display::fmt(&self.version, f)?;
        f.write_char('.')?;
        Display::fmt(&self.sequence, f)
    }
}

#[cfg(test)]
mod test {
    use super::SqlVersionPart::*;
    use super::*;
    use cqrs::Version;
    use rstest::rstest;

    #[test]
    fn increment_should_increase_version() {
        // arrange
        let previous = Version::default();

        // act
        let version = previous.increment(Version);

        // assert
        assert_eq!(version.number(), 1);
    }

    #[test]
    fn add_should_increment_sequence_number() {
        // arrange
        let mut version = Version::default().increment(Version);
        let mut versions = Vec::new();

        // act
        for _ in 0..3 {
            versions.push(version);
            version = version.increment(Sequence);
        }

        // assert
        assert_eq!(
            versions,
            vec![new_version(1, 0), new_version(1, 1), new_version(1, 2)]
        );
    }

    #[rstest]
    #[case(42u64)]
    #[case((42u64 << 8) | 2u64)]
    #[case(0b00000000000000000000_000001_100000_000000000000000000000010_00000000)]
    fn encoding_should_be_invalid_due_to_tampering(#[case] value: u64) {
        // arrange
        let version = Version::from(value);

        // act
        let invalid = version.invalid();

        // assert
        assert!(invalid);
    }

    #[rstest]
    #[case((6u64 << 38) | (3u64 << 32) | (42u64 << 8))]
    #[case((6u64 << 38) | (4u64 << 32) | (42u64 << 8) | 2u64)]
    #[case(0b00000000000000000000_000110_000011_000000000000000000101010_00000000)]
    fn encoding_should_be_valid(#[case] value: u64) {
        // arrange
        let version = Version::from(value);

        // act
        let valid = !version.invalid();

        // assert
        assert!(valid);
    }
}
