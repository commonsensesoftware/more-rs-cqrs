use cqrs::Version;
use std::fmt::{Debug, Display, Formatter, Result as FormatResult, Write};

pub fn new_version(version: i32, sequence: i16) -> Version {
    Version::new((version as u64) << 8 | (sequence as u64))
}

// version encoding
//
// | 32-bits | 24-bits | 8-bits   |
// | ------- | ------- | -------- |
// | unused  | version | sequence |

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
}

/// Represents the display for a SQL-encoded [version](Version).
#[derive(Debug)]
pub struct SqlVersionDisplay {
    version: i32,
    sequence: i16,
}

impl SqlVersion for Version {
    fn max() -> Self {
        Self::new(((u16::MAX as u64) << 24) | u8::MAX as u64)
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
                Self::new(((self.number() as u16).saturating_add(1) << 8) as u64)
            }
            SqlVersionPart::Sequence => Self::new(
                ((self.number() as u64) << 8) | (self.sequence() as u8).saturating_add(1) as u64,
            ),
        }
    }

    fn display(&self) -> SqlVersionDisplay {
        SqlVersionDisplay {
            version: self.number(),
            sequence: self.sequence(),
        }
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

    #[inline]
    fn ver(ver: u16, seq: u8) -> Version {
        Version::new((ver as u64) << 8 | seq as u64)
    }

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
            vec![ver(1, 0), ver(1, 1), ver(1, 2)]
        );
    }
}
