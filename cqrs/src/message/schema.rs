use std::cmp::{Ordering, PartialOrd};
use std::hash::{Hash, Hasher};

const ANY: u8 = 0;

/// Represents a message schema.
#[derive(Clone, Debug, Eq)]
pub struct Schema {
    kind: String,
    version: u8,
}

impl Schema {
    /// Initializes a new [Schema].
    ///
    /// # Arguments
    ///
    /// * `kind` - the message type
    /// * `version` - the message version
    pub fn new<K: AsRef<str>>(kind: K, version: u8) -> Self {
        Self {
            kind: kind.as_ref().into(),
            version,
        }
    }

    /// Initializes a new [Schema].
    ///
    /// # Arguments
    ///
    /// * `kind` - the message type
    ///
    /// # Remarks
    ///
    /// The initial [Self::version] is `1`.
    #[inline]
    pub fn initial<K: AsRef<str>>(kind: K) -> Self {
        Self::new(kind, 1)
    }

    /// Initializes a new [Schema].
    ///
    /// # Arguments
    ///
    /// * `kind` - the message type
    ///
    /// # Remarks
    ///
    /// A schema without a version is only meant to be used in a scenario where a specific version of a schema
    /// is not required or is undesirable, such as querying all versions of a specific message type from storage.
    #[inline]
    pub fn versionless<K: AsRef<str>>(kind: K) -> Self {
        Self::new(kind, ANY)
    }

    /// Gets the schema type.
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Gets the schema version.
    ///
    /// # Remarks
    ///
    /// The default value is `1`.
    pub fn version(&self) -> u8 {
        self.version
    }
}

impl Hash for Schema {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.version.hash(state);
    }
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
            && (self.version == other.version || self.version == ANY || other.version == ANY)
    }
}

impl PartialOrd for Schema {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if let Some(Ordering::Equal) = self.kind.partial_cmp(&other.kind) {
            if self.version == other.version {
                Some(Ordering::Equal)
            } else if self.version == ANY {
                Some(Ordering::Less)
            } else if other.version == ANY {
                Some(Ordering::Greater)
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versionless_schema_version_should_be_0() {
        // arrange

        // act
        let schema = Schema::versionless("urn:test:example");

        // assert
        assert_eq!(schema.version(), 0);
    }

    #[test]
    fn initial_schema_version_should_be_1() {
        // arrange

        // act
        let schema = Schema::initial("urn:test:example");

        // assert
        assert_eq!(schema.version(), 1);
    }

    #[test]
    fn schemas_should_be_equal() {
        // arrange
        let schema = Schema::initial("urn:test:example");
        let other = Schema::initial("urn:test:example");

        // act
        let equal = schema == other;

        // assert
        assert!(equal);
        assert_eq!(schema.partial_cmp(&other), Some(Ordering::Equal));
    }

    #[test]
    fn schemas_of_different_kinds_should_be_not_equal() {
        // arrange
        let schema = Schema::initial("urn:test:example:1");
        let other = Schema::initial("urn:test:example:2");

        // act
        let not_equal = schema != other;

        // assert
        assert!(not_equal);
    }

    #[test]
    fn schemas_of_different_versions_should_be_not_equal() {
        // arrange
        let schema = Schema::new("urn:test:example", 1);
        let other = Schema::new("urn:test:example", 2);

        // act
        let not_equal = schema != other;

        // assert
        assert!(not_equal);
    }

    #[test]
    fn versionless_schema_should_be_less_than_versioned_schema() {
        // arrange
        let schema = Schema::versionless("urn:test:example");
        let other = Schema::initial("urn:test:example");

        // act
        let less_than = schema < other;

        // assert
        assert!(less_than);
    }

    #[test]
    fn versioned_schema_should_be_greater_than_versionless_schema() {
        // arrange
        let schema = Schema::initial("urn:test:example");
        let other = Schema::versionless("urn:test:example");

        // act
        let greater_than = schema > other;

        // assert
        assert!(greater_than);
    }
}
