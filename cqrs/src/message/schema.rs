use std::num::NonZeroU8;

/// Represents a message schema.
///
/// # Remarks
///
/// A schema is a value; two schemas are equal only when their [kind](Self::kind) and [revision](Self::revision) are
/// both equal. A schema always identifies exactly one revision of a message. Use a message [type](super::Type) to
/// express a filter that applies to any revision.
///
/// A message version is declared, such as by the `version` argument of the `#[event]` attribute, and is recorded as
/// the schema [revision](Self::revision). The version of a message is unrelated to the [version](crate::Version) of
/// an aggregate, which is why the two are named differently.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Schema {
    kind: String,
    revision: NonZeroU8,
}

impl Schema {
    /// Initializes a new [Schema].
    ///
    /// # Arguments
    ///
    /// * `kind` - the message type
    /// * `revision` - the message revision
    ///
    /// # Remarks
    ///
    /// Prefer [Self::version] when the message version is known at compile time.
    pub fn new<K: AsRef<str>>(kind: K, revision: NonZeroU8) -> Self {
        Self {
            kind: kind.as_ref().into(),
            revision,
        }
    }

    /// Initializes a new [Schema] for the specified message version.
    ///
    /// # Arguments
    ///
    /// * `kind` - the message type
    ///
    /// # Remarks
    ///
    /// The declared `VERSION` is recorded as the schema [revision](Self::revision). A `VERSION` of `0` fails to
    /// compile.
    pub fn version<const VERSION: u8>(kind: impl AsRef<str>) -> Self {
        Self::new(
            kind,
            const { NonZeroU8::new(VERSION).expect("a message version must be greater than 0") },
        )
    }

    /// Gets the schema type.
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Gets the schema revision.
    ///
    /// # Remarks
    ///
    /// The revision is the declared message version. The default value is `1`.
    pub fn revision(&self) -> NonZeroU8 {
        self.revision
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn hash(schema: &Schema) -> u64 {
        let mut hasher = DefaultHasher::new();

        schema.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn schema_revision_should_default_to_1() {
        // arrange

        // act
        let schema = Schema::version::<1>("urn:test:example");

        // assert
        assert_eq!(schema.revision().get(), 1);
    }

    #[test]
    fn schema_should_record_declared_version_as_revision() {
        // arrange

        // act
        let schema = Schema::version::<2>("urn:test:example");

        // assert
        assert_eq!(schema.revision().get(), 2);
        assert_eq!(schema, Schema::new("urn:test:example", NonZeroU8::new(2).unwrap()));
    }

    #[test]
    fn schemas_should_be_equal() {
        // arrange
        let schema = Schema::version::<1>("urn:test:example");
        let other = Schema::version::<1>("urn:test:example");

        // act
        let equal = schema == other;

        // assert
        assert!(equal);
        assert_eq!(schema.partial_cmp(&other), Some(Ordering::Equal));
    }

    #[test]
    fn schemas_of_different_kinds_should_be_not_equal() {
        // arrange
        let schema = Schema::version::<1>("urn:test:example:1");
        let other = Schema::version::<1>("urn:test:example:2");

        // act
        let not_equal = schema != other;

        // assert
        assert!(not_equal);
    }

    #[test]
    fn schemas_of_different_versions_should_be_not_equal() {
        // arrange
        let schema = Schema::version::<1>("urn:test:example");
        let other = Schema::version::<2>("urn:test:example");

        // act
        let not_equal = schema != other;

        // assert
        assert!(not_equal);
    }

    #[test]
    fn schema_should_be_ordered_by_kind_then_version() {
        // arrange
        let mut schemas = vec![
            Schema::version::<2>("urn:test:example"),
            Schema::version::<1>("urn:test:other"),
            Schema::version::<1>("urn:test:example"),
        ];

        // act
        schemas.sort();

        // assert
        assert_eq!(
            schemas,
            vec![
                Schema::version::<1>("urn:test:example"),
                Schema::version::<2>("urn:test:example"),
                Schema::version::<1>("urn:test:other"),
            ]
        );
    }

    #[test]
    fn equal_schemas_should_have_equal_hashes() {
        // arrange
        let schemas = [
            Schema::version::<1>("urn:test:example"),
            Schema::version::<2>("urn:test:example"),
            Schema::version::<1>("urn:test:other"),
        ];

        // act, assert
        for schema in &schemas {
            for other in &schemas {
                if schema == other {
                    assert_eq!(hash(schema), hash(other), "{schema:?} == {other:?}, but hashes differ");
                }
            }
        }
    }

    #[test]
    fn schema_equality_should_be_transitive() {
        // arrange
        let schemas = [
            Schema::version::<1>("urn:test:example"),
            Schema::version::<2>("urn:test:example"),
            Schema::version::<1>("urn:test:other"),
        ];

        // act, assert
        for first in &schemas {
            for second in &schemas {
                for third in &schemas {
                    if first == second && second == third {
                        assert_eq!(
                            first, third,
                            "{first:?} == {second:?} == {third:?}, but the first and last differ"
                        );
                    }
                }
            }
        }
    }
}
