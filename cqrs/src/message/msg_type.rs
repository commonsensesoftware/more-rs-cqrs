use super::Schema;
use std::num::NonZeroU8;

/// Represents a message type, which optionally applies to a specific revision.
///
/// # Remarks
///
/// A [schema](Schema) identifies exactly one revision of a message. A type identifies a message across one or every
/// revision and is used to filter the messages a query applies to. Matching a type against a schema is deliberately
/// not equality; a type that applies to every revision matches all of them, which is not transitive and, therefore,
/// cannot be an equivalence relation.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Type {
    kind: String,
    revision: Option<NonZeroU8>,
}

impl Type {
    /// Initializes a new [Type] for a specific revision.
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
            revision: Some(revision),
        }
    }

    /// Initializes a new [Type] for the specified message version.
    ///
    /// # Arguments
    ///
    /// * `kind` - the message type
    ///
    /// # Remarks
    ///
    /// The declared `VERSION` is matched against the schema [revision](Schema::revision). A `VERSION` of `0` fails to
    /// compile. Use [Self::any] to apply a type to every revision.
    pub fn version<const VERSION: u8>(kind: impl AsRef<str>) -> Self {
        Self::new(
            kind,
            const { NonZeroU8::new(VERSION).expect("a message version must be greater than 0") },
        )
    }

    /// Initializes a new [Type] that applies to every revision.
    ///
    /// # Arguments
    ///
    /// * `kind` - the message type
    pub fn any<K: AsRef<str>>(kind: K) -> Self {
        Self {
            kind: kind.as_ref().into(),
            revision: None,
        }
    }

    /// Gets the message type.
    #[inline]
    pub const fn kind(&self) -> &str {
        self.kind.as_str()
    }

    /// Gets the message revision the type applies to, if any.
    ///
    /// # Remarks
    ///
    /// [None] indicates the type applies to every revision.
    #[inline]
    pub const fn revision(&self) -> Option<NonZeroU8> {
        self.revision
    }

    /// Gets a value indicating whether the type matches the specified [schema](Schema).
    ///
    /// # Arguments
    ///
    /// * `schema` - the [schema](Schema) to match against
    #[inline]
    pub fn matches(&self, schema: &Schema) -> bool {
        self.kind == schema.kind() && self.revision.is_none_or(|revision| revision == schema.revision())
    }
}

impl From<Schema> for Type {
    fn from(value: Schema) -> Self {
        Self::new(value.kind(), value.revision())
    }
}

impl From<&Schema> for Type {
    fn from(value: &Schema) -> Self {
        Self::new(value.kind(), value.revision())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_should_match_only_its_revision() {
        // arrange
        let type_ = Type::version::<1>("urn:test:example");

        // act
        let matches = type_.matches(&Schema::version::<1>("urn:test:example"));

        // assert
        assert!(matches);
        assert!(!type_.matches(&Schema::version::<2>("urn:test:example")));
        assert!(!type_.matches(&Schema::version::<1>("urn:test:other")));
    }

    #[test]
    fn any_type_should_match_every_revision() {
        // arrange
        let type_ = Type::any("urn:test:example");

        // act
        let matches = (1..=u8::MAX)
            .filter_map(NonZeroU8::new)
            .all(|revision| type_.matches(&Schema::new("urn:test:example", revision)));

        // assert
        assert!(matches);
        assert!(type_.revision().is_none());
    }

    #[test]
    fn any_type_should_not_match_another_kind() {
        // arrange
        let type_ = Type::any("urn:test:example");

        // act
        let matches = type_.matches(&Schema::version::<1>("urn:test:other"));

        // assert
        assert!(!matches);
    }

    #[test]
    fn type_should_convert_from_schema() {
        // arrange
        let schema = Schema::version::<2>("urn:test:example");

        // act
        let type_: Type = schema.clone().into();

        // assert
        assert_eq!(type_, Type::version::<2>("urn:test:example"));
        assert!(type_.matches(&schema));
    }
}
