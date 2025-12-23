use std::borrow::Cow;
use std::fmt::Debug;

const DBL_QUOTE: char = '"';
const UNDERSCORE: char = '_';

#[inline]
fn all_allowed(text: &str) -> bool {
    text.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == UNDERSCORE)
}

fn escape(text: &str) -> String {
    let mut buffer = String::with_capacity(text.len() + 1);
    escape_into(text, &mut buffer);
    buffer
}

fn escape_into(text: &str, buffer: &mut String) {
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() {
            buffer.push(ch);
        } else {
            buffer.push(UNDERSCORE);
        }
    }
}

/// Represents the defined identifier parts.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum IdentPart {
    /// Indicates the schema name.
    Schema,

    /// Indicates the object name.
    Object,
}

/// Represents a SQL identifier.
#[derive(Clone, PartialEq, Eq)]
pub struct Ident<'a>(Option<&'a str>, &'a str);

impl<'a> Ident<'a> {
    /// Creates and returns a new unqualified identifier.
    ///
    /// # Arguments
    ///
    /// * `name` - the object name
    pub fn unqualified<S: ?Sized + AsRef<str> + 'a>(name: &'a S) -> Self {
        Self(None, name.as_ref())
    }

    /// Creates and returns a new qualified identifier.
    ///
    /// # Arguments
    ///
    /// * `schema` - the schema name
    /// * `name` - the object name
    pub fn qualified<S: ?Sized + AsRef<str> + 'a>(schema: &'a S, name: &'a S) -> Self {
        Self(Some(schema.as_ref()), name.as_ref())
    }

    /// Gets the associated schema name, if any.
    pub fn schema(&self) -> Option<&str> {
        self.0
    }

    /// Gets the object identifier name.
    pub fn name(&self) -> &str {
        self.1
    }

    /// Returns the full identifier name, including quotes if necessary.
    #[inline]
    pub fn quote(&self) -> Cow<'a, str> {
        self._quote(None)
    }

    /// Returns an identifier part, including quotes if necessary.
    ///
    /// # Arguments
    ///
    /// * `part` - the [part](IdentPart) to quote
    #[inline]
    pub fn quote_part(&self, part: IdentPart) -> Option<Cow<'a, str>> {
        if part == IdentPart::Schema && self.0.is_none() {
            None
        } else {
            Some(self._quote(Some(part)))
        }
    }

    fn _quote(&self, part: Option<IdentPart>) -> Cow<'a, str> {
        let mut quoted = String::new();
        let full = part.is_none();

        if (full || part == Some(IdentPart::Schema))
            && let Some(schema) = self.0
        {
            if all_allowed(schema) {
                if full {
                    if self.1.is_empty() {
                        return Cow::Borrowed(schema);
                    }
                } else {
                    return Cow::Borrowed(schema);
                }
            }

            if !schema.is_empty() {
                quoted.push(DBL_QUOTE);
                quoted.push_str(schema);
                quoted.push(DBL_QUOTE);
            }
        }

        if (full || part == Some(IdentPart::Object)) && !self.1.is_empty() {
            if quoted.is_empty() {
                if all_allowed(self.1) {
                    return Cow::Borrowed(self.1);
                }
            } else {
                quoted.push('.');
            }

            quoted.push(DBL_QUOTE);
            quoted.push_str(self.1);
            quoted.push(DBL_QUOTE);
        }

        Cow::Owned(quoted)
    }

    /// Returns the identifier as an object name.
    ///
    /// # Remarks
    ///
    /// Returns the identifier as an object name for use in prefixes and suffixes of other database
    /// objects, such as indexes. The name `"events"."my-events"` is returned as `events_my_events`.
    #[inline]
    pub fn as_object_name(&self) -> Cow<'a, str> {
        self._as_object_name(None)
    }

    /// Returns part of the identifier as an object name.
    ///
    /// # Remarks
    ///
    /// Returns part of the identifier as an object name for use in prefixes and suffixes of other database
    /// objects, such as indexes. The [IdentPart::Object] `"my-events"` is returned as `my_events`.
    #[inline]
    pub fn part_as_object_name(&self, part: IdentPart) -> Cow<'a, str> {
        self._as_object_name(Some(part))
    }

    fn _as_object_name(&self, part: Option<IdentPart>) -> Cow<'a, str> {
        if let Some(part) = part {
            match part {
                IdentPart::Object => {
                    if all_allowed(self.1) {
                        Cow::Borrowed(self.1)
                    } else {
                        Cow::Owned(escape(self.1))
                    }
                }
                IdentPart::Schema => {
                    if let Some(schema) = self.0 {
                        if all_allowed(schema) {
                            Cow::Borrowed(schema)
                        } else {
                            Cow::Owned(escape(schema))
                        }
                    } else {
                        Cow::Owned(String::new())
                    }
                }
            }
        } else if let Some(schema) = self.0 {
            let mut name = String::with_capacity(schema.len() + self.1.len() + 1);

            escape_into(schema, &mut name);

            if !name.is_empty() {
                name.push(UNDERSCORE);
            }

            escape_into(self.1, &mut name);
            Cow::Owned(name)
        } else if all_allowed(self.1) {
            Cow::Borrowed(self.1)
        } else {
            Cow::Owned(escape(self.1))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(Some("dbo"), "Table", "\"dbo\".\"Table\"")]
    #[case(Some("main"), "My Table", "\"main\".\"My Table\"")]
    #[case(None, "Table", "Table")]
    #[case(None, "My Table", "\"My Table\"")]
    fn identifier_should_be_escaped(
        #[case] schema: Option<&str>,
        #[case] table: &str,
        #[case] expected: &str,
    ) {
        // arrange
        let ident = Ident(schema, table);

        // act
        let name = ident.quote();

        // assert
        assert_eq!(&name, expected)
    }

    #[rstest]
    #[case(Some("dbo"), IdentPart::Schema, Some("dbo"))]
    #[case(Some("dbo"), IdentPart::Object, Some("\"My Table\""))]
    #[case(None, IdentPart::Schema, None)]
    #[case(None, IdentPart::Object, Some("\"My Table\""))]
    fn identifier_should_escape_part(
        #[case] schema: Option<&str>,
        #[case] part: IdentPart,
        #[case] expected: Option<&str>,
    ) {
        // arrange
        let ident = Ident(schema, "My Table");

        // act
        let name = ident.quote_part(part);

        // assert
        assert_eq!(name.as_deref(), expected)
    }

    #[rstest]
    #[case(Some("dbo"), "Table", "dbo_Table")]
    #[case(None, "Table", "Table")]
    #[case(Some("dbo"), "My Table", "dbo_My_Table")]
    #[case(None, "My Table", "My_Table")]
    fn identifier_should_return_object_name(
        #[case] schema: Option<&str>,
        #[case] object: &str,
        #[case] expected: &str,
    ) {
        // arrange
        let ident = Ident(schema, object);

        // act
        let name = ident.as_object_name();

        // assert
        assert_eq!(&name, expected)
    }

    #[rstest]
    #[case(Some("dbo"), "Table", IdentPart::Schema, "dbo")]
    #[case(Some("dbo"), "My Table", IdentPart::Schema, "dbo")]
    #[case(None, "Table", IdentPart::Schema, "")]
    #[case(None, "Table", IdentPart::Object, "Table")]
    #[case(None, "My Table", IdentPart::Object, "My_Table")]
    fn identifier_should_return_part_object_name(
        #[case] schema: Option<&str>,
        #[case] object: &str,
        #[case] part: IdentPart,
        #[case] expected: &str,
    ) {
        // arrange
        let ident = Ident(schema, object);

        // act
        let name = ident.part_as_object_name(part);

        // assert
        assert_eq!(&name, expected)
    }
}
