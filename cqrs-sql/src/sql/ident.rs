use std::borrow::Cow;
use std::fmt::Debug;

const UNDERSCORE: char = '_';

/// Represents the pair of delimiters which enclose a quoted SQL identifier.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Delimiters {
    open: char,
    close: char,
}

impl Delimiters {
    /// The ANSI SQL delimiters, which enclose an identifier in double quotes.
    ///
    /// # Remarks
    ///
    /// Used by PostgreSQL and SQLite. SQL Server also accepts these, but only while
    /// `QUOTED_IDENTIFIER` is `ON`, so it prefers [`Delimiters::BRACKET`].
    pub const ANSI: Self = Self::new('"', '"');

    /// The MySQL delimiters, which enclose an identifier in backticks.
    ///
    /// # Remarks
    ///
    /// MySQL only treats a double quote as an identifier delimiter when `ANSI_QUOTES` is
    /// among its `sql_mode` flags, which is not the default and cannot be assumed.
    pub const BACKTICK: Self = Self::new('`', '`');

    /// The T-SQL delimiters, which enclose an identifier in square brackets.
    pub const BRACKET: Self = Self::new('[', ']');

    /// Initializes new [delimiters](Delimiters).
    ///
    /// # Arguments
    ///
    /// * `open` - the character which opens a quoted identifier
    /// * `close` - the character which closes a quoted identifier
    pub const fn new(open: char, close: char) -> Self {
        Self { open, close }
    }

    /// Gets the character which opens a quoted identifier.
    pub const fn open(&self) -> char {
        self.open
    }

    /// Gets the character which closes a quoted identifier.
    pub const fn close(&self) -> char {
        self.close
    }
}

impl Default for Delimiters {
    fn default() -> Self {
        Self::ANSI
    }
}

/// Appends a quoted identifier, escaping any closing delimiter by doubling it.
fn push_quoted(text: &str, delimiters: Delimiters, buffer: &mut String) {
    buffer.push(delimiters.open());

    for ch in text.chars() {
        if ch == delimiters.close() {
            buffer.push(ch);
        }

        buffer.push(ch);
    }

    buffer.push(delimiters.close());
}

#[inline]
fn all_allowed(text: &str) -> bool {
    text.chars().all(|c| c.is_ascii_alphanumeric() || c == UNDERSCORE)
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
pub struct Ident<'a>(Option<Cow<'a, str>>, Cow<'a, str>);

impl<'a> Ident<'a> {
    /// Creates and returns a new unqualified identifier.
    ///
    /// # Arguments
    ///
    /// * `name` - the object name
    pub fn unqualified<S: Into<Cow<'a, str>>>(name: S) -> Self {
        Self(None, name.into())
    }

    /// Creates and returns a new qualified identifier.
    ///
    /// # Arguments
    ///
    /// * `schema` - the schema name
    /// * `name` - the object name
    pub fn qualified<S1: Into<Cow<'a, str>>, S2: Into<Cow<'a, str>>>(schema: S1, name: S2) -> Self {
        Self(Some(schema.into()), name.into())
    }

    /// Gets the associated schema name, if any.
    pub fn schema(&self) -> Option<&str> {
        self.0.as_deref()
    }

    /// Gets the object identifier name.
    pub fn name(&self) -> &str {
        &self.1
    }

    /// Returns the full identifier name, including ANSI quotes if necessary.
    ///
    /// # Remarks
    ///
    /// Prefer [`Provider::quote`](crate::sql::Provider::quote), which applies the
    /// [delimiters](Delimiters) of the target database.
    #[inline]
    pub fn quote(&self) -> Cow<'_, str> {
        self.quote_with(Delimiters::ANSI)
    }

    /// Returns the full identifier name, including quotes if necessary.
    ///
    /// # Arguments
    ///
    /// * `delimiters` - the [delimiters](Delimiters) which enclose a quoted identifier
    #[inline]
    pub fn quote_with(&self, delimiters: Delimiters) -> Cow<'_, str> {
        self._quote(None, delimiters)
    }

    /// Returns an identifier part, including ANSI quotes if necessary.
    ///
    /// # Arguments
    ///
    /// * `part` - the [part](IdentPart) to quote
    #[inline]
    pub fn quote_part(&self, part: IdentPart) -> Option<Cow<'_, str>> {
        self.quote_part_with(part, Delimiters::ANSI)
    }

    /// Returns an identifier part, including quotes if necessary.
    ///
    /// # Arguments
    ///
    /// * `part` - the [part](IdentPart) to quote
    /// * `delimiters` - the [delimiters](Delimiters) which enclose a quoted identifier
    #[inline]
    pub fn quote_part_with(&self, part: IdentPart, delimiters: Delimiters) -> Option<Cow<'_, str>> {
        if part == IdentPart::Schema && self.0.is_none() {
            None
        } else {
            Some(self._quote(Some(part), delimiters))
        }
    }

    fn _quote(&self, part: Option<IdentPart>, delimiters: Delimiters) -> Cow<'_, str> {
        let mut quoted = String::new();
        let full = part.is_none();

        if (full || part == Some(IdentPart::Schema))
            && let Some(schema) = self.schema()
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
                push_quoted(schema, delimiters, &mut quoted);
            }
        }

        if (full || part == Some(IdentPart::Object)) && !self.1.is_empty() {
            if quoted.is_empty() {
                if all_allowed(&self.1) {
                    return Cow::Borrowed(&self.1);
                }
            } else {
                quoted.push('.');
            }

            push_quoted(&self.1, delimiters, &mut quoted);
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
    pub fn as_object_name(&self) -> Cow<'_, str> {
        self._as_object_name(None)
    }

    /// Returns part of the identifier as an object name.
    ///
    /// # Remarks
    ///
    /// Returns part of the identifier as an object name for use in prefixes and suffixes of other database
    /// objects, such as indexes. The [IdentPart::Object] `"my-events"` is returned as `my_events`.
    #[inline]
    pub fn part_as_object_name(&self, part: IdentPart) -> Cow<'_, str> {
        self._as_object_name(Some(part))
    }

    fn _as_object_name(&self, part: Option<IdentPart>) -> Cow<'_, str> {
        if let Some(part) = part {
            match part {
                IdentPart::Object => {
                    if all_allowed(&self.1) {
                        Cow::Borrowed(&self.1)
                    } else {
                        Cow::Owned(escape(&self.1))
                    }
                }
                IdentPart::Schema => {
                    if let Some(schema) = self.schema() {
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
        } else if let Some(schema) = self.schema() {
            let mut name = String::with_capacity(schema.len() + self.1.len() + 1);

            escape_into(schema, &mut name);

            if !name.is_empty() {
                name.push(UNDERSCORE);
            }

            escape_into(&self.1, &mut name);
            Cow::Owned(name)
        } else if all_allowed(&self.1) {
            Cow::Borrowed(&self.1)
        } else {
            Cow::Owned(escape(&self.1))
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
    fn identifier_should_be_escaped(#[case] schema: Option<&str>, #[case] table: &str, #[case] expected: &str) {
        // arrange
        let ident = Ident(schema.map(Cow::Borrowed), Cow::Borrowed(table));

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
        let ident = Ident(schema.map(Cow::Borrowed), Cow::Borrowed("My Table"));

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
        let ident = Ident(schema.map(Cow::Borrowed), Cow::Borrowed(object));

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
        let ident = Ident(schema.map(Cow::Borrowed), Cow::Borrowed(object));

        // act
        let name = ident.part_as_object_name(part);

        // assert
        assert_eq!(&name, expected)
    }

    #[rstest]
    #[case(Delimiters::ANSI, "\"dbo\".\"My Table\"")]
    #[case(Delimiters::BACKTICK, "`dbo`.`My Table`")]
    #[case(Delimiters::BRACKET, "[dbo].[My Table]")]
    fn identifier_should_quote_with_delimiters(#[case] delimiters: Delimiters, #[case] expected: &str) {
        // arrange
        let ident = Ident::qualified("dbo", "My Table");

        // act
        let name = ident.quote_with(delimiters);

        // assert
        assert_eq!(&name, expected)
    }

    #[rstest]
    #[case(Delimiters::ANSI, "my\"table", "\"my\"\"table\"")]
    #[case(Delimiters::BACKTICK, "my`table", "`my``table`")]
    #[case(Delimiters::BRACKET, "my]table", "[my]]table]")]
    fn identifier_should_escape_closing_delimiter(
        #[case] delimiters: Delimiters,
        #[case] object: &str,
        #[case] expected: &str,
    ) {
        // arrange
        let ident = Ident::unqualified(object);

        // act
        let name = ident.quote_with(delimiters);

        // assert
        assert_eq!(&name, expected)
    }
}
