use crate::{SqlVersion, SqlVersionPart::*};
use cqrs::{
    event::{Event, StoreError},
    message::Transcoder,
    Clock, Version,
};
use std::fmt::Debug;

/// Represents a stored row.
pub struct Row<ID> {
    /// Gets or sets the row identifier.
    pub id: ID,

    /// Gets or sets the event version.
    pub version: i32,

    /// Gets or sets the zero-based sequence in the event version.
    pub sequence: i16,

    /// Gets or sets the date and time the event was store as seconds from Unix epoch.
    pub stored_on: i64,

    /// Gets or sets the event type.
    pub kind: String,

    /// Gets or sets the event type version.
    pub revision: i16,

    /// Gets or sets the event content.
    pub content: Vec<u8>,

    /// Gets or sets the event correlation identifier, if any.
    pub correlation_id: Option<String>,
}

/// Defines the behavior to iterate events as rows.
pub trait IntoRows<'a, ID, M: ?Sized + Sync> {
    fn into_rows(
        self,
        id: ID,
        expected_version: Version,
        clock: &'a dyn Clock,
        transcoder: &'a Transcoder<M>,
    ) -> Iter<'a, ID, M>;
}

impl<'a, ID> IntoRows<'a, ID, dyn Event> for &'a [Box<dyn Event>]
where
    ID: Debug + Send,
{
    fn into_rows(
        self,
        id: ID,
        expected_version: Version,
        clock: &'a dyn Clock,
        transcoder: &'a Transcoder<dyn Event>,
    ) -> Iter<'a, ID, dyn Event> {
        Iter {
            messages: self,
            id,
            index: 0,
            version: expected_version.increment(Version),
            stored_on: crate::to_secs(clock.now()),
            transcoder,
        }
    }
}

/// Represents an iterator of [rows](Row) for the provided [events](Event).
pub struct Iter<'a, ID, M: ?Sized + Sync> {
    messages: &'a [Box<M>],
    index: usize,
    id: ID,
    version: Version,
    stored_on: i64,
    transcoder: &'a Transcoder<M>,
}

impl<'a, ID> Iterator for Iter<'a, ID, dyn Event>
where
    ID: Clone + Debug + Send,
{
    type Item = Result<Row<ID>, StoreError<ID>>;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.index;

        if i < self.messages.len() {
            let version = self.version;
            let event = &self.messages[i];
            let schema = event.schema();
            let content = match self.transcoder.encode(event.as_ref()) {
                Ok(content) => content,
                Err(error) => return Some(Err(StoreError::InvalidEncoding(error))),
            };

            self.index += 1;
            self.version = self.version.increment(Sequence);

            Some(Ok(Row::<ID> {
                id: self.id.clone(),
                version: version.number(),
                sequence: version.sequence(),
                stored_on: self.stored_on,
                kind: schema.kind().into(),
                revision: schema.version() as i16,
                content,
                correlation_id: event.correlation_id().map(Into::into),
            }))
        } else {
            None
        }
    }
}
