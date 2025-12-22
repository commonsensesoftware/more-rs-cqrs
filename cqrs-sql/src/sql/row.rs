use crate::{SqlVersion, SqlVersionPart::Sequence};
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

impl<ID: Clone> Row<ID> {
    pub fn previous(&self) -> Option<Self> {
        if self.version < 2 {
            None
        } else {
            Some(Self {
                id: self.id.clone(),
                version: self.version - 1,
                sequence: 0,
                stored_on: self.stored_on,
                kind: self.kind.clone(),
                revision: self.revision,
                content: Default::default(),
                correlation_id: None,
            })
        }
    }
}

/// Represents the context used while transforming events into rows.
pub struct Context<'a, ID, M: ?Sized + Sync> {
    pub id: ID,
    pub version: Version,
    pub clock: &'a dyn Clock,
    pub transcoder: &'a Transcoder<M>,
}

/// Defines the behavior to iterate events as rows.
pub trait IntoRows<'a, ID, M: ?Sized + Sync> {
    fn into_rows(self, context: Context<'a, ID, M>) -> Iter<'a, ID, M>;
}

impl<'a, ID> IntoRows<'a, ID, dyn Event> for &'a [Box<dyn Event>]
where
    ID: Debug + Send,
{
    fn into_rows(self, context: Context<'a, ID, dyn Event>) -> Iter<'a, ID, dyn Event> {
        let version = context.version;

        Iter {
            messages: self,
            index: 0,
            stored_on: crate::to_secs(context.clock.now()),
            context,
            version,
        }
    }
}

/// Represents an iterator of [rows](Row) for the provided [events](Event).
pub struct Iter<'a, ID, M: ?Sized + Sync> {
    messages: &'a [Box<M>],
    index: usize,
    stored_on: i64,
    context: Context<'a, ID, M>,
    version: Version,
}

impl<'a, ID, M: ?Sized + Sync> Iter<'a, ID, M> {
    pub fn version(&self) -> Version {
        self.version
    }
}

impl<'a, ID> Iterator for Iter<'a, ID, dyn Event>
where
    ID: Clone + Debug + Send,
{
    type Item = Result<Row<ID>, StoreError<ID>>;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.index;

        if i < self.messages.len() {
            let event = &self.messages[i];
            let schema = event.schema();
            let content = match self.context.transcoder.encode(event.as_ref()) {
                Ok(content) => content,
                Err(error) => return Some(Err(StoreError::InvalidEncoding(error))),
            };
            
            self.index += 1;
            self.version = self.context.version;
            self.context.version = self.version.increment(Sequence);

            Some(Ok(Row::<ID> {
                id: self.context.id.clone(),
                version: self.version.number(),
                sequence: self.version.sequence(),
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
