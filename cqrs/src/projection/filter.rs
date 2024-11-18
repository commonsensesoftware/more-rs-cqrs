use crate::event::PredicateBuilder;
use crate::Range;
use std::fmt::Debug;
use std::time::SystemTime;
use uuid::Uuid;

/// Represents the filter applied to projections.
#[derive(Clone, Default)]
pub struct Filter<'a, T: Debug + Send = Uuid> {
    id: Option<&'a T>,
    stored_on: Range<SystemTime>,
}

impl<'a, T: Debug + Send> Filter<'a, T> {
    /// Initializes a new [`Filter`].
    ///
    /// # Arguments
    ///
    /// * `id` - the optional identifier to apply
    pub fn new(id: Option<&'a T>) -> Self {
        Self {
            id,
            stored_on: Range::all(),
        }
    }

    /// Gets the associated identifier, if any.
    pub fn id(&self) -> Option<&T> {
        self.id
    }

    /// Gets the [date](SystemTime) [range](Range) of recorded events to a predicate.
    pub fn stored_on(&self) -> &Range<SystemTime> {
        &self.stored_on
    }
}

/// Represents a builder to create a [`Filter`].
#[derive(Default)]
pub struct FilterBuilder<'a, T: Debug + Send = Uuid>(Filter<'a, T>);

impl<'a, T: Debug + Send> FilterBuilder<'a, T> {
    /// Initializes a new [`FilterBuilder`].
    ///
    /// # Arguments
    ///
    /// * `id` - the optional identifier to filter by
    pub fn new(id: Option<&'a T>) -> Self {
        Self(Filter::new(id))
    }

    /// Sets the [date](SystemTime) [range](Range) of recorded events to filter by.
    ///
    /// # Arguments
    ///
    /// * `value` - the [date](SystemTime) [range](Range)
    pub fn stored_on<R: Into<Range<SystemTime>>>(mut self, value: R) -> Self {
        self.0.stored_on = value.into();
        self
    }

    /// Builds and returns a new [`Filter`].
    pub fn build(self) -> Filter<'a, T> {
        self.0
    }
}

impl<'a, T: Debug + Send> From<FilterBuilder<'a, T>> for Filter<'a, T> {
    fn from(value: FilterBuilder<'a, T>) -> Self {
        value.build()
    }
}

impl<'a, T: Debug + Send> From<&Filter<'a, T>> for PredicateBuilder<'a, T> {
    fn from(value: &Filter<'a, T>) -> Self {
        Self::new(value.id).stored_on(value.stored_on().clone())
    }
}
