use crate::{event, Version};
use std::{
    fmt::Debug,
    ops::{Bound, Bound::Unbounded},
    time::SystemTime,
};

/// Represents the predicate that can be applied to filter events.
#[derive(Clone, Debug)]
pub struct Predicate {
    /// Gets or sets the minimum snapshot [version](Version) to apply to the predicate, if any.
    ///
    /// # Remarks
    ///
    /// The specified value is typically exclusive.
    pub min_version: Bound<Version>,

    /// Gets or sets the [date and time](SystemTime) to apply to the predicate since a
    /// snapshot was taken, if any.
    pub since: Bound<SystemTime>,
}

impl Default for Predicate {
    fn default() -> Self {
        Self {
            min_version: Unbounded,
            since: Unbounded,
        }
    }
}

/// Represents a builder to create a [`Predicate`].
#[derive(Default)]
pub struct PredicateBuilder(Predicate);

impl PredicateBuilder {
    /// Initializes a new [`PredicateBuilder`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the minimum [version](Version) to apply to the predicate.
    ///
    /// # Arguments
    ///
    /// * `value` - the minimum snapshot [version](Version)
    pub fn min_version(mut self, value: Bound<Version>) -> Self {
        self.0.min_version = value;
        self
    }

    /// Sets the [date and time](SystemTime) to apply to the predicate since a
    /// snapshot was taken.
    ///
    /// # Arguments
    ///
    /// * `value` - the [date and time](SystemTime) since the snapshot was taken
    pub fn since(mut self, value: Bound<SystemTime>) -> Self {
        self.0.since = value;
        self
    }

    /// Builds and returns a new [`Predicate`].
    pub fn build(self) -> Predicate {
        self.0
    }
}

impl From<PredicateBuilder> for Predicate {
    fn from(value: PredicateBuilder) -> Self {
        value.build()
    }
}

impl<'a, T: Debug + Send> From<&'a event::Predicate<'a, T>> for Predicate {
    fn from(value: &'a event::Predicate<'a, T>) -> Self {
        PredicateBuilder::new()
            .min_version(value.version)
            .since(value.stored_on.from)
            .build()
    }
}

impl<'a, T: Debug + Send> From<event::Predicate<'a, T>> for Predicate {
    fn from(value: event::Predicate<'a, T>) -> Self {
        (&value).into()
    }
}
