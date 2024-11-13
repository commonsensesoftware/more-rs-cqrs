use crate::event::PredicateBuilder;
use std::fmt::Debug;
use std::time::SystemTime;
use uuid::Uuid;

/// Represents the filter applied to projections.
#[derive(Clone, Default)]
pub struct Filter<'a, T: Debug + Send = Uuid> {
    id: Option<&'a T>,
    from: Option<SystemTime>,
    to: Option<SystemTime>,
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
            from: Default::default(),
            to: Default::default(),
        }
    }

    /// Gets the associated identifier, if any.
    pub fn id(&self) -> Option<&T> {
        self.id
    }

    /// Gets the first event recorded date and time to filter by, if any.
    ///
    /// # Remarks
    ///
    /// The specified value is typically inclusive.
    pub fn from(&self) -> Option<SystemTime> {
        self.from
    }

    /// Gets the last event recorded date and time to filter by, if any.
    ///
    /// # Remarks
    ///
    /// The specified value is typically inclusive.
    pub fn to(&self) -> Option<SystemTime> {
        self.to
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

    /// Sets the first event recorded date and time to filter by.
    ///
    /// # Arguments
    ///
    /// * `value` - the first recorded date and time
    ///
    /// # Remarks
    ///
    /// The specified value is typically inclusive.
    pub fn from(mut self, value: SystemTime) -> Self {
        self.0.from = Some(value);
        self
    }

    /// Sets the last event recorded date and time to filter by.
    ///
    /// # Arguments
    ///
    /// * `value` - the last recorded date and time
    ///
    /// # Remarks
    ///
    /// The specified value is typically inclusive.
    pub fn to(mut self, value: SystemTime) -> Self {
        self.0.to = Some(value);
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
        let mut me = Self::new(value.id);

        if let Some(from) = value.from() {
            me = me.from(from);
        }

        if let Some(to) = value.to() {
            me = me.to(to);
        }

        me
    }
}
