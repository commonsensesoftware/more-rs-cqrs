use crate::message::Schema;
use crate::Version;
use std::fmt::Debug;
use std::time::SystemTime;
use uuid::Uuid;

/// Represents the supported load options.
#[derive(Clone, Debug)]
pub struct LoadOptions {
    /// Gets or sets a value indicating whether snapshots are loaded.
    ///
    /// # Remarks
    ///
    /// The default value is `true`.
    pub snapshots: bool,
}

impl Default for LoadOptions {
    fn default() -> Self {
        Self { snapshots: true }
    }
}

/// Represents the predicate that can be applied to filter events.
#[derive(Clone, Default, Debug)]
pub struct Predicate<'a, T: Debug + Send = Uuid> {
    /// Gets or sets the associated identifier, if any.
    pub id: Option<&'a T>,

    /// Gets or sets the event [version](Version) to apply to a predicate, if any.
    ///
    /// # Remarks
    ///
    /// The specified value is typically exclusive.
    pub version: Option<Version>,

    /// Gets or sets the event types to apply to the predicate.
    pub types: Vec<Schema>,

    /// Gets or sets the first event recorded [date and time](SystemTime) to apply to a predicate, if any.
    ///
    /// # Remarks
    ///
    /// The specified value is typically inclusive.
    pub from: Option<SystemTime>,

    /// Gets or sets the last event recorded [date and time](SystemTime) to apply to a predicate, if any.
    ///
    /// # Remarks
    ///
    /// The specified value is typically inclusive.    
    pub to: Option<SystemTime>,

    /// Gets or sets the associated [load options](LoadOptions).
    pub load: LoadOptions,
}

impl<'a, T: Debug + Send> Predicate<'a, T> {
    /// Initializes a new [`Predicate`].
    ///
    /// # Arguments
    ///
    /// * `id` - the optional identifier to apply
    pub fn new(id: Option<&'a T>) -> Self {
        Self {
            id,
            version: Default::default(),
            types: Default::default(),
            from: Default::default(),
            to: Default::default(),
            load: Default::default(),
        }
    }
}

/// Represents a builder to create a [`Predicate`].
#[derive(Default)]
pub struct PredicateBuilder<'a, T: Debug + Send = Uuid>(Predicate<'a, T>);

impl<'a, T: Debug + Send> PredicateBuilder<'a, T> {
    /// Initializes a new [`PredicateBuilder`].
    ///
    /// # Arguments
    ///
    /// * `id` - the optional identifier to apply
    pub fn new(id: Option<&'a T>) -> Self {
        Self(Predicate::new(id))
    }

    /// Sets the event [version](Version) to apply to a predicate.
    ///
    /// # Arguments
    ///
    /// * `value` - the event [version](Version)
    ///
    /// # Remarks
    ///
    /// The specified value is typically exclusive.
    pub fn version(mut self, value: Version) -> Self {
        self.0.version = Some(value);
        self
    }

    /// Adds an event type to apply to a predicate.
    ///
    /// # Arguments
    ///
    /// * `value` - the event type [schema](Schema)
    ///
    /// # Remarks
    ///
    /// Use [`Schema::versionless`] to match any version of a [schema](Schema).
    pub fn add_type(mut self, value: Schema) -> Self {
        self.0.types.push(value);
        self
    }

    /// Sets the first event recorded date and time to apply to a predicate.
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

    /// Sets the last event recorded date and time to apply to a predicate.
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

    /// Sets the load options applied to a predicate.
    ///
    /// # Arguments
    ///
    /// * `setup` - the function used to configure [load options](LoadOptions)
    pub fn load(mut self, setup: fn(&mut LoadOptions)) -> Self {
        setup(&mut self.0.load);
        self
    }

    /// Merges an existing predicate into the current builder.
    ///
    /// # Arguments
    ///
    /// * `predicate` - the existing [predicate](Predicate) to merge
    ///
    /// # Remarks
    ///
    /// Only non-default values are merged.
    pub fn merge(mut self, predicate: &'a Predicate<'a, T>) -> Self {
        if let Some(value) = predicate.from {
            self.0.from = Some(value);
        }

        if let Some(value) = predicate.id {
            self.0.id = Some(value);
        }

        if let Some(value) = predicate.to {
            self.0.to = Some(value);
        }

        self.0.types.extend(predicate.types.iter().cloned());

        if let Some(value) = predicate.version {
            self.0.version = Some(value);
        }

        if !predicate.load.snapshots {
            self.0.load.snapshots = false;
        }

        self
    }

    /// Builds and returns a new [`Predicate`].
    pub fn build(self) -> Predicate<'a, T> {
        self.0
    }
}

impl<'a, T: Debug + Send> From<PredicateBuilder<'a, T>> for Predicate<'a, T> {
    fn from(value: PredicateBuilder<'a, T>) -> Self {
        value.build()
    }
}
