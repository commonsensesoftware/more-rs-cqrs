use crate::message::Type;
use crate::{Range, Version};
use std::{
    fmt::Debug,
    num::NonZeroU8,
    ops::{Bound, Bound::Unbounded},
    time::SystemTime,
};
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
#[derive(Clone, Debug)]
pub struct Predicate<'a, T: Debug + Send = Uuid> {
    /// Gets or sets the associated identifier, if any.
    pub id: Option<&'a T>,

    /// Gets or sets the event [version](Version) to apply to a predicate, if any.
    pub version: Bound<Version>,

    /// Gets or sets the event types to apply to the predicate.
    pub types: Vec<Type>,

    /// Gets or sets the [date](SystemTime) [range](Range) to apply to a predicate, if any.
    pub stored_on: Range<SystemTime>,

    /// Gets or sets the associated [load options](LoadOptions).
    pub load: LoadOptions,
}

impl<'a, T: Debug + Send> Predicate<'a, T> {
    /// Initializes a new [Predicate].
    ///
    /// # Arguments
    ///
    /// * `id` - the optional identifier to apply
    pub fn new(id: Option<&'a T>) -> Self {
        Self {
            id,
            version: Unbounded,
            types: Default::default(),
            stored_on: Default::default(),
            load: Default::default(),
        }
    }
}

impl<'a, T: Debug + Send> Default for Predicate<'a, T> {
    fn default() -> Self {
        Self::new(None)
    }
}

/// Represents a builder to create a [Predicate].
#[derive(Default)]
pub struct PredicateBuilder<'a, T: Debug + Send = Uuid>(Predicate<'a, T>);

impl<'a, T: Debug + Send> PredicateBuilder<'a, T> {
    /// Initializes a new [PredicateBuilder].
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
    pub fn version(mut self, value: Bound<Version>) -> Self {
        self.0.version = value;
        self
    }

    /// Adds an event type to apply to a predicate.
    ///
    /// # Arguments
    ///
    /// * `value` - the event [type](Type), which a [schema](crate::message::Schema) converts into
    ///
    /// # Remarks
    ///
    /// Use [Type::any] to apply a predicate to every version of a type.
    pub fn add_type<V: Into<Type>>(mut self, value: V) -> Self {
        self.0.types.push(value.into());
        self
    }

    /// Sets the [date](SystemTime) [range](Range) of recorded events to a predicate.
    ///
    /// # Arguments
    ///
    /// * `value` - the [date](SystemTime) [range](Range)
    pub fn stored_on<R: Into<Range<SystemTime>>>(mut self, value: R) -> Self {
        self.0.stored_on = value.into();
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
        if let Some(value) = predicate.id {
            self.0.id = Some(value);
        }

        self.0.version = predicate.version;
        self.0.stored_on = predicate.stored_on.clone();
        self.0.types.extend(predicate.types.iter().cloned());

        if !predicate.load.snapshots {
            self.0.load.snapshots = false;
        }

        self
    }

    /// Builds and returns a new [Predicate].
    pub fn build(self) -> Predicate<'a, T> {
        self.0
    }
}

impl<'a, T: Debug + Send> From<PredicateBuilder<'a, T>> for Predicate<'a, T> {
    fn from(value: PredicateBuilder<'a, T>) -> Self {
        value.build()
    }
}

/// Defines the behavior used to translate the message types of a [predicate](Predicate) into a store-specific filter.
///
/// # Remarks
///
/// A store implements this trait and drives it with [filter_types], which decides the shape of the filter and, in
/// particular, whether a message type constrains the revision. A [type](Type) that applies to every version must not
/// constrain the revision at all; getting that wrong silently matches nothing instead of everything, so the decision is
/// made in one place for every store.
pub trait TypeFilter {
    /// The error that can occur while a condition is added.
    type Error;

    /// Begins the filter.
    ///
    /// # Arguments
    ///
    /// * `many` - indicates whether the filter contains more than one condition
    fn begin(&mut self, many: bool);

    /// Adds the condition for a single message type.
    ///
    /// # Arguments
    ///
    /// * `index` - the zero-based index of the condition
    /// * `kind` - the message type the condition applies to
    /// * `revision` - the message revision the condition applies to, if any. [None] indicates the
    ///   condition applies to every version and must not constrain the revision
    fn condition(&mut self, index: usize, kind: &str, revision: Option<NonZeroU8>) -> Result<(), Self::Error>;

    /// Adds the separator between two conditions.
    fn or(&mut self);

    /// Ends the filter.
    ///
    /// # Arguments
    ///
    /// * `many` - indicates whether the filter contained more than one condition
    fn end(&mut self, many: bool);
}

/// Translates the message types of a [predicate](Predicate) into a store-specific filter.
///
/// # Arguments
///
/// * `types` - the message [types](Type) to translate
/// * `filter` - the [filter](TypeFilter) the translation is applied to
///
/// # Remarks
///
/// The [filter](TypeFilter) is not visited at all when there are no types.
pub fn filter_types<F: TypeFilter + ?Sized>(types: &[Type], filter: &mut F) -> Result<(), F::Error> {
    let Some((first, rest)) = types.split_first() else {
        return Ok(());
    };
    let many = !rest.is_empty();

    filter.begin(many);
    filter.condition(0, first.kind(), first.revision())?;

    for (index, type_) in rest.iter().enumerate() {
        filter.or();
        filter.condition(index + 1, type_.kind(), type_.revision())?;
    }

    filter.end(many);
    Ok(())
}
