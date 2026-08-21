use std::ops::{Bound, Range as RangeOp, RangeBounds, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive};

/// Represents a range of values.
#[derive(Debug)]
pub struct Range<T> {
    /// Gets or sets the starting bounds of the range.
    pub from: Bound<T>,

    /// Gets or sets the starting bounds of the range.
    pub to: Bound<T>,
}

impl<T> Default for Range<T> {
    fn default() -> Self {
        Self::all()
    }
}

impl<T: Clone> Clone for Range<T> {
    fn clone(&self) -> Self {
        Self {
            from: self.from.clone(),
            to: self.to.clone(),
        }
    }
}

impl<T> From<RangeFull> for Range<T> {
    fn from(_value: RangeFull) -> Self {
        Self::default()
    }
}

impl<T> From<RangeOp<T>> for Range<T> {
    fn from(value: RangeOp<T>) -> Self {
        Self::between(value.start, value.end)
    }
}

impl<T: Clone> From<RangeInclusive<T>> for Range<T> {
    fn from(value: RangeInclusive<T>) -> Self {
        Self::new(value)
    }
}

impl<T> From<RangeFrom<T>> for Range<T> {
    fn from(value: RangeFrom<T>) -> Self {
        Self::from(value.start)
    }
}

impl<T> From<RangeTo<T>> for Range<T> {
    fn from(value: RangeTo<T>) -> Self {
        Self::to(value.end)
    }
}

impl<T> From<RangeToInclusive<T>> for Range<T> {
    fn from(value: RangeToInclusive<T>) -> Self {
        Self::before(value.end)
    }
}

fn clone<T: Clone>(bound: Bound<&T>) -> Bound<T> {
    match bound {
        Bound::Excluded(value) => Bound::Excluded(value.clone()),
        Bound::Included(value) => Bound::Included(value.clone()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl<T: Clone> Range<T> {
    /// Initializes a new [Range].
    ///
    /// # Arguments
    ///
    /// * `range` - the [range](RangeBounds) to initialize from
    pub fn new<R: RangeBounds<T>>(range: R) -> Self {
        Self {
            from: clone(range.start_bound()),
            to: clone(range.end_bound()),
        }
    }
}

impl<T> Range<T> {
    /// Initializes a new, unbounded [Range].
    pub const fn all() -> Self {
        Self {
            from: Bound::Unbounded,
            to: Bound::Unbounded,
        }
    }

    /// Initializes a new [Range] between two values.
    ///
    /// # Arguments
    ///
    /// * `from` - the inclusive value the range starts from
    /// * `to` - the inclusive value the range goes to
    pub const fn between(from: T, to: T) -> Self {
        Self {
            from: Bound::Included(from),
            to: Bound::Included(to),
        }
    }

    /// Initializes a new [Range] from an inclusive value.
    ///
    /// # Arguments
    ///
    /// * `from` - the inclusive value the range starts from
    pub const fn from(from: T) -> Self {
        Self {
            from: Bound::Included(from),
            to: Bound::Unbounded,
        }
    }

    /// Initializes a new [Range] from an exclusive value.
    ///
    /// # Arguments
    ///
    /// * `from` - the exclusive value the range starts after
    pub const fn after(from: T) -> Self {
        Self {
            from: Bound::Excluded(from),
            to: Bound::Unbounded,
        }
    }

    /// Initializes a new [Range] up to an inclusive value.
    ///
    /// # Arguments
    ///
    /// * `to` - the inclusive value the range ends at
    pub const fn to(to: T) -> Self {
        Self {
            from: Bound::Unbounded,
            to: Bound::Included(to),
        }
    }

    /// Initializes a new [Range] up to an exclusive value.
    ///
    /// # Arguments
    ///
    /// * `to` - the exclusive value the range ends before
    pub const fn before(to: T) -> Self {
        Self {
            from: Bound::Unbounded,
            to: Bound::Excluded(to),
        }
    }

    /// Gets a value indicating whether the range is unbounded.
    pub const fn unbounded(&self) -> bool {
        matches!(self.from, Bound::Unbounded) && matches!(self.to, Bound::Unbounded)
    }
}
