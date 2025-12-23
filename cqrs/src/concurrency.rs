/// Defines the possible concurrency behaviors.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum Concurrency {
    /// Indicates no concurrency is enforced.
    None,

    /// Indicates strict concurrency is enforced.
    #[default]
    Enforced,
}

impl Concurrency {
    /// Gets a value indicating whether concurrency is enforced.
    #[inline]
    pub fn enforced(&self) -> bool {
        matches!(self, Self::Enforced)
    }
}
