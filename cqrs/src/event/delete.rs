/// Defines the possible delete operation behaviors.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum Delete {
    /// Indicates that delete operations are unsupported.
    #[default]
    Unsupported,

    /// Indicates that delete operations are supported.
    Supported,
}

impl Delete {
    /// Gets a value indicating whether delete is supported.
    #[inline]
    pub fn supported(&self) -> bool {
        matches!(self, Delete::Supported)
    }

    /// Gets a value indicating whether delete is unsupported.
    #[inline]
    pub fn unsupported(&self) -> bool {
        matches!(self, Delete::Unsupported)
    }
}
