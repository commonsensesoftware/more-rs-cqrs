/// Defines the possible delete operation behaviors.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Delete {
    /// Indicates that delete operations are unsupported.
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

impl Default for Delete {
    fn default() -> Self {
        Self::Unsupported
    }
}