mod aggregate;
mod clock;
mod mask;
mod migration;
mod range;
mod repository;
mod version;

pub use aggregate::{Aggregate, ChangeSet, EventHistory};
pub use clock::{Clock, VirtualClock, WallClock};
pub use mask::{Mask, SecureMask};
pub use migration::{StoreMigration, StoreMigrator};
pub use range::Range;
pub use repository::{Repository, RepositoryError};
pub use version::Version;

/// Contains support for commands.
pub mod command;

/// Contains support for message encoding and decoding.
pub mod encoding;

/// Contains support for events.
pub mod event;

/// Contains support the foundational support for messages.
pub mod message;

/// Contains support for data projections.
pub mod projection;

/// Contains support for data snapshots.
pub mod snapshot;

pub use cqrs_macros::*;

use cfg_if::cfg_if;

cfg_if! {
    if #[cfg(feature = "mem")] {
        mod mem;

        /// Provides in-memory storage.
        ///
        /// # Remarks
        ///
        /// In-memory storage is typically only useful for testing purposes.
        pub mod in_memory {
            use super::*;
            pub use mem::{EventStore, SnapshotStore};
        }
    }
}

cfg_if! {
    if #[cfg(feature = "di")] {
        /// Provides Dependency Injection (DI) extensions.
        pub mod di;
    }
}
