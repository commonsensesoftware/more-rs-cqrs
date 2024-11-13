use super::Schema;
use std::any::type_name;

/// Defines the behavior of an encoded message.
pub trait Encoded {
    /// Get the message [schema](Schema).
    /// 
    /// # Remarks
    /// 
    /// The default implementation uses the fully-qualified name of the associated type.
    fn schema() -> Schema {
        Schema::initial(type_name::<Self>())
    }
}
