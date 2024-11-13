use super::Command;
use async_trait::async_trait;
use std::error::Error;

/// Represents a command handler.
#[async_trait]
pub trait Handler<T: ?Sized + Command> {
    /// Handles the specified command.
    ///
    /// # Arguments
    ///
    /// * `command` - the [command](Command) to handle
    async fn handle(&mut self, command: &T) -> Result<(), Box<dyn Error + Send>>;
}
