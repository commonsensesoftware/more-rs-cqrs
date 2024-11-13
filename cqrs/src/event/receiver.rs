use super::Event;
use async_trait::async_trait;
use std::error::Error;

/// Represents an event receiver.
#[async_trait]
pub trait Receiver<T: ?Sized + Event> {
    /// Receives the specified event.
    ///
    /// # Arguments
    ///
    /// * `event` - the [event](Event) to receive
    async fn receive(&mut self, event: &T) -> Result<(), Box<dyn Error + Send>>;
}
