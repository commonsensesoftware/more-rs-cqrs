use super::Filter;
use async_trait::async_trait;
use std::{error::Error, fmt::Debug};
use uuid::Uuid;

/// Defines the behavior of a projector.
#[async_trait]
pub trait Projector<I = Uuid, P = ()>
where
    I: Debug + Send,
    P: Default,
{
    /// Creates and returns a projection by running the projector.
    ///
    /// # Arguments
    ///
    /// * `filter` - the optional [filter](Filter) applied during projection
    async fn run(&mut self, filter: Option<&Filter<I>>) -> Result<P, Box<dyn Error + Send>>;
}
