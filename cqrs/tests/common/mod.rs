#![allow(dead_code)]

pub mod domain;
pub mod projector;

use std::error::Error;

pub type TestResult<T = Box<dyn Error + Send>> = Result<(), T>;

pub trait BoxErr<T> {
    fn box_err(self) -> Result<T, Box<dyn Error + Send>>;
}

impl<T, E: Error + Send + 'static> BoxErr<T> for Result<T, E> {
    fn box_err(self) -> Result<T, Box<dyn Error + Send>> {
        self.map_err(|e| Box::new(e) as Box<dyn Error + Send>)
    }
}