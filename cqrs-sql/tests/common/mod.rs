pub mod domain;
pub mod scenario;

use std::error::Error;

pub type TestResult<T = Box<dyn Error + 'static>> = Result<(), T>;