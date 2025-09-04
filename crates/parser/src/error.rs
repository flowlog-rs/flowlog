use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParserError {
    #[error("Example directory not found: {0}")]
    ExampleDirectoryNotFound(String),
    #[error("Error reading example directory: {0}")]
    FileReadError(String),
    #[error("No .dl files found in {0} directory")]
    EmptyDirectory(String),
}
