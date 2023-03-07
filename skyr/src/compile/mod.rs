mod location;

pub use location::*;

mod ast;
pub use ast::*;

mod parse;
pub use parse::*;

use std::fmt;
use crate::analyze::TypeError;

#[derive(Debug)]
pub enum CompileError {
    ParseError(ParseError),
    TypeError(TypeError),
    UndefinedReference(String, Span),
    Cons(Box<CompileError>, Box<CompileError>),
}

impl fmt::Display for CompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for CompileError {}

impl CompileError {
    pub fn coalesce<T>(
        result: Result<T, CompileError>,
        error: &mut Option<CompileError>,
    ) -> Option<T> {
        match result {
            Ok(t) => Some(t),
            Err(e) => {
                let combined = match error.take() {
                    Some(e2) => CompileError::Cons(Box::new(e2), Box::new(e)),
                    None => e,
                };
                *error = Some(combined);
                None
            }
        }
    }

    pub fn from_iter(errors: impl IntoIterator<Item = CompileError>) -> Result<(), CompileError> {
        let mut result = Ok(());
        for error in errors {
            result = Err(match result {
                Ok(()) => error,
                Err(e) => CompileError::Cons(Box::new(e), Box::new(error)),
            });
        }
        result
    }
}
