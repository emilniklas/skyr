use crate::compile::{CompileError, Module, Lexer, Parser};

#[derive(Debug)]
pub struct Source {
    name: String,
    code: String,
}

impl Source {
    pub fn new(name: impl Into<String>, code: impl Into<String>) -> Source {
        Source {
            name: name.into(),
            code: code.into(),
        }
    }

    pub fn parse(&self) -> Result<Module, CompileError> {
        let tokens = Lexer::new(&self.code).collect::<Vec<_>>();
        let (module, _) = Module::parse(&tokens).map_err(CompileError::ParseError)?;
        Ok(module)
    }
}
