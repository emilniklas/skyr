use crate::compile::{CompileError, Lexer, Module, Parser, TokenKind};

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

    pub fn module_name(&self) -> Option<String> {
        self.name
            .split('/')
            .flat_map(|s| s.split('\\'))
            .last()
            .and_then(|s| s.split('.').next())
            .and_then(|s| {
                let tokens = Lexer::new(s).collect::<Vec<_>>();
                if tokens.len() != 1 {
                    None
                } else if let TokenKind::Symbol(s) = tokens[0].kind {
                    Some(s.into())
                } else {
                    None
                }
            })
    }
}
