use crate::compile::{Import, Module};

#[derive(Clone)]
pub struct ImportMap<'a> {
    modules: &'a [Module],
}

impl<'a> ImportMap<'a> {
    pub fn new(modules: &'a [Module]) -> Self {
        Self { modules }
    }

    pub fn resolve(&self, import: &'a Import) -> Option<External<'a>> {
        self.modules
            .iter()
            .find(|m| m.name.as_ref() == Some(&import.identifier.symbol))
            .map(External::Module)
    }
}

pub enum External<'a> {
    Module(&'a Module),
}
