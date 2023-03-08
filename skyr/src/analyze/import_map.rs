use std::ops::Deref;

use crate::compile::{Import, Module};
use crate::Plugin;

#[derive(Clone)]
pub struct ImportMap<'a> {
    modules: &'a [Module],
    plugins: &'a [Box<dyn 'a + Plugin>],
}

impl<'a> ImportMap<'a> {
    pub fn new(modules: &'a [Module], plugins: &'a [Box<dyn 'a + Plugin>]) -> Self {
        Self { modules, plugins }
    }

    pub fn resolve(&self, import: &'a Import) -> Option<External<'a>> {
        self.plugins
            .into_iter()
            .find(|p| p.import_name() == &import.identifier.symbol)
            .map(Deref::deref)
            .map(External::Plugin)
            .or_else(|| {
                self.modules
                    .into_iter()
                    .find(|m| m.name.as_ref() == Some(&import.identifier.symbol))
                    .map(External::Module)
            })
    }
}

pub enum External<'a> {
    Module(&'a Module),
    Plugin(&'a dyn Plugin),
}
