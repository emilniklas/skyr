use std::sync::Arc;

use crate::compile::{Import, Module};
use crate::{Plugin, PluginCell};

#[derive(Clone)]
pub struct ImportMap<'a> {
    modules: &'a [Module],
    plugins: &'a [PluginCell],
}

impl<'a> ImportMap<'a> {
    pub fn new(modules: &'a [Module], plugins: &'a [PluginCell]) -> Self {
        Self { modules, plugins }
    }

    pub fn resolve(&self, import: &'a Import) -> Option<External<'a>> {
        let symbol = &import.identifier.symbol;

        for module in self.modules {
            if module.name.as_ref() == Some(symbol) {
                return Some(External::Module(module));
            }
        }

        for plugin in self.plugins {
            let plugin = plugin.get();

            if plugin.import_name() == symbol {
                return Some(External::Plugin(plugin));
            }
        }

        None
    }
}

pub enum External<'a> {
    Module(&'a Module),
    Plugin(Arc<dyn Plugin>),
}
