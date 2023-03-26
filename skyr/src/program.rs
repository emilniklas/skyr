use async_std::stream::StreamExt;
use futures::stream::FuturesOrdered;

use crate::analyze::{
    ImportMap, SymbolCollector, SymbolTable, TypeCheckResult, TypeChecker, TypeEnvironment,
};
use crate::compile::{CompileError, Module, Visitable};
use crate::execute::{ExecutionContext, Executor};
use crate::{Plan, PluginCell, PluginFactory, ResourceError, Source, State};

pub struct Program<'a> {
    sources: &'a Vec<Source>,
}

impl<'a> Program<'a> {
    pub fn new(sources: &'a Vec<Source>) -> Self {
        Self { sources }
    }

    pub fn compile(&self, plugins: Vec<Box<PluginFactory>>) -> Result<ParsedProgram, CompileError> {
        let mut error = None;
        let mut modules = vec![];
        for source in self.sources {
            modules.extend(
                CompileError::coalesce(source.parse(), &mut error).map(|mut module| {
                    module.name = source.module_name();
                    module
                }),
            );
        }
        if let Some(error) = error {
            Err(error)
        } else {
            Ok(ParsedProgram {
                modules,
                plugins: plugins.into_iter().map(PluginCell::new).collect(),
            })
        }
    }
}

pub struct ParsedProgram {
    modules: Vec<Module>,
    plugins: Vec<PluginCell>,
}

impl ParsedProgram {
    pub async fn analyze(&self) -> Result<AnalyzedProgram, CompileError> {
        let mut error = None;

        let mut collector = SymbolCollector::new();
        for module in self.modules.iter() {
            module.visit(&mut collector);
        }
        let mut table = collector.finalize(&mut error);

        let import_map = ImportMap::new(self.modules.as_slice(), self.plugins.as_slice());
        let checker = TypeChecker::new(&mut table, import_map.clone());

        let mut fo = FuturesOrdered::new();

        for module in self.modules.iter() {
            fo.push_back(async {
                let mut env = TypeEnvironment::new();
                checker.check_module(module, &mut env).await
            });
        }

        let type_check_result = fo
            .fold(TypeCheckResult::new(), |accum, result| {
                accum.flat_map(|_| result.map(|_| ()))
            })
            .await;

        CompileError::coalesce(
            CompileError::from_iter(type_check_result.into_iter().map(CompileError::TypeError)),
            &mut error,
        );

        if let Some(error) = error {
            Err(error)
        } else {
            Ok(AnalyzedProgram {
                import_map,
                modules: &self.modules,
                table,
                plugins: self.plugins.as_slice(),
            })
        }
    }
}

pub struct AnalyzedProgram<'a> {
    import_map: ImportMap<'a>,
    modules: &'a Vec<Module>,
    table: SymbolTable<'a>,
    plugins: &'a [PluginCell],
}

impl<'a> AnalyzedProgram<'a> {
    pub async fn plan(&'a self, state: &'a State) -> Result<Plan<'a>, Vec<ResourceError>> {
        let ctx = ExecutionContext::new(state, &self.table, self.import_map.clone());

        let executor = Executor::new(self.plugins);
        for module in self.modules {
            executor.execute_module(ctx.clone(), module).await;
        }
        executor.finalize(state)
    }
}
