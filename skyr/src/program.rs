use crate::analyze::{SymbolCollector, SymbolTable, TypeChecker};
use crate::compile::{CompileError, Module, Visitable};
use crate::execute::{ExecutionContext, Executor};
use crate::{Plan, Source, State};

pub struct Program {
    sources: Vec<Source>,
}

impl Program {
    pub fn new(sources: Vec<Source>) -> Self {
        Self { sources }
    }

    pub fn compile(&self) -> Result<ParsedProgram, CompileError> {
        let mut error = None;
        let mut modules = vec![];
        for source in &self.sources {
            modules.extend(CompileError::coalesce(source.parse(), &mut error));
        }
        if let Some(error) = error {
            Err(error)
        } else {
            Ok(ParsedProgram { modules })
        }
    }
}

pub struct ParsedProgram {
    modules: Vec<Module>,
}

impl ParsedProgram {
    pub fn analyze(&self) -> Result<AnalyzedProgram, CompileError> {
        let mut error = None;

        let mut collector = SymbolCollector::new();
        for module in self.modules.iter() {
            module.visit(&mut collector);
        }
        let mut table = collector.finalize(&mut error);

        let mut checker = TypeChecker::new(&mut table);
        for module in self.modules.iter() {
            checker.check_module(module);
        }

        CompileError::coalesce(
            CompileError::from_iter(checker.finalize().into_iter().map(CompileError::TypeError)),
            &mut error,
        );

        if let Some(error) = error {
            Err(error)
        } else {
            Ok(AnalyzedProgram {
                modules: &self.modules,
                table,
            })
        }
    }
}

pub struct AnalyzedProgram<'a> {
    modules: &'a Vec<Module>,
    table: SymbolTable<'a>,
}

impl<'a> AnalyzedProgram<'a> {
    pub async fn plan(&'a self, state: &'a State) -> Plan<'a> {
        let ctx = ExecutionContext::new(state, &self.table);

        let executor = Executor::new();
        for module in self.modules {
            executor.execute_module(ctx.clone(), module).await;
        }
        executor.finalize()
    }
}
