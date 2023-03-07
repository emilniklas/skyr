use std::collections::BTreeMap;

use crate::compile::{
    Assignment, CompileError, Expression, Function, Identifier, Module, NodeId, Parameter, Record,
    TypeDefinition, TypeExpression, Visitable, Visitor,
};

#[derive(Debug)]
pub struct SymbolTable<'a> {
    references: BTreeMap<NodeId, Vec<&'a Identifier>>,
    declarations: BTreeMap<NodeId, Declaration<'a>>,
}

impl<'a> SymbolTable<'a> {
    pub fn new() -> Self {
        Self {
            references: Default::default(),
            declarations: Default::default(),
        }
    }

    pub fn insert(&mut self, declaration: Declaration<'a>, reference: &'a Identifier) {
        self.declarations.insert(reference.id, declaration);
        self.references
            .entry(declaration.id())
            .or_default()
            .push(reference);
    }

    pub fn references(&self, declaration: Declaration<'a>) -> &[&'a Identifier] {
        self.references
            .get(&declaration.id())
            .map(|v| v.as_slice())
            .unwrap_or_default()
    }

    pub fn declaration(&self, reference: &'a Identifier) -> Option<Declaration<'a>> {
        self.declarations.get(&reference.id).as_ref().map(|d| **d)
    }

    pub fn extend(&mut self, other: Self) {
        self.references.extend(other.references);
        self.declarations.extend(other.declarations);
    }
}

impl<'a> Default for SymbolTable<'a> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Declaration<'a> {
    Assignment(&'a Assignment),
    TypeDefinition(&'a TypeDefinition),
    Parameter(&'a Parameter),
}

impl<'a> Declaration<'a> {
    pub fn id(&self) -> NodeId {
        match self {
            Declaration::Assignment(a) => a.id,
            Declaration::TypeDefinition(a) => a.id,
            Declaration::Parameter(a) => a.id,
        }
    }

    pub fn matches(&self, id: &'a Identifier) -> bool {
        match self {
            Declaration::Assignment(a) => a.identifier == *id,
            Declaration::TypeDefinition(a) => a.identifier == *id,
            Declaration::Parameter(a) => a.identifier == *id,
        }
    }
}

#[derive(Default)]
struct Scope<'a> {
    parent_scope: Option<Box<Scope<'a>>>,
    child_scopes: Vec<Scope<'a>>,
    declarations: Vec<Declaration<'a>>,
    references: Vec<&'a Identifier>,
}

impl<'a> Scope<'a> {
    pub fn enter(&mut self) {
        let parent = std::mem::replace(self, Default::default());
        self.parent_scope = Some(Box::new(parent));
    }

    pub fn leave(&mut self) {
        if let Some(parent) = self.parent_scope.take() {
            let child = std::mem::replace(self, *parent);
            self.child_scopes.push(child);
        }
    }

    pub fn add_declaration(&mut self, declaration: Declaration<'a>) {
        self.declarations.push(declaration);
    }

    pub fn add_reference(&mut self, reference: &'a Identifier) {
        self.references.push(reference);
    }

    fn find_declaration(&self, reference: &'a Identifier) -> Option<Declaration<'a>> {
        for declaration in &self.declarations {
            if declaration.matches(reference) {
                return Some(*declaration);
            }
        }
        if let Some(parent) = &self.parent_scope {
            parent.find_declaration(reference)
        } else {
            None
        }
    }

    pub fn resolve(&mut self, table: &mut SymbolTable<'a>) -> Result<(), CompileError> {
        let mut error = None;
        for child in std::mem::replace(&mut self.child_scopes, vec![]) {
            let parent = std::mem::replace(self, child);
            self.parent_scope = Some(Box::new(parent));
            CompileError::coalesce(self.resolve(table), &mut error);
            let parent = self.parent_scope.take().unwrap();
            *self = *parent;
        }
        let mut errors = vec![];
        errors.extend(error);
        for reference in std::mem::replace(&mut self.references, vec![]) {
            if let Some(dec) = self.find_declaration(reference) {
                table.insert(dec, reference);
            } else {
                errors.push(CompileError::UndefinedReference(
                    reference.symbol.clone(),
                    reference.span.clone(),
                ));
            }
        }
        CompileError::from_iter(errors)
    }
}

#[derive(Default)]
pub struct SymbolCollector<'a> {
    table: SymbolTable<'a>,
    scope: Scope<'a>,
    type_table: SymbolTable<'a>,
    type_scope: Scope<'a>,
}

impl<'a> SymbolCollector<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn finalize(mut self, error: &mut Option<CompileError>) -> SymbolTable<'a> {
        CompileError::coalesce(self.scope.resolve(&mut self.table), error);
        CompileError::coalesce(self.type_scope.resolve(&mut self.type_table), error);
        self.table.extend(self.type_table);
        self.table
    }
}

impl<'a> Visitor<'a> for SymbolCollector<'a> {
    fn enter_assignment(&mut self, assignment: &'a Assignment) {
        self.scope
            .add_declaration(Declaration::Assignment(assignment));
    }

    fn enter_type_definition(&mut self, type_definition: &'a TypeDefinition) {
        self.type_scope
            .add_declaration(Declaration::TypeDefinition(type_definition));
    }

    fn enter_parameter(&mut self, parameter: &'a Parameter) {
        self.scope
            .add_declaration(Declaration::Parameter(parameter));
    }

    fn enter_expression(&mut self, expression: &'a Expression) {
        if let Expression::Identifier(ref id) = expression {
            self.scope.add_reference(id);
        }
    }

    fn enter_type_expression(&mut self, type_expression: &'a TypeExpression) {
        if let TypeExpression::Identifier(ref id) = type_expression {
            self.type_scope.add_reference(id);
        }
    }

    fn enter_module(&mut self, _module: &'a Module) {
        self.scope.enter();
    }

    fn leave_module(&mut self, _module: &'a Module) {
        self.scope.leave();
    }

    fn enter_record<T: Visitable>(&mut self, _record: &'a Record<T>) {
        self.scope.enter();
    }

    fn leave_record<T: Visitable>(&mut self, _record: &'a Record<T>) {
        self.scope.leave();
    }

    fn enter_function(&mut self, _function: &'a Function) {
        self.scope.enter();
    }

    fn leave_function(&mut self, _function: &'a Function) {
        self.scope.leave();
    }
}
