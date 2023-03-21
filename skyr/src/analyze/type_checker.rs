use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::identity;
use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;

use serde::{Deserialize, Serialize};

use crate::{compile::*, DisplayAsDebug};

use super::{Declaration, External, ImportMap, SymbolTable};

#[derive(Debug)]
pub enum TypeError {
    Mismatch {
        lhs: Type,
        rhs: Type,
        span: Span,
    },

    MissingField {
        lhs: Type,
        rhs: Type,
        field: String,
        span: Span,
    },

    WrongNumberOfArguments {
        lhs: usize,
        rhs: usize,
        span: Span,
    },

    UnresolvedImport {
        name: String,
        span: Span,
    },
}

struct TypeEnvironment<'a> {
    bindings: BTreeMap<TypeId, Type>,
    errors: &'a mut Vec<TypeError>,
}

impl<'a> TypeEnvironment<'a> {
    pub fn new(errors: &'a mut Vec<TypeError>) -> Self {
        Self {
            bindings: Default::default(),
            errors,
        }
    }

    pub fn unify(&mut self, lhs: Type, rhs: Type, span: &Span) -> Type {
        self.do_unify(lhs, &identity, rhs, &identity, span)
    }

    fn do_unify(
        &mut self,
        lhs: Type,
        lhs_wrap: &dyn Fn(Type) -> Type,
        rhs: Type,
        rhs_wrap: &dyn Fn(Type) -> Type,
        span: &Span,
    ) -> Type {
        match (lhs, rhs) {
            (Type::Void, Type::Void) => Type::Void,
            (Type::String, Type::String) => Type::String,
            (Type::Integer, Type::Integer) => Type::Integer,
            (Type::Boolean, Type::Boolean) => Type::Boolean,
            (Type::Optional(lhs), Type::Optional(rhs)) => Type::Optional(Box::new(
                self.do_unify(*lhs, lhs_wrap, *rhs, rhs_wrap, span),
            )),
            (Type::Optional(lhs), rhs) => {
                Type::Optional(Box::new(self.do_unify(*lhs, lhs_wrap, rhs, rhs_wrap, span)))
            }
            (Type::List(lhs), Type::List(rhs)) => Type::List(Box::new(
                self.do_unify(*lhs, lhs_wrap, *rhs, rhs_wrap, span),
            )),
            (Type::Function(lhs_a, lhs_r), Type::Function(rhs_a, rhs_r)) => {
                let arity = lhs_a.len().min(rhs_a.len());

                let params = (0..arity)
                    .map(|idx| self.unify(lhs_a[idx].clone(), rhs_a[idx].clone(), span))
                    .collect();

                if lhs_a.len() != rhs_a.len() {
                    self.errors.push(TypeError::WrongNumberOfArguments {
                        lhs: lhs_a.len(),
                        rhs: rhs_a.len(),
                        span: span.clone(),
                    });
                }

                let return_type = self.unify(self.resolve(*lhs_r), self.resolve(*rhs_r), span);

                Type::Function(params, Box::new(return_type))
            }
            (Type::Named(n, lhs), rhs) => Type::Named(
                n.clone(),
                Box::new(self.do_unify(
                    *lhs,
                    &|l| Type::Named(n.clone(), Box::new(lhs_wrap(l))),
                    rhs,
                    rhs_wrap,
                    span,
                )),
            ),
            (lhs, Type::Named(n, rhs)) => Type::Named(
                n.clone(),
                Box::new(self.do_unify(
                    lhs,
                    lhs_wrap,
                    *rhs,
                    &|r| Type::Named(n.clone(), Box::new(rhs_wrap(r))),
                    span,
                )),
            ),
            (Type::Record(lhf), Type::Record(rhf)) => {
                let mut result = vec![];
                'fields: for (lhn, lht) in &lhf {
                    for (rhn, rht) in &rhf {
                        if lhn == rhn {
                            result.push((lhn.clone(), self.unify(lht.clone(), rht.clone(), span)));
                            continue 'fields;
                        }
                    }
                    result.push((lhn.clone(), lht.clone()));

                    if let Type::Optional(_) = lht {
                    } else {
                        self.errors.push(TypeError::MissingField {
                            lhs: lhs_wrap(Type::Record(lhf.clone())),
                            rhs: rhs_wrap(Type::Record(rhf.clone())),
                            field: lhn.clone(),
                            span: span.clone(),
                        });
                    }
                }
                self.resolve(Type::Record(result))
            }
            (Type::Open(lhs), rhs) => match self.bindings.get(&lhs).cloned() {
                None => {
                    self.bindings.insert(lhs, rhs_wrap(rhs.clone()));
                    rhs
                }
                Some(lhs) => self.do_unify(lhs, lhs_wrap, rhs, rhs_wrap, span),
            },
            (lhs, Type::Open(rhs)) => match self.bindings.get(&rhs).cloned() {
                None => {
                    self.bindings.insert(rhs, lhs_wrap(lhs.clone()));
                    lhs
                }
                Some(rhs) => self.do_unify(lhs, lhs_wrap, rhs, rhs_wrap, span),
            },
            (lhs, rhs) => {
                self.errors.push(TypeError::Mismatch {
                    lhs: lhs_wrap(lhs.clone()),
                    rhs: rhs_wrap(rhs),
                    span: span.clone(),
                });
                lhs
            }
        }
    }

    pub fn resolve(&self, type_: Type) -> Type {
        self.do_resolve(type_, Default::default())
    }

    fn do_resolve(&self, type_: Type, seen_ids: BTreeSet<TypeId>) -> Type {
        match type_ {
            Type::Void => Type::Void,
            Type::String => Type::String,
            Type::Integer => Type::Integer,
            Type::Float => Type::Float,
            Type::Boolean => Type::Boolean,
            Type::List(e) => Type::List(Box::new(self.do_resolve(*e, seen_ids))),
            Type::Map(k, v) => Type::Map(
                Box::new(self.do_resolve(*k, seen_ids.clone())),
                Box::new(self.do_resolve(*v, seen_ids)),
            ),
            Type::Open(id) if seen_ids.contains(&id) => Type::Open(id),
            Type::Open(id) => self
                .bindings
                .get(&id)
                .cloned()
                .map(|t| {
                    let mut sid = seen_ids.clone();
                    sid.insert(id);
                    self.do_resolve(t, sid)
                })
                .unwrap_or(Type::Open(id)),
            Type::Record(fields) => Type::Record(
                fields
                    .into_iter()
                    .map(|(n, t)| (n, self.do_resolve(t, seen_ids.clone())))
                    .collect(),
            ),
            Type::Named(n, t) => Type::Named(n, Box::new(self.do_resolve(*t, seen_ids))),
            Type::Function(p, r) => Type::Function(
                p.into_iter()
                    .map(|p| self.do_resolve(p, seen_ids.clone()))
                    .collect(),
                Box::new(self.do_resolve(*r, seen_ids)),
            ),
            Type::Optional(t) => Type::Optional(Box::new(self.do_resolve(*t, seen_ids))),
        }
    }
}

pub struct TypeChecker<'t, 'a> {
    symbols: &'t mut SymbolTable<'a>,
    cache: BTreeMap<NodeId, Type>,
    checking: BTreeMap<NodeId, Vec<Type>>,
    errors: Vec<TypeError>,
    import_map: ImportMap<'a>,
}

impl<'t, 'a> TypeChecker<'t, 'a> {
    pub fn new(symbols: &'t mut SymbolTable<'a>, import_map: ImportMap<'a>) -> Self {
        Self {
            symbols,
            import_map,
            cache: Default::default(),
            checking: Default::default(),
            errors: Default::default(),
        }
    }

    pub fn finalize(self) -> Vec<TypeError> {
        self.errors
    }

    pub fn check_module(&mut self, module: &'a Module) -> Type {
        if let Some(t) = self.cache.get(&module.id) {
            return t.clone();
        }

        if let Some(c) = self.checking.get_mut(&module.id) {
            let type_ = Type::open();
            c.push(type_.clone());
            return type_;
        }

        self.checking.insert(module.id, vec![]);

        let mut exports = vec![];

        for statement in module.statements.iter() {
            if let Statement::Assignment(n) = statement {
                exports.push((n.identifier.symbol.clone(), self.check_assignment(n)));
            } else {
                self.check_statement(statement);
            }
        }

        let type_ = Type::Record(exports);

        let mut env = TypeEnvironment::new(&mut self.errors);

        let mut type_ = type_;
        for recursive in self.checking.remove(&module.id).unwrap_or_default() {
            type_ = env.unify(type_, recursive, &module.span);
        }

        self.cache.insert(module.id, type_.clone());
        type_
    }

    pub fn check_statement(&mut self, statement: &'a Statement) -> Type {
        match statement {
            Statement::Expression(n) => self.check_expression(n),
            Statement::Assignment(n) => self.check_assignment(n),
            Statement::TypeDefinition(n) => self.check_type_definition(n),
            Statement::Return(n) => return self.check_expression(&n.expression),
            Statement::Debug(n) => self.check_expression(&n.expression),
            Statement::Import(n) => self.check_import(n),
            Statement::Block(n) => return self.check_block(n),
            Statement::If(n) => return self.check_if(n),
        };
        Type::Void
    }

    pub fn check_if(&mut self, if_: &'a If) -> Type {
        let condition_type = self.check_expression(&if_.condition);
        let mut consequence_type = self.check_statement(&if_.consequence);
        let else_type = if_
            .else_clause
            .as_ref()
            .map(|c| (c, self.check_statement(c)));

        let mut env = TypeEnvironment::new(&mut self.errors);

        env.unify(Type::Boolean, condition_type, &if_.condition.span());
        if let Some((clause, type_)) = else_type {
            consequence_type = env.unify(consequence_type, type_, &clause.span());
        }

        env.resolve(consequence_type)
    }

    pub fn check_import(&mut self, import: &'a Import) -> Type {
        if let Some(t) = self.cache.get(&import.id) {
            return t.clone();
        }
        let type_ = self
            .import_map
            .resolve(import)
            .map(|i| match i {
                External::Module(m) => self.check_module(m),
                External::Plugin(p) => p.module_type(),
            })
            .unwrap_or_else(|| {
                self.errors.push(TypeError::UnresolvedImport {
                    name: import.identifier.symbol.clone(),
                    span: import.span.clone(),
                });
                Type::default()
            });
        self.cache.insert(import.id, type_.clone());
        type_
    }

    pub fn check_type_definition(&mut self, typedef: &'a TypeDefinition) -> Type {
        if let Some(t) = self.cache.get(&typedef.id) {
            return t.clone();
        }

        if let Some(c) = self.checking.get_mut(&typedef.id) {
            let type_ = Type::open();
            c.push(type_.clone());
            return type_;
        }

        self.checking.insert(typedef.id, vec![]);

        let type_ = self.check_type_expression(&typedef.type_);
        let type_ = Type::Named(typedef.identifier.symbol.to_string(), Box::new(type_));

        let mut env = TypeEnvironment::new(&mut self.errors);

        let mut type_ = type_;
        for recursive in self.checking.remove(&typedef.id).unwrap_or_default() {
            type_ = env.unify(type_, recursive, &typedef.type_.span());
        }

        self.cache.insert(typedef.id, type_.clone());
        type_
    }

    pub fn check_expression(&mut self, expression: &'a Expression) -> Type {
        match expression {
            Expression::StringLiteral(_) => Type::String,
            Expression::IntegerLiteral(_) => Type::Integer,
            Expression::BooleanLiteral(_) => Type::Boolean,
            Expression::Identifier(id) => self
                .symbols
                .declaration(id)
                .map(|d| self.check_declaration(d))
                .unwrap_or_default(),
            Expression::Record(r) => self.check_record(r),
            Expression::Function(f) => self.check_function(f),
            Expression::Call(c) => self.check_call(c),
            Expression::Construct(c) => self.check_construct(c),
            Expression::MemberAccess(ma) => self.check_member_access(ma),
            Expression::BinaryOperation(op) => self.check_binary_operation(op),
            Expression::List(list) => self.check_list(list),
            Expression::Nil(_) => Type::Optional(Box::new(Type::open())),
        }
    }

    pub fn check_list(&mut self, list: &'a List) -> Type {
        let mut list_type = Type::default();

        let elements = list
            .elements
            .iter()
            .map(|e| (e, self.check_expression(e)))
            .collect::<Vec<_>>();

        let mut env = TypeEnvironment::new(&mut self.errors);
        for (node, element_type) in elements {
            list_type = env.unify(list_type, element_type, &node.span());
        }

        Type::List(Box::new(env.resolve(list_type)))
    }

    pub fn check_binary_operation(&mut self, operation: &'a BinaryOperation) -> Type {
        let mut type_ = Type::default();

        let lhs_type = self.check_expression(&operation.lhs);
        let rhs_type = self.check_expression(&operation.rhs);

        let mut env = TypeEnvironment::new(&mut self.errors);

        type_ = env.unify(type_, lhs_type, &operation.span);
        type_ = env.unify(type_, rhs_type, &operation.span);

        env.resolve(match &operation.operator.kind {
            BinaryOperatorKind::LessThan
            | BinaryOperatorKind::LessThanOrEqualTo
            | BinaryOperatorKind::EqualTo
            | BinaryOperatorKind::NotEqualTo
            | BinaryOperatorKind::GreaterThanOrEqualTo
            | BinaryOperatorKind::GreaterThan => Type::Boolean,

            _ => type_,
        })
    }

    pub fn check_member_access(&mut self, member_access: &'a MemberAccess) -> Type {
        let return_type = Type::default();
        let type_ = Type::Record(vec![(
            member_access.identifier.symbol.clone(),
            return_type.clone(),
        )]);

        let subject_type = self.check_expression(&member_access.subject);

        let mut env = TypeEnvironment::new(&mut self.errors);

        env.unify(type_, subject_type, &member_access.span);

        env.resolve(return_type)
    }

    pub fn check_construct(&mut self, construct: &'a Construct) -> Type {
        let return_type = Type::default();

        let type_ = Type::Function(
            vec![self.check_record(&construct.record)],
            Box::new(return_type.clone()),
        );

        let callee_type = self.check_expression(&construct.subject);

        let mut env = TypeEnvironment::new(&mut self.errors);

        env.unify(callee_type, type_, &construct.span);

        env.resolve(return_type)
    }

    pub fn check_call(&mut self, call: &'a Call) -> Type {
        let return_type = Type::default();

        let type_ = Type::Function(
            call.arguments
                .iter()
                .map(|arg| self.check_expression(arg))
                .collect(),
            Box::new(return_type.clone()),
        );

        let callee_type = self.check_expression(&call.callee);

        let mut env = TypeEnvironment::new(&mut self.errors);

        env.unify(callee_type, type_, &call.span);

        env.resolve(return_type)
    }

    pub fn check_block(&mut self, block: &'a Block) -> Type {
        let mut return_type = Type::Void;
        for statement in &block.statements {
            let t = self.check_statement(statement);
            if !matches!(t, Type::Void) {
                return_type = t;
            }
        }
        return_type
    }

    pub fn check_function(&mut self, function: &'a Function) -> Type {
        let parameter_types = function
            .parameters
            .iter()
            .map(|param| {
                param
                    .type_
                    .as_ref()
                    .map(|pt| self.check_type_expression(pt))
                    .unwrap_or_default()
            })
            .collect();

        let return_type = function
            .return_type
            .as_ref()
            .map(|rt| self.check_type_expression(rt))
            .unwrap_or_default();

        let actual_return_type = self.check_block(&function.body);

        let mut env = TypeEnvironment::new(&mut self.errors);

        env.unify(return_type.clone(), actual_return_type, &function.span);

        env.resolve(Type::Function(parameter_types, Box::new(return_type)))
    }

    pub fn check_type_expression(&mut self, type_expression: &'a TypeExpression) -> Type {
        match type_expression {
            TypeExpression::Identifier(id) => self
                .symbols
                .declaration(id)
                .map(|d| Type::Named(id.symbol.clone(), Box::new(self.check_declaration(d))))
                .unwrap_or_default(),
            TypeExpression::Record(r) => self.check_type_record(r),
            TypeExpression::List(l) => {
                Type::List(Box::new(self.check_type_expression(&l.element_type)))
            }
            TypeExpression::Optional(o) => {
                let type_ = self.check_type_expression(&o.type_);
                Type::Optional(Box::new(type_))
            }
        }
    }

    pub fn check_declaration(&mut self, declaration: Declaration<'a>) -> Type {
        match declaration {
            Declaration::Parameter(p) => p
                .type_
                .as_ref()
                .map(|t| self.check_type_expression(t))
                .unwrap_or_default(),
            Declaration::Assignment(a) => self.check_assignment(a),
            Declaration::TypeDefinition(td) => self.check_type_definition(td),
            Declaration::Import(i) => self.check_import(i),
            Declaration::Builtin(_, "String") => Type::String,
            Declaration::Builtin(_, "Integer") => Type::Integer,
            Declaration::Builtin(_, "Boolean") => Type::Boolean,
            Declaration::Builtin(_, s) => panic!("unknown builtin {:?}", s),
        }
    }

    pub fn check_assignment(&mut self, assignment: &'a Assignment) -> Type {
        if let Some(t) = self.cache.get(&assignment.id) {
            return t.clone();
        }

        if let Some(c) = self.checking.get_mut(&assignment.id) {
            let type_ = Type::open();
            c.push(type_.clone());
            return type_;
        }

        self.checking.insert(assignment.id, vec![]);

        let type_ = Type::open();

        let type_expression_type = assignment
            .type_
            .as_ref()
            .map(|te| self.check_type_expression(te))
            .unwrap_or_default();
        let expression_type = self.check_expression(&assignment.value);

        let mut errors = vec![];
        let mut env = TypeEnvironment::new(&mut errors);

        let type_ = env.unify(type_, type_expression_type, &assignment.value.span());
        let type_ = env.unify(type_, expression_type, &assignment.value.span());

        let mut type_ = type_;
        for recursive in self.checking.remove(&assignment.id).unwrap_or_default() {
            type_ = env.unify(type_, recursive, &assignment.value.span());
        }

        self.cache.insert(assignment.id, type_.clone());

        let t = env.resolve(type_);
        self.errors.extend(errors);
        t
    }

    pub fn check_record(&mut self, record: &'a Record<Expression>) -> Type {
        Type::Record(
            record
                .fields
                .iter()
                .map(|f| (f.identifier.symbol.clone(), self.check_expression(&f.value)))
                .collect(),
        )
    }

    pub fn check_type_record(&mut self, record: &'a Record<TypeExpression>) -> Type {
        Type::Record(
            record
                .fields
                .iter()
                .map(|f| {
                    (
                        f.identifier.symbol.clone(),
                        self.check_type_expression(&f.value),
                    )
                })
                .collect(),
        )
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TypeId(u64);

static ID_GEN: AtomicU64 = AtomicU64::new(0);

impl TypeId {
    pub fn new() -> Self {
        Self(ID_GEN.fetch_add(1, SeqCst))
    }

    pub fn preload(i: u64) {
        ID_GEN.fetch_add(1000 * i, SeqCst);
    }
}

impl Default for TypeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for TypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{:0>4x}", self.0)
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Type {
    Void,
    String,
    Integer,
    Float,
    Boolean,
    Open(TypeId),
    Record(Vec<(String, Type)>),
    Named(String, Box<Type>),
    Function(Vec<Type>, Box<Type>),
    List(Box<Type>),
    Map(Box<Type>, Box<Type>),
    Optional(Box<Type>),
}

impl Type {
    pub fn open() -> Self {
        Self::Open(Default::default())
    }

    pub fn named(n: impl Into<String>, t: Type) -> Self {
        Self::Named(n.into(), Box::new(t))
    }

    pub fn function(a: impl IntoIterator<Item = Type>, r: Type) -> Self {
        Self::Function(a.into_iter().collect(), Box::new(r))
    }

    pub fn optional(t: impl Into<Type>) -> Self {
        Self::Optional(Box::new(t.into()))
    }

    pub fn record(i: impl IntoIterator<Item = (impl Into<String>, Type)>) -> Self {
        Self::Record(i.into_iter().map(|(n, t)| (n.into(), t)).collect())
    }

    pub fn list(t: impl Into<Type>) -> Self {
        Self::List(Box::new(t.into()))
    }

    pub fn return_type(&self) -> &Type {
        if let Type::Function(_, r) = self {
            &*r
        } else {
            panic!("{:?} is not a function", self)
        }
    }

    pub fn is_optional(&self) -> bool {
        matches!(self, Type::Optional(_))
    }

    pub fn into_named(self, name: impl Into<String>) -> Self {
        Self::Named(name.into(), Box::new(self))
    }
}

struct PrettyTypeDebug<'a> {
    type_: &'a Type,
    open_types: &'a RefCell<Vec<TypeId>>,
}

impl<'a> PrettyTypeDebug<'a> {
    const TYPE_VAR_CHARS: [char; 26] = [
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
        's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    ];

    pub fn inner<'b>(&'b self, type_: &'b Type) -> PrettyTypeDebug {
        PrettyTypeDebug {
            type_,
            open_types: self.open_types,
        }
    }
}

impl<'a> fmt::Debug for PrettyTypeDebug<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.type_ {
            Type::Void => write!(f, "Void"),
            Type::String => write!(f, "String"),
            Type::Integer => write!(f, "Integer"),
            Type::Float => write!(f, "Float"),
            Type::Boolean => write!(f, "Boolean"),
            Type::Map(key, value) => {
                let mut s = f.debug_map();
                s.entry(&self.inner(&key), &self.inner(&value));
                s.finish()
            }
            Type::List(element) => {
                let mut s = f.debug_list();
                s.entry(&self.inner(&element));
                s.finish()
            }
            Type::Record(fields) => {
                let mut s = f.debug_map();
                for field in fields {
                    s.entry(&DisplayAsDebug(&field.0), &self.inner(&field.1));
                }
                s.finish()
            }
            Type::Open(id) => {
                let mut open_types = self.open_types.borrow_mut();
                let idx = open_types
                    .iter()
                    .enumerate()
                    .find(|(_, oid)| *oid == id)
                    .map(|(idx, _)| idx)
                    .unwrap_or_else(|| {
                        let idx = open_types.len();
                        open_types.push(*id);
                        idx
                    });
                let idx = idx % Self::TYPE_VAR_CHARS.len();
                let ch = Self::TYPE_VAR_CHARS[idx];
                write!(f, "{}{:?}", ch, id)
            }
            Type::Named(n, _) => write!(f, "{}", n),
            Type::Function(p, r) => {
                let mut t = f.debug_tuple("");
                for param in p {
                    t.field(&self.inner(param));
                }
                t.finish()?;

                write!(f, " -> ")?;

                self.inner(r).fmt(f)
            }
            Type::Optional(t) => {
                self.inner(t).fmt(f)?;
                write!(f, "?")
            }
        }
    }
}

impl Default for Type {
    fn default() -> Self {
        Self::open()
    }
}

impl fmt::Debug for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        PrettyTypeDebug {
            type_: self,
            open_types: &mut Default::default(),
        }
        .fmt(f)
    }
}
