use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;

use super::{HasSpan, ParseError, ParseResult, Parser, Span, Token, TokenKind};

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(u64);

static ID_GEN: AtomicU64 = AtomicU64::new(0);

impl NodeId {
    pub fn new() -> Self {
        Self(ID_GEN.fetch_add(1, SeqCst))
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{:0>4x}", self.0)
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(unused)]
pub trait Visitor<'a> {
    fn enter_module(&mut self, module: &'a Module) {}
    fn leave_module(&mut self, module: &'a Module) {}

    fn enter_statement(&mut self, statement: &'a Statement) {}
    fn leave_statement(&mut self, statement: &'a Statement) {}

    fn enter_expression(&mut self, expression: &'a Expression) {}
    fn leave_expression(&mut self, expression: &'a Expression) {}

    fn visit_identifier(&mut self, identifier: &'a Identifier) {}

    fn visit_string_literal(&mut self, string_literal: &'a StringLiteral) {}

    fn visit_integer_literal(&mut self, integer_literal: &'a IntegerLiteral) {}

    fn visit_boolean_literal(&mut self, boolean_literal: &'a BooleanLiteral) {}

    fn enter_member_access(&mut self, member_access: &'a MemberAccess) {}
    fn leave_member_access(&mut self, member_access: &'a MemberAccess) {}

    fn enter_assignment(&mut self, assignment: &'a Assignment) {}
    fn leave_assignment(&mut self, assignment: &'a Assignment) {}

    fn enter_type_definition(&mut self, type_definition: &'a TypeDefinition) {}
    fn leave_type_definition(&mut self, type_definition: &'a TypeDefinition) {}

    fn enter_field<T: Visitable>(&mut self, field: &'a Field<T>) {}
    fn leave_field<T: Visitable>(&mut self, field: &'a Field<T>) {}

    fn enter_construct(&mut self, construct: &'a Construct) {}
    fn leave_construct(&mut self, construct: &'a Construct) {}

    fn enter_record<T: Visitable>(&mut self, record: &'a Record<T>) {}
    fn leave_record<T: Visitable>(&mut self, record: &'a Record<T>) {}

    fn enter_type_expression(&mut self, type_expression: &'a TypeExpression) {}
    fn leave_type_expression(&mut self, type_expression: &'a TypeExpression) {}

    fn enter_function(&mut self, function: &'a Function) {}
    fn leave_function(&mut self, function: &'a Function) {}

    fn enter_parameter(&mut self, parameter: &'a Parameter) {}
    fn leave_parameter(&mut self, parameter: &'a Parameter) {}

    fn enter_return(&mut self, return_: &'a Return) {}
    fn leave_return(&mut self, return_: &'a Return) {}

    fn enter_debug(&mut self, debug: &'a Debug) {}
    fn leave_debug(&mut self, debug: &'a Debug) {}

    fn enter_call(&mut self, call: &'a Call) {}
    fn leave_call(&mut self, call: &'a Call) {}

    fn enter_import(&mut self, import: &'a Import) {}
    fn leave_import(&mut self, import: &'a Import) {}

    fn enter_if(&mut self, if_: &'a If) {}
    fn leave_if(&mut self, if_: &'a If) {}

    fn enter_block(&mut self, block: &'a Block) {}
    fn leave_block(&mut self, block: &'a Block) {}
}

pub trait Visitable {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>);
}

#[derive(Debug)]
pub struct Module {
    pub id: NodeId,
    pub span: Span,
    pub name: Option<String>,
    pub statements: Vec<Statement>,
}

impl Visitable for Module {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.enter_module(self);
        for statement in self.statements.iter() {
            statement.visit(visitor);
        }
        visitor.leave_module(self);
    }
}

impl Parser for Module {
    type Output = Self;

    fn parse<'a>(mut tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        let mut statements = vec![];
        let start = tokens.get(0).map(|t| t.span.start).unwrap_or_default();
        let mut end = start;
        while !tokens.is_empty() {
            let statement;
            (statement, tokens) = Statement::parse(tokens)?;
            end = statement.span().end;
            statements.push(statement);
        }
        Ok((
            Module {
                id: NodeId::new(),
                name: None,
                span: start..end,
                statements,
            },
            tokens,
        ))
    }
}

#[derive(Debug)]
pub enum Statement {
    Expression(Expression),
    Assignment(Assignment),
    TypeDefinition(TypeDefinition),
    Return(Return),
    Debug(Debug),
    Import(Import),
    If(Box<If>),
    Block(Block),
}

impl Visitable for Statement {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.enter_statement(self);
        match self {
            Statement::Expression(n) => n.visit(visitor),
            Statement::Assignment(n) => n.visit(visitor),
            Statement::TypeDefinition(n) => n.visit(visitor),
            Statement::Return(n) => n.visit(visitor),
            Statement::Debug(n) => n.visit(visitor),
            Statement::Import(n) => n.visit(visitor),
            Statement::If(n) => n.visit(visitor),
            Statement::Block(n) => n.visit(visitor),
        }
        visitor.leave_statement(self);
    }
}

impl HasSpan for Statement {
    fn span(&self) -> Span {
        match self {
            Statement::Expression(e) => e.span(),
            Statement::Assignment(a) => a.span.clone(),
            Statement::TypeDefinition(a) => a.span.clone(),
            Statement::Return(r) => r.span.clone(),
            Statement::Debug(r) => r.span.clone(),
            Statement::Import(r) => r.span.clone(),
            Statement::If(r) => r.span.clone(),
            Statement::Block(r) => r.span.clone(),
        }
    }
}

impl Parser for Statement {
    type Output = Self;

    fn parse<'a>(tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        match tokens.get(0) {
            Some(Token {
                kind: TokenKind::TypeKeyword,
                ..
            }) => {
                let (def, tokens) = TypeDefinition::parse(tokens)?;
                return Ok((Statement::TypeDefinition(def), tokens));
            }

            Some(Token {
                kind: TokenKind::ReturnKeyword,
                ..
            }) => {
                let (ret, tokens) = Return::parse(tokens)?;
                return Ok((Statement::Return(ret), tokens));
            }

            Some(Token {
                kind: TokenKind::DebugKeyword,
                ..
            }) => {
                let (ret, tokens) = Debug::parse(tokens)?;
                return Ok((Statement::Debug(ret), tokens));
            }

            Some(Token {
                kind: TokenKind::ImportKeyword,
                ..
            }) => {
                let (import, tokens) = Import::parse(tokens)?;
                return Ok((Statement::Import(import), tokens));
            }

            Some(Token {
                kind: TokenKind::IfKeyword,
                ..
            }) => {
                let (if_, tokens) = If::parse(tokens)?;
                return Ok((Statement::If(Box::new(if_)), tokens));
            }

            Some(Token {
                kind: TokenKind::OpenCurly,
                ..
            }) => {
                let (block, tokens) = Block::parse(tokens)?;
                return Ok((Statement::Block(block), tokens));
            }

            _ => {}
        }

        let (expression, tokens) = Expression::parse(tokens)?;
        match tokens.get(0) {
            Some(Token {
                kind: TokenKind::EqualSign,
                ..
            }) => {
                if let Expression::Identifier(identifier) = expression {
                    let tokens = &tokens[1..];
                    let (value, tokens) = Expression::parse(tokens)?;
                    return Ok((
                        Statement::Assignment(Assignment {
                            id: NodeId::new(),
                            span: identifier.span.start..value.span().end,
                            type_: None,
                            identifier,
                            value,
                        }),
                        tokens,
                    ));
                }
            }

            Some(Token {
                kind: TokenKind::Colon,
                ..
            }) => {
                if let Expression::Identifier(identifier) = expression {
                    let tokens = &tokens[1..];
                    let (type_, tokens) = TypeExpression::parse(tokens)?;
                    match tokens.get(0) {
                        Some(Token {
                            kind: TokenKind::EqualSign,
                            ..
                        }) => {
                            let tokens = &tokens[1..];
                            let (value, tokens) = Expression::parse(tokens)?;
                            return Ok((
                                Statement::Assignment(Assignment {
                                    id: NodeId::new(),
                                    span: identifier.span.start..value.span().end,
                                    type_: Some(type_),
                                    identifier,
                                    value,
                                }),
                                tokens,
                            ));
                        }
                        t => {
                            return Err(ParseError::Expected(
                                "equal sign",
                                t.map(|t| t.span.clone()),
                            ))
                        }
                    }
                }
            }
            _ => {}
        }
        Ok((Statement::Expression(expression), tokens))
    }
}

#[derive(Debug)]
pub enum Expression {
    StringLiteral(StringLiteral),
    IntegerLiteral(IntegerLiteral),
    BooleanLiteral(BooleanLiteral),
    Identifier(Identifier),
    Construct(Box<Construct>),
    Record(Record<Expression>),
    MemberAccess(Box<MemberAccess>),
    Function(Box<Function>),
    Call(Box<Call>),
}

impl Visitable for Expression {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.enter_expression(self);
        match self {
            Expression::StringLiteral(n) => n.visit(visitor),
            Expression::IntegerLiteral(n) => n.visit(visitor),
            Expression::BooleanLiteral(n) => n.visit(visitor),
            Expression::Identifier(n) => n.visit(visitor),
            Expression::Construct(n) => n.visit(visitor),
            Expression::Record(n) => n.visit(visitor),
            Expression::MemberAccess(n) => n.visit(visitor),
            Expression::Function(n) => n.visit(visitor),
            Expression::Call(n) => n.visit(visitor),
        }
        visitor.leave_expression(self);
    }
}

impl HasSpan for Expression {
    fn span(&self) -> Span {
        match self {
            Expression::StringLiteral(n) => n.span.clone(),
            Expression::IntegerLiteral(n) => n.span.clone(),
            Expression::BooleanLiteral(n) => n.span.clone(),
            Expression::Construct(n) => n.span.clone(),
            Expression::Identifier(n) => n.span.clone(),
            Expression::Record(n) => n.span.clone(),
            Expression::MemberAccess(n) => n.span.clone(),
            Expression::Function(n) => n.span.clone(),
            Expression::Call(n) => n.span.clone(),
        }
    }
}

struct LeafExpression;

impl Parser for LeafExpression {
    type Output = Expression;

    fn parse<'a>(tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        match tokens.get(0) {
            Some(Token {
                kind: TokenKind::StringLiteral(_, _),
                ..
            }) => {
                let (literal, tokens) = StringLiteral::parse(tokens)?;
                Ok((Expression::StringLiteral(literal), tokens))
            }

            Some(Token {
                kind: TokenKind::Integer(_),
                ..
            }) => {
                let (literal, tokens) = IntegerLiteral::parse(tokens)?;
                Ok((Expression::IntegerLiteral(literal), tokens))
            }

            Some(Token {
                kind: TokenKind::TrueKeyword | TokenKind::FalseKeyword,
                ..
            }) => {
                let (literal, tokens) = BooleanLiteral::parse(tokens)?;
                Ok((Expression::BooleanLiteral(literal), tokens))
            }

            Some(Token {
                kind: TokenKind::Symbol(_),
                ..
            }) => {
                let (id, tokens) = Identifier::parse(tokens)?;
                Ok((Expression::Identifier(id), tokens))
            }

            Some(Token {
                kind: TokenKind::OpenCurly,
                ..
            }) => {
                let (record, tokens) = Record::parse(tokens)?;
                Ok((Expression::Record(record), tokens))
            }

            Some(Token {
                kind: TokenKind::FnKeyword,
                ..
            }) => {
                let (function, tokens) = Function::parse(tokens)?;
                Ok((Expression::Function(Box::new(function)), tokens))
            }

            t => Err(ParseError::Expected(
                "expression",
                t.map(|t| t.span.clone()),
            )),
        }
    }
}

impl Parser for Expression {
    type Output = Self;

    fn parse<'a>(mut tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        let mut expression;
        (expression, tokens) = LeafExpression::parse(tokens)?;

        loop {
            match tokens.get(0) {
                Some(Token {
                    kind: TokenKind::OpenCurly,
                    ..
                }) => {
                    let record;
                    (record, tokens) = Record::parse(tokens)?;
                    expression = Expression::Construct(Box::new(Construct {
                        span: expression.span().start..record.span.end,
                        subject: expression,
                        record,
                    }))
                }

                Some(Token {
                    kind: TokenKind::Period,
                    ..
                }) => {
                    tokens = &tokens[1..];
                    let identifier;
                    (identifier, tokens) = Identifier::parse(tokens)?;
                    expression = Expression::MemberAccess(Box::new(MemberAccess {
                        span: expression.span().start..identifier.span.end,
                        subject: expression,
                        identifier,
                    }))
                }

                Some(Token {
                    kind: TokenKind::OpenParen,
                    ..
                }) => {
                    tokens = &tokens[1..];

                    let mut arguments = vec![];
                    while tokens
                        .get(0)
                        .map(|t| !matches!(t.kind, TokenKind::CloseParen))
                        .unwrap_or(false)
                    {
                        let argument;
                        (argument, tokens) = Expression::parse(tokens)?;
                        arguments.push(argument);

                        if let Some(Token {
                            kind: TokenKind::Comma,
                            ..
                        }) = tokens.get(0)
                        {
                            tokens = &tokens[1..];
                        }
                    }

                    let end;
                    if let Some(Token {
                        kind: TokenKind::CloseParen,
                        span,
                    }) = tokens.get(0)
                    {
                        tokens = &tokens[1..];
                        end = span.end;
                    } else {
                        return Err(ParseError::Expected(
                            "end of argument list",
                            tokens.get(0).map(|t| t.span.clone()),
                        ));
                    }

                    expression = Expression::Call(Box::new(Call {
                        span: expression.span().start..end,
                        callee: expression,
                        arguments,
                    }));
                }

                _ => return Ok((expression, tokens)),
            }
        }
    }
}

#[derive(Debug)]
pub struct IntegerLiteral {
    pub span: Span,
    pub value: i128,
}

impl Visitable for IntegerLiteral {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.visit_integer_literal(self);
    }
}

impl Parser for IntegerLiteral {
    type Output = Self;

    fn parse<'a>(tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        match tokens.get(0) {
            Some(Token {
                span,
                kind: TokenKind::Integer(value),
            }) => {
                return Ok((
                    IntegerLiteral {
                        span: span.clone(),
                        value: *value,
                    },
                    &tokens[1..],
                ));
            }
            t => Err(ParseError::Expected(
                "integer literal",
                t.map(|t| t.span.clone()),
            )),
        }
    }
}

#[derive(Debug)]
pub struct BooleanLiteral {
    pub span: Span,
    pub value: bool,
}

impl Visitable for BooleanLiteral {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.visit_boolean_literal(self);
    }
}

impl Parser for BooleanLiteral {
    type Output = Self;

    fn parse<'a>(tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        match tokens.get(0) {
            Some(Token {
                span,
                kind: TokenKind::TrueKeyword,
            }) => {
                return Ok((
                    BooleanLiteral {
                        span: span.clone(),
                        value: true,
                    },
                    &tokens[1..],
                ));
            }
            Some(Token {
                span,
                kind: TokenKind::FalseKeyword,
            }) => {
                return Ok((
                    BooleanLiteral {
                        span: span.clone(),
                        value: false,
                    },
                    &tokens[1..],
                ));
            }
            t => Err(ParseError::Expected(
                "boolean literal",
                t.map(|t| t.span.clone()),
            )),
        }
    }
}

#[derive(Debug)]
pub struct StringLiteral {
    pub span: Span,
    pub value: String,
}

impl Visitable for StringLiteral {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.visit_string_literal(self);
    }
}

impl Parser for StringLiteral {
    type Output = Self;

    fn parse<'a>(tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        match tokens.get(0) {
            Some(Token {
                span,
                kind: TokenKind::StringLiteral(value, _),
            }) => {
                return Ok((
                    StringLiteral {
                        span: span.clone(),
                        value: value.to_string(),
                    },
                    &tokens[1..],
                ));
            }
            t => Err(ParseError::Expected(
                "string literal",
                t.map(|t| t.span.clone()),
            )),
        }
    }
}

#[derive(Debug)]
pub struct Construct {
    pub span: Span,
    pub subject: Expression,
    pub record: Record<Expression>,
}

impl Visitable for Construct {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.enter_construct(self);
        self.subject.visit(visitor);
        self.record.visit(visitor);
        visitor.leave_construct(self);
    }
}

#[derive(Debug)]
pub struct Record<T> {
    pub span: Span,
    pub fields: Vec<Field<T>>,
}

impl<T: Visitable> Visitable for Record<T> {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.enter_record(self);
        for field in self.fields.iter() {
            field.visit(visitor);
        }
        visitor.leave_record(self);
    }
}

impl<T: Parser<Output = T> + HasSpan> Parser for Record<T> {
    type Output = Self;

    fn parse<'a>(mut tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        let start;
        if let Some(Token {
            kind: TokenKind::OpenCurly,
            ..
        }) = tokens.get(0)
        {
            start = tokens[0].span.start;
            tokens = &tokens[1..];
        } else {
            return Err(ParseError::Expected(
                "record",
                tokens.get(0).map(|t| t.span.clone()),
            ));
        }

        let mut fields = vec![];
        while tokens
            .get(0)
            .map(|t| !matches!(t.kind, TokenKind::CloseCurly))
            .unwrap_or(false)
        {
            let field;
            (field, tokens) = Field::parse(tokens)?;
            fields.push(field);

            if let Some(Token {
                kind: TokenKind::Comma,
                ..
            }) = tokens.get(0)
            {
                tokens = &tokens[1..];
            }
        }

        let end;
        if let Some(Token {
            kind: TokenKind::CloseCurly,
            ..
        }) = tokens.get(0)
        {
            end = tokens[0].span.end;
            tokens = &tokens[1..];
        } else {
            return Err(ParseError::Expected(
                "record terminating curly brace",
                tokens.get(0).map(|t| t.span.clone()),
            ));
        }

        Ok((
            Record {
                span: start..end,
                fields,
            },
            tokens,
        ))
    }
}

#[derive(Debug)]
pub struct Field<T> {
    pub span: Span,
    pub identifier: Identifier,
    pub value: T,
}

impl<T: Visitable> Visitable for Field<T> {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.enter_field(self);
        self.identifier.visit(visitor);
        self.value.visit(visitor);
        visitor.leave_field(self);
    }
}

impl<T: Parser<Output = T> + HasSpan> Parser for Field<T> {
    type Output = Self;

    fn parse<'a>(tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        let (identifier, mut tokens) = Identifier::parse(tokens)?;

        if let Some(Token {
            kind: TokenKind::Colon,
            ..
        }) = tokens.get(0)
        {
            tokens = &tokens[1..];
        } else {
            return Err(ParseError::Expected(
                "colon",
                tokens.get(0).map(|t| t.span.clone()),
            ));
        }

        let (value, tokens) = T::parse(tokens)?;

        Ok((
            Field {
                span: identifier.span.start..value.span().end,
                identifier,
                value,
            },
            tokens,
        ))
    }
}

#[derive(Debug)]
pub struct Identifier {
    pub id: NodeId,
    pub span: Span,
    pub symbol: String,
}

impl Visitable for Identifier {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.visit_identifier(self);
    }
}

impl PartialEq for Identifier {
    fn eq(&self, other: &Self) -> bool {
        self.symbol == other.symbol
    }
}

impl Parser for Identifier {
    type Output = Self;

    fn parse<'a>(tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        if let Some(Token {
            kind: TokenKind::Symbol(symbol),
            span,
        }) = tokens.get(0)
        {
            Ok((
                Identifier {
                    id: NodeId::new(),
                    span: span.clone(),
                    symbol: symbol.to_string(),
                },
                &tokens[1..],
            ))
        } else {
            return Err(ParseError::Expected(
                "identifier",
                tokens.get(0).map(|t| t.span.clone()),
            ));
        }
    }
}

#[derive(Debug)]
pub struct MemberAccess {
    pub span: Span,
    pub subject: Expression,
    pub identifier: Identifier,
}

impl Visitable for MemberAccess {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.enter_member_access(self);
        self.subject.visit(visitor);
        self.identifier.visit(visitor);
        visitor.leave_member_access(self);
    }
}

#[derive(Debug)]
pub struct Assignment {
    pub id: NodeId,
    pub span: Span,
    pub identifier: Identifier,
    pub type_: Option<TypeExpression>,
    pub value: Expression,
}

impl Visitable for Assignment {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.enter_assignment(self);
        self.identifier.visit(visitor);
        if let Some(type_) = &self.type_ {
            type_.visit(visitor);
        }
        self.value.visit(visitor);
        visitor.leave_assignment(self);
    }
}

#[derive(Debug)]
pub struct TypeDefinition {
    pub id: NodeId,
    pub span: Span,
    pub identifier: Identifier,
    pub type_: TypeExpression,
}

impl Visitable for TypeDefinition {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.enter_type_definition(self);
        self.identifier.visit(visitor);
        self.type_.visit(visitor);
        visitor.leave_type_definition(self);
    }
}

impl Parser for TypeDefinition {
    type Output = Self;

    fn parse<'a>(mut tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        let start;
        if let Some(Token {
            kind: TokenKind::TypeKeyword,
            ..
        }) = tokens.get(0)
        {
            start = tokens[0].span.start;
            tokens = &tokens[1..];
        } else {
            return Err(ParseError::Expected(
                "type definition",
                tokens.get(0).map(|t| t.span.clone()),
            ));
        }

        let (identifier, mut tokens) = Identifier::parse(tokens)?;

        if let Some(Token {
            kind: TokenKind::EqualSign,
            ..
        }) = tokens.get(0)
        {
            tokens = &tokens[1..];
        } else {
            return Err(ParseError::Expected(
                "equal sign",
                tokens.get(0).map(|t| t.span.clone()),
            ));
        }

        let (type_, tokens) = TypeExpression::parse(tokens)?;

        Ok((
            TypeDefinition {
                id: NodeId::new(),
                span: start..type_.span().end,
                identifier,
                type_,
            },
            tokens,
        ))
    }
}

#[derive(Debug)]
pub enum TypeExpression {
    Identifier(Identifier),
    Record(Record<TypeExpression>),
}

impl Visitable for TypeExpression {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.enter_type_expression(self);
        match self {
            TypeExpression::Identifier(id) => id.visit(visitor),
            TypeExpression::Record(r) => r.visit(visitor),
        }
        visitor.leave_type_expression(self);
    }
}

impl HasSpan for TypeExpression {
    fn span(&self) -> Span {
        match self {
            TypeExpression::Identifier(id) => id.span.clone(),
            TypeExpression::Record(r) => r.span.clone(),
        }
    }
}

impl Parser for TypeExpression {
    type Output = Self;

    fn parse<'a>(tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        match tokens.get(0) {
            Some(Token {
                kind: TokenKind::OpenCurly,
                ..
            }) => {
                let (record, tokens) = Record::parse(tokens)?;
                Ok((TypeExpression::Record(record), tokens))
            }

            Some(Token {
                kind: TokenKind::Symbol(_),
                ..
            }) => {
                let (id, tokens) = Identifier::parse(tokens)?;
                Ok((TypeExpression::Identifier(id), tokens))
            }

            t => Err(ParseError::Expected(
                "type expression",
                t.map(|t| t.span.clone()),
            )),
        }
    }
}

#[derive(Debug)]
pub struct Function {
    pub span: Span,
    pub parameters: Vec<Parameter>,
    pub return_type: Option<TypeExpression>,
    pub body: Block,
}

impl Visitable for Function {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.enter_function(self);
        for parameter in &self.parameters {
            parameter.visit(visitor);
        }
        if let Some(return_type) = &self.return_type {
            return_type.visit(visitor);
        }
        self.body.visit(visitor);
        visitor.leave_function(self);
    }
}

impl Parser for Function {
    type Output = Self;

    fn parse<'a>(mut tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        let start;
        if let Some(Token {
            kind: TokenKind::FnKeyword,
            ..
        }) = tokens.get(0)
        {
            start = tokens[0].span.start;
            tokens = &tokens[1..];
        } else {
            return Err(ParseError::Expected(
                "function",
                tokens.get(0).map(|t| t.span.clone()),
            ));
        }

        if let Some(Token {
            kind: TokenKind::OpenParen,
            ..
        }) = tokens.get(0)
        {
            tokens = &tokens[1..];
        } else {
            return Err(ParseError::Expected(
                "parameter list",
                tokens.get(0).map(|t| t.span.clone()),
            ));
        }

        let mut parameters = vec![];
        while tokens
            .get(0)
            .map(|t| !matches!(t.kind, TokenKind::CloseParen))
            .unwrap_or(false)
        {
            let parameter;
            (parameter, tokens) = Parameter::parse(tokens)?;
            parameters.push(parameter);
        }

        if let Some(Token {
            kind: TokenKind::CloseParen,
            ..
        }) = tokens.get(0)
        {
            tokens = &tokens[1..];
        } else {
            return Err(ParseError::Expected(
                "parenthesis terminating parameter list",
                tokens.get(0).map(|t| t.span.clone()),
            ));
        }

        let mut return_type = None;
        if let Some(Token {
            kind: TokenKind::Arrow,
            ..
        }) = tokens.get(0)
        {
            tokens = &tokens[1..];

            let t;
            (t, tokens) = TypeExpression::parse(tokens)?;
            return_type = Some(t);
        }

        let (body, tokens) = Block::parse(tokens)?;

        Ok((
            Function {
                span: start..body.span.end,
                parameters,
                return_type,
                body,
            },
            tokens,
        ))
    }
}

#[derive(Debug)]
pub struct Parameter {
    pub id: NodeId,
    pub span: Span,
    pub identifier: Identifier,
    pub type_: Option<TypeExpression>,
}

impl Visitable for Parameter {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.enter_parameter(self);
        self.identifier.visit(visitor);
        if let Some(type_) = &self.type_ {
            type_.visit(visitor);
        }
        visitor.leave_parameter(self);
    }
}

impl Parser for Parameter {
    type Output = Self;

    fn parse<'a>(tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        let (identifier, tokens) = Identifier::parse(tokens)?;

        if let Some(Token {
            kind: TokenKind::Colon,
            ..
        }) = tokens.get(0)
        {
            let tokens = &tokens[1..];

            let (type_, tokens) = TypeExpression::parse(tokens)?;

            Ok((
                Parameter {
                    id: NodeId::default(),
                    span: identifier.span.start..type_.span().end,
                    identifier,
                    type_: Some(type_),
                },
                tokens,
            ))
        } else {
            Ok((
                Parameter {
                    id: NodeId::default(),
                    span: identifier.span.clone(),
                    identifier,
                    type_: None,
                },
                tokens,
            ))
        }
    }
}

#[derive(Debug)]
pub struct Return {
    pub span: Span,
    pub expression: Expression,
}

impl Visitable for Return {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.enter_return(self);
        self.expression.visit(visitor);
        visitor.leave_return(self);
    }
}

impl Parser for Return {
    type Output = Self;

    fn parse<'a>(mut tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        let start;
        if let Some(Token {
            kind: TokenKind::ReturnKeyword,
            ..
        }) = tokens.get(0)
        {
            start = tokens[0].span.start;
            tokens = &tokens[1..];
        } else {
            return Err(ParseError::Expected(
                "return statement",
                tokens.get(0).map(|t| t.span.clone()),
            ));
        }

        let (expression, tokens) = Expression::parse(tokens)?;

        Ok((
            Return {
                span: start..expression.span().end,
                expression,
            },
            tokens,
        ))
    }
}

#[derive(Debug)]
pub struct Debug {
    pub span: Span,
    pub expression: Expression,
}

impl Visitable for Debug {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.enter_debug(self);
        self.expression.visit(visitor);
        visitor.leave_debug(self);
    }
}

impl Parser for Debug {
    type Output = Self;

    fn parse<'a>(mut tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        let start;
        if let Some(Token {
            kind: TokenKind::DebugKeyword,
            ..
        }) = tokens.get(0)
        {
            start = tokens[0].span.start;
            tokens = &tokens[1..];
        } else {
            return Err(ParseError::Expected(
                "debug statement",
                tokens.get(0).map(|t| t.span.clone()),
            ));
        }

        let (expression, tokens) = Expression::parse(tokens)?;

        Ok((
            Debug {
                span: start..expression.span().end,
                expression,
            },
            tokens,
        ))
    }
}

#[derive(Debug)]
pub struct Call {
    pub span: Span,
    pub callee: Expression,
    pub arguments: Vec<Expression>,
}

impl Visitable for Call {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.enter_call(self);
        self.callee.visit(visitor);
        for argument in &self.arguments {
            argument.visit(visitor);
        }
        visitor.leave_call(self);
    }
}

#[derive(Debug)]
pub struct Import {
    pub id: NodeId,
    pub span: Span,
    pub identifier: Identifier,
}

impl Visitable for Import {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.enter_import(self);
        self.identifier.visit(visitor);
        visitor.leave_import(self);
    }
}

impl Parser for Import {
    type Output = Self;

    fn parse<'a>(mut tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        let start;
        if let Some(Token {
            kind: TokenKind::ImportKeyword,
            ..
        }) = tokens.get(0)
        {
            start = tokens[0].span.start;
            tokens = &tokens[1..];
        } else {
            return Err(ParseError::Expected(
                "import statement",
                tokens.get(0).map(|t| t.span.clone()),
            ));
        }

        let (identifier, tokens) = Identifier::parse(tokens)?;

        Ok((
            Self {
                id: Default::default(),
                span: start..identifier.span.end,
                identifier,
            },
            tokens,
        ))
    }
}

#[derive(Debug)]
pub struct If {
    pub span: Span,
    pub condition: Expression,
    pub consequence: Statement,
    pub else_clause: Option<Statement>,
}

impl Visitable for If {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.enter_if(self);
        self.condition.visit(visitor);
        self.consequence.visit(visitor);
        if let Some(else_clause) = &self.else_clause {
            else_clause.visit(visitor);
        }
        visitor.leave_if(self);
    }
}

impl Parser for If {
    type Output = Self;

    fn parse<'a>(mut tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        let start;
        if let Some(Token {
            kind: TokenKind::IfKeyword,
            span,
        }) = tokens.get(0)
        {
            start = span.start;
            tokens = &tokens[1..];
        } else {
            return Err(ParseError::Expected(
                "if statement",
                tokens.get(0).map(|t| t.span.clone()),
            ));
        }

        if let Some(Token {
            kind: TokenKind::OpenParen,
            ..
        }) = tokens.get(0)
        {
            tokens = &tokens[1..];
        } else {
            return Err(ParseError::Expected(
                "parenthesized condition",
                tokens.get(0).map(|t| t.span.clone()),
            ));
        }

        let (condition, mut tokens) = Expression::parse(tokens)?;

        if let Some(Token {
            kind: TokenKind::CloseParen,
            ..
        }) = tokens.get(0)
        {
            tokens = &tokens[1..];
        } else {
            return Err(ParseError::Expected(
                "closing parenthesis",
                tokens.get(0).map(|t| t.span.clone()),
            ));
        }

        let (consequence, mut tokens) = Statement::parse(tokens)?;

        let mut end = consequence.span().end;
        let mut else_clause = None;
        if let Some(Token {
            kind: TokenKind::ElseKeyword,
            ..
        }) = tokens.get(0)
        {
            tokens = &tokens[1..];

            let clause;
            (clause, tokens) = Statement::parse(tokens)?;
            end = clause.span().end;
            else_clause = Some(clause);
        }

        Ok((
            If {
                span: start..end,
                condition,
                consequence,
                else_clause,
            },
            tokens,
        ))
    }
}

#[derive(Debug)]
pub struct Block {
    pub span: Span,
    pub statements: Vec<Statement>,
}

impl Visitable for Block {
    fn visit<'a>(&'a self, visitor: &mut impl Visitor<'a>) {
        visitor.enter_block(self);
        for statement in self.statements.iter() {
            statement.visit(visitor);
        }
        visitor.leave_block(self);
    }
}

impl Parser for Block {
    type Output = Self;

    fn parse<'a>(mut tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output> {
        let start;
        if let Some(Token {
            kind: TokenKind::OpenCurly,
            span,
        }) = tokens.get(0)
        {
            tokens = &tokens[1..];
            start = span.start;
        } else {
            return Err(ParseError::Expected(
                "block",
                tokens.get(0).map(|t| t.span.clone()),
            ));
        }

        let mut statements = vec![];
        while tokens
            .get(0)
            .map(|t| !matches!(t.kind, TokenKind::CloseCurly))
            .unwrap_or(false)
        {
            let statement;
            (statement, tokens) = Statement::parse(tokens)?;
            statements.push(statement);
        }

        let end;
        if let Some(Token {
            kind: TokenKind::CloseCurly,
            span,
        }) = tokens.get(0)
        {
            tokens = &tokens[1..];
            end = span.end;
        } else {
            return Err(ParseError::Expected(
                "end of block",
                tokens.get(0).map(|t| t.span.clone()),
            ));
        }

        Ok((
            Block {
                span: start..end,
                statements,
            },
            tokens,
        ))
    }
}
