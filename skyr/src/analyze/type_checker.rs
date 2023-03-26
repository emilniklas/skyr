use std::borrow::Cow;
use std::collections::BTreeMap;
use std::pin::Pin;

use async_std::sync::{RwLock, RwLockUpgradableReadGuard};
use futures::stream::FuturesOrdered;
use futures::{Future, StreamExt};

use crate::compile::*;

use super::{
    CompositeType, Declaration, External, ImportMap, PrimitiveType, SpanType, SymbolTable, Type,
    TypeCheckResult, TypeEnvironment,
};

pub struct TypeChecker<'t, 'a> {
    symbols: &'t SymbolTable<'a>,
    cache: RwLock<BTreeMap<NodeId, SpanType>>,
    import_map: ImportMap<'a>,
}

impl<'t, 'a> TypeChecker<'t, 'a> {
    pub fn new(symbols: &'t SymbolTable<'a>, import_map: ImportMap<'a>) -> Self {
        Self {
            symbols,
            import_map,
            cache: Default::default(),
            //checking: Default::default(),
        }
    }

    pub async fn check_module(
        &self,
        module: &'a Module,
        env: &mut TypeEnvironment<'_>,
    ) -> TypeCheckResult<SpanType> {
        self.check_statements(&module.statements, module.span.clone(), env, &None)
            .await
    }

    pub async fn check_statements(
        &self,
        statements: impl IntoIterator<Item = &'a Statement>,
        span: Span,
        env: &mut TypeEnvironment<'_>,
        fn_return: &Option<Type>,
    ) -> TypeCheckResult<SpanType> {
        let mut results = vec![];
        for statement in statements {
            results.push(self.check_statement(statement, env, fn_return).await);
        }

        results
            .into_iter()
            .fold(TypeCheckResult::success(()), |accum, res| {
                accum.flat_map(|()| res.map(|_| ()))
            })
            .map(|()| SpanType::Primitive(span, PrimitiveType::Void))
    }

    pub async fn check_statement(
        &self,
        statement: &'a Statement,
        env: &mut TypeEnvironment<'_>,
        fn_return: &Option<Type>,
    ) -> TypeCheckResult<SpanType> {
        match statement {
            Statement::Expression(n) => self.check_expression(n, env).await,
            Statement::Return(r) => self.check_return(r, env, fn_return).await,
            Statement::Assignment(a) => self.check_assignment(a, env).await,
            Statement::Import(i) => self.check_import(i, env).await,
            _ => todo!(),
        }
    }

    pub fn check_import<'s>(
        &'s self,
        import: &'a Import,
        _env: &'s mut TypeEnvironment<'_>,
    ) -> Pin<Box<dyn 's + Future<Output = TypeCheckResult<SpanType>>>> {
        Box::pin(async move {
            self.cached_check(import.id, || async move {
                match self.import_map.resolve(import).await {
                    None => todo!("undefined import"),
                    Some(External::Module(m)) => {
                        self.check_module(m, &mut TypeEnvironment::new()).await
                    }
                    Some(External::Plugin(p)) => {
                        TypeCheckResult::success(p.module_type()).map(|t| {
                            SpanType::Referenced(
                                import.identifier.span.clone(),
                                t,
                                Some(import.identifier.symbol.clone().into()),
                            )
                        })
                    }
                }
            }).await
        })
    }

    pub async fn check_assignment(
        &self,
        assignment: &'a Assignment,
        env: &mut TypeEnvironment<'_>,
    ) -> TypeCheckResult<SpanType> {
        let expression_type = self.check_expression(&assignment.value, env).await;

        let annotation_type = {
            if let Some(type_) = &assignment.type_ {
                self.check_type_expression(type_, env)
                    .await
                    .map(|st| st.into())
            } else {
                TypeCheckResult::success(Type::open())
            }
        };

        annotation_type
            .flat_map(|at| expression_type.map(|et| (at, et)))
            .flat_map(|(at, et)| env.unify(at, et))
    }

    pub async fn check_return(
        &self,
        return_: &'a Return,
        env: &mut TypeEnvironment<'_>,
        fn_return: &Option<Type>,
    ) -> TypeCheckResult<SpanType> {
        self.check_expression(&return_.expression, env)
            .await
            .flat_map_async(|t| async move {
                if let Some(fn_ret) = fn_return {
                    env.unify(fn_ret.clone(), t).flat_map(|_st| {
                        TypeCheckResult::success(SpanType::Primitive(
                            return_.span.clone(),
                            PrimitiveType::Void,
                        ))
                    })
                } else {
                    TypeCheckResult::success(SpanType::Primitive(
                        return_.span.clone(),
                        PrimitiveType::Void,
                    ));
                    todo!("malplaced return")
                }
            })
            .await
    }

    pub fn check_expression<'s>(
        &'s self,
        expression: &'a Expression,
        env: &'s mut TypeEnvironment<'_>,
    ) -> Pin<Box<dyn '_ + Future<Output = TypeCheckResult<SpanType>>>> {
        Box::pin(async move {
            match expression {
                Expression::Nil(n) => self.check_nil(n, env),
                Expression::StringLiteral(n) => TypeCheckResult::success(SpanType::Primitive(
                    n.span.clone(),
                    PrimitiveType::String,
                )),
                Expression::IntegerLiteral(n) => TypeCheckResult::success(SpanType::Primitive(
                    n.span.clone(),
                    PrimitiveType::Integer,
                )),
                Expression::BooleanLiteral(n) => TypeCheckResult::success(SpanType::Primitive(
                    n.span.clone(),
                    PrimitiveType::Boolean,
                )),
                Expression::Function(n) => self.check_function(n, env).await,
                Expression::Identifier(id) => match self.symbols.declaration(id) {
                    None => todo!("undefined reference"),
                    Some(d) => self
                        .check_declaration(d, id, env)
                        .await
                        .map(|t| t.when_not_named()),
                },
                Expression::Call(call) => self.check_call(call, env).await,
                Expression::Tuple(t) => self.check_tuple_expression(t, env).await,
                Expression::Construct(c) => self.check_construct(c, env).await,
                Expression::Record(r) => self.check_record_expression(r, env).await,
                Expression::MemberAccess(ma) => self.check_member_access(ma, env).await,
                e => todo!("{:?}", e),
            }
        })
    }

    pub async fn check_member_access(
        &self,
        member_access: &'a MemberAccess,
        env: &mut TypeEnvironment<'_>,
    ) -> TypeCheckResult<SpanType> {
        self.check_expression(&member_access.subject, env)
            .await
            .map(|subject| {
                let type_ = match Type::from(subject).when_not_named() {
                    Type::Composite(CompositeType::Record(fields)) => fields
                        .into_iter()
                        .find_map(|(n, v)| (n == member_access.identifier.symbol).then_some(v)),
                    _ => None,
                }
                .unwrap_or_else(|| todo!("undefined field"));

                SpanType::Referenced(member_access.identifier.span.clone(), type_, None)
            })
    }

    pub async fn check_tuple_expression(
        &self,
        tuple: &'a Tuple<Expression>,
        env: &mut TypeEnvironment<'_>,
    ) -> TypeCheckResult<SpanType> {
        let mut fo = FuturesOrdered::new();

        for element in &tuple.elements {
            fo.push_back(async { self.check_expression(element, &mut env.inner()).await });
        }

        fo.fold(TypeCheckResult::success(vec![]), |accum, res| async {
            accum.flat_map(|mut args| {
                res.map(|arg| {
                    args.push(arg);
                    args
                })
            })
        })
        .await
        .map(|mut elements| {
            if elements.len() == 1 {
                elements.remove(0)
            } else {
                SpanType::Composite(tuple.span.clone(), CompositeType::Tuple(elements))
            }
        })
    }

    pub async fn check_call(
        &self,
        call: &'a Call,
        env: &mut TypeEnvironment<'_>,
    ) -> TypeCheckResult<SpanType> {
        let mut env = env.inner();
        let callee = self.check_expression(&call.callee, &mut env).await;
        let arguments = {
            let mut args = vec![];

            for arg in &call.arguments {
                args.push(self.check_expression(arg, &mut env).await);
            }

            args.into_iter()
                .fold(TypeCheckResult::success(vec![]), |accum, res| {
                    accum.flat_map(|mut args| {
                        res.map(|arg| {
                            args.push(arg);
                            args
                        })
                    })
                })
        };

        callee
            .flat_map_async(|callee| async move {
                arguments
                    .flat_map_async(|arguments| async move {
                        let call_site = SpanType::Composite(
                            call.span.clone(),
                            CompositeType::function(
                                arguments,
                                SpanType::Open(call.span.clone(), Default::default()),
                            ),
                        );

                        env.unify(callee.into(), call_site).map(|r| r.return_type())
                    })
                    .await
            })
            .await
    }

    pub async fn check_construct(
        &self,
        construct: &'a Construct,
        env: &mut TypeEnvironment<'_>,
    ) -> TypeCheckResult<SpanType> {
        let mut env = env.inner();
        let callee = self.check_expression(&construct.subject, &mut env).await;
        let arguments = self
            .check_record_expression(&construct.record, &mut env)
            .await
            .map(|t| vec![t]);

        callee
            .flat_map_async(|callee| async move {
                arguments
                    .flat_map_async(|arguments| async move {
                        let call_site = SpanType::Composite(
                            construct.span.clone(),
                            CompositeType::function(
                                arguments,
                                SpanType::Open(construct.span.clone(), Default::default()),
                            ),
                        );

                        env.unify(callee.into(), call_site).map(|r| r.return_type())
                    })
                    .await
            })
            .await
    }

    pub async fn check_record_expression(
        &self,
        record: &'a Record<Expression>,
        env: &mut TypeEnvironment<'_>,
    ) -> TypeCheckResult<SpanType> {
        let mut fo = FuturesOrdered::new();

        for field in &record.fields {
            fo.push_back(async {
                self.check_expression(&field.value, &mut env.inner())
                    .await
                    .map(|v| (Cow::Owned(field.identifier.symbol.clone()), v))
            });
        }

        fo.fold(TypeCheckResult::success(vec![]), |accum, res| async {
            accum.flat_map(|mut args| {
                res.map(|arg| {
                    args.push(arg);
                    args
                })
            })
        })
        .await
        .map(|elements| SpanType::Composite(record.span.clone(), CompositeType::Record(elements)))
    }

    pub fn check_nil(
        &self,
        nil: &'a Nil,
        _env: &mut TypeEnvironment<'_>,
    ) -> TypeCheckResult<SpanType> {
        TypeCheckResult::success(SpanType::Referenced(
            nil.span.clone(),
            Type::Composite(CompositeType::Optional(Default::default())),
            Some(Cow::Borrowed("nil")),
        ))
    }

    pub async fn check_function(
        &self,
        function: &'a Function,
        env: &mut TypeEnvironment<'_>,
    ) -> TypeCheckResult<SpanType> {
        let mut env = env.inner();
        let mut results = vec![];
        for param in &function.parameters {
            results.push(self.check_parameter(param, &mut env).await);
        }
        results
            .into_iter()
            .fold(TypeCheckResult::success(vec![]), |accum, res| {
                accum.flat_map(|mut accum| {
                    res.map(|t| {
                        accum.push(t);
                        accum
                    })
                })
            })
            .flat_map_async(|parameters| async {
                match &function.return_type {
                    None => TypeCheckResult::success(Type::default()),
                    Some(t) => self
                        .check_type_expression(t, &mut env)
                        .await
                        .map(|s| s.into()),
                }
                .flat_map_async(|return_type| async {
                    self.check_block(&function.body, &mut env, &Some(return_type.clone()))
                        .await
                        .map(|_| {
                            SpanType::Referenced(
                                function.span.clone(),
                                env.resolve(Type::Composite(CompositeType::function(
                                    parameters,
                                    return_type,
                                ))),
                                None,
                            )
                        })
                })
                .await
            })
            .await
    }

    pub async fn check_block(
        &self,
        block: &'a Block,
        env: &mut TypeEnvironment<'_>,
        fn_return: &Option<Type>,
    ) -> TypeCheckResult<SpanType> {
        self.check_statements(&block.statements, block.span.clone(), env, fn_return)
            .await
    }

    async fn cached_check<F, R>(&self, id: NodeId, f: F) -> R::Output
    where
        F: FnOnce() -> R,
        R: Future<Output = TypeCheckResult<SpanType>>,
    {
        // TODO: protect against deadlock on recursive types

        let guard = self.cache.upgradable_read().await;

        if let Some(t) = guard.get(&id) {
            return TypeCheckResult::success(t.clone());
        }

        let mut guard = RwLockUpgradableReadGuard::upgrade(guard).await;

        let t = f().await;

        t.map(|t| {
            guard.insert(id, t.clone());

            t
        })
    }

    pub async fn check_parameter(
        &self,
        parameter: &'a Parameter,
        env: &mut TypeEnvironment<'_>,
    ) -> TypeCheckResult<SpanType> {
        self.cached_check(parameter.id, || async move {
            if let Some(t) = &parameter.type_ {
                self.check_type_expression(t, env).await
            } else {
                TypeCheckResult::success(SpanType::Open(parameter.span.clone(), Default::default()))
            }
        })
        .await
    }

    pub fn check_type_expression<'s>(
        &'s self,
        type_expression: &'a TypeExpression,
        env: &'s mut TypeEnvironment<'_>,
    ) -> Pin<Box<dyn '_ + Future<Output = TypeCheckResult<SpanType>>>> {
        Box::pin(async move {
            match type_expression {
                TypeExpression::Identifier(id) => match self.symbols.declaration(id) {
                    None => todo!("undefined reference"),

                    Some(d) => self.check_declaration(d, id, env).await,
                },

                TypeExpression::Optional(t) => self
                    .check_type_expression(&t.type_, env)
                    .await
                    .map(Box::new)
                    .map(CompositeType::Optional)
                    .map(|r| SpanType::Composite(t.span.clone(), r)),

                _ => todo!(),
            }
        })
    }

    pub fn check_declaration<'s>(
        &'s self,
        declaration: Declaration<'a>,
        id: &'s Identifier,
        env: &'s mut TypeEnvironment<'_>,
    ) -> Pin<Box<dyn '_ + Future<Output = TypeCheckResult<SpanType>>>> {
        Box::pin(async move {
            match declaration {
                Declaration::Builtin(_, n) => match n {
                    "String" => TypeCheckResult::success(SpanType::Primitive(
                        id.span.clone(),
                        PrimitiveType::String,
                    )),
                    "Integer" => TypeCheckResult::success(SpanType::Primitive(
                        id.span.clone(),
                        PrimitiveType::Integer,
                    )),
                    "Float" => TypeCheckResult::success(SpanType::Primitive(
                        id.span.clone(),
                        PrimitiveType::Float,
                    )),
                    "Boolean" => TypeCheckResult::success(SpanType::Primitive(
                        id.span.clone(),
                        PrimitiveType::Boolean,
                    )),
                    "Void" => TypeCheckResult::success(SpanType::Primitive(
                        id.span.clone(),
                        PrimitiveType::Void,
                    )),
                    _ => todo!(),
                },
                Declaration::Assignment(a) => self.check_assignment(a, env).await,
                Declaration::Parameter(p) => self.check_parameter(p, env).await,
                Declaration::Import(i) => self.check_import(i, env).await,
                d => todo!("{:?}", d),
            }
            .map(|st| {
                SpanType::Referenced(id.span.clone(), st.into(), Some(id.symbol.clone().into()))
            })
        })
    }
}
