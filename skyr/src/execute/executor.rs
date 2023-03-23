use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::{fmt, io};

use async_std::sync::RwLock;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::StreamExt;

use crate::analyze::{External, ImportMap, SymbolTable};
use crate::{
    compile::*, DisplayAsDebug, IdentifyResource, PluginCell, Primitive, ResourceError, ResourceId,
    ResourceState, TypeBasedResource, TypeOf,
};
use crate::{Collection, Plan, Value};
use crate::{Resource, State};

use super::DependentValue;

#[derive(Clone)]
pub struct ExecutionContext<'a> {
    parent: Option<Box<ExecutionContext<'a>>>,
    pub state: &'a State,
    table: &'a SymbolTable<'a>,
    bindings: Arc<RwLock<BTreeMap<NodeId, DependentValue<RuntimeValue<'a>>>>>,
    import_map: ImportMap<'a>,
}

impl<'a> ExecutionContext<'a> {
    pub fn new(state: &'a State, table: &'a SymbolTable<'a>, import_map: ImportMap<'a>) -> Self {
        Self {
            parent: None,
            state,
            table,
            bindings: Default::default(),
            import_map,
        }
    }

    pub fn new_empty(&self) -> ExecutionContext<'a> {
        ExecutionContext {
            bindings: Default::default(),
            ..self.clone()
        }
    }

    pub fn inner(&self) -> ExecutionContext<'a> {
        ExecutionContext {
            parent: Some(Box::new(self.clone())),
            ..self.new_empty()
        }
    }

    pub fn get_binding(
        &self,
        id: NodeId,
    ) -> Pin<Box<dyn '_ + Future<Output = DependentValue<RuntimeValue<'a>>>>> {
        Box::pin(async move {
            let bindings = self.bindings.read().await;
            match bindings.get(&id) {
                None => match self.parent.as_ref() {
                    None => panic!("undeclared binding"),
                    Some(p) => p.get_binding(id).await,
                },
                Some(v) => v.clone(),
            }
        })
    }
}

pub struct Executor<'a> {
    plan: RwLock<Plan<'a>>,
    used_resources: RwLock<BTreeSet<ResourceId>>,
    is_pending: AtomicBool,
    plugins: &'a [PluginCell],
    read_errors: RwLock<Vec<ResourceError>>,
}

impl<'a> Executor<'a> {
    pub fn new(plugins: &'a [PluginCell]) -> Self {
        Executor {
            plan: Default::default(),
            used_resources: Default::default(),
            is_pending: Default::default(),
            plugins,
            read_errors: Default::default(),
        }
    }

    pub fn finalize(self, state: &'a State) -> Result<Plan<'a>, Vec<ResourceError>> {
        let read_errors = self.read_errors.into_inner();
        if !read_errors.is_empty() {
            return Err(read_errors);
        }

        let mut plan = self.plan.into_inner();

        if !self.is_pending.into_inner() {
            let used = self.used_resources.into_inner();

            for resource in state.all_not_in(&used) {
                plan.register_delete(
                    resource.id.clone(),
                    resource.dependencies.clone(),
                    move |_, _| {
                        Box::pin(async move {
                            for plugin in self.plugins.iter() {
                                let plugin = plugin.get().await;
                                if let Some(()) = plugin.delete_matching_resource(&resource).await?
                                {
                                    return Ok(());
                                }
                            }

                            Err(io::Error::new(
                                io::ErrorKind::Other,
                                format!("no plugin took ownership of resource {:?}", resource.id),
                            ))
                        })
                    },
                );
            }
        }

        Ok(plan)
    }

    pub fn execute_module(
        &self,
        ctx: ExecutionContext<'a>,
        module: &'a Module,
    ) -> Pin<Box<dyn '_ + Future<Output = DependentValue<RuntimeValue<'a>>>>> {
        Box::pin(async move {
            let mut exports = vec![];

            let mut values = vec![];
            for statement in module.statements.iter() {
                if let Statement::Assignment(a) = statement {
                    let v = self.execute_assignment(ctx.clone(), a).await;
                    exports.push((a.identifier.symbol.clone(), v.clone()));
                    values.push(v);
                } else {
                    values.push(self.execute_statement(ctx.clone(), statement).await);
                }
            }

            let fo = FuturesUnordered::new();
            for value in values {
                fo.push(value.resolve(self));
            }
            fo.collect::<Vec<_>>().await;

            let fo = FuturesUnordered::new();
            for (name, value) in exports {
                fo.push(async move { value.resolve(self).await.map(|v| (name, v)) });
            }

            let dep: DependentValue<Vec<(String, RuntimeValue<'a>)>> =
                fo.collect::<Vec<_>>().await.into();
            dep.map(Collection::record).map(RuntimeValue::Collection)
        })
    }

    pub async fn execute_statement(
        &self,
        ctx: ExecutionContext<'a>,
        statement: &'a Statement,
    ) -> DependentValue<RuntimeValue<'a>> {
        let mut debug = None;
        let exp = match statement {
            Statement::Expression(e) => self.execute_expression(ctx, e),
            Statement::Assignment(a) => self.execute_assignment(ctx, a).await,
            Statement::Debug(d) => {
                debug = Some(d.span.clone());
                self.execute_expression(ctx, &d.expression)
            }
            Statement::Return(r) => return self.execute_expression(ctx, &r.expression),
            Statement::TypeDefinition(_) => RuntimeValue::Primitive(Primitive::Nil).into(),
            Statement::Import(i) => self.execute_import(ctx, i).await,
            Statement::If(i) => return self.execute_if(ctx, i).await,
            Statement::Block(b) => return self.execute_block(ctx, b).await,
        };
        RuntimeValue::defer(move |e| {
            let exp = exp.clone();
            let debug = debug.clone();
            Box::pin(async move {
                let v = exp.resolve(e).await;
                if let Some(span) = debug {
                    v.map_async(|v| async move {
                        e.plan.write().await.register_debug(span, v);
                        RuntimeValue::Primitive(Primitive::Nil)
                    })
                    .await
                } else {
                    RuntimeValue::Primitive(Primitive::Nil).into()
                }
            })
        })
    }

    pub async fn execute_if(
        &self,
        ctx: ExecutionContext<'a>,
        if_: &'a If,
    ) -> DependentValue<RuntimeValue<'a>> {
        let condition = self.execute_expression(ctx.clone(), &if_.condition);

        RuntimeValue::defer(move |e| {
            let condition = condition.clone();
            let ctx = ctx.clone();
            Box::pin(async move {
                condition
                    .resolve(e)
                    .await
                    .flat_map_async(|condition, _| async move {
                        if condition.as_primitive().as_bool() {
                            e.execute_statement(ctx, &if_.consequence).await
                        } else if let Some(else_clause) = &if_.else_clause {
                            e.execute_statement(ctx, else_clause).await
                        } else {
                            RuntimeValue::Primitive(Primitive::Nil).into()
                        }
                    })
                    .await
            })
        })
    }

    pub fn execute_block(
        &self,
        ctx: ExecutionContext<'a>,
        block: &'a Block,
    ) -> Pin<Box<dyn '_ + Future<Output = DependentValue<RuntimeValue<'a>>>>> {
        Box::pin(async move {
            let fo = FuturesUnordered::new();

            let mut values = vec![];
            for statement in block.statements.iter() {
                values.push(self.execute_statement(ctx.clone(), statement).await);
            }

            for value in values {
                fo.push(value.resolve(self));
            }

            fo.collect::<Vec<_>>()
                .await
                .into_iter()
                .filter_map(|v| v.into_result().ok())
                .find(|v| !matches!(v, RuntimeValue::Primitive(Primitive::Nil)))
                .unwrap_or(RuntimeValue::Primitive(Primitive::Nil))
                .into()
        })
    }

    pub async fn execute_import(
        &self,
        ctx: ExecutionContext<'a>,
        import: &'a Import,
    ) -> DependentValue<RuntimeValue<'a>> {
        let external = ctx.import_map.resolve(import).await.expect("unresolved import");

        let value = match external {
            External::Module(m) => self.execute_module(ctx.new_empty(), m).await,
            External::Plugin(p) => p.module_value(ctx.clone()).into(),
        };

        ctx.bindings.write().await.insert(import.id, value.clone());

        value
    }

    pub async fn execute_assignment(
        &self,
        ctx: ExecutionContext<'a>,
        assignment: &'a Assignment,
    ) -> DependentValue<RuntimeValue<'a>> {
        let value = self.execute_expression(ctx.clone(), &assignment.value);
        let mut bindings = ctx.bindings.write().await;
        bindings.insert(assignment.id, value.clone());
        value
    }

    pub async fn execute_record(
        &self,
        ctx: ExecutionContext<'a>,
        record: &'a Record<Expression>,
    ) -> DependentValue<RuntimeValue<'a>> {
        RuntimeValue::Collection(Collection::Record({
            let mut fo = FuturesOrdered::new();
            for f in &record.fields {
                let n = f.identifier.symbol.clone();
                let v = self.execute_expression(ctx.clone(), &f.value);
                fo.push_back(async move { (n.into(), v.resolve(self).await) });
            }
            fo.collect().await
        }))
        .into()
    }

    async fn resolve_execute_expression(
        &self,
        ctx: ExecutionContext<'a>,
        expression: &'a Expression,
    ) -> DependentValue<RuntimeValue<'a>> {
        match expression {
            Expression::Nil(_) => RuntimeValue::Primitive(Primitive::Nil).into(),
            Expression::StringLiteral(s) => {
                RuntimeValue::Primitive(Primitive::string(s.value.clone())).into()
            }
            Expression::IntegerLiteral(i) => {
                RuntimeValue::Primitive(Primitive::Integer(i.value)).into()
            }
            Expression::BooleanLiteral(i) => {
                RuntimeValue::Primitive(Primitive::Boolean(i.value)).into()
            }
            Expression::Identifier(id) => {
                let binding_id = ctx.table.declaration(id).expect("undefined reference").id();
                ctx.get_binding(binding_id).await
            }
            Expression::Record(record) => self.execute_record(ctx, record).await,
            Expression::Function(f) => self.execute_function(ctx, f),
            Expression::Call(c) => {
                self.execute_expression(ctx.clone(), &c.callee)
                    .resolve(self)
                    .await
                    .flat_map_async(|callee, _| async move {
                        if let RuntimeValue::Function(f) = callee {
                            let mut fo = FuturesOrdered::new();
                            for arg in &c.arguments {
                                let v = self.execute_expression(ctx.clone(), arg);
                                fo.push_back(async move { v.resolve(self).await });
                            }
                            let args = fo.collect::<Vec<_>>().await;
                            f(self, args).await
                        } else {
                            panic!("cannot call {:?}", callee);
                        }
                    })
                    .await
            }
            Expression::MemberAccess(ma) => self
                .execute_expression(ctx, &ma.subject)
                .resolve(self)
                .await
                .flat_map(|subject| {
                    subject
                        .as_collection()
                        .access_member(ma.identifier.symbol.as_str())
                        .as_ref()
                        .clone()
                }),
            Expression::Construct(construct) => {
                self.execute_expression(ctx.clone(), &construct.subject)
                    .resolve(self)
                    .await
                    .flat_map_async(|subject, _| async move {
                        if let RuntimeValue::Function(f) = subject {
                            let args = vec![self.execute_record(ctx, &construct.record).await];
                            f(self, args).await
                        } else {
                            panic!("cannot construct {:?}", subject);
                        }
                    })
                    .await
            }
            Expression::BinaryOperation(op) => {
                let (lhs, rhs) = futures::future::join(
                    self.execute_expression(ctx.clone(), &op.lhs).resolve(self),
                    self.execute_expression(ctx.clone(), &op.rhs).resolve(self),
                )
                .await;

                lhs.flat_map(|lhs| rhs.map(|rhs| (lhs, rhs)))
                    .map(|(lhs, rhs)| match (lhs, &op.operator.kind, rhs) {
                        (
                            RuntimeValue::Primitive(Primitive::Integer(lhs)),
                            BinaryOperatorKind::LessThan,
                            RuntimeValue::Primitive(Primitive::Integer(rhs)),
                        ) => RuntimeValue::Primitive(Primitive::Boolean(lhs < rhs)),
                        (
                            RuntimeValue::Primitive(Primitive::Integer(lhs)),
                            BinaryOperatorKind::LessThanOrEqualTo,
                            RuntimeValue::Primitive(Primitive::Integer(rhs)),
                        ) => RuntimeValue::Primitive(Primitive::Boolean(lhs <= rhs)),
                        (
                            RuntimeValue::Primitive(Primitive::Integer(lhs)),
                            BinaryOperatorKind::EqualTo,
                            RuntimeValue::Primitive(Primitive::Integer(rhs)),
                        ) => RuntimeValue::Primitive(Primitive::Boolean(lhs == rhs)),
                        (
                            RuntimeValue::Primitive(Primitive::Integer(lhs)),
                            BinaryOperatorKind::NotEqualTo,
                            RuntimeValue::Primitive(Primitive::Integer(rhs)),
                        ) => RuntimeValue::Primitive(Primitive::Boolean(lhs != rhs)),
                        (
                            RuntimeValue::Primitive(Primitive::Integer(lhs)),
                            BinaryOperatorKind::GreaterThanOrEqualTo,
                            RuntimeValue::Primitive(Primitive::Integer(rhs)),
                        ) => RuntimeValue::Primitive(Primitive::Boolean(lhs >= rhs)),
                        (
                            RuntimeValue::Primitive(Primitive::Integer(lhs)),
                            BinaryOperatorKind::GreaterThan,
                            RuntimeValue::Primitive(Primitive::Integer(rhs)),
                        ) => RuntimeValue::Primitive(Primitive::Boolean(lhs > rhs)),

                        (
                            RuntimeValue::Primitive(Primitive::Integer(lhs)),
                            BinaryOperatorKind::Plus,
                            RuntimeValue::Primitive(Primitive::Integer(rhs)),
                        ) => RuntimeValue::Primitive(Primitive::Integer(lhs + rhs)),
                        (
                            RuntimeValue::Primitive(Primitive::Integer(lhs)),
                            BinaryOperatorKind::Minus,
                            RuntimeValue::Primitive(Primitive::Integer(rhs)),
                        ) => RuntimeValue::Primitive(Primitive::Integer(lhs - rhs)),
                        (
                            RuntimeValue::Primitive(Primitive::Integer(lhs)),
                            BinaryOperatorKind::Multiply,
                            RuntimeValue::Primitive(Primitive::Integer(rhs)),
                        ) => RuntimeValue::Primitive(Primitive::Integer(lhs * rhs)),
                        (
                            RuntimeValue::Primitive(Primitive::Integer(lhs)),
                            BinaryOperatorKind::Divide,
                            RuntimeValue::Primitive(Primitive::Integer(rhs)),
                        ) => RuntimeValue::Primitive(Primitive::Integer(lhs / rhs)),

                        (
                            RuntimeValue::Primitive(Primitive::Boolean(lhs)),
                            BinaryOperatorKind::And,
                            RuntimeValue::Primitive(Primitive::Boolean(rhs)),
                        ) => RuntimeValue::Primitive(Primitive::Boolean(lhs && rhs)),
                        (
                            RuntimeValue::Primitive(Primitive::Boolean(lhs)),
                            BinaryOperatorKind::Or,
                            RuntimeValue::Primitive(Primitive::Boolean(rhs)),
                        ) => RuntimeValue::Primitive(Primitive::Boolean(lhs || rhs)),

                        (
                            RuntimeValue::Primitive(Primitive::String(lhs)),
                            BinaryOperatorKind::Plus,
                            RuntimeValue::Primitive(Primitive::String(rhs)),
                        ) => RuntimeValue::Primitive(Primitive::String({
                            let mut s = lhs.to_string();
                            s.push_str(&rhs);
                            s.into()
                        })),
                        (
                            RuntimeValue::Primitive(Primitive::String(lhs)),
                            BinaryOperatorKind::Plus,
                            RuntimeValue::Primitive(Primitive::Integer(rhs)),
                        ) => RuntimeValue::Primitive(Primitive::String({
                            let mut s = lhs.to_string();
                            s.push_str(&format!("{}", rhs));
                            s.into()
                        })),
                        (
                            RuntimeValue::Primitive(Primitive::String(lhs)),
                            BinaryOperatorKind::EqualTo,
                            RuntimeValue::Primitive(Primitive::String(rhs)),
                        ) => RuntimeValue::Primitive(Primitive::Boolean(lhs == rhs)),
                        (
                            RuntimeValue::Primitive(Primitive::String(lhs)),
                            BinaryOperatorKind::NotEqualTo,
                            RuntimeValue::Primitive(Primitive::String(rhs)),
                        ) => RuntimeValue::Primitive(Primitive::Boolean(lhs != rhs)),

                        (lhs, op, rhs) => {
                            panic!("invalid operation: {:?} {:?} {:?}", lhs, op, rhs)
                        }
                    })
            }

            Expression::List(list) => RuntimeValue::Collection(Collection::List(
                list.elements
                    .iter()
                    .map(|e| self.execute_expression(ctx.clone(), e))
                    .collect(),
            ))
            .into(),
        }
    }

    pub fn execute_expression(
        &self,
        ctx: ExecutionContext<'a>,
        expression: &'a Expression,
    ) -> DependentValue<RuntimeValue<'a>> {
        RuntimeValue::defer(move |executor| {
            let ctx = ctx.clone();
            Box::pin(async move { executor.resolve_execute_expression(ctx, expression).await })
        })
    }

    pub fn execute_function(
        &self,
        ctx: ExecutionContext<'a>,
        function: &'a Function,
    ) -> DependentValue<RuntimeValue<'a>> {
        RuntimeValue::Function(Arc::new(move |executor, args| {
            let ctx = ctx.inner();

            Box::pin(async move {
                {
                    let mut bindings = ctx.bindings.write().await;
                    for (param, arg) in function.parameters.iter().zip(args.into_iter()) {
                        bindings.insert(param.id, arg);
                    }
                }

                executor.execute_block(ctx, &function.body).await
            })
        }))
        .into()
    }

    async fn register_create<R: Resource + 'a>(
        &self,
        resource: R,
        id: ResourceId,
        arg_value: Value,
        dependencies: Vec<ResourceId>,
    ) {
        let mut plan = self.plan.write().await;
        plan.register_create(id.clone(), arg_value.clone(), dependencies.clone(), {
            move || {
                Box::pin(async move {
                    let arg: R::Arguments = arg_value.deserialize().unwrap();
                    let state = resource.create(arg).await?;
                    Ok(ResourceState {
                        id,
                        arg: arg_value,
                        state: Value::serialize(&state).unwrap(),
                        dependencies,
                    })
                })
            }
        });
    }
}

#[derive(Clone)]
pub enum RuntimeValue<'a> {
    Primitive(Primitive),
    Collection(Collection<DependentValue<RuntimeValue<'a>>>),
    Deferred(Deferred<'a>),
    Function(
        Arc<
            dyn 'a
                + Send
                + Sync
                + for<'e> Fn(
                    &'e Executor<'a>,
                    Vec<DependentValue<RuntimeValue<'a>>>,
                )
                    -> Pin<Box<dyn Future<Output = DependentValue<RuntimeValue<'a>>> + 'e>>,
        >,
    ),
}

impl<'a> From<Value> for RuntimeValue<'a> {
    fn from(value: Value) -> Self {
        match value {
            Value::Primitive(p) => RuntimeValue::Primitive(p),
            Value::Collection(c) => {
                RuntimeValue::Collection(c.map(Into::<RuntimeValue<'a>>::into).map(Into::into))
            }
        }
    }
}

struct TrackedFmtValue<'v, 'a> {
    value: &'v RuntimeValue<'a>,
    arcs: &'v std::cell::RefCell<Vec<Arc<RwLock<DeferredState<'a>>>>>,
}

impl<'v, 'a> fmt::Debug for TrackedFmtValue<'v, 'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.value {
            RuntimeValue::Deferred(Deferred { inner }) => match inner.try_read() {
                None => write!(f, "<deferred>"),
                Some(guard) => match &*guard {
                    DeferredState::Resolved(r) => {
                        let has_seen_deferred_before =
                            self.arcs.borrow().iter().any(|a| Arc::ptr_eq(a, inner));

                        if has_seen_deferred_before {
                            return write!(f, "<circular>");
                        }

                        (self.arcs.borrow_mut()).push(inner.clone());

                        write!(f, "~")?;

                        r.as_ref()
                            .map(|value| TrackedFmtValue {
                                value,
                                arcs: self.arcs,
                            })
                            .fmt(f)
                    }
                    DeferredState::Deferred(_) => write!(f, "<deferred>"),
                },
            },
            RuntimeValue::Function(_) => write!(f, "<fn>"),
            RuntimeValue::Collection(Collection::Record(fields)) => {
                let mut s = f.debug_map();
                for (name, value) in fields.iter() {
                    s.entry(
                        &DisplayAsDebug(name),
                        &value.as_ref().map(|value| TrackedFmtValue {
                            value,
                            arcs: self.arcs,
                        }),
                    );
                }
                s.finish()
            }
            RuntimeValue::Collection(Collection::List(elements)) => {
                let mut l = f.debug_list();
                for value in elements.iter() {
                    l.entry(&value.as_ref().map(|value| TrackedFmtValue {
                        value,
                        arcs: self.arcs,
                    }));
                }
                l.finish()
            }
            RuntimeValue::Collection(Collection::Tuple(elements)) => {
                let mut l = f.debug_tuple("");
                for value in elements.iter() {
                    l.field(&value.as_ref().map(|value| TrackedFmtValue {
                        value,
                        arcs: self.arcs,
                    }));
                }
                l.finish()
            }
            RuntimeValue::Collection(Collection::Dict(fields)) => {
                let mut s = f.debug_map();
                for (key, value) in fields.iter() {
                    s.entry(
                        &key.as_ref().map(|value| TrackedFmtValue {
                            value,
                            arcs: self.arcs,
                        }),
                        &value.as_ref().map(|value| TrackedFmtValue {
                            value,
                            arcs: self.arcs,
                        }),
                    );
                }
                s.finish()
            }
            RuntimeValue::Primitive(v) => v.fmt(f),
        }
    }
}

impl<'a> RuntimeValue<'a> {
    pub fn function_sync(
        f: impl 'a
            + Clone
            + Send
            + Sync
            + for<'e> Fn(
                &'e Executor<'a>,
                Vec<DependentValue<RuntimeValue<'a>>>,
            ) -> DependentValue<RuntimeValue<'a>>,
    ) -> Self {
        Self::Function(Arc::new(move |e, a| {
            let f = f.clone();
            Box::pin(async move { f(e, a) })
        }))
    }

    pub fn function_async(
        f: impl 'a
            + Clone
            + Send
            + Sync
            + for<'e> Fn(
                &'e Executor<'a>,
                Vec<DependentValue<RuntimeValue<'a>>>,
            )
                -> Pin<Box<dyn 'e + Future<Output = DependentValue<RuntimeValue<'a>>>>>,
    ) -> Self {
        Self::Function(Arc::new(move |e, a| {
            let f = f.clone();
            Box::pin(async move { f(e, a).await })
        }))
    }

    pub fn record(
        r: impl IntoIterator<Item = (impl Into<String>, impl Into<DependentValue<Self>>)>,
    ) -> Self {
        Self::Collection(Collection::record(r))
    }
}

impl<'a> fmt::Debug for RuntimeValue<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        TrackedFmtValue {
            value: self,
            arcs: &RefCell::new(vec![]),
        }
        .fmt(f)
    }
}

impl<'a> fmt::Display for RuntimeValue<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RuntimeValue::Primitive(v) => v.fmt(f),
            v => write!(f, "{:#?}", v),
        }
    }
}

impl<'a> RuntimeValue<'a> {
    pub fn resolve<'e>(
        self,
        executor: &'e Executor<'a>,
    ) -> Pin<Box<dyn Future<Output = DependentValue<RuntimeValue<'a>>> + 'e>> {
        Box::pin(async move {
            match self {
                RuntimeValue::Deferred(d) => d.resolve(executor).await.into(),
                RuntimeValue::Collection(Collection::Record(r)) => {
                    let mut fo = FuturesOrdered::new();

                    for (n, v) in r {
                        fo.push_back(async move { (n, v.resolve(executor).await) })
                    }

                    RuntimeValue::Collection(Collection::Record(fo.collect().await)).into()
                }
                RuntimeValue::Collection(Collection::List(l)) => {
                    let mut fo = FuturesOrdered::new();

                    for v in l {
                        fo.push_back(v.resolve(executor));
                    }

                    RuntimeValue::Collection(Collection::List(fo.collect().await)).into()
                }
                _ => self.into(),
            }
        })
    }

    pub fn defer(
        f: impl 'a
            + Send
            + Sync
            + for<'e> Fn(
                &'e Executor<'a>,
            )
                -> Pin<Box<dyn 'e + Future<Output = DependentValue<RuntimeValue<'a>>>>>,
    ) -> DependentValue<RuntimeValue<'a>> {
        RuntimeValue::Deferred(Deferred {
            inner: Arc::new(DeferredState::Deferred(Box::pin(f)).into()),
        })
        .into()
    }

    pub fn resource<R>(ctx: ExecutionContext<'a>, r: R) -> DependentValue<RuntimeValue<'a>>
    where
        R: 'a + Clone + Resource + Send + Sync,
        R::State: TypeOf,
    {
        Self::dynamic_resource(ctx, TypeBasedResource(r))
    }

    pub fn dynamic_resource<R>(ctx: ExecutionContext<'a>, r: R) -> DependentValue<RuntimeValue<'a>>
    where
        R: 'a + Clone + Resource + IdentifyResource + Send + Sync,
    {
        RuntimeValue::Function(Arc::new(move |executor, mut args| {
            let r = r.clone();
            Box::pin(
                args.remove(0)
                    .flat_map(|raw_arg| raw_arg.into_value())
                    .flat_map_async(move |arg_value, dependencies| async move {
                        let new_arg: R::Arguments = arg_value.deserialize().unwrap();

                        let id = r.resource_id(&new_arg);
                        match ctx.state.get(&id) {
                            None => {
                                executor
                                    .register_create(r, id.clone(), arg_value, dependencies)
                                    .await;
                                DependentValue::pending(vec![id])
                            }
                            Some(mut resource) => {
                                executor
                                    .used_resources
                                    .write()
                                    .await
                                    .insert(resource.id.clone());

                                let mut prev_state = resource.state.deserialize().unwrap();
                                let mut prev_arg = resource.arg.deserialize().unwrap();

                                prev_state = match r.read(prev_state, &mut prev_arg).await {
                                    Ok(Some(s)) => s,
                                    Ok(None) => {
                                        executor
                                            .register_create(r, id.clone(), arg_value, dependencies)
                                            .await;
                                        return DependentValue::pending(vec![id]);
                                    }
                                    Err(e) => {
                                        executor
                                            .read_errors
                                            .write()
                                            .await
                                            .push(ResourceError(resource.id, e));
                                        return DependentValue::pending(vec![id]);
                                    }
                                };

                                if new_arg != prev_arg {
                                    let mut plan = executor.plan.write().await;
                                    plan.register_update(
                                        id.clone(),
                                        Value::serialize(&prev_arg).unwrap(),
                                        Value::serialize(&new_arg).unwrap(),
                                        dependencies.clone(),
                                        {
                                            move || {
                                                let r = r.clone();
                                                Box::pin(async move {
                                                    resource.arg =
                                                        Value::serialize(&new_arg).unwrap();
                                                    let new_state =
                                                        r.update(new_arg, prev_state).await?;
                                                    resource.state =
                                                        Value::serialize(&new_state).unwrap();
                                                    resource.dependencies = dependencies;
                                                    Ok(resource)
                                                })
                                            }
                                        },
                                    );
                                    return DependentValue::pending(vec![id]);
                                }

                                DependentValue::new(resource.state.into()).with_dependency(id)
                            }
                        }
                    }),
            )
        }))
        .into()
    }

    pub fn as_func(
        &self,
    ) -> &dyn for<'e> Fn(
        &'e Executor<'a>,
        Vec<DependentValue<RuntimeValue<'a>>>,
    )
        -> Pin<Box<dyn 'e + Future<Output = DependentValue<RuntimeValue<'a>>>>> {
        if let RuntimeValue::Function(f) = self {
            f.as_ref()
        } else {
            panic!("{:?} is not a function", self)
        }
    }

    pub fn into_value(self) -> DependentValue<Value> {
        match self.try_into() {
            Ok(v) => v,
            Err(rv) => panic!("{:?} is not a known value", rv),
        }
    }

    pub fn as_primitive(&self) -> &Primitive {
        if let RuntimeValue::Primitive(v) = self {
            v
        } else {
            panic!("{:?} is not a known primitive", self)
        }
    }

    pub fn as_collection(&self) -> &Collection<DependentValue<RuntimeValue<'a>>> {
        if let RuntimeValue::Collection(v) = self {
            v
        } else {
            panic!("{:?} is not a known collection", self)
        }
    }
}

#[derive(Clone)]
pub struct Deferred<'a> {
    inner: Arc<RwLock<DeferredState<'a>>>,
}

impl<'a> Deferred<'a> {
    pub async fn resolve(&self, executor: &Executor<'a>) -> DependentValue<RuntimeValue<'a>> {
        {
            if let DeferredState::Resolved(v) = &*self.inner.read().await {
                return v.clone();
            }
        }

        {
            let mut guard = self.inner.write().await;
            match &*guard {
                DeferredState::Resolved(v) => v.clone(),
                DeferredState::Deferred(f) => {
                    let v = f(&executor).await;
                    *guard = DeferredState::Resolved(v.clone());
                    v
                }
            }
        }
        .resolve(executor)
        .await
    }
}

enum DeferredState<'a> {
    Deferred(
        Pin<
            Box<
                dyn 'a
                    + Send
                    + Sync
                    + for<'e> Fn(
                        &'e Executor<'a>,
                    ) -> Pin<
                        Box<dyn Future<Output = DependentValue<RuntimeValue<'a>>> + 'e>,
                    >,
            >,
        >,
    ),
    Resolved(DependentValue<RuntimeValue<'a>>),
}
