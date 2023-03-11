use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{fmt, io};

use async_std::sync::RwLock;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::StreamExt;

use crate::analyze::{External, ImportMap, SymbolTable};
use crate::{
    compile::*, DisplayAsDebug, Plugin, Primitive, ResourceError, ResourceId, ResourceState,
};
use crate::{Collection, Plan, Value};
use crate::{Resource, State};

#[derive(Clone)]
pub struct ExecutionContext<'a> {
    parent: Option<Box<ExecutionContext<'a>>>,
    pub state: &'a State,
    table: &'a SymbolTable<'a>,
    bindings: Arc<RwLock<BTreeMap<NodeId, RuntimeValue<'a>>>>,
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

    pub fn get_binding(&self, id: NodeId) -> Pin<Box<dyn '_ + Future<Output = RuntimeValue<'a>>>> {
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
    plugins: &'a [Box<dyn Plugin>],
    read_errors: RwLock<Vec<ResourceError>>,
}

impl<'a> Executor<'a> {
    pub fn new(plugins: &'a [Box<dyn Plugin>]) -> Self {
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
                plan.register_delete(resource.id.clone(), move |_, _| {
                    Box::pin(async move {
                        for plugin in self.plugins.iter() {
                            if let Some(()) = plugin.delete_matching_resource(&resource).await? {
                                return Ok(());
                            }
                        }

                        Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("no plugin took ownership of resource {:?}", resource.id),
                        ))
                    })
                });
            }
        }

        Ok(plan)
    }

    pub fn execute_module(
        &self,
        ctx: ExecutionContext<'a>,
        module: &'a Module,
    ) -> Pin<Box<dyn '_ + Future<Output = RuntimeValue<'a>>>> {
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
                fo.push(async move { (name, value.resolve(self).await) });
            }

            RuntimeValue::Collection(Collection::record(fo.collect::<Vec<_>>().await))
        })
    }

    pub async fn execute_statement(
        &self,
        ctx: ExecutionContext<'a>,
        statement: &'a Statement,
    ) -> RuntimeValue<'a> {
        let mut debug = None;
        let exp = match statement {
            Statement::Expression(e) => self.execute_expression(ctx, e),
            Statement::Assignment(a) => self.execute_assignment(ctx, a).await,
            Statement::Debug(d) => {
                debug = Some(d.span.clone());
                self.execute_expression(ctx, &d.expression)
            }
            Statement::Return(r) => return self.execute_expression(ctx, &r.expression),
            Statement::TypeDefinition(_) => RuntimeValue::Primitive(Primitive::Nil),
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
                    if !v.is_pending() {
                        e.plan.write().await.register_debug(span, v);
                    }
                }
                RuntimeValue::Primitive(Primitive::Nil)
            })
        })
    }

    pub async fn execute_if(&self, ctx: ExecutionContext<'a>, if_: &'a If) -> RuntimeValue<'a> {
        let condition = self.execute_expression(ctx.clone(), &if_.condition);

        RuntimeValue::defer(move |e| {
            let condition = condition.clone();
            let ctx = ctx.clone();
            Box::pin(async move {
                let condition = condition.resolve(e).await;

                if let RuntimeValue::Pending(deps) = condition {
                    e.is_pending.fetch_or(true, Ordering::SeqCst);
                    return RuntimeValue::Pending(deps);
                }

                if condition.as_primitive().as_bool() {
                    e.execute_statement(ctx, &if_.consequence).await
                } else if let Some(else_clause) = &if_.else_clause {
                    e.execute_statement(ctx, else_clause).await
                } else {
                    RuntimeValue::Primitive(Primitive::Nil)
                }
            })
        })
    }

    pub fn execute_block(
        &self,
        ctx: ExecutionContext<'a>,
        block: &'a Block,
    ) -> Pin<Box<dyn '_ + Future<Output = RuntimeValue<'a>>>> {
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
                .find(|v| !matches!(v, RuntimeValue::Primitive(Primitive::Nil)))
                .unwrap_or(RuntimeValue::Primitive(Primitive::Nil))
        })
    }

    pub async fn execute_import(
        &self,
        ctx: ExecutionContext<'a>,
        import: &'a Import,
    ) -> RuntimeValue<'a> {
        let external = ctx.import_map.resolve(import).expect("unresolved import");

        let value = match external {
            External::Module(m) => self.execute_module(ctx.new_empty(), m).await,
            External::Plugin(p) => p.module_value(ctx.clone()),
        };

        ctx.bindings.write().await.insert(import.id, value.clone());

        value
    }

    pub async fn execute_assignment(
        &self,
        ctx: ExecutionContext<'a>,
        assignment: &'a Assignment,
    ) -> RuntimeValue<'a> {
        let value = self.execute_expression(ctx.clone(), &assignment.value);
        let mut bindings = ctx.bindings.write().await;
        bindings.insert(assignment.id, value.clone());
        value
    }

    pub async fn execute_record(
        &self,
        ctx: ExecutionContext<'a>,
        record: &'a Record<Expression>,
    ) -> RuntimeValue<'a> {
        RuntimeValue::Collection(Collection::Record({
            let mut fo = FuturesOrdered::new();
            for f in &record.fields {
                let n = f.identifier.symbol.clone();
                let v = self.execute_expression(ctx.clone(), &f.value);
                fo.push_back(async move { (n.into(), v.resolve(self).await) });
            }
            fo.collect().await
        }))
    }

    pub fn execute_expression(
        &self,
        ctx: ExecutionContext<'a>,
        expression: &'a Expression,
    ) -> RuntimeValue<'a> {
        RuntimeValue::defer(move |executor| {
            let ctx = ctx.clone();
            Box::pin(async move {
                match expression {
                    Expression::StringLiteral(s) => {
                        RuntimeValue::Primitive(Primitive::string(s.value.clone()))
                    }
                    Expression::IntegerLiteral(i) => {
                        RuntimeValue::Primitive(Primitive::Integer(i.value))
                    }
                    Expression::BooleanLiteral(i) => {
                        RuntimeValue::Primitive(Primitive::Boolean(i.value))
                    }
                    Expression::Identifier(id) => {
                        let binding_id =
                            ctx.table.declaration(id).expect("undefined reference").id();
                        ctx.get_binding(binding_id).await
                    }
                    Expression::Record(record) => executor.execute_record(ctx, record).await,
                    Expression::Function(f) => executor.execute_function(ctx, f),
                    Expression::Call(c) => {
                        let callee = executor
                            .execute_expression(ctx.clone(), &c.callee)
                            .resolve(executor)
                            .await;

                        if let RuntimeValue::Pending(deps) = callee {
                            executor.is_pending.fetch_or(true, Ordering::SeqCst);
                            return RuntimeValue::Pending(deps);
                        }

                        if let RuntimeValue::Function(f) = callee {
                            let mut fo = FuturesOrdered::new();
                            for arg in &c.arguments {
                                let v = executor.execute_expression(ctx.clone(), arg);
                                fo.push_back(async move { v.resolve(executor).await });
                            }
                            let args = fo.collect::<Vec<_>>().await;
                            f(executor, args).await
                        } else {
                            panic!("cannot call {:?}", callee);
                        }
                    }
                    Expression::MemberAccess(ma) => {
                        let subject = executor
                            .execute_expression(ctx, &ma.subject)
                            .resolve(executor)
                            .await;

                        if let RuntimeValue::Pending(deps) = subject {
                            executor.is_pending.fetch_or(true, Ordering::SeqCst);
                            return RuntimeValue::Pending(deps);
                        }

                        subject
                            .as_collection()
                            .access_member(ma.identifier.symbol.as_str())
                            .clone()
                    }
                    Expression::Construct(construct) => {
                        let subject = executor
                            .execute_expression(ctx.clone(), &construct.subject)
                            .resolve(executor)
                            .await;

                        if let RuntimeValue::Pending(deps) = subject {
                            executor.is_pending.fetch_or(true, Ordering::SeqCst);
                            return RuntimeValue::Pending(deps);
                        }

                        if let RuntimeValue::Function(f) = subject {
                            let args = vec![executor.execute_record(ctx, &construct.record).await];
                            f(executor, args).await
                        } else {
                            panic!("cannot construct {:?}", subject);
                        }
                    }
                    Expression::BinaryOperation(op) => {
                        let (lhs, rhs) = futures::future::join(
                            executor
                                .execute_expression(ctx.clone(), &op.lhs)
                                .resolve(executor),
                            executor
                                .execute_expression(ctx.clone(), &op.rhs)
                                .resolve(executor),
                        )
                        .await;

                        match (lhs, &op.operator.kind, rhs) {
                            (RuntimeValue::Pending(mut l), _, RuntimeValue::Pending(r)) => {
                                l.extend(r);
                                RuntimeValue::Pending(l)
                            }

                            (RuntimeValue::Pending(ids), _, _)
                            | (_, _, RuntimeValue::Pending(ids)) => RuntimeValue::Pending(ids),

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
                        }
                    }

                    Expression::List(list) => RuntimeValue::Collection(Collection::List(
                        list.elements
                            .iter()
                            .map(|e| executor.execute_expression(ctx.clone(), e))
                            .collect(),
                    )),
                }
            })
        })
    }

    pub fn execute_function(
        &self,
        ctx: ExecutionContext<'a>,
        function: &'a Function,
    ) -> RuntimeValue<'a> {
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
    }

    async fn register_create<R: Resource + 'a>(
        &self,
        resource: R,
        id: ResourceId,
        mut args: Vec<RuntimeValue<'a>>,
    ) {
        let mut plan = self.plan.write().await;
        let arg_value = args.remove(0).into_value();
        plan.register_create(id.clone(), arg_value.clone(), {
            move || {
                Box::pin(async move {
                    let arg: R::Arguments = arg_value.deserialize().unwrap();
                    let state = resource.create(arg).await?;
                    Ok(ResourceState {
                        id,
                        arg: arg_value,
                        state: Value::serialize(&state).unwrap(),
                    })
                })
            }
        });
    }
}

#[derive(Clone)]
pub enum RuntimeValue<'a> {
    Primitive(Primitive),
    Collection(Collection<RuntimeValue<'a>>),
    Deferred(Deferred<'a>),
    Function(
        Arc<
            dyn 'a
                + Send
                + Sync
                + for<'e> Fn(
                    &'e Executor<'a>,
                    Vec<RuntimeValue<'a>>,
                ) -> Pin<Box<dyn Future<Output = RuntimeValue<'a>> + 'e>>,
        >,
    ),
    Pending(Vec<ResourceId>),
}

impl<'a> From<Value> for RuntimeValue<'a> {
    fn from(value: Value) -> Self {
        match value {
            Value::Primitive(p) => RuntimeValue::Primitive(p),
            Value::Collection(c) => RuntimeValue::Collection(c.map(Into::into)),
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

                        TrackedFmtValue {
                            value: r,
                            arcs: self.arcs,
                        }
                        .fmt(f)
                    }
                    DeferredState::Deferred(_) => write!(f, "<deferred>"),
                },
            },
            RuntimeValue::Function(_) => write!(f, "<fn>"),
            RuntimeValue::Pending(_) => write!(f, "<pending>"),
            RuntimeValue::Collection(Collection::Record(fields)) => {
                let mut s = f.debug_map();
                for (name, value) in fields.iter() {
                    s.entry(
                        &DisplayAsDebug(name),
                        &TrackedFmtValue {
                            value,
                            arcs: self.arcs,
                        },
                    );
                }
                s.finish()
            }
            RuntimeValue::Collection(Collection::List(elements)) => {
                let mut l = f.debug_list();
                for value in elements.iter() {
                    l.entry(&TrackedFmtValue {
                        value,
                        arcs: self.arcs,
                    });
                }
                l.finish()
            }
            RuntimeValue::Collection(Collection::Tuple(elements)) => {
                let mut l = f.debug_tuple("");
                for value in elements.iter() {
                    l.field(&TrackedFmtValue {
                        value,
                        arcs: self.arcs,
                    });
                }
                l.finish()
            }
            RuntimeValue::Primitive(v) => v.fmt(f),
        }
    }
}

impl<'a> RuntimeValue<'a> {
    pub fn dependencies(&self) -> Vec<ResourceId> {
        match self {
            RuntimeValue::Deferred(_) => panic!("cannot get dependencies from deferred"),
            RuntimeValue::Function(_) => todo!(),
            RuntimeValue::Pending(ids) => ids.clone(),

            RuntimeValue::Collection(Collection::Record(r)) => {
                let mut result = vec![];
                for (_, v) in r {
                    result.extend(v.dependencies());
                }
                result
            }
            RuntimeValue::Collection(Collection::List(l)) => {
                let mut result = vec![];
                for v in l {
                    result.extend(v.dependencies());
                }
                result
            }
            RuntimeValue::Collection(Collection::Tuple(l)) => {
                let mut result = vec![];
                for v in l {
                    result.extend(v.dependencies());
                }
                result
            }
            RuntimeValue::Primitive(_) => vec![],
        }
    }

    pub fn is_pending(&self) -> bool {
        match self {
            RuntimeValue::Function(_) => todo!(),
            RuntimeValue::Pending(_) | RuntimeValue::Deferred(_) => true,

            RuntimeValue::Collection(Collection::Record(r)) => {
                r.iter().any(|(_, v)| v.is_pending())
            }
            RuntimeValue::Collection(Collection::List(l)) => l.iter().any(|e| e.is_pending()),
            RuntimeValue::Collection(Collection::Tuple(l)) => l.iter().any(|e| e.is_pending()),

            RuntimeValue::Primitive(_) => false,
        }
    }

    pub fn function_sync(
        f: impl 'a
            + Copy
            + Send
            + Sync
            + for<'e> Fn(&'e Executor<'a>, Vec<RuntimeValue<'a>>) -> RuntimeValue<'a>,
    ) -> Self {
        Self::Function(Arc::new(move |e, a| Box::pin(async move { f(e, a) })))
    }

    pub fn function_async(
        f: impl 'a
            + Copy
            + Send
            + Sync
            + for<'e> Fn(
                &'e Executor<'a>,
                Vec<RuntimeValue<'a>>,
            ) -> Pin<Box<dyn 'e + Future<Output = RuntimeValue<'a>>>>,
    ) -> Self {
        Self::Function(Arc::new(move |e, a| Box::pin(async move { f(e, a).await })))
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
    ) -> Pin<Box<dyn Future<Output = RuntimeValue<'a>> + 'e>> {
        Box::pin(async move {
            match self {
                RuntimeValue::Deferred(d) => d.resolve(executor).await,
                RuntimeValue::Collection(Collection::Record(r)) => {
                    let mut fo = FuturesOrdered::new();

                    for (n, v) in r {
                        fo.push_back(async move { (n, v.resolve(executor).await) })
                    }

                    RuntimeValue::Collection(Collection::Record(fo.collect().await))
                }
                RuntimeValue::Collection(Collection::List(l)) => {
                    let mut fo = FuturesOrdered::new();

                    for v in l {
                        fo.push_back(v.resolve(executor));
                    }

                    RuntimeValue::Collection(Collection::List(fo.collect().await))
                }
                _ => self,
            }
        })
    }

    pub fn defer(
        f: impl 'a
            + Send
            + Sync
            + for<'e> Fn(&'e Executor<'a>) -> Pin<Box<dyn 'e + Future<Output = RuntimeValue<'a>>>>,
    ) -> RuntimeValue<'a> {
        RuntimeValue::Deferred(Deferred {
            inner: Arc::new(DeferredState::Deferred(Box::pin(f)).into()),
        })
    }

    pub fn resource<R>(ctx: ExecutionContext<'a>, r: R) -> RuntimeValue<'a>
    where
        R: 'a + Clone + Resource + Send + Sync,
    {
        RuntimeValue::Function(Arc::new(move |executor, args| {
            let r = r.clone();
            let dependencies = args[0].dependencies();
            if !dependencies.is_empty() {
                return Box::pin(async move { RuntimeValue::Pending(dependencies) });
            }

            let new_arg: R::Arguments = args[0].clone().into_value().deserialize().unwrap();

            let id = r.resource_id(&new_arg);
            Box::pin(async move {
                match ctx.state.get(&id) {
                    None => {
                        executor.register_create(r, id.clone(), args).await;
                        RuntimeValue::Pending(vec![id])
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
                                executor.register_create(r, id.clone(), args).await;
                                return RuntimeValue::Pending(vec![id]);
                            }
                            Err(e) => {
                                executor
                                    .read_errors
                                    .write()
                                    .await
                                    .push(ResourceError(resource.id, e));
                                return RuntimeValue::Pending(vec![id]);
                            }
                        };

                        if new_arg != prev_arg {
                            let mut plan = executor.plan.write().await;
                            plan.register_update(
                                id.clone(),
                                Value::serialize(&prev_arg).unwrap(),
                                Value::serialize(&new_arg).unwrap(),
                                {
                                    move || {
                                        let r = r.clone();
                                        Box::pin(async move {
                                            resource.arg = Value::serialize(&new_arg).unwrap();
                                            let new_state = r.update(new_arg, prev_state).await?;
                                            resource.state = Value::serialize(&new_state).unwrap();
                                            Ok(resource)
                                        })
                                    }
                                },
                            );
                            return RuntimeValue::Pending(vec![id]);
                        }

                        resource.state.into()
                    }
                }
            })
        }))
    }

    pub fn as_func(
        &self,
    ) -> &dyn for<'e> Fn(
        &'e Executor<'a>,
        Vec<RuntimeValue<'a>>,
    ) -> Pin<Box<dyn 'e + Future<Output = RuntimeValue<'a>>>> {
        if let RuntimeValue::Function(f) = self {
            f.as_ref()
        } else {
            panic!("{:?} is not a function", self)
        }
    }

    pub fn into_value(self) -> Value {
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

    pub fn as_collection(&self) -> &Collection<RuntimeValue<'a>> {
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
    pub async fn resolve(&self, executor: &Executor<'a>) -> RuntimeValue<'a> {
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
                    )
                        -> Pin<Box<dyn Future<Output = RuntimeValue<'a>> + 'e>>,
            >,
        >,
    ),
    Resolved(RuntimeValue<'a>),
}
