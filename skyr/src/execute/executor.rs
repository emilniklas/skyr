use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_std::sync::RwLock;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::StreamExt;

use crate::analyze::{External, ImportMap, SymbolTable};
use crate::{compile::*, DisplayAsDebug, Plugin, Resource, ResourceError, ResourceId};
use crate::{Plan, ResourceValue};
use crate::{PluginResource, State};

#[derive(Clone)]
pub struct ExecutionContext<'a> {
    parent: Option<Box<ExecutionContext<'a>>>,
    pub state: &'a State,
    table: &'a SymbolTable<'a>,
    bindings: Arc<RwLock<BTreeMap<NodeId, Value<'a>>>>,
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

    pub fn get_binding(&self, id: NodeId) -> Pin<Box<dyn '_ + Future<Output = Value<'a>>>> {
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

            'resources: for resource in state.all_not_in(&used) {
                for plugin in self.plugins.iter() {
                    if let Some(plugin_resource) = plugin.find_resource(&resource) {
                        plan.register_delete(resource.id.clone(), move |_, _| {
                            Box::pin(async move {
                                plugin_resource.delete(resource.state.clone()).await?;
                                Ok(())
                            })
                        });
                        continue 'resources;
                    }
                }

                panic!("no plugin took ownership of {:?}", &resource);
            }
        }

        Ok(plan)
    }

    pub fn execute_module(
        &self,
        ctx: ExecutionContext<'a>,
        module: &'a Module,
    ) -> Pin<Box<dyn '_ + Future<Output = Value<'a>>>> {
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

            Value::Record(fo.collect::<Vec<_>>().await)
        })
    }

    pub async fn execute_statement(
        &self,
        ctx: ExecutionContext<'a>,
        statement: &'a Statement,
    ) -> Value<'a> {
        let mut debug = None;
        let exp = match statement {
            Statement::Expression(e) => self.execute_expression(ctx, e),
            Statement::Assignment(a) => self.execute_assignment(ctx, a).await,
            Statement::Debug(d) => {
                debug = Some(d.span.clone());
                self.execute_expression(ctx, &d.expression)
            }
            Statement::Return(r) => return self.execute_expression(ctx, &r.expression),
            Statement::TypeDefinition(_) => Value::Nil,
            Statement::Import(i) => self.execute_import(ctx, i).await,
            Statement::If(i) => return self.execute_if(ctx, i).await,
            Statement::Block(b) => return self.execute_block(ctx, b).await,
        };
        Value::defer(move |e| {
            let exp = exp.clone();
            let debug = debug.clone();
            Box::pin(async move {
                let v = exp.resolve(e).await;
                if let Some(span) = debug {
                    if !v.is_pending() {
                        e.plan.write().await.register_debug(span, v);
                    }
                }
                Value::Nil
            })
        })
    }

    pub async fn execute_if(&self, ctx: ExecutionContext<'a>, if_: &'a If) -> Value<'a> {
        let condition = self.execute_expression(ctx.clone(), &if_.condition);

        Value::defer(move |e| {
            let condition = condition.clone();
            let ctx = ctx.clone();
            Box::pin(async move {
                let condition = condition.resolve(e).await;

                if let Value::Pending(deps) = condition {
                    e.is_pending.fetch_or(true, Ordering::SeqCst);
                    return Value::Pending(deps);
                }

                if condition.as_bool() {
                    e.execute_statement(ctx, &if_.consequence).await
                } else if let Some(else_clause) = &if_.else_clause {
                    e.execute_statement(ctx, else_clause).await
                } else {
                    Value::Nil
                }
            })
        })
    }

    pub fn execute_block(
        &self,
        ctx: ExecutionContext<'a>,
        block: &'a Block,
    ) -> Pin<Box<dyn '_ + Future<Output = Value<'a>>>> {
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
                .find(|v| !matches!(v, Value::Nil))
                .unwrap_or(Value::Nil)
        })
    }

    pub async fn execute_import(&self, ctx: ExecutionContext<'a>, import: &'a Import) -> Value<'a> {
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
    ) -> Value<'a> {
        let value = self.execute_expression(ctx.clone(), &assignment.value);
        let mut bindings = ctx.bindings.write().await;
        bindings.insert(assignment.id, value.clone());
        value
    }

    pub async fn execute_record(
        &self,
        ctx: ExecutionContext<'a>,
        record: &'a Record<Expression>,
    ) -> Value<'a> {
        Value::Record({
            let mut fo = FuturesOrdered::new();
            for f in &record.fields {
                let n = f.identifier.symbol.clone();
                let v = self.execute_expression(ctx.clone(), &f.value);
                fo.push_back(async move { (n, v.resolve(self).await) });
            }
            fo.collect().await
        })
    }

    pub fn execute_expression(
        &self,
        ctx: ExecutionContext<'a>,
        expression: &'a Expression,
    ) -> Value<'a> {
        Value::defer(move |executor| {
            let ctx = ctx.clone();
            Box::pin(async move {
                match expression {
                    Expression::StringLiteral(s) => Value::String(s.value.clone()),
                    Expression::IntegerLiteral(i) => Value::Integer(i.value),
                    Expression::BooleanLiteral(i) => Value::Boolean(i.value),
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

                        if let Value::Pending(deps) = callee {
                            executor.is_pending.fetch_or(true, Ordering::SeqCst);
                            return Value::Pending(deps);
                        }

                        if let Value::Function(f) = callee {
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

                        if let Value::Pending(deps) = subject {
                            executor.is_pending.fetch_or(true, Ordering::SeqCst);
                            return Value::Pending(deps);
                        }

                        subject.access_member(ma.identifier.symbol.as_str()).clone()
                    }
                    Expression::Construct(construct) => {
                        let subject = executor
                            .execute_expression(ctx.clone(), &construct.subject)
                            .resolve(executor)
                            .await;

                        if let Value::Pending(deps) = subject {
                            executor.is_pending.fetch_or(true, Ordering::SeqCst);
                            return Value::Pending(deps);
                        }

                        if let Value::Function(f) = subject {
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
                            (Value::Pending(mut l), _, Value::Pending(r)) => {
                                l.extend(r);
                                Value::Pending(l)
                            }

                            (Value::Pending(ids), _, _) | (_, _, Value::Pending(ids)) => {
                                Value::Pending(ids)
                            }

                            (
                                Value::Integer(lhs),
                                BinaryOperatorKind::LessThan,
                                Value::Integer(rhs),
                            ) => Value::Boolean(lhs < rhs),
                            (
                                Value::Integer(lhs),
                                BinaryOperatorKind::LessThanOrEqualTo,
                                Value::Integer(rhs),
                            ) => Value::Boolean(lhs <= rhs),
                            (
                                Value::Integer(lhs),
                                BinaryOperatorKind::EqualTo,
                                Value::Integer(rhs),
                            ) => Value::Boolean(lhs == rhs),
                            (
                                Value::Integer(lhs),
                                BinaryOperatorKind::NotEqualTo,
                                Value::Integer(rhs),
                            ) => Value::Boolean(lhs != rhs),
                            (
                                Value::Integer(lhs),
                                BinaryOperatorKind::GreaterThanOrEqualTo,
                                Value::Integer(rhs),
                            ) => Value::Boolean(lhs >= rhs),
                            (
                                Value::Integer(lhs),
                                BinaryOperatorKind::GreaterThan,
                                Value::Integer(rhs),
                            ) => Value::Boolean(lhs > rhs),

                            (
                                Value::Integer(lhs),
                                BinaryOperatorKind::Plus,
                                Value::Integer(rhs),
                            ) => Value::Integer(lhs + rhs),
                            (
                                Value::Integer(lhs),
                                BinaryOperatorKind::Minus,
                                Value::Integer(rhs),
                            ) => Value::Integer(lhs - rhs),
                            (
                                Value::Integer(lhs),
                                BinaryOperatorKind::Multiply,
                                Value::Integer(rhs),
                            ) => Value::Integer(lhs * rhs),
                            (
                                Value::Integer(lhs),
                                BinaryOperatorKind::Divide,
                                Value::Integer(rhs),
                            ) => Value::Integer(lhs / rhs),

                            (Value::Boolean(lhs), BinaryOperatorKind::And, Value::Boolean(rhs)) => {
                                Value::Boolean(lhs && rhs)
                            }
                            (Value::Boolean(lhs), BinaryOperatorKind::Or, Value::Boolean(rhs)) => {
                                Value::Boolean(lhs || rhs)
                            }

                            (
                                Value::String(mut lhs),
                                BinaryOperatorKind::Plus,
                                Value::String(rhs),
                            ) => Value::String({
                                lhs.push_str(&rhs);
                                lhs
                            }),
                            (
                                Value::String(mut lhs),
                                BinaryOperatorKind::Plus,
                                Value::Integer(rhs),
                            ) => Value::String({
                                lhs.push_str(&format!("{}", rhs));
                                lhs
                            }),
                            (
                                Value::String(lhs),
                                BinaryOperatorKind::EqualTo,
                                Value::String(rhs),
                            ) => Value::Boolean(lhs == rhs),
                            (
                                Value::String(lhs),
                                BinaryOperatorKind::NotEqualTo,
                                Value::String(rhs),
                            ) => Value::Boolean(lhs != rhs),

                            (lhs, op, rhs) => {
                                panic!("invalid operation: {:?} {:?} {:?}", lhs, op, rhs)
                            }
                        }
                    }

                    Expression::List(list) => Value::List(
                        list.elements
                            .iter()
                            .map(|e| executor.execute_expression(ctx.clone(), e))
                            .collect(),
                    ),
                }
            })
        })
    }

    pub fn execute_function(&self, ctx: ExecutionContext<'a>, function: &'a Function) -> Value<'a> {
        Value::Function(Arc::new(move |executor, args| {
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

    async fn register_create(
        &self,
        resource: impl PluginResource + 'a,
        id: ResourceId,
        args: Vec<Value<'a>>,
    ) {
        let mut plan = self.plan.write().await;
        plan.register_create(id.clone(), args[0].clone(), {
            move |id, arg| {
                Box::pin(async move {
                    Ok(Resource {
                        id,
                        arg: arg.clone().into(),
                        state: resource.create(arg).await?,
                    })
                })
            }
        });
    }
}

#[derive(Clone)]
pub enum Value<'a> {
    Nil,
    String(String),
    Integer(i128),
    Boolean(bool),
    Deferred(Deferred<'a>),
    Record(Vec<(String, Value<'a>)>),
    Function(
        Arc<
            dyn 'a
                + Send
                + Sync
                + for<'e> Fn(
                    &'e Executor<'a>,
                    Vec<Value<'a>>,
                ) -> Pin<Box<dyn Future<Output = Value<'a>> + 'e>>,
        >,
    ),
    Pending(Vec<ResourceId>),
    List(Vec<Value<'a>>),
}

struct TrackedFmtValue<'v, 'a> {
    value: &'v Value<'a>,
    arcs: &'v std::cell::RefCell<Vec<Arc<RwLock<DeferredState<'a>>>>>,
}

impl<'v, 'a> fmt::Debug for TrackedFmtValue<'v, 'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.value {
            Value::Deferred(Deferred { inner }) => match inner.try_read() {
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
            Value::String(s) => fmt::Debug::fmt(s, f),
            Value::Integer(i) => fmt::Debug::fmt(i, f),
            Value::Boolean(b) => fmt::Debug::fmt(b, f),
            Value::Nil => write!(f, "<nil>"),
            Value::Record(fields) => {
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
            Value::List(elements) => {
                let mut l = f.debug_list();
                for value in elements.iter() {
                    l.entry(&TrackedFmtValue {
                        value,
                        arcs: self.arcs,
                    });
                }
                l.finish()
            }
            Value::Function(_) => write!(f, "<fn>"),
            Value::Pending(_) => write!(f, "<pending>"),
        }
    }
}

impl<'a> Value<'a> {
    pub fn dependencies(&self) -> Vec<ResourceId> {
        match self {
            Value::Nil | Value::String(_) | Value::Integer(_) | Value::Boolean(_) => vec![],
            Value::Deferred(_) => panic!("cannot get dependencies from deferred"),
            Value::Record(r) => {
                let mut result = vec![];
                for (_, v) in r {
                    result.extend(v.dependencies());
                }
                result
            }
            Value::List(l) => {
                let mut result = vec![];
                for v in l {
                    result.extend(v.dependencies());
                }
                result
            }
            Value::Function(_) => todo!(),
            Value::Pending(ids) => ids.clone(),
        }
    }

    pub fn is_pending(&self) -> bool {
        match self {
            Value::Nil | Value::String(_) | Value::Integer(_) | Value::Boolean(_) => false,

            Value::Record(r) => r.iter().any(|(_, v)| v.is_pending()),
            Value::List(l) => l.iter().any(|e| e.is_pending()),

            Value::Function(_) => todo!(),

            Value::Pending(_) | Value::Deferred(_) => true,
        }
    }

    pub fn record(i: impl IntoIterator<Item = (impl Into<String>, Value<'a>)>) -> Self {
        Self::Record(i.into_iter().map(|(n, t)| (n.into(), t)).collect())
    }

    pub fn function_sync(
        f: impl 'a + Copy + Send + Sync + for<'e> Fn(&'e Executor<'a>, Vec<Value<'a>>) -> Value<'a>,
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
                Vec<Value<'a>>,
            ) -> Pin<Box<dyn 'e + Future<Output = Value<'a>>>>,
    ) -> Self {
        Self::Function(Arc::new(move |e, a| Box::pin(async move { f(e, a).await })))
    }
}

impl<'a> fmt::Debug for Value<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        TrackedFmtValue {
            value: self,
            arcs: &RefCell::new(vec![]),
        }
        .fmt(f)
    }
}

impl<'a> fmt::Display for Value<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::String(s) => s.fmt(f),
            Value::Integer(i) => i.fmt(f),
            v => write!(f, "{:#?}", v),
        }
    }
}

impl<'a> Value<'a> {
    pub fn resolve<'e>(
        self,
        executor: &'e Executor<'a>,
    ) -> Pin<Box<dyn Future<Output = Value<'a>> + 'e>> {
        Box::pin(async move {
            match self {
                Value::Deferred(d) => d.resolve(executor).await,
                Value::Record(r) => {
                    let mut fo = FuturesOrdered::new();

                    for (n, v) in r {
                        fo.push_back(async move { (n, v.resolve(executor).await) })
                    }

                    Value::Record(fo.collect().await)
                }
                Value::List(l) => {
                    let mut fo = FuturesOrdered::new();

                    for v in l {
                        fo.push_back(v.resolve(executor));
                    }

                    Value::List(fo.collect().await)
                }
                _ => self,
            }
        })
    }

    pub fn defer(
        f: impl 'a
            + Send
            + Sync
            + for<'e> Fn(&'e Executor<'a>) -> Pin<Box<dyn 'e + Future<Output = Value<'a>>>>,
    ) -> Value<'a> {
        Value::Deferred(Deferred {
            inner: Arc::new(DeferredState::Deferred(Box::pin(f)).into()),
        })
    }

    pub fn resource(
        ctx: ExecutionContext<'a>,
        r: impl 'a + Clone + PluginResource + Send + Sync,
    ) -> Value<'a> {
        Value::Function(Arc::new(move |executor, args| {
            let r = r.clone();
            let dependencies = args[0].dependencies();
            if !dependencies.is_empty() {
                return Box::pin(async move { Value::Pending(dependencies) });
            }

            let id = r.resource_id(&args[0]);
            Box::pin(async move {
                match ctx.state.get(&id) {
                    None => {
                        executor.register_create(r, id.clone(), args).await;
                        Value::Pending(vec![id])
                    }
                    Some(mut resource) => {
                        executor
                            .used_resources
                            .write()
                            .await
                            .insert(resource.id.clone());

                        resource.state = match r.read(resource.state, &mut resource.arg).await {
                            Ok(Some(s)) => s,
                            Ok(None) => {
                                executor.register_create(r, id.clone(), args).await;
                                return Value::Pending(vec![id]);
                            }
                            Err(e) => {
                                executor
                                    .read_errors
                                    .write()
                                    .await
                                    .push(ResourceError(resource.id, e));
                                return Value::Pending(vec![id]);
                            }
                        };

                        if resource.arg != args[0].clone().into() {
                            let mut plan = executor.plan.write().await;
                            plan.register_update(
                                id.clone(),
                                resource.arg.clone(),
                                args[0].clone(),
                                {
                                    move |_id, new_arg| {
                                        let r = r.clone();
                                        Box::pin(async move {
                                            resource.arg = new_arg.clone().into();
                                            resource.state =
                                                r.update(new_arg, resource.state).await?;
                                            Ok(resource)
                                        })
                                    }
                                },
                            );
                            return Value::Pending(vec![id]);
                        }

                        resource.state.into()
                    }
                }
            })
        }))
    }

    pub fn access_member(&self, name: &str) -> &Value<'a> {
        if let Value::Record(r) = self {
            for (n, v) in r {
                if name == *n {
                    return v;
                }
            }
            panic!("undefined field {}", name);
        } else {
            panic!("cannot access {} on {:?}", name, self);
        }
    }

    pub fn as_str(&self) -> &str {
        if let Value::String(s) = self {
            &s
        } else {
            panic!("{:?} is not a string", self)
        }
    }

    pub fn as_bool(&self) -> bool {
        if let Value::Boolean(b) = self {
            *b
        } else {
            panic!("{:?} is not a boolean", self)
        }
    }

    pub fn as_vec(&self) -> &Vec<Value<'a>> {
        if let Value::List(l) = self {
            l
        } else {
            panic!("{:?} is not a list", self)
        }
    }

    pub fn as_func(
        &self,
    ) -> &dyn for<'e> Fn(
        &'e Executor<'a>,
        Vec<Value<'a>>,
    ) -> Pin<Box<dyn 'e + Future<Output = Value<'a>>>> {
        if let Value::Function(f) = self {
            f.as_ref()
        } else {
            panic!("{:?} is not a function", self)
        }
    }

    pub fn as_i128(&self) -> i128 {
        if let Value::Integer(i) = self {
            *i
        } else {
            panic!("{:?} is not an integer", self)
        }
    }

    pub fn as_usize(&self) -> usize {
        self.as_i128() as usize
    }
}

#[derive(Clone)]
pub struct Deferred<'a> {
    inner: Arc<RwLock<DeferredState<'a>>>,
}

impl<'a> Deferred<'a> {
    pub async fn resolve(&self, executor: &Executor<'a>) -> Value<'a> {
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
                    + for<'e> Fn(&'e Executor<'a>) -> Pin<Box<dyn Future<Output = Value<'a>> + 'e>>,
            >,
        >,
    ),
    Resolved(Value<'a>),
}

impl<'a> From<ResourceValue> for Value<'a> {
    fn from(value: ResourceValue) -> Self {
        match value {
            ResourceValue::Nil => Self::Nil,
            ResourceValue::String(s) => Self::String(s),
            ResourceValue::Integer(i) => Self::Integer(i),
            ResourceValue::Boolean(b) => Self::Boolean(b),
            ResourceValue::Record(r) => {
                Self::Record(r.into_iter().map(|(n, v)| (n, v.into())).collect())
            }
            ResourceValue::List(l) => Self::List(l.into_iter().map(|e| e.into()).collect()),
        }
    }
}
