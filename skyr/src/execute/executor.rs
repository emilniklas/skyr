use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_std::sync::RwLock;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::StreamExt;

use crate::analyze::{External, ImportMap, SymbolTable};
use crate::{compile::*, Resource, ResourceId};
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

#[derive(Default)]
pub struct Executor<'a> {
    pub plan: RwLock<Plan<'a>>,
}

impl<'a> Executor<'a> {
    pub fn new() -> Self {
        Executor::default()
    }

    pub fn finalize(self) -> Plan<'a> {
        self.plan.into_inner()
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
        };
        Value::defer(move |e| {
            let exp = exp.clone();
            let debug = debug.clone();
            Box::pin(async move {
                let v = exp.resolve(e).await;
                if let Some(span) = debug {
                    println!("{:?} -> {:#?}", span, v);
                }
                Value::Nil
            })
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
                            return Value::Pending(deps);
                        }

                        subject.access_member(ma.identifier.symbol.as_str())
                    }
                    Expression::Construct(construct) => {
                        let subject = executor
                            .execute_expression(ctx.clone(), &construct.subject)
                            .resolve(executor)
                            .await;

                        if let Value::Pending(deps) = subject {
                            return Value::Pending(deps);
                        }

                        if let Value::Function(f) = subject {
                            let args = vec![executor.execute_record(ctx, &construct.record).await];
                            f(executor, args).await
                        } else {
                            panic!("cannot construct {:?}", subject);
                        }
                    }
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

                let fo = FuturesUnordered::new();

                let mut values = vec![];
                for statement in function.body.iter() {
                    values.push(executor.execute_statement(ctx.clone(), statement).await);
                }

                for value in values {
                    fo.push(value.resolve(executor));
                }

                fo.collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .find(|v| !matches!(v, Value::Nil))
                    .unwrap_or(Value::Nil)
            })
        }))
    }
}

#[derive(Clone)]
pub enum Value<'a> {
    Nil,
    String(String),
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
            Value::Function(_) => write!(f, "<fn>"),
            Value::Pending(_) => write!(f, "<pending>"),
        }
    }
}

impl<'a> Value<'a> {
    pub fn dependencies(&self) -> Vec<ResourceId> {
        match self {
            Value::Nil | Value::String(_) => vec![],
            Value::Deferred(_) => panic!("cannot get dependencies from deferred"),
            Value::Record(r) => {
                let mut result = vec![];
                for (_, v) in r {
                    result.extend(v.dependencies());
                }
                result
            }
            Value::Function(_) => todo!(),
            Value::Pending(ids) => ids.clone(),
        }
    }

    pub fn record(i: impl IntoIterator<Item = (impl Into<String>, Value<'a>)>) -> Self {
        Self::Record(i.into_iter().map(|(n, t)| (n.into(), t)).collect())
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

impl<'a> Value<'a> {
    pub fn resolve<'e>(
        self,
        executor: &'e Executor<'a>,
    ) -> Pin<Box<dyn Future<Output = Value<'a>> + 'e>> {
        Box::pin(async move {
            if let Value::Deferred(d) = self {
                d.resolve(executor).await
            } else {
                self
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

            let id = r.id(&args[0]);
            Box::pin(async move {
                let r = r.clone();
                match ctx.state.get(&id) {
                    None => {
                        let mut plan = executor.plan.write().await;
                        plan.register_create(id.clone(), args[0].clone(), {
                            move |id, arg| {
                                let r = r.clone();
                                Box::pin(async move {
                                    Resource {
                                        id,
                                        state: r.create(arg).await,
                                    }
                                })
                            }
                        });
                        Value::Pending(vec![id])
                    }
                    Some(resource) => resource.state.into(),
                }
            })
        }))
    }

    pub fn access_member(&self, name: &str) -> Value<'a> {
        if let Value::Record(r) = self {
            for (n, v) in r {
                if name == *n {
                    return v.clone();
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

struct DisplayAsDebug<T>(T);

impl<T: fmt::Display> fmt::Debug for DisplayAsDebug<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a> From<ResourceValue> for Value<'a> {
    fn from(value: ResourceValue) -> Self {
        match value {
            ResourceValue::String(s) => Self::String(s),
            ResourceValue::Record(r) => {
                Self::Record(r.into_iter().map(|(n, v)| (n, v.into())).collect())
            }
        }
    }
}
