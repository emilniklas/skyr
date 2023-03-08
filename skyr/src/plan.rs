use std::fmt;
use std::pin::Pin;
use std::time::{Duration, Instant};

use async_std::channel::Sender;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};

use crate::compile::Span;
use crate::execute::Value;
use crate::{AnalyzedProgram, Resource, ResourceId, ResourceValue, State};

pub enum PlanStepKind<'a> {
    Create(
        Value<'a>,
        Box<dyn 'a + Fn(ResourceId, Value<'a>) -> Pin<Box<dyn 'a + Future<Output = Resource>>>>,
    ),
    Update(
        Value<'a>,
        Box<dyn 'a + Fn(ResourceId, Value<'a>) -> Pin<Box<dyn 'a + Future<Output = Resource>>>>,
    ),
    Delete(Box<dyn 'a + Fn(ResourceId, ResourceValue) -> Pin<Box<dyn 'a + Future<Output = ()>>>>),
}

#[derive(Default)]
pub struct Plan<'a> {
    steps: Vec<(ResourceId, PlanStepKind<'a>)>,
    debug_messages: Vec<(Span, Value<'a>)>,
}

impl<'a> fmt::Debug for Plan<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, (_, value)) in self.debug_messages.iter().enumerate() {
            if i > 0 {
                write!(f, "\n")?;
            }
            write!(f, "ℹ️ {}", value)?;
        }

        for (i, (id, kind)) in self.steps.iter().enumerate() {
            if i > 0 {
                write!(f, "\n")?;
            }
            match kind {
                PlanStepKind::Create(value, _) => write!(f, "✴️ Create {:?} {:#?}", id, value)?,
                PlanStepKind::Update(value, _) => write!(f, "⬆️ Update {:?} {:#?}", id, value)?,
                PlanStepKind::Delete(_) => write!(f, "🚨 Delete {:?}", id)?,
            }
        }
        Ok(())
    }
}

impl<'a> Plan<'a> {
    pub fn new() -> Self {
        Plan::default()
    }

    pub fn register_create(
        &mut self,
        id: ResourceId,
        args: Value<'a>,
        f: impl 'a + Fn(ResourceId, Value<'a>) -> Pin<Box<dyn 'a + Future<Output = Resource>>>,
    ) {
        self.steps
            .push((id, PlanStepKind::Create(args, Box::new(f))));
    }

    pub fn register_update(
        &mut self,
        id: ResourceId,
        args: Value<'a>,
        f: impl 'a + Fn(ResourceId, Value<'a>) -> Pin<Box<dyn 'a + Future<Output = Resource>>>,
    ) {
        self.steps
            .push((id, PlanStepKind::Update(args, Box::new(f))));
    }

    pub fn register_delete(
        &mut self,
        id: ResourceId,
        f: impl 'a + Fn(ResourceId, ResourceValue) -> Pin<Box<dyn 'a + Future<Output = ()>>>,
    ) {
        self.steps.push((id, PlanStepKind::Delete(Box::new(f))));
    }

    pub fn register_debug(&mut self, span: Span, value: Value<'a>) {
        self.debug_messages.push((span, value));
    }

    pub async fn execute(
        self,
        program: &'a AnalyzedProgram<'a>,
        state: &'a State,
        events_tx: Sender<PlanExecutionEvent>,
    ) -> Plan<'a> {
        let fo = FuturesUnordered::<Pin<Box<dyn Future<Output = ()>>>>::new();
        let before_all = Instant::now();
        for (id, kind) in self.steps {
            let start = Instant::now();
            let (tx, mut rx) = futures::channel::oneshot::channel::<()>();
            fo.push({
                let id = id.clone();
                let mut event = match &kind {
                    PlanStepKind::Create(_, _) => PlanExecutionEvent::Creating(id, Duration::ZERO),
                    PlanStepKind::Update(_, _) => PlanExecutionEvent::Updating(id, Duration::ZERO),
                    PlanStepKind::Delete(_) => PlanExecutionEvent::Deleting(id, Duration::ZERO),
                };
                let events_tx = events_tx.clone();
                Box::pin(async move {
                    let mut first = true;
                    while rx.try_recv().is_ok() {
                        if !first {
                            match &mut event {
                                PlanExecutionEvent::Creating(_, d)
                                | PlanExecutionEvent::Updating(_, d)
                                | PlanExecutionEvent::Deleting(_, d) => {
                                    *d = Instant::now().duration_since(start)
                                }
                                _ => {}
                            }
                            events_tx.send(event.clone()).await.unwrap_or(());
                        }
                        first = false;
                        async_std::task::sleep(Duration::from_secs(2)).await;
                    }
                })
            });
            let events_tx = events_tx.clone();
            match kind {
                PlanStepKind::Create(arg, f) => fo.push(Box::pin(async move {
                    state.insert(f(id.clone(), arg).await);
                    drop(tx);
                    events_tx
                        .send(PlanExecutionEvent::Created(
                            id,
                            Instant::now().duration_since(start),
                        ))
                        .await
                        .unwrap_or(());
                })),
                PlanStepKind::Update(arg, f) => fo.push(Box::pin(async move {
                    state.insert(f(id.clone(), arg).await);
                    drop(tx);
                    events_tx
                        .send(PlanExecutionEvent::Updated(
                            id,
                            Instant::now().duration_since(start),
                        ))
                        .await
                        .unwrap_or(());
                })),
                PlanStepKind::Delete(f) => fo.push(Box::pin(async move {
                    if let Some(resource) = state.remove(&id) {
                        f(id.clone(), resource.state).await;
                    }
                    drop(tx);
                    events_tx
                        .send(PlanExecutionEvent::Deleted(
                            id,
                            Instant::now().duration_since(start),
                        ))
                        .await
                        .unwrap_or(());
                })),
            }
        }
        fo.collect::<Vec<_>>().await;
        events_tx
            .send(PlanExecutionEvent::Done(
                Instant::now().duration_since(before_all),
            ))
            .await
            .unwrap_or(());

        program.plan(&state).await
    }

    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }
}

#[derive(Clone, Debug)]
pub enum PlanExecutionEvent {
    Creating(ResourceId, Duration),
    Updating(ResourceId, Duration),
    Deleting(ResourceId, Duration),
    Created(ResourceId, Duration),
    Updated(ResourceId, Duration),
    Deleted(ResourceId, Duration),
    Done(Duration),
}