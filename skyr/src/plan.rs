use std::fmt;
use std::io;
use std::pin::Pin;
use std::time::{Duration, Instant};

use async_std::channel::Sender;
use futures::future::Either;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};

use crate::compile::Span;
use crate::execute::RuntimeValue;
use crate::Diff;
use crate::Value;
use crate::{AnalyzedProgram, ResourceId, ResourceState, State};

#[derive(Debug)]
pub struct ResourceError(pub ResourceId, pub io::Error);

pub enum PlanStepKind<'a> {
    Create(
        Value,
        Box<dyn 'a + FnOnce() -> Pin<Box<dyn 'a + Future<Output = io::Result<ResourceState>>>>>,
    ),
    Update(
        Value,
        Value,
        Box<dyn 'a + FnOnce() -> Pin<Box<dyn 'a + Future<Output = io::Result<ResourceState>>>>>,
    ),
    Delete(
        Box<
            dyn 'a
                + FnOnce(ResourceId, Value) -> Pin<Box<dyn 'a + Future<Output = io::Result<()>>>>,
        >,
    ),
}

#[derive(Default)]
pub struct Plan<'a> {
    steps: Vec<(ResourceId, Vec<ResourceId>, PlanStepKind<'a>)>,
    debug_messages: Vec<(Span, RuntimeValue<'a>)>,
    phase_index: usize,
}

impl<'a> fmt::Debug for Plan<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (_, value) in self.debug_messages.iter() {
            write!(f, "ℹ️ {}\n", value)?;
        }

        if self.is_empty() {
            if self.is_continuation() {
                return write!(f, "✅ Nothing more to do");
            } else {
                return write!(f, "✅ Nothing to do");
            }
        }

        for (i, (id, _, kind)) in self.steps.iter().enumerate() {
            if i > 0 {
                write!(f, "\n")?;
            }
            match kind {
                PlanStepKind::Create(value, _) => write!(f, "✴️ Create {:?} {:#?}", id, value)?,
                PlanStepKind::Update(old, value, _) => {
                    write!(f, "⬆️ Update {:?} {:#?}", id, old.diff(value))?
                }
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

    pub fn is_continuation(&self) -> bool {
        self.phase_index > 0
    }

    pub fn phase_index(&self) -> usize {
        self.phase_index
    }

    pub fn to_be_created(&self) -> Vec<(&ResourceId, &Value)> {
        self.steps
            .iter()
            .filter_map(|(id, _, step)| match step {
                PlanStepKind::Create(args, _) => Some((id, args)),
                _ => None,
            })
            .collect()
    }

    pub fn to_be_updated(&self) -> Vec<(&ResourceId, Diff)> {
        self.steps
            .iter()
            .filter_map(|(id, _, step)| match step {
                PlanStepKind::Update(old, new, _) => Some((id, old.diff(new))),
                _ => None,
            })
            .collect()
    }

    pub fn to_be_deleted(&self) -> Vec<&ResourceId> {
        self.steps
            .iter()
            .filter_map(|(id, _, step)| match step {
                PlanStepKind::Delete(_) => Some(id),
                _ => None,
            })
            .collect()
    }

    pub fn register_create(
        &mut self,
        id: ResourceId,
        args: Value,
        dependencies: Vec<ResourceId>,
        f: impl 'a + FnOnce() -> Pin<Box<dyn 'a + Future<Output = io::Result<ResourceState>>>>,
    ) {
        self.steps
            .push((id, dependencies, PlanStepKind::Create(args, Box::new(f))));
    }

    pub fn register_update(
        &mut self,
        id: ResourceId,
        old_args: Value,
        args: Value,
        dependencies: Vec<ResourceId>,
        f: impl 'a + FnOnce() -> Pin<Box<dyn 'a + Future<Output = io::Result<ResourceState>>>>,
    ) {
        self.steps.push((
            id,
            dependencies,
            PlanStepKind::Update(old_args, args, Box::new(f)),
        ));
    }

    pub fn register_delete(
        &mut self,
        id: ResourceId,
        dependencies: Vec<ResourceId>,
        f: impl 'a + FnOnce(ResourceId, Value) -> Pin<Box<dyn 'a + Future<Output = io::Result<()>>>>,
    ) {
        self.steps
            .push((id, dependencies, PlanStepKind::Delete(Box::new(f))));
    }

    pub fn register_debug(&mut self, span: Span, value: RuntimeValue<'a>) {
        self.debug_messages.push((span, value));
    }

    pub async fn execute_once(
        self,
        state: &'a State,
        events_tx: Sender<PlanExecutionEvent>,
    ) -> Result<(), Vec<ResourceError>> {
        let fo =
            FuturesUnordered::<Pin<Box<dyn Future<Output = Result<(), ResourceError>>>>>::new();

        let before_all = Instant::now();
        for (id, _, kind) in self.steps {
            let start = Instant::now();
            let (tx, rx) = futures::channel::oneshot::channel::<()>();
            fo.push({
                let id = id.clone();
                let mut event = match &kind {
                    PlanStepKind::Create(_, _) => {
                        PlanExecutionEvent::Creating(id, Duration::ZERO)
                    }
                    PlanStepKind::Update(_, _, _) => {
                        PlanExecutionEvent::Updating(id, Duration::ZERO)
                    }
                    PlanStepKind::Delete(_) => PlanExecutionEvent::Deleting(id, Duration::ZERO),
                };
                let events_tx = events_tx.clone();
                Box::pin(async move {
                    let mut done = Box::pin(rx);
                    loop {
                        match futures::future::select(
                            Box::pin(async {
                                async_std::task::sleep(Duration::from_secs(2)).await;
                            }),
                            done,
                        )
                        .await
                        {
                            Either::Left((_, l)) => {
                                done = l;
                            }
                            Either::Right(_) => break,
                        }

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
                    Ok(())
                })
            });
            let events_tx = events_tx.clone();
            match kind {
                PlanStepKind::Create(_, f) => fo.push(Box::pin(async move {
                    state.insert(f().await.map_err(|e| ResourceError(id.clone(), e))?);
                    drop(tx);
                    events_tx
                        .send(PlanExecutionEvent::Created(
                            id,
                            Instant::now().duration_since(start),
                        ))
                        .await
                        .unwrap_or(());
                    Ok(())
                })),
                PlanStepKind::Update(_, _, f) => fo.push(Box::pin(async move {
                    state.insert(f().await.map_err(|e| ResourceError(id.clone(), e))?);
                    drop(tx);
                    events_tx
                        .send(PlanExecutionEvent::Updated(
                            id,
                            Instant::now().duration_since(start),
                        ))
                        .await
                        .unwrap_or(());
                    Ok(())
                })),
                PlanStepKind::Delete(f) => fo.push(Box::pin(async move {
                    if let Some(resource) = state.get(&id) {
                        f(id.clone(), resource.state)
                            .await
                            .map_err(|e| ResourceError(resource.id, e))?;
                    }
                    state.remove(&id);
                    drop(tx);
                    events_tx
                        .send(PlanExecutionEvent::Deleted(
                            id,
                            Instant::now().duration_since(start),
                        ))
                        .await
                        .unwrap_or(());
                    Ok(())
                })),
            }
        }
        let results = fo.collect::<Vec<Result<(), ResourceError>>>().await;
        events_tx
            .send(PlanExecutionEvent::Done(
                Instant::now().duration_since(before_all),
            ))
            .await
            .unwrap_or(());

        let errors = results
            .into_iter()
            .filter_map(|r| r.err())
            .collect::<Vec<_>>();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub async fn execute(
        self,
        program: &'a AnalyzedProgram<'a>,
        state: &'a State,
        events_tx: Sender<PlanExecutionEvent>,
    ) -> Result<Plan<'a>, Vec<ResourceError>> {
        let new_phase_index = self.phase_index + 1;

        self.execute_once(state, events_tx).await?;

        let mut plan = program.plan(&state).await?;
        plan.phase_index = new_phase_index;
        Ok(plan)
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
