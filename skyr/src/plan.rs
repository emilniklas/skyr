use std::fmt;
use std::pin::Pin;

use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};

use crate::execute::Value;
use crate::{AnalyzedProgram, Resource, ResourceId, State};

#[derive(Default)]
pub struct Plan<'a> {
    create: Vec<(
        ResourceId,
        Value<'a>,
        Box<dyn 'a + Fn(ResourceId, Value<'a>) -> Pin<Box<dyn 'a + Future<Output = Resource>>>>,
    )>,
}

impl<'a> fmt::Debug for Plan<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (id, value, _) in self.create.iter() {
            write!(f, "Create {:?} {:?}\n", id, value)?;
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
        self.create.push((id, args, Box::new(f)));
    }

    pub async fn execute(self, program: &'a AnalyzedProgram<'a>, state: &'a State) -> Plan<'a> {
        let fo = FuturesUnordered::new();
        for (id, arg, f) in self.create {
            fo.push(async move {
                state.insert(f(id, arg).await);
            });
        }
        fo.collect::<Vec<_>>().await;

        program.plan(&state).await
    }

    pub fn is_empty(&self) -> bool {
        self.create.is_empty()
    }
}
