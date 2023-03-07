use futures::stream::FuturesUnordered;
use futures::StreamExt;

use crate::execute::Value;
use crate::{AnalyzedProgram, Resource, ResourceId, State};

#[derive(Default, Debug)]
pub struct Plan<'a> {
    create: Vec<(ResourceId, Value<'a>)>,
}

impl<'a> Plan<'a> {
    pub fn new() -> Self {
        Plan::default()
    }

    pub fn register_create(&mut self, id: ResourceId, args: Value<'a>) {
        self.create.push((id, args));
    }

    pub async fn execute(
        self,
        program: &'a AnalyzedProgram<'a>,
        state: &'a State,
    ) -> Plan<'a> {
        let fo = FuturesUnordered::new();
        for (id, arg) in self.create {
            fo.push(async move {
                println!("CREATE {:?} {:?}", id, arg);
                state.insert(Resource { id });
            });
        }
        fo.collect::<Vec<_>>().await;

        program.plan(&state).await
    }

    pub fn is_empty(&self) -> bool {
        self.create.is_empty()
    }
}
