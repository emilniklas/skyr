use crate::{ResourceId, ResourceValue};
use crate::analyze::Type;
use crate::execute::{Value, ExecutionContext};

pub trait Plugin: Send + Sync {
    fn import_name(&self) -> &str;
    fn module_type(&self) -> Type;
    fn module_value<'a>(&self, ctx: ExecutionContext<'a>) -> Value<'a>;
}

#[async_trait::async_trait]
pub trait PluginResource {
    fn id<'a>(&self, arg: &Value<'a>) -> ResourceId;
    async fn create<'a>(&self, arg: Value<'a>) -> ResourceValue;
}
