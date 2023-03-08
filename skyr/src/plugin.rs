use crate::analyze::Type;
use crate::execute::{ExecutionContext, Value};
use crate::{ResourceId, ResourceValue};

pub trait Plugin: Send + Sync {
    fn import_name(&self) -> &str;
    fn module_type(&self) -> Type;
    fn module_value<'a>(&self, ctx: ExecutionContext<'a>) -> Value<'a>;
}

#[async_trait::async_trait]
pub trait PluginResource {
    fn resource_id<'a>(&self, arg: &Value<'a>) -> ResourceId {
        ResourceId::new(match self.type_() {
            Type::Function(_, r) => *r,
            t => t,
        }, self.id(arg))
    }

    fn type_(&self) -> Type;

    fn id<'a>(&self, arg: &Value<'a>) -> String;

    async fn create<'a>(&self, arg: Value<'a>) -> ResourceValue;

    async fn read<'a>(&self, prev: ResourceValue) -> ResourceValue {
        prev
    }

    async fn update<'a>(&self, arg: Value<'a>, prev: ResourceValue) -> ResourceValue;

    async fn delete<'a>(&self, prev: ResourceValue) {
        drop(prev);
    }
}
