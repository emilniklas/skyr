use std::io;

use crate::analyze::Type;
use crate::execute::{ExecutionContext, Value};
use crate::{Resource, ResourceId, ResourceValue};

#[macro_export]
macro_rules! export_plugin {
    ($plugin:tt) => {
        #[no_mangle]
        extern "C" fn hello() -> *mut dyn Plugin {
            Box::into_raw(Box::new($plugin))
        }
    };
}

#[macro_export]
macro_rules! known {
    ($value:expr) => {{
        match $value {
            p @ Value::Pending(_) => return p.clone(),
            v => v,
        }
    }};
}

pub trait Plugin: Send + Sync {
    fn import_name(&self) -> &str;
    fn module_type(&self) -> Type;
    fn module_value<'a>(&self, ctx: ExecutionContext<'a>) -> Value<'a>;
    fn find_resource(&self, resource: &Resource) -> Option<Box<dyn PluginResource>>;
}

#[async_trait::async_trait]
pub trait PluginResource {
    fn resource_id<'a>(&self, arg: &Value<'a>) -> ResourceId {
        ResourceId::new(
            match self.type_() {
                Type::Function(_, r) => *r,
                t => t,
            },
            self.id(arg),
        )
    }

    fn type_(&self) -> Type;

    fn id<'a>(&self, arg: &Value<'a>) -> String;

    async fn create<'a>(&self, arg: Value<'a>) -> io::Result<ResourceValue>;

    async fn read<'a>(
        &self,
        prev: ResourceValue,
        arg: &mut ResourceValue,
    ) -> io::Result<Option<ResourceValue>>;

    async fn update<'a>(&self, arg: Value<'a>, prev: ResourceValue) -> io::Result<ResourceValue>;

    async fn delete<'a>(&self, prev: ResourceValue) -> io::Result<()>;
}
