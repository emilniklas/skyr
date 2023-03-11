use std::io;

use serde::{Deserialize, Serialize};

use crate::analyze::Type;
use crate::execute::{ExecutionContext, RuntimeValue};
use crate::{ResourceId, ResourceState};

#[macro_export]
macro_rules! export_plugin {
    ($plugin:tt) => {
        #[no_mangle]
        extern "C" fn skyr_plugin() -> *mut dyn Plugin {
            Box::into_raw(Box::new($plugin))
        }
    };
}

#[macro_export]
macro_rules! known {
    ($value:expr) => {{
        match $value {
            p @ skyr::execute::RuntimeValue::Pending(_) => return p.clone(),
            v => v,
        }
    }};
}

#[async_trait::async_trait]
pub trait Plugin: Send + Sync {
    fn import_name(&self) -> &str;
    fn module_type(&self) -> Type;
    fn module_value<'a>(&self, ctx: ExecutionContext<'a>) -> RuntimeValue<'a>;
    async fn delete_matching_resource(&self, _resource: &ResourceState) -> io::Result<Option<()>> {
        Ok(None)
    }
}

pub trait TypeOf {
    fn type_of() -> Type;
}

#[async_trait::async_trait]
pub trait Resource: TypeOf {
    type Arguments: PartialEq + Send + Sync + TypeOf + Serialize + for<'a> Deserialize<'a>;
    type State: Send + Sync + TypeOf + Serialize + for<'a> Deserialize<'a>;

    fn resource_id(&self, arg: &Self::Arguments) -> ResourceId {
        ResourceId::new(
            match Self::type_of() {
                Type::Function(_, r) => *r,
                t => t,
            },
            self.id(arg),
        )
    }

    fn id(&self, arg: &Self::Arguments) -> String;

    async fn create(&self, arg: Self::Arguments) -> io::Result<Self::State>;

    async fn read(
        &self,
        prev: Self::State,
        _arg: &mut Self::Arguments,
    ) -> io::Result<Option<Self::State>> {
        Ok(Some(prev))
    }

    async fn update(&self, arg: Self::Arguments, prev: Self::State) -> io::Result<Self::State>;

    async fn delete(&self, _prev: Self::State) -> io::Result<()> {
        Ok(())
    }
}
