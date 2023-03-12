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

impl<T: Resource> TypeOf for T {
    fn type_of() -> Type {
        Type::function([T::Arguments::type_of()], T::State::type_of())
    }
}

impl TypeOf for String {
    fn type_of() -> Type {
        Type::String
    }
}

impl TypeOf for &str {
    fn type_of() -> Type {
        Type::String
    }
}

impl<T: TypeOf> TypeOf for Vec<T> {
    fn type_of() -> Type {
        Type::list(T::type_of())
    }
}

impl TypeOf for i128 {
    fn type_of() -> Type {
        Type::Integer
    }
}

impl TypeOf for i64 {
    fn type_of() -> Type {
        Type::Integer
    }
}

impl TypeOf for i32 {
    fn type_of() -> Type {
        Type::Integer
    }
}

impl TypeOf for i16 {
    fn type_of() -> Type {
        Type::Integer
    }
}

impl TypeOf for i8 {
    fn type_of() -> Type {
        Type::Integer
    }
}

impl TypeOf for isize {
    fn type_of() -> Type {
        Type::Integer
    }
}

impl TypeOf for u128 {
    fn type_of() -> Type {
        Type::Integer
    }
}

impl TypeOf for u64 {
    fn type_of() -> Type {
        Type::Integer
    }
}

impl TypeOf for u32 {
    fn type_of() -> Type {
        Type::Integer
    }
}

impl TypeOf for u16 {
    fn type_of() -> Type {
        Type::Integer
    }
}

impl TypeOf for u8 {
    fn type_of() -> Type {
        Type::Integer
    }
}

impl TypeOf for usize {
    fn type_of() -> Type {
        Type::Integer
    }
}

#[async_trait::async_trait]
pub trait Resource: TypeOf {
    type Arguments: PartialEq + Send + Sync + TypeOf + Serialize + for<'a> Deserialize<'a>;
    type State: Send + Sync + TypeOf + Serialize + for<'a> Deserialize<'a>;

    fn resource_id(&self, arg: &Self::Arguments) -> ResourceId {
        ResourceId::new(
            Self::State::type_of(),
            self.id(arg),
        )
    }

    fn id(&self, arg: &Self::Arguments) -> String;

    fn try_match(&self, resource: &ResourceState) -> Option<Self::State> {
        if resource.has_type(&Self::State::type_of()) {
            resource.state.deserialize().ok()
        } else {
            None
        }
    }

    async fn try_match_delete(&self, resource: &ResourceState) -> io::Result<Option<()>> {
        if let Some(state) = self.try_match(resource) {
            Ok(Some(self.delete(state).await?))
        } else {
            Ok(None)
        }
    }

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
