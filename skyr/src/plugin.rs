use std::io;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use std::sync::RwLock;

use crate::analyze::Type;
use crate::execute::{ExecutionContext, RuntimeValue};
use crate::{ResourceId, ResourceState};

#[macro_export]
macro_rules! export_plugin {
    ($plugin:expr) => {
        #[no_mangle]
        extern "C" fn skyr_plugin(idx: u64) -> *mut dyn skyr::Plugin {
            skyr::analyze::TypeId::preload(idx);
            Box::into_raw(Box::new($plugin))
        }
    };
}

#[macro_export]
macro_rules! deletable_resources {
    ($($resource_type:ty),*) => {
        fn delete_matching_resource<'life0, 'life1, 'async_trait>(
            &'life0 self,
            resource: &'life1 skyr::ResourceState,
        ) -> std::pin::Pin<Box<
                dyn std::future::Future<Output = std::io::Result<Option<()>>>
                    + Send + 'async_trait
            >>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move {
                $(
                    if let Some(()) = <skyr::TypeBasedResource<$resource_type>>::try_match_delete(resource).await? {
                        return Ok(Some(()));
                    }
                )*
                Ok(None)
            })
        }
    }
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

impl<T: Resource> TypeOf for T
where
    T::Arguments: TypeOf,
    T::State: TypeOf,
{
    fn type_of() -> Type {
        Type::function([T::Arguments::type_of()], T::State::type_of())
    }
}

impl<T: TypeOf> TypeOf for Option<T> {
    fn type_of() -> Type {
        Type::optional(T::type_of())
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

pub trait IdentifyResource: Resource {
    fn resource_id(&self, arg: &Self::Arguments) -> ResourceId;

    fn try_match(_resource: &ResourceState) -> Option<Self::State> {
        None
    }
}

#[async_trait::async_trait]
pub trait Resource {
    type Arguments: PartialEq + Send + Sync + Serialize + for<'a> Deserialize<'a>;
    type State: Send + Sync + Serialize + for<'a> Deserialize<'a>;

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

    async fn delete(_prev: Self::State) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct TypeBasedResource<R>(pub R);

#[async_trait::async_trait]
impl<R: Resource + Send + Sync> Resource for TypeBasedResource<R> {
    type Arguments = R::Arguments;
    type State = R::State;

    #[inline]
    fn id(&self, arg: &Self::Arguments) -> String {
        self.0.id(arg)
    }

    #[inline]
    async fn create(&self, arg: Self::Arguments) -> io::Result<Self::State> {
        self.0.create(arg).await
    }

    #[inline]
    async fn read(
        &self,
        prev: Self::State,
        arg: &mut Self::Arguments,
    ) -> io::Result<Option<Self::State>> {
        self.0.read(prev, arg).await
    }

    #[inline]
    async fn update(&self, arg: Self::Arguments, prev: Self::State) -> io::Result<Self::State> {
        self.0.update(arg, prev).await
    }

    #[inline]
    async fn delete(prev: Self::State) -> io::Result<()> {
        R::delete(prev).await
    }
}

impl<R: Resource + Sync + Send> IdentifyResource for TypeBasedResource<R>
where
    R::State: TypeOf,
{
    fn resource_id(&self, arg: &R::Arguments) -> ResourceId {
        ResourceId::new(R::State::type_of(), self.0.id(arg))
    }

    fn try_match(resource: &ResourceState) -> Option<R::State> {
        if resource.has_type(&R::State::type_of()) {
            resource.state.deserialize().ok()
        } else {
            None
        }
    }
}

impl<R> TypeBasedResource<R>
where
    R: Resource + Send + Sync,
    R::State: TypeOf,
{
    pub async fn try_match_delete(resource: &ResourceState) -> io::Result<Option<()>> {
        if let Some(s) = <Self as IdentifyResource>::try_match(resource) {
            R::delete(s).await?;
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }
}

enum PluginCellState {
    Unloaded(Box<dyn Send + Sync + Fn() -> Box<dyn Plugin>>),
    Loaded(Arc<dyn Plugin>),
}

pub struct PluginCell {
    cell: RwLock<PluginCellState>,
}

impl PluginCell {
    pub fn new(f: impl 'static + Send + Sync + Fn() -> Box<dyn Plugin>) -> Self {
        Self {
            cell: PluginCellState::Unloaded(Box::new(f)).into(),
        }
    }
}

impl PluginCell {
    pub fn get(&self) -> Arc<dyn Plugin> {
        {
            let guard = self.cell.read().unwrap();
            if let PluginCellState::Loaded(p) = &*guard {
                return p.clone();
            }
        }

        let mut guard = self.cell.write().unwrap();
        let f = match &mut *guard {
            PluginCellState::Loaded(p) => return p.clone(),
            PluginCellState::Unloaded(f) => f,
        };

        let plugin: Arc<dyn Plugin> = f().into();

        *guard = PluginCellState::Loaded(plugin.clone());

        plugin
    }
}
