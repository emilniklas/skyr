use std::io;
use std::path::PathBuf;

use crate::{ProviderClient, ProviderSchema, Schema};
use inflector::Inflector;
use skyr::execute::{ExecutionContext, RuntimeValue};
use skyr::{
    known, IdentifyResource, Plugin, Primitive, Resource, ResourceId, ResourceState, Value,
};

pub struct ProviderPlugin {
    name: String,
    upper_name: String,
    _client: ProviderClient,
    schema: ProviderSchema,
}

impl ProviderPlugin {
    pub fn new(name: impl Into<String>, path: impl Into<PathBuf>) -> io::Result<Self> {
        let rt = tokio::runtime::Runtime::new()?;
        let path = path.into();
        let name = name.into();
        rt.block_on(async move {
            let mut client = ProviderClient::connect(&path).await?;
            let schema = client
                .schema()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            Ok(ProviderPlugin {
                _client: client,
                upper_name: name.to_class_case(),
                name,
                schema,
            })
        })
    }
}

#[async_trait::async_trait]
impl Plugin for ProviderPlugin {
    fn import_name(&self) -> &str {
        &self.upper_name
    }

    fn module_type(&self) -> skyr::analyze::Type {
        self.schema.as_type(&self.name)
    }

    fn module_value<'a>(&self, ctx: ExecutionContext<'a>) -> RuntimeValue<'a> {
        let provider_name = self.name.clone();
        let schema = self.schema.clone();
        RuntimeValue::function_async(move |_, mut args| {
            let ctx = ctx.clone();
            let schema = schema.clone();
            let provider_name = provider_name.clone();
            Box::pin(async move {
                let provider_arg = known!(args.remove(0));

                let provider = Provider::new(provider_name, schema, provider_arg);

                RuntimeValue::record(provider.resources().map(|resource| {
                    (
                        resource.skyr_name(),
                        RuntimeValue::dynamic_resource(ctx.clone(), resource),
                    )
                }))
                .into()
            })
        })
    }

    async fn delete_matching_resource(&self, resource: &ResourceState) -> io::Result<Option<()>> {
        let skyr_name = self.name.to_class_case();
        if let Value::Collection(c) = &resource.state {
            if let Some(Value::Primitive(Primitive::String(provider_name))) =
                c.access_member("tf$providerName")
            {
                if provider_name == &skyr_name {
                    ResourceResource::delete(resource.state.clone()).await?;
                    return Ok(Some(()));
                }
            }
        }
        Ok(None)
    }
}

#[derive(Clone)]
struct Provider<'a> {
    name: String,
    schema: ProviderSchema,
    _arg: RuntimeValue<'a>,
}

impl<'a> Provider<'a> {
    pub fn new(name: impl Into<String>, schema: ProviderSchema, arg: RuntimeValue<'a>) -> Self {
        Self {
            schema,
            name: name.into(),
            _arg: arg,
        }
    }

    pub fn skyr_name(&self) -> String {
        self.name.to_class_case()
    }

    pub fn resources(&self) -> impl '_ + Iterator<Item = ResourceResource<'a>> {
        self.schema
            .resource_schemas()
            .into_iter()
            .map(|(name, schema)| ResourceResource::new(self.clone(), name, schema))
    }
}

#[derive(Clone)]
struct ResourceResource<'a> {
    name: String,
    schema: Schema<'static>,
    provider: Provider<'a>,
}

impl<'a> ResourceResource<'a> {
    pub fn new(provider: Provider<'a>, name: impl Into<String>, schema: Schema) -> Self {
        ResourceResource {
            schema: schema.as_static(),
            name: name.into(),
            provider,
        }
    }

    pub fn skyr_name(&self) -> String {
        let prefix = format!("{}_", self.provider.name);
        self.name.replacen(&prefix, "", 1).to_class_case()
    }
}

impl<'a> IdentifyResource for ResourceResource<'a> {
    fn resource_id(&self, arg: &Value) -> ResourceId {
        ResourceId::new(
            self.schema.as_attributes_type().into_named(format!(
                "{}.{}",
                self.provider.skyr_name(),
                self.skyr_name()
            )),
            self.id(arg),
        )
    }
}

#[async_trait::async_trait]
impl<'a> Resource for ResourceResource<'a> {
    type Arguments = Value;
    type State = Value;

    fn id(&self, arg: &Self::Arguments) -> String {
        arg.as_collection()
            .access_member("skyrKey")
            .expect("expected a skyrKey member in arguments")
            .as_primitive()
            .as_str()
            .into()
    }

    async fn create(&self, mut arg: Self::Arguments) -> io::Result<Self::State> {
        dbg!(("CREATE", &self.name, &arg));
        let args = arg.as_collection_mut();
        args.set_member(
            "tf$providerName",
            Primitive::string(self.provider.skyr_name()),
        );
        args.set_member("tf$resourceName", Primitive::string(self.skyr_name()));
        Ok(arg)
    }

    async fn read(
        &self,
        prev: Self::State,
        arg: &mut Self::Arguments,
    ) -> io::Result<Option<Self::State>> {
        dbg!(("READ", &prev, &arg));
        Ok(Some(prev))
    }

    async fn update(&self, arg: Self::Arguments, prev: Self::State) -> io::Result<Self::State> {
        dbg!(("UPDATE", &self.name, &arg, &prev));
        Ok(prev)
    }

    async fn delete(prev: Self::State) -> io::Result<()> {
        dbg!(("DELETE", &prev));
        Ok(())
    }
}
