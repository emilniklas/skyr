use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use crate::{ProviderClient, ProviderSchema, Schema};
use async_compat::CompatExt;
use inflector::Inflector;
use serde::{Deserialize, Serialize};
use skyr::execute::{ExecutionContext, RuntimeValue};
use skyr::{
    known, Collection, IdentifyResource, Plugin, Primitive, Resource, ResourceId, ResourceState,
    Value,
};

pub struct ProviderPlugin {
    name: String,
    upper_name: String,
    client: ProviderClient,
    schema: Arc<ProviderSchema>,
}

impl ProviderPlugin {
    pub fn new(name: impl Into<String>, path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        let name = name.into();
        async_std::task::block_on(
            async move {
                let mut client = ProviderClient::connect(&path).await?;
                let schema = client
                    .schema()
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                Ok(ProviderPlugin {
                    client,
                    upper_name: name.to_class_case(),
                    name,
                    schema: Arc::new(schema),
                })
            }
            .compat(),
        )
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
        let client = self.client.clone();
        RuntimeValue::function_async(move |_, mut args| {
            let ctx = ctx.clone();
            let schema = schema.clone();
            let client = client.clone();
            let provider_name = provider_name.clone();
            Box::pin(async move {
                let provider_arg = known!(args.remove(0).into_value());

                let provider = Provider::new(provider_name, schema, client, provider_arg).await;

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
        if let Value::Collection(c) = &resource.state {
            if let Some(v) = c.access_member(TerraformMetaState::FIELD_NAME) {
                let meta_state: TerraformMetaState = v
                    .deserialize()
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

                if meta_state.provider_name == self.name {
                    let provider = Provider::new(
                        meta_state.provider_name,
                        Arc::new(ProviderSchema::None),
                        self.client.clone(),
                        meta_state.provider_arg,
                    )
                    .await;

                    provider
                        .delete_resource(&meta_state.resource_name, resource.state.clone())
                        .await?;

                    return Ok(Some(()));
                }
            }
        }
        Ok(None)
    }
}

#[derive(Clone)]
struct Provider {
    name: String,
    schema: Arc<ProviderSchema>,
    client: ProviderClient,
    arg: Value,
}

impl Provider {
    pub async fn delete_resource(&self, resource_name: &str, state: Value) -> io::Result<()> {
        self.client
            .clone()
            .delete(resource_name, self.arg.clone(), state)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(())
    }

    pub async fn new(
        name: impl Into<String>,
        schema: Arc<ProviderSchema>,
        mut client: ProviderClient,
        arg: Value,
    ) -> Self {
        if let Some(schema) = schema.provider_schema() {
            let arg = apply_arg_to_schema(&schema, &arg);
            client.configure_provider(&arg).await.unwrap();
        }

        Self {
            schema,
            client,
            name: name.into(),
            arg,
        }
    }

    pub fn skyr_name(&self) -> String {
        self.name.to_class_case()
    }

    pub fn resources(&self) -> impl '_ + Iterator<Item = ResourceResource> {
        self.schema
            .resource_schemas()
            .into_iter()
            .map(|(name, schema)| ResourceResource::new(self.clone(), name, schema))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TerraformMetaState {
    provider_name: String,
    provider_arg: Value,
    resource_name: String,
}

impl TerraformMetaState {
    const FIELD_NAME: &'static str = "_$tf";
}

#[derive(Clone)]
struct ResourceResource {
    name: String,
    schema: Schema<'static>,
    provider: Provider,
}

impl ResourceResource {
    pub fn new(provider: Provider, name: impl Into<String>, schema: Schema) -> Self {
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

impl IdentifyResource for ResourceResource {
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

fn apply_arg_to_schema(schema: &Schema, arg: &Value) -> Value {
    let mut fields = vec![];
    if let Some(block) = schema.block() {
        'attributes: for attribute in block.attributes() {
            let skyrname = attribute.skyr_name();

            if let Value::Collection(c) = &arg {
                if let Some(v) = c.access_member(&skyrname) {
                    fields.push((attribute.name().to_string().into(), v.clone()));
                    continue 'attributes;
                }
            }

            fields.push((
                attribute.name().to_string().into(),
                Value::Primitive(Primitive::Nil),
            ));
        }
        'blocks: for block in block.nested_blocks() {
            let tfname = block.type_name().to_string();
            let skyrname = block.skyr_name();

            if let Value::Collection(c) = &arg {
                if let Some(v) = c.access_member(&skyrname) {
                    fields.push((tfname.into(), v.clone()));
                    continue 'blocks;
                }
            }

            fields.push((tfname.into(), Value::Primitive(Primitive::Nil)));
        }
    }
    Value::Collection(Collection::Record(fields))
}

#[async_trait::async_trait]
impl Resource for ResourceResource {
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

    async fn create(&self, arg: Self::Arguments) -> io::Result<Self::State> {
        let state = apply_arg_to_schema(&self.schema, &arg);

        let mut state = self
            .provider
            .client
            .clone()
            .create(&self.name, state)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        {
            let state = state.as_collection_mut();

            state.set_member(
                TerraformMetaState::FIELD_NAME,
                Value::serialize(&TerraformMetaState {
                    provider_name: self.provider.name.clone(),
                    provider_arg: self.provider.arg.clone(),
                    resource_name: self.name.clone(),
                })
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
            );

            state.set_member(
                "skyrKey",
                arg.as_collection()
                    .access_member("skyrKey")
                    .expect("expected a skyrKey member in arguments")
                    .clone(),
            );
        }

        Ok(state)
    }

    async fn read(
        &self,
        prev: Self::State,
        arg: &mut Self::Arguments,
    ) -> io::Result<Option<Self::State>> {
        let state = apply_arg_to_schema(&self.schema, &prev);
        self.provider
            .client
            .clone()
            .read(&self.name, state, arg)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    async fn update(&self, arg: Self::Arguments, prev: Self::State) -> io::Result<Self::State> {
        self.provider
            .client
            .clone()
            .update(&self.name, arg, prev)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}
