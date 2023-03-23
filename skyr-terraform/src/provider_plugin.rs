use std::collections::BTreeMap;
use std::io;
use std::os::unix::prelude::PermissionsExt;
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
use tokio::io::AsyncWriteExt;

pub struct ProviderPlugin {
    name: String,
    upper_name: String,
    client: ProviderClient,
    schema: Arc<ProviderSchema>,
    identity_fields: BTreeMap<&'static str, Vec<&'static str>>,
}

impl ProviderPlugin {
    pub async fn new(provider_name: &str, executable_bytes: &[u8]) -> io::Result<Self> {
        async move {
            let mut executable_file = async_tempfile::TempFile::new()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            executable_file
                .set_permissions(std::fs::Permissions::from_mode(0o755))
                .await?;

            executable_file.write_all(executable_bytes).await?;
            executable_file.flush().await?;

            let mut client = ProviderClient::connect(executable_file.file_path()).await?;
            let schema = client
                .schema()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            Ok(ProviderPlugin {
                client,
                upper_name: provider_name.to_class_case(),
                name: provider_name.to_string(),
                schema: Arc::new(schema),
                identity_fields: Default::default(),
            })
        }
        .compat()
        .await
    }

    pub fn with_identity_field(mut self, resource: &'static str, field_name: &'static str) -> Self {
        self.identity_fields
            .entry(resource)
            .and_modify(|v| v.push(field_name))
            .or_insert_with(|| vec![field_name]);
        self
    }

    pub fn with_identity_fields(
        mut self,
        resource: &'static str,
        field_names: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        for field in field_names {
            self = self.with_identity_field(resource, field);
        }
        self
    }
}

#[async_trait::async_trait]
impl Plugin for ProviderPlugin {
    fn import_name(&self) -> &str {
        &self.upper_name
    }

    fn module_type(&self) -> skyr::analyze::Type {
        self.schema.as_type(&self.name, &self.identity_fields)
    }

    fn module_value<'a>(&self, ctx: ExecutionContext<'a>) -> RuntimeValue<'a> {
        let provider_name = self.name.clone();
        let schema = self.schema.clone();
        let client = self.client.clone();
        let identity_fields = self.identity_fields.clone();
        RuntimeValue::function_async(move |_, mut args| {
            let ctx = ctx.clone();
            let schema = schema.clone();
            let client = client.clone();
            let provider_name = provider_name.clone();
            let identity_fields = identity_fields.clone();
            Box::pin(async move {
                let provider_arg = known!(args.remove(0).into_value());

                let provider =
                    Provider::new(provider_name, schema, client, provider_arg, identity_fields)
                        .await;

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
                    let mut client = self.client.clone();

                    let schema = client
                        .schema()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

                    let provider = Provider::new(
                        meta_state.provider_name,
                        Arc::new(schema),
                        client,
                        meta_state.provider_arg,
                        self.identity_fields.clone(),
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
    identity_fields: BTreeMap<&'static str, Vec<&'static str>>,
}

impl Provider {
    pub async fn delete_resource(&self, resource_name: &str, state: Value) -> io::Result<()> {
        let schemas = self.schema.resource_schemas();

        let resource_schema = schemas.get(resource_name).ok_or(io::Error::new(
            io::ErrorKind::NotFound,
            format!("{:?} didn't match a known resource type", resource_name),
        ))?;

        let state = ResourceResource::apply_arg_to_schema(&resource_schema, &state);

        self.client
            .clone()
            .delete(resource_name, state)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(())
    }

    pub async fn new(
        name: impl Into<String>,
        schema: Arc<ProviderSchema>,
        mut client: ProviderClient,
        arg: Value,
        identity_fields: BTreeMap<&'static str, Vec<&'static str>>,
    ) -> Self {
        if let Some(schema) = schema.provider_schema() {
            let arg = ResourceResource::apply_arg_to_schema(&schema, &arg);
            client.configure_provider(&arg).await.unwrap();
        }

        Self {
            schema,
            client,
            name: name.into(),
            arg,
            identity_fields,
        }
    }

    pub fn skyr_name(&self) -> String {
        self.name.to_class_case()
    }

    pub fn resources(&self) -> impl '_ + Iterator<Item = ResourceResource> {
        self.schema
            .resource_schemas()
            .into_iter()
            .map(|(name, schema)| {
                ResourceResource::new(self.clone(), name, schema, &self.identity_fields)
            })
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
    identity_fields: Option<Vec<&'static str>>,
}

impl ResourceResource {
    pub fn new(
        provider: Provider,
        name: impl Into<String>,
        schema: Schema,
        identity_fields: &BTreeMap<&'static str, Vec<&'static str>>,
    ) -> Self {
        let mut rr = ResourceResource {
            schema: schema.as_static(),
            name: name.into(),
            provider,
            identity_fields: Default::default(),
        };
        rr.identity_fields = identity_fields.get(rr.skyr_name().as_str()).cloned();
        rr
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

impl ResourceResource {
    fn apply_meta_to_state(&self, state: &mut Value, arg: &Value) -> io::Result<()> {
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

        if let None = self.identity_fields {
            state.set_member(
                "skyrKey",
                arg.as_collection()
                    .access_member("skyrKey")
                    .expect("expected a skyrKey member in arguments")
                    .clone(),
            );
        }

        Ok(())
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
}

#[async_trait::async_trait]
impl Resource for ResourceResource {
    type Arguments = Value;
    type State = Value;

    fn id(&self, arg: &Self::Arguments) -> String {
        let id_fields = self
            .identity_fields
            .as_ref()
            .map(Vec::as_slice)
            .unwrap_or(&["skyrKey"]);
        let mut segments = vec![];

        for field in id_fields {
            segments.push(
                arg.as_collection()
                    .access_member(field)
                    .or_else(|| self.provider.arg.as_collection().access_member(field))
                    .map(|v| v.as_primitive().as_str())
                    .unwrap_or(""),
            );
        }

        segments.join("::")
    }

    async fn create(&self, arg: Self::Arguments) -> io::Result<Self::State> {
        let state = Self::apply_arg_to_schema(&self.schema, &arg);

        let mut state = self
            .provider
            .client
            .clone()
            .create(&self.name, state)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        self.apply_meta_to_state(&mut state, &arg)?;

        Ok(state)
    }

    async fn read(
        &self,
        prev: Self::State,
        arg: &mut Self::Arguments,
    ) -> io::Result<Option<Self::State>> {
        let state = Self::apply_arg_to_schema(&self.schema, &prev);
        self.provider
            .client
            .clone()
            .read(&self.name, state, arg)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    async fn update(&self, arg: Self::Arguments, prev: Self::State) -> io::Result<Self::State> {
        let new_state = Self::apply_arg_to_schema(&self.schema, &arg);

        let mut state = self
            .provider
            .client
            .clone()
            .update(&self.name, new_state, prev)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        self.apply_meta_to_state(&mut state, &arg)?;

        Ok(state)
    }
}
