use std::collections::BTreeMap;
use std::io;
use std::os::unix::prelude::PermissionsExt;
use std::sync::Arc;

use crate::{ProviderClient, ProviderSchema, Schema};
use async_compat::CompatExt;
use inflector::Inflector;
use serde::{Deserialize, Serialize};
use skyr::analyze::Type;
use skyr::execute::{ExecutionContext, RuntimeValue};
use skyr::{known, IdentifyResource, Plugin, Resource, ResourceId, ResourceState, Value};
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
                    let skyr_name = resource.skyr_name();
                    let arg_type = resource.arg_type.clone();
                    let state_type = resource.state_type.clone();

                    (
                        skyr_name,
                        RuntimeValue::dynamic_resource(ctx.clone(), resource, arg_type, state_type),
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
                    .deserialize(TerraformMetaState::type_of(
                        self.schema
                            .provider_schema()
                            .as_ref()
                            .map(Schema::as_arguments_type)
                            .unwrap_or(Type::VOID),
                    ))
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

        self.client
            .clone()
            .delete(resource_name, state, resource_schema)
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
            client
                .configure_provider(arg.clone(), &schema)
                .await
                .unwrap();
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
        let provider_arg_type = self
            .schema
            .provider_schema()
            .as_ref()
            .map(Schema::as_arguments_type)
            .unwrap_or(Type::VOID);

        self.schema
            .resource_schemas()
            .into_iter()
            .map(move |(name, schema)| {
                let arg_type = schema.as_arguments_type();
                let state_type = schema.as_attributes_type();
                ResourceResource::new(
                    self.clone(),
                    name,
                    schema,
                    &self.identity_fields,
                    provider_arg_type.clone(),
                    state_type,
                    arg_type,
                )
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

impl TerraformMetaState {
    fn type_of(provider_arg_type: Type) -> Type {
        Type::record([
            ("providerName", Type::STRING),
            ("providerArg", provider_arg_type),
            ("resourceName", Type::STRING),
        ])
    }
}

#[derive(Clone)]
struct ResourceResource {
    name: String,
    schema: Schema<'static>,
    provider: Provider,
    identity_fields: Option<Vec<&'static str>>,
    state_type: Type,
    arg_type: Type,
}

impl ResourceResource {
    pub fn new(
        provider: Provider,
        name: impl Into<String>,
        schema: Schema,
        identity_fields: &BTreeMap<&'static str, Vec<&'static str>>,
        provider_arg_type: Type,
        mut state_type: Type,
        arg_type: Type,
    ) -> Self {
        state_type.set_member(
            TerraformMetaState::FIELD_NAME,
            TerraformMetaState::type_of(provider_arg_type),
        );
        let mut rr = ResourceResource {
            schema: schema.as_static(),
            name: name.into(),
            provider,
            identity_fields: Default::default(),
            state_type,
            arg_type,
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
            self.state_type.clone().into_named(format!(
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

    async fn create(&self, mut arg: Self::Arguments) -> io::Result<Self::State> {
        arg = arg.coerce(&self.arg_type);

        let mut state = self
            .provider
            .client
            .clone()
            .create(&self.name, arg.clone(), &self.state_type, &self.schema)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        self.apply_meta_to_state(&mut state, &arg)?;

        Ok(state)
    }

    async fn read(
        &self,
        mut prev: Self::State,
        arg: &mut Self::Arguments,
    ) -> io::Result<Option<Self::State>> {
        prev = prev.coerce(&self.state_type);
        *arg = std::mem::take(arg).coerce(&self.arg_type);

        let state = self
            .provider
            .client
            .clone()
            .read(&self.name, prev, arg, &self.state_type, &self.schema)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(state)
    }

    async fn update(
        &self,
        mut arg: Self::Arguments,
        mut prev: Self::State,
    ) -> io::Result<Self::State> {
        arg = arg.coerce(&self.arg_type);
        prev = prev.coerce(&self.state_type);

        let mut state = self
            .provider
            .client
            .clone()
            .update(
                &self.name,
                arg.clone(),
                prev,
                &self.state_type,
                &self.schema,
            )
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        self.apply_meta_to_state(&mut state, &arg)?;

        Ok(state)
    }
}
