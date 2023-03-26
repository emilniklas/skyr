use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::sync::Arc;

use async_compat::CompatExt;
use async_std::io::prelude::BufReadExt;
use async_std::io::BufReader;
use async_std::os::unix::net::UnixStream;
use async_std::process::{Child, Command};
use inflector::Inflector;
use serde_json::Value as JSONValue;
use skyr::analyze::{CompositeType, PrimitiveType, Type};
use skyr::{Collection, Primitive, Value};
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

use crate::{tfplugin5, tfplugin6};

#[derive(Debug, Clone)]
pub enum ProviderClient {
    V5(
        Arc<ExitOnDrop>,
        tfplugin5::provider_client::ProviderClient<Channel>,
    ),
    V6(
        Arc<ExitOnDrop>,
        tfplugin6::provider_client::ProviderClient<Channel>,
    ),
}

#[derive(Debug)]
pub enum ExitOnDrop {
    V5(Child, tfplugin5::provider_client::ProviderClient<Channel>),
    V6(Child, tfplugin6::provider_client::ProviderClient<Channel>),
}

impl Drop for ExitOnDrop {
    fn drop(&mut self) {
        async_std::task::block_on(
            async move {
                match self {
                    Self::V5(c, p) => {
                        p.stop(tfplugin5::stop::Request {}).await.unwrap();
                        c.kill().unwrap();
                    }
                    Self::V6(c, p) => {
                        p.stop_provider(tfplugin6::stop_provider::Request {})
                            .await
                            .unwrap();
                        c.kill().unwrap();
                    }
                }
            }
            .compat(),
        )
    }
}

impl ProviderClient {
    pub async fn connect(executable: &Path) -> io::Result<Self> {
        let mut c = Command::new(executable)
            .env(
                "TF_PLUGIN_MAGIC_COOKIE",
                "d602bf8f470bc67ca7faa0386276bbdd4330efaf76d1a219cb4d6991ca9872b2",
            )
            .env("PLUGIN_PROTOCOL_VERSIONS", "6,5")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()?;

        let mut stdout = BufReader::new(c.stdout.take().unwrap());

        let mut line = String::new();
        stdout.read_line(&mut line).await?;

        let mut props = line.split('|');
        let _core_protocol_version: Option<i32> = props.next().and_then(|s| s.parse().ok());
        let proto_version: i32 = props.next().and_then(|s| s.parse().ok()).unwrap();
        let _network = props.next();
        let uri = props.next().unwrap().to_string();
        let _proto_type = props.next();

        let channel = Endpoint::try_from("http://[::]:50051")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .connect_with_connector(service_fn(move |_| {
                let uri = uri.clone();
                async move {
                    let file = UnixStream::connect(uri).await?;
                    std::io::Result::Ok(file.compat())
                }
                .compat()
            }))
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        match proto_version {
            5 => {
                let p = tfplugin5::provider_client::ProviderClient::new(channel);
                Ok(Self::V5(Arc::new(ExitOnDrop::V5(c, p.clone())), p))
            }
            6 => {
                let p = tfplugin6::provider_client::ProviderClient::new(channel);
                Ok(Self::V6(Arc::new(ExitOnDrop::V6(c, p.clone())), p))
            }
            v => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported proto version {}", v),
            )),
        }
    }

    pub async fn schema(&mut self) -> tonic::Result<ProviderSchema> {
        match self {
            Self::V5(_, p) => Ok(ProviderSchema::V5(
                p.get_schema(tfplugin5::get_provider_schema::Request {})
                    .await?
                    .into_inner(),
            )),
            Self::V6(_, p) => Ok(ProviderSchema::V6(
                p.get_provider_schema(tfplugin6::get_provider_schema::Request {})
                    .await?
                    .into_inner(),
            )),
        }
    }

    pub async fn configure_provider(&mut self, arg: &Value) -> io::Result<()> {
        let mut diagnostics: Vec<_> = match self {
            Self::V5(_, p) => {
                let response = p
                    .prepare_provider_config(tfplugin5::prepare_provider_config::Request {
                        config: Some(tfplugin5::DynamicValue {
                            json: vec![],
                            msgpack: rmp_serde::to_vec(arg)
                                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
                        }),
                    })
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                    .into_inner();

                response
                    .diagnostics
                    .into_iter()
                    .map(Diagnostic::V5)
                    .collect()
            }
            Self::V6(_, p) => p
                .validate_provider_config(tfplugin6::validate_provider_config::Request {
                    config: Some(tfplugin6::DynamicValue {
                        json: vec![],
                        msgpack: rmp_serde::to_vec(arg)
                            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
                    }),
                })
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                .into_inner()
                .diagnostics
                .into_iter()
                .map(Diagnostic::V6)
                .collect(),
        };

        match self {
            Self::V5(_, p) => {
                let response = p
                    .configure(tfplugin5::configure::Request {
                        terraform_version: "x".into(),
                        config: Some(tfplugin5::DynamicValue {
                            json: vec![],
                            msgpack: rmp_serde::to_vec(arg)
                                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
                        }),
                    })
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                    .into_inner();

                diagnostics.extend(response.diagnostics.into_iter().map(Diagnostic::V5));
            }
            Self::V6(_, p) => {
                let response = p
                    .configure_provider(tfplugin6::configure_provider::Request {
                        terraform_version: "x".into(),
                        config: Some(tfplugin6::DynamicValue {
                            json: vec![],
                            msgpack: rmp_serde::to_vec(arg)
                                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
                        }),
                    })
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                    .into_inner();

                diagnostics.extend(response.diagnostics.into_iter().map(Diagnostic::V6));
            }
        }

        if diagnostics.len() > 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("{:?}", diagnostics),
            ));
        }

        Ok(())
    }

    pub async fn create(&mut self, resource_name: &str, state: Value) -> tonic::Result<Value> {
        let new_state = match self {
            Self::V5(_, c) => {
                let state = DynamicValue::v5(&state);
                let response = c
                    .apply_resource_change(tfplugin5::apply_resource_change::Request {
                        type_name: resource_name.into(),
                        provider_meta: None,
                        planned_private: vec![],
                        planned_state: Some(state.clone()),
                        prior_state: Some(DynamicValue::v5(&Value::Collection(
                            Collection::Record(vec![]),
                        ))),
                        config: Some(state),
                    })
                    .await?
                    .into_inner();
                if !response.diagnostics.is_empty() {
                    return Err(tonic::Status::new(
                        tonic::Code::Aborted,
                        format!("{:?}", response.diagnostics),
                    ));
                }
                response.new_state.map(Cow::Owned).map(DynamicValue::V5)
            }
            Self::V6(_, _) => todo!(),
        };
        Ok(new_state.unwrap().into_value())
    }

    pub async fn read(
        &mut self,
        resource_name: &str,
        state: Value,
        _arg: &mut Value,
    ) -> tonic::Result<Option<Value>> {
        let new_state = match self {
            Self::V5(_, c) => c
                .read_resource(tfplugin5::read_resource::Request {
                    type_name: resource_name.into(),
                    current_state: Some(DynamicValue::v5(&state)),
                    private: vec![],
                    provider_meta: None,
                })
                .await?
                .into_inner()
                .new_state
                .map(Cow::Owned)
                .map(DynamicValue::V5),
            Self::V6(_, c) => c
                .read_resource(tfplugin6::read_resource::Request {
                    type_name: resource_name.into(),
                    current_state: Some(DynamicValue::v6(&state)),
                    private: vec![],
                    provider_meta: None,
                })
                .await?
                .into_inner()
                .new_state
                .map(Cow::Owned)
                .map(DynamicValue::V6),
        };
        Ok(new_state
            .map(DynamicValue::into_value)
            .and_then(|v| v.nil_to_none()))
    }

    pub async fn update(
        &mut self,
        resource_name: &str,
        new_state: Value,
        prev_state: Value,
    ) -> tonic::Result<Value> {
        let new_state = match self {
            Self::V5(_, c) => {
                let new_state = DynamicValue::v5(&new_state);
                let response = c
                    .apply_resource_change(tfplugin5::apply_resource_change::Request {
                        type_name: resource_name.into(),
                        provider_meta: None,
                        planned_private: vec![],
                        planned_state: Some(new_state.clone()),
                        prior_state: Some(DynamicValue::v5(&prev_state)),
                        config: Some(new_state),
                    })
                    .await?
                    .into_inner();
                if !response.diagnostics.is_empty() {
                    return Err(tonic::Status::new(
                        tonic::Code::Aborted,
                        format!("{:?}", response.diagnostics),
                    ));
                }
                response.new_state.map(Cow::Owned).map(DynamicValue::V5)
            }
            Self::V6(_, _) => todo!(),
        };
        Ok(new_state.unwrap().into_value())
    }

    pub async fn delete(&mut self, resource_name: &str, prev_state: Value) -> tonic::Result<()> {
        match self {
            Self::V5(_, c) => {
                let response = c
                    .apply_resource_change(tfplugin5::apply_resource_change::Request {
                        type_name: resource_name.into(),
                        provider_meta: None,
                        planned_private: vec![],
                        planned_state: Some(DynamicValue::v5(&Value::Primitive(Primitive::Nil))),
                        prior_state: Some(DynamicValue::v5(&prev_state)),
                        config: Some(DynamicValue::v5(&Value::Primitive(Primitive::Nil))),
                    })
                    .await?
                    .into_inner();
                if !response.diagnostics.is_empty() {
                    return Err(tonic::Status::new(
                        tonic::Code::Aborted,
                        format!("{:?}", response.diagnostics),
                    ));
                }
                Ok(())
            }
            Self::V6(_, _) => todo!(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ProviderSchema {
    V5(tfplugin5::get_provider_schema::Response),
    V6(tfplugin6::get_provider_schema::Response),
}

impl ProviderSchema {
    pub fn provider_schema(&self) -> Option<Schema> {
        match self {
            Self::V5(s) => s.provider.as_ref().map(Cow::Borrowed).map(Schema::V5),
            Self::V6(s) => s.provider.as_ref().map(Cow::Borrowed).map(Schema::V6),
        }
    }

    pub fn resource_schemas(&self) -> BTreeMap<&str, Schema> {
        match self {
            Self::V5(s) => s
                .resource_schemas
                .iter()
                .map(|(name, schema)| (name.as_str(), Schema::V5(Cow::Borrowed(schema))))
                .collect(),
            Self::V6(s) => s
                .resource_schemas
                .iter()
                .map(|(name, schema)| (name.as_str(), Schema::V6(Cow::Borrowed(schema))))
                .collect(),
        }
    }

    pub fn as_type(
        &self,
        with_name: &str,
        identity_fields: &BTreeMap<&'static str, Vec<&'static str>>,
    ) -> Type {
        let prefix = format!("{}_", with_name);
        let upper_name = with_name.to_class_case();
        let resources = Type::record(
            self.resource_schemas()
                .into_iter()
                .map(|(name, schema)| (name.replacen(&prefix, "", 1).to_class_case(), schema))
                .map(|(name, schema)| {
                    let mut arg_type = schema.as_arguments_type();
                    let identity_fields = identity_fields.get(name.as_str());
                    if identity_fields.is_none() {
                        if let Type::Composite(CompositeType::Record(fields)) = &mut arg_type {
                            fields.insert(
                                0,
                                ("skyrKey".into(), Type::Primitive(PrimitiveType::String)),
                            );
                        }
                    }
                    let result_type = schema.as_attributes_type();
                    (
                        name.clone(),
                        Type::Composite(CompositeType::function(
                            [arg_type],
                            result_type.into_named(format!("{}.{}", upper_name, name)),
                        )),
                    )
                }),
        );

        if let Some(arg) = self
            .provider_schema()
            .as_ref()
            .map(Schema::as_arguments_type)
        {
            Type::Composite(CompositeType::function([arg], resources)).into_named(upper_name)
        } else {
            resources.into_named(upper_name)
        }
    }
}

#[derive(Debug, Clone)]
pub enum Schema<'a> {
    V5(Cow<'a, tfplugin5::Schema>),
    V6(Cow<'a, tfplugin6::Schema>),
}

impl<'a> Schema<'a> {
    pub fn as_static(self) -> Schema<'static> {
        match self {
            Self::V5(s) => Schema::V5(Cow::Owned(s.as_ref().clone())),
            Self::V6(s) => Schema::V6(Cow::Owned(s.as_ref().clone())),
        }
    }

    pub fn block(&self) -> Option<Block> {
        match self {
            Self::V5(s) => s.block.as_ref().map(Cow::Borrowed).map(Block::V5),
            Self::V6(s) => s.block.as_ref().map(Cow::Borrowed).map(Block::V6),
        }
    }

    pub fn as_arguments_type(&self) -> Type {
        self.block()
            .map(|b| b.as_arguments_type())
            .unwrap_or(Type::Primitive(PrimitiveType::Void))
    }

    pub fn as_attributes_type(&self) -> Type {
        self.block()
            .map(|b| b.as_attributes_type())
            .unwrap_or(Type::Primitive(PrimitiveType::Void))
    }
}

#[derive(Debug, Clone)]
pub enum DynamicValue<'a> {
    V5(Cow<'a, tfplugin5::DynamicValue>),
    V6(Cow<'a, tfplugin6::DynamicValue>),
}

impl<'a> DynamicValue<'a> {
    pub fn v5(value: &Value) -> tfplugin5::DynamicValue {
        tfplugin5::DynamicValue {
            json: vec![],
            msgpack: rmp_serde::to_vec(value).unwrap(),
        }
    }

    pub fn v6(value: &Value) -> tfplugin6::DynamicValue {
        tfplugin6::DynamicValue {
            json: vec![],
            msgpack: rmp_serde::to_vec(value).unwrap(),
        }
    }

    pub fn into_value(self) -> Value {
        match self {
            Self::V5(v) => rmp_serde::from_slice(&v.msgpack).unwrap(),
            Self::V6(v) => rmp_serde::from_slice(&v.msgpack).unwrap(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Block<'a> {
    V5(Cow<'a, tfplugin5::schema::Block>),
    V6(Cow<'a, tfplugin6::schema::Block>),
}

impl<'a> Block<'a> {
    pub fn as_static(self) -> Block<'static> {
        match self {
            Self::V5(s) => Block::V5(Cow::Owned(s.as_ref().clone())),
            Self::V6(s) => Block::V6(Cow::Owned(s.as_ref().clone())),
        }
    }

    pub fn attribute(&self, name: &str) -> Option<Attribute> {
        self.attributes().into_iter().find(|a| a.name() == name)
    }

    pub fn attributes(&self) -> Vec<Attribute> {
        match self {
            Self::V5(b) => b
                .attributes
                .iter()
                .map(Cow::Borrowed)
                .map(Attribute::V5)
                .collect(),
            Self::V6(b) => b
                .attributes
                .iter()
                .map(Cow::Borrowed)
                .map(Attribute::V6)
                .collect(),
        }
    }

    pub fn nested_blocks(&self) -> Vec<NestedBlock> {
        match self {
            Self::V5(b) => b
                .block_types
                .iter()
                .map(Cow::Borrowed)
                .map(NestedBlock::V5)
                .collect(),
            Self::V6(b) => b
                .block_types
                .iter()
                .map(Cow::Borrowed)
                .map(NestedBlock::V6)
                .collect(),
        }
    }

    pub fn as_arguments_type(&self) -> Type {
        self.as_type(false)
    }

    pub fn as_attributes_type(&self) -> Type {
        self.as_type(true)
    }

    fn as_type(&self, include_computed: bool) -> Type {
        let mut fields = self
            .attributes()
            .into_iter()
            .filter(|attribute| include_computed || !attribute.computed())
            .map(|attribute| (attribute.name().to_camel_case(), attribute.type_()))
            .chain(self.nested_blocks().into_iter().map(|nb| {
                (
                    nb.type_name().to_camel_case().to_plural(),
                    nb.as_type(include_computed),
                )
            }))
            .collect::<Vec<_>>();

        fields.sort_by(
            |(_, lt), (_, rt)| match (lt.is_optional(), rt.is_optional()) {
                (true, true) | (false, false) => Ordering::Equal,
                (false, true) => Ordering::Less,
                (true, false) => Ordering::Greater,
            },
        );

        Type::record(fields)
    }
}

#[derive(Debug, Clone)]
pub enum NestedBlock<'a> {
    V5(Cow<'a, tfplugin5::schema::NestedBlock>),
    V6(Cow<'a, tfplugin6::schema::NestedBlock>),
}

impl<'a> NestedBlock<'a> {
    pub fn as_static(self) -> NestedBlock<'static> {
        match self {
            Self::V5(s) => NestedBlock::V5(Cow::Owned(s.as_ref().clone())),
            Self::V6(s) => NestedBlock::V6(Cow::Owned(s.as_ref().clone())),
        }
    }

    pub fn type_name(&self) -> &str {
        match self {
            Self::V5(b) => b.type_name.as_str(),
            Self::V6(b) => b.type_name.as_str(),
        }
    }

    pub fn skyr_name(&self) -> String {
        self.type_name().to_plural().to_camel_case()
    }

    pub fn block(&self) -> Option<Block> {
        match self {
            Self::V5(b) => b.block.as_ref().map(Cow::Borrowed).map(Block::V5),
            Self::V6(b) => b.block.as_ref().map(Cow::Borrowed).map(Block::V6),
        }
    }

    pub fn min_items(&self) -> i64 {
        match self {
            Self::V5(b) => b.min_items,
            Self::V6(b) => b.min_items,
        }
    }

    pub fn max_items(&self) -> i64 {
        match self {
            Self::V5(b) => b.max_items,
            Self::V6(b) => b.max_items,
        }
    }

    pub fn as_arguments_type(&self) -> Type {
        self.as_type(false)
    }

    pub fn as_attributes_type(&self) -> Type {
        self.as_type(true)
    }

    fn as_type(&self, include_computed: bool) -> Type {
        let list = Type::Composite(CompositeType::list(
            self.block()
                .as_ref()
                .map(|b| b.as_type(include_computed))
                .unwrap_or(Type::Composite(CompositeType::Record(vec![]))),
        ));

        if self.min_items() == 0 {
            Type::Composite(CompositeType::optional(list))
        } else {
            list
        }
    }
}

#[derive(Debug, Clone)]
pub enum Attribute<'a> {
    V5(Cow<'a, tfplugin5::schema::Attribute>),
    V6(Cow<'a, tfplugin6::schema::Attribute>),
}

impl<'a> Attribute<'a> {
    pub fn as_static(self) -> Attribute<'static> {
        match self {
            Self::V5(s) => Attribute::V5(Cow::Owned(s.as_ref().clone())),
            Self::V6(s) => Attribute::V6(Cow::Owned(s.as_ref().clone())),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::V5(a) => a.name.as_str(),
            Self::V6(a) => a.name.as_str(),
        }
    }

    pub fn skyr_name(&self) -> String {
        self.name().to_camel_case()
    }

    pub fn computed(&self) -> bool {
        match self {
            Self::V5(a) => a.computed,
            Self::V6(a) => a.computed,
        }
    }

    pub fn optional(&self) -> bool {
        match self {
            Self::V5(a) => a.optional || !a.required,
            Self::V6(a) => a.optional || !a.required,
        }
    }

    pub fn type_(&self) -> Type {
        let t = match self {
            Self::V5(a) => type_from_bytes(a.r#type.as_slice()),
            Self::V6(a) => type_from_bytes(a.r#type.as_slice()),
        };
        if self.optional() {
            Type::Composite(CompositeType::optional(t))
        } else {
            t
        }
    }
}

fn type_from_bytes(bytes: &[u8]) -> Type {
    let value: JSONValue = serde_json::from_slice(bytes).unwrap();

    type_from_json_value(&value)
}

fn type_from_json_value(value: &JSONValue) -> Type {
    match value.as_str() {
        Some("string") => Type::Primitive(PrimitiveType::String),
        Some("bool") => Type::Primitive(PrimitiveType::Boolean),
        Some("number") => Type::Primitive(PrimitiveType::Float),
        _ => {
            if let Some(a) = value.as_array() {
                let mut i = a.iter();
                let c = i.next().and_then(|v| v.as_str());
                let v = i.next();

                match (c, v) {
                    (Some("set"), Some(v)) => {
                        Type::Composite(CompositeType::list(type_from_json_value(v)))
                    }
                    (Some("list"), Some(v)) => {
                        Type::Composite(CompositeType::list(type_from_json_value(v)))
                    }
                    (Some("map"), Some(v)) => Type::Composite(CompositeType::dict(
                        Type::Primitive(PrimitiveType::String),
                        type_from_json_value(v),
                    )),
                    (Some("object"), Some(JSONValue::Object(fields))) => {
                        Type::Composite(CompositeType::record(
                            fields
                                .iter()
                                .map(|(k, v)| (k.to_camel_case(), type_from_json_value(v))),
                        ))
                    }
                    x => panic!("unsupported: {:?}", x),
                }
            } else {
                panic!("unsupported: {:?}", value);
            }
        }
    }
}

#[derive(Debug)]
pub enum Diagnostic {
    V5(tfplugin5::Diagnostic),
    V6(tfplugin6::Diagnostic),
}
