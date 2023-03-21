use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use inflector::Inflector;
use serde_json::Value as JSONValue;
use skyr::analyze::Type;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

use crate::{tfplugin5, tfplugin6};

#[derive(Debug, Clone)]
pub enum ProviderClient {
    V5(tfplugin5::provider_client::ProviderClient<Channel>),
    V6(tfplugin6::provider_client::ProviderClient<Channel>),
}

impl ProviderClient {
    pub async fn connect(executable: &Path) -> io::Result<Self> {
        let mut c = tokio::process::Command::new(executable)
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
        let uri = props.next().unwrap();
        let _proto_type = props.next();

        let mut file = Some(UnixStream::connect(uri).await?);
        let channel = Endpoint::try_from("http://[::]:50051")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .connect_with_connector(service_fn(move |_| {
                let file = file.take();
                async move { std::io::Result::Ok(file.unwrap()) }
            }))
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        match proto_version {
            5 => Ok(Self::V5(tfplugin5::provider_client::ProviderClient::new(
                channel,
            ))),
            6 => Ok(Self::V6(tfplugin6::provider_client::ProviderClient::new(
                channel,
            ))),
            v => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported proto version {}", v),
            )),
        }
    }

    pub async fn schema(&mut self) -> tonic::Result<ProviderSchema> {
        match self {
            Self::V5(p) => Ok(ProviderSchema::V5(
                p.get_schema(tfplugin5::get_provider_schema::Request {})
                    .await?
                    .into_inner(),
            )),
            Self::V6(p) => Ok(ProviderSchema::V6(
                p.get_provider_schema(tfplugin6::get_provider_schema::Request {})
                    .await?
                    .into_inner(),
            )),
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

    pub fn as_type(&self, with_name: &str) -> Type {
        let prefix = format!("{}_", with_name);
        let upper_name = with_name.to_class_case();
        let resources = Type::record(
            self.resource_schemas()
                .into_iter()
                .map(|(name, schema)| (name.replacen(&prefix, "", 1).to_class_case(), schema))
                .map(|(name, schema)| {
                    let mut arg_type = schema.as_arguments_type();
                    if let Type::Record(fields) = &mut arg_type {
                        fields.insert(0, ("skyrKey".into(), Type::String));
                    }
                    let result_type = schema.as_attributes_type();
                    (
                        name.clone(),
                        Type::function(
                            [arg_type],
                            result_type.into_named(format!("{}.{}", upper_name, name)),
                        ),
                    )
                }),
        );

        if let Some(arg) = self.provider_schema().as_ref().map(Schema::as_arguments_type) {
            Type::function([arg], resources).into_named(upper_name)
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
            .unwrap_or(Type::Void)
    }

    pub fn as_attributes_type(&self) -> Type {
        self.block()
            .map(|b| b.as_attributes_type())
            .unwrap_or(Type::Void)
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
        let list = Type::list(
            self.block()
                .as_ref()
                .map(|b| b.as_type(include_computed))
                .unwrap_or(Type::Record(vec![])),
        );

        if self.min_items() == 0 {
            Type::optional(list)
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
            Type::optional(t)
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
        Some("string") => Type::String,
        Some("bool") => Type::Boolean,
        Some("number") => Type::Float,
        _ => {
            if let Some(a) = value.as_array() {
                let mut i = a.iter();
                let c = i.next().and_then(|v| v.as_str());
                let v = i.next();

                match (c, v) {
                    (Some("set"), Some(v)) => Type::List(Box::new(type_from_json_value(v))),
                    (Some("list"), Some(v)) => Type::List(Box::new(type_from_json_value(v))),
                    (Some("map"), Some(v)) => {
                        Type::Map(Box::new(Type::String), Box::new(type_from_json_value(v)))
                    }
                    (Some("object"), Some(JSONValue::Object(fields))) => Type::Record(
                        fields
                            .iter()
                            .map(|(k, v)| (k.to_camel_case(), type_from_json_value(v)))
                            .collect(),
                    ),
                    x => panic!("unsupported: {:?}", x),
                }
            } else {
                panic!("unsupported: {:?}", value);
            }
        }
    }
}
