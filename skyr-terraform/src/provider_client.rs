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

#[derive(Debug)]
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
            .stderr(std::process::Stdio::inherit())
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

#[derive(Debug)]
pub enum ProviderSchema {
    V5(tfplugin5::get_provider_schema::Response),
    V6(tfplugin6::get_provider_schema::Response),
}

impl ProviderSchema {
    pub fn provider_schema(&self) -> Option<Schema> {
        match self {
            Self::V5(s) => s.provider.as_ref().map(Schema::V5),
            Self::V6(s) => s.provider.as_ref().map(Schema::V6),
        }
    }

    pub fn resource_schemas(&self) -> BTreeMap<&str, Schema> {
        match self {
            Self::V5(s) => s
                .resource_schemas
                .iter()
                .map(|(name, schema)| (name.as_str(), Schema::V5(schema)))
                .collect(),
            Self::V6(s) => s
                .resource_schemas
                .iter()
                .map(|(name, schema)| (name.as_str(), Schema::V6(schema)))
                .collect(),
        }
    }

    pub fn as_type(&self) -> Type {
        let resources = Type::record(
            self.resource_schemas()
                .into_iter()
                .map(|(name, schema)| (name.to_class_case(), schema.as_type())),
        );

        if let Some(arg) = self.provider_schema().as_ref().map(Schema::as_type) {
            Type::function([arg], resources)
        } else {
            resources
        }
    }
}

#[derive(Debug)]
pub enum Schema<'a> {
    V5(&'a tfplugin5::Schema),
    V6(&'a tfplugin6::Schema),
}

impl<'a> Schema<'a> {
    pub fn block(&self) -> Option<Block<'a>> {
        match self {
            Self::V5(s) => s.block.as_ref().map(Block::V5),
            Self::V6(s) => s.block.as_ref().map(Block::V6),
        }
    }

    pub fn as_type(&self) -> Type {
        self.block().map(|b| b.as_type()).unwrap_or(Type::Void)
    }
}

#[derive(Debug)]
pub enum Block<'a> {
    V5(&'a tfplugin5::schema::Block),
    V6(&'a tfplugin6::schema::Block),
}

impl<'a> Block<'a> {
    pub fn attributes(&self) -> Vec<Attribute<'a>> {
        match self {
            Self::V5(b) => b.attributes.iter().map(Attribute::V5).collect(),
            Self::V6(b) => b.attributes.iter().map(Attribute::V6).collect(),
        }
    }

    pub fn nested_blocks(&self) -> Vec<NestedBlock<'a>> {
        match self {
            Self::V5(b) => b.block_types.iter().map(NestedBlock::V5).collect(),
            Self::V6(b) => b.block_types.iter().map(NestedBlock::V6).collect(),
        }
    }

    pub fn as_type(&self) -> Type {
        Type::record(
            self.attributes()
                .into_iter()
                .map(|attribute| (attribute.name().to_camel_case(), attribute.type_()))
                .chain(
                    self.nested_blocks()
                        .into_iter()
                        .map(|nb| (nb.type_name().to_camel_case().to_plural(), nb.as_type())),
                ),
        )
    }
}

#[derive(Debug)]
pub enum NestedBlock<'a> {
    V5(&'a tfplugin5::schema::NestedBlock),
    V6(&'a tfplugin6::schema::NestedBlock),
}

impl<'a> NestedBlock<'a> {
    pub fn type_name(&self) -> &'a str {
        match self {
            Self::V5(b) => b.type_name.as_str(),
            Self::V6(b) => b.type_name.as_str(),
        }
    }

    pub fn block(&self) -> Option<Block<'a>> {
        match self {
            Self::V5(b) => b.block.as_ref().map(Block::V5),
            Self::V6(b) => b.block.as_ref().map(Block::V6),
        }
    }

    pub fn as_type(&self) -> Type {
        Type::list(
            self.block()
                .as_ref()
                .map(Block::as_type)
                .unwrap_or(Type::Record(vec![])),
        )
    }
}

#[derive(Debug)]
pub enum Attribute<'a> {
    V5(&'a tfplugin5::schema::Attribute),
    V6(&'a tfplugin6::schema::Attribute),
}

impl<'a> Attribute<'a> {
    pub fn name(&self) -> &'a str {
        match self {
            Self::V5(a) => a.name.as_str(),
            Self::V6(a) => a.name.as_str(),
        }
    }

    pub fn type_(&self) -> Type {
        match self {
            Self::V5(a) => type_from_bytes(a.r#type.as_slice()),
            Self::V6(a) => type_from_bytes(a.r#type.as_slice()),
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
