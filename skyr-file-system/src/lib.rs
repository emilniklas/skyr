use std::io;

use async_std::fs::OpenOptions;
use async_std::io::{ReadExt, WriteExt};
use serde::{Deserialize, Serialize};
use skyr::{export_plugin, Collection, ResourceState, TypeOf};

export_plugin!(FileSystem);

use skyr::analyze::Type;
use skyr::execute::{ExecutionContext, RuntimeValue};
use skyr::{Plugin, Resource};

pub struct FileSystem;

#[async_trait::async_trait]
impl Plugin for FileSystem {
    fn import_name(&self) -> &str {
        "FileSystem"
    }

    fn module_type(&self) -> Type {
        Type::named(
            "FileSystem",
            Type::record([("File", FileConstructor::type_of())]),
        )
    }

    fn module_value<'a>(&self, ctx: ExecutionContext<'a>) -> RuntimeValue<'a> {
        RuntimeValue::Collection(Collection::record([(
            "File",
            RuntimeValue::resource(ctx, FileConstructor),
        )]))
    }

    async fn delete_matching_resource(&self, resource: &ResourceState) -> io::Result<Option<()>> {
        if resource.has_type(&File::type_of()) {
            Ok(Some(
                FileConstructor
                    .delete(resource.state.deserialize().unwrap())
                    .await?,
            ))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone)]
struct FileConstructor;

impl TypeOf for FileConstructor {
    fn type_of() -> Type {
        Type::function([FileArgs::type_of()], File::type_of())
    }
}

#[derive(Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct FileArgs {
    path: String,
    content: String,
}

impl TypeOf for FileArgs {
    fn type_of() -> Type {
        Type::record([("path", Type::String), ("content", Type::String)])
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct File {
    path: String,
    content: String,
    bytes: Vec<u8>,
}

impl TypeOf for File {
    fn type_of() -> Type {
        Type::named(
            "FileSystem.File",
            Type::record([
                ("path", Type::String),
                ("content", Type::String),
                ("bytes", Type::list(Type::Integer)),
            ]),
        )
    }
}

#[async_trait::async_trait]
impl Resource for FileConstructor {
    type Arguments = FileArgs;
    type State = File;

    fn id(&self, arg: &Self::Arguments) -> String {
        arg.path.clone()
    }

    async fn create(&self, arg: Self::Arguments) -> io::Result<Self::State> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&arg.path)
            .await?;

        let bytes = arg.content.as_bytes();
        file.write_all(bytes).await?;

        Ok(File {
            path: arg.path,
            bytes: arg.content.as_bytes().to_vec(),
            content: arg.content,
        })
    }

    async fn read(
        &self,
        prev: Self::State,
        arg: &mut Self::Arguments,
    ) -> io::Result<Option<Self::State>> {
        let mut file = match OpenOptions::new().read(true).open(&prev.path).await {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };

        let mut bytes = vec![];
        file.read_to_end(&mut bytes).await?;

        let content = String::from_utf8_lossy(&bytes).to_string();

        arg.path = prev.path.clone();
        arg.content = content.clone();

        Ok(Some(File {
            path: prev.path,
            content,
            bytes,
        }))
    }

    async fn update(&self, arg: Self::Arguments, _prev: Self::State) -> io::Result<Self::State> {
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&arg.path)
            .await?;

        let bytes = arg.content.as_bytes();
        file.write_all(bytes).await?;

        Ok(File {
            path: arg.path,
            bytes: bytes.to_vec(),
            content: arg.content,
        })
    }

    async fn delete(&self, prev: Self::State) -> io::Result<()> {
        async_std::fs::remove_file(&prev.path).await
    }
}
