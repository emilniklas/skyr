use std::io;

use async_std::fs::OpenOptions;
use async_std::io::{ReadExt, WriteExt};
use skyr::export_plugin;

export_plugin!(FileSystem);

use skyr::analyze::Type;
use skyr::execute::{ExecutionContext, Value};
use skyr::{Plugin, PluginResource, ResourceValue};

pub struct FileSystem;

impl Plugin for FileSystem {
    fn import_name(&self) -> &str {
        "FileSystem"
    }

    fn module_type(&self) -> Type {
        Type::named("FileSystem", Type::record([("File", File.type_())]))
    }

    fn module_value<'a>(&self, ctx: ExecutionContext<'a>) -> Value<'a> {
        Value::record([("File", Value::resource(ctx, File))])
    }

    fn find_resource(&self, resource: &skyr::Resource) -> Option<Box<dyn PluginResource>> {
        if resource.has_type(File.type_().return_type()) {
            Some(Box::new(File))
        } else {
            None
        }
    }
}

#[derive(Clone)]
struct File;

#[async_trait::async_trait]
impl PluginResource for File {
    fn type_(&self) -> Type {
        Type::function(
            [Type::record([
                ("path", Type::String),
                ("content", Type::String),
            ])],
            Type::named(
                "FileSystem.File",
                Type::record([
                    ("path", Type::String),
                    ("content", Type::String),
                    ("bytes", Type::list(Type::Integer)),
                ]),
            ),
        )
    }

    fn id<'a>(&self, arg: &Value<'a>) -> String {
        arg.access_member("path").as_str().into()
    }

    async fn create<'a>(&self, arg: Value<'a>) -> io::Result<ResourceValue> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(arg.access_member("path").as_str())
            .await?;

        let bytes = arg.access_member("content").as_str().as_bytes();
        file.write_all(bytes).await?;

        let bytes = ResourceValue::list(bytes.iter().copied());

        let mut v: ResourceValue = arg.into();
        v.set_member("bytes", bytes);
        Ok(v)
    }

    async fn read<'a>(
        &self,
        mut value: ResourceValue,
        arg: &mut ResourceValue,
    ) -> io::Result<Option<ResourceValue>> {
        let mut file = match OpenOptions::new()
            .read(true)
            .open(value.access_member("path").as_str())
            .await
        {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };

        let mut bytes = vec![];
        file.read_to_end(&mut bytes).await?;

        let content = String::from_utf8_lossy(&bytes).to_string();

        arg.set_member("content", content.as_str());
        value.set_member("content", content);
        value.set_member("bytes", ResourceValue::list(bytes));
        Ok(Some(value))
    }

    async fn update<'a>(&self, arg: Value<'a>, _prev: ResourceValue) -> io::Result<ResourceValue> {
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(arg.access_member("path").as_str())
            .await?;

        let content = arg.access_member("content").as_str().to_string();
        let bytes = content.as_bytes();
        file.write_all(bytes).await?;

        let bytes = ResourceValue::list(bytes.iter().copied());

        let mut v: ResourceValue = arg.into();
        v.set_member("content", content);
        v.set_member("bytes", bytes);
        Ok(v)
    }

    async fn delete<'a>(&self, prev: ResourceValue) -> io::Result<()> {
        async_std::fs::remove_file(prev.access_member("path").as_str()).await
    }
}
