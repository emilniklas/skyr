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

    async fn create<'a>(&self, arg: Value<'a>) -> ResourceValue {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(arg.access_member("path").as_str())
            .await
            .unwrap();

        let bytes = arg.access_member("content").as_str().as_bytes();
        file.write_all(bytes).await.unwrap();

        let bytes = ResourceValue::list(bytes.iter().copied());

        let mut v: ResourceValue = arg.into();
        v.set_member("bytes", bytes);
        v
    }

    async fn read<'a>(&self, mut value: ResourceValue) -> ResourceValue {
        let mut file = OpenOptions::new()
            .read(true)
            .open(value.access_member("path").as_str())
            .await
            .unwrap();

        let mut bytes = vec![];
        file.read_to_end(&mut bytes).await.unwrap();

        value.set_member("content", String::from_utf8_lossy(&bytes).to_string());
        value.set_member("bytes", ResourceValue::list(bytes));
        value
    }

    async fn update<'a>(&self, arg: Value<'a>, _prev: ResourceValue) -> ResourceValue {
        let mut file = OpenOptions::new()
            .write(true)
            .open(arg.access_member("path").as_str())
            .await
            .unwrap();

        let bytes = arg.access_member("content").as_str().as_bytes();
        file.write_all(bytes).await.unwrap();

        let bytes = ResourceValue::list(bytes.iter().copied());

        let mut v: ResourceValue = arg.into();
        v.set_member("bytes", bytes);
        v
    }

    async fn delete<'a>(&self, prev: ResourceValue) {
        async_std::fs::remove_file(prev.access_member("path").as_str())
            .await
            .unwrap();
    }
}
