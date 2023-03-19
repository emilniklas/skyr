use std::io;

use skyr::analyze::Type;
use skyr::execute::{ExecutionContext, RuntimeValue};
use skyr::{export_plugin, ResourceState};
use skyr::{Plugin, Resource, TypeOf};

mod directory;
mod file;

export_plugin!(FileSystem);

pub struct FileSystem;

#[async_trait::async_trait]
impl Plugin for FileSystem {
    fn import_name(&self) -> &str {
        "FileSystem"
    }

    fn module_type(&self) -> Type {
        Type::named(
            "FileSystem",
            Type::record([
                ("File", file::FileResource::type_of()),
                ("Directory", directory::DirectoryResource::type_of()),
            ]),
        )
    }

    fn module_value<'a>(&self, ctx: ExecutionContext<'a>) -> RuntimeValue<'a> {
        RuntimeValue::record([
            (
                "File",
                RuntimeValue::resource(ctx.clone(), file::FileResource),
            ),
            (
                "Directory",
                RuntimeValue::resource(ctx, directory::DirectoryResource),
            ),
        ])
    }

    async fn delete_matching_resource(&self, resource: &ResourceState) -> io::Result<Option<()>> {
        if let Some(()) = file::FileResource.try_match_delete(resource).await? {
            return Ok(Some(()));
        }

        if let Some(()) = directory::DirectoryResource
            .try_match_delete(resource)
            .await?
        {
            return Ok(Some(()));
        }

        Ok(None)
    }
}
