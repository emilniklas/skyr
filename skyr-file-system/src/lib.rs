use skyr::analyze::Type;
use skyr::execute::{ExecutionContext, RuntimeValue};
use skyr::{deletable_resources, export_plugin};
use skyr::{Plugin, TypeOf};

mod directory;
mod file;

export_plugin!(FileSystem);

pub struct FileSystem;

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

    deletable_resources!(file::FileResource, directory::DirectoryResource);
}
