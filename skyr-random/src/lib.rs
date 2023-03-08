use skyr::export_plugin;

export_plugin!(Random);

use rand::RngCore;

use skyr::analyze::Type;
use skyr::execute::{ExecutionContext, Value};
use skyr::{Plugin, PluginResource, ResourceValue};

pub struct Random;

impl Plugin for Random {
    fn import_name(&self) -> &str {
        "Random"
    }

    fn module_type(&self) -> Type {
        Type::named("Random", Type::record([("Identifier", Identifier.type_())]))
    }

    fn module_value<'a>(&self, ctx: ExecutionContext<'a>) -> Value<'a> {
        Value::record([("Identifier", Value::resource(ctx, Identifier))])
    }
}

#[derive(Clone)]
struct Identifier;

#[async_trait::async_trait]
impl PluginResource for Identifier {
    fn type_(&self) -> Type {
        Type::function(
            [Type::record([
                ("name", Type::String),
                ("byteLength", Type::Integer),
            ])],
            Type::named(
                "Random.Identifier",
                Type::record([("name", Type::String), ("hex", Type::String)]),
            ),
        )
    }

    fn id<'a>(&self, arg: &Value<'a>) -> String {
        arg.access_member("name").as_str().into()
    }

    async fn create<'a>(&self, arg: Value<'a>) -> ResourceValue {
        let record = ResourceValue::record([("name", arg.access_member("name").as_str())]);

        self.update(arg, record).await
    }

    async fn update<'a>(&self, arg: Value<'a>, mut prev: ResourceValue) -> ResourceValue {
        let mut v = vec![0u8; arg.access_member("byteLength").as_usize()];
        rand::thread_rng().fill_bytes(&mut v);
        prev.set_member(
            "hex",
            v.into_iter()
                .map(|c| format!("{:x}", c))
                .collect::<String>(),
        );
        prev
    }
}