use crate::analyze::Type;
use crate::execute::{ExecutionContext, Value};
use crate::{Plugin, PluginResource, ResourceId, ResourceValue};

pub struct Random;

impl Plugin for Random {
    fn import_name(&self) -> &str {
        "Random"
    }

    fn module_type(&self) -> Type {
        Type::named(
            "Random",
            Type::record([("Identifier", Identifier::type_())]),
        )
    }

    fn module_value<'a>(&self, ctx: ExecutionContext<'a>) -> Value<'a> {
        Value::record([("Identifier", Value::resource(ctx, Identifier))])
    }
}

#[derive(Clone)]
struct Identifier;

impl Identifier {
    fn type_() -> Type {
        Type::function(
            [Type::record([("name", Type::String)])],
            Type::named(
                "Random.Identifier",
                Type::record([("name", Type::String), ("hex", Type::String)]),
            ),
        )
    }
}

#[async_trait::async_trait]
impl PluginResource for Identifier {
    fn id<'a>(&self, arg: &Value<'a>) -> ResourceId {
        arg.access_member("name").as_str().into()
    }

    async fn create<'a>(&self, arg: Value<'a>) -> ResourceValue {
        let i: u64 = rand::random();
        ResourceValue::record([
            ("name", arg.access_member("name").as_str().to_string()),
            ("hex", format!("{:x}", i)),
        ])
    }
}
