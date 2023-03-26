use std::io;

use serde::{Deserialize, Serialize};
use skyr::{deletable_resources, export_plugin, Collection, TypeOf};

export_plugin!(Random);

use rand::RngCore;

use skyr::analyze::{CompositeType, Type};
use skyr::execute::{ExecutionContext, RuntimeValue};
use skyr::{Plugin, Resource};

pub struct Random;

impl Plugin for Random {
    fn import_name(&self) -> &str {
        "Random"
    }

    fn module_type(&self) -> Type {
        Type::named(
            "Random",
            Type::Composite(CompositeType::record([(
                "Identifier",
                IdentifierResource::type_of(),
            )])),
        )
    }

    fn module_value<'a>(&self, ctx: ExecutionContext<'a>) -> RuntimeValue<'a> {
        RuntimeValue::Collection(Collection::record([(
            "Identifier",
            RuntimeValue::resource(ctx, IdentifierResource),
        )]))
    }

    deletable_resources!(IdentifierResource);
}

#[derive(PartialEq, Serialize, Deserialize, TypeOf)]
#[serde(rename_all = "camelCase")]
#[module = "Random"]
struct IdentifierArgs {
    name: String,
    byte_length: Option<usize>,
}

#[derive(Serialize, Deserialize, TypeOf)]
#[serde(rename_all = "camelCase")]
#[module = "Random"]
struct Identifier {
    name: String,
    hex: String,
    bytes: Vec<u8>,
}

#[derive(Clone)]
struct IdentifierResource;

#[async_trait::async_trait]
impl Resource for IdentifierResource {
    type Arguments = IdentifierArgs;
    type State = Identifier;

    fn id(&self, arg: &Self::Arguments) -> String {
        arg.name.clone()
    }

    async fn create(&self, arg: Self::Arguments) -> io::Result<Self::State> {
        let mut bytes = vec![0u8; arg.byte_length.unwrap_or(4)];
        rand::thread_rng().fill_bytes(&mut bytes);

        Ok(Identifier {
            name: arg.name.clone(),
            hex: bytes.iter().map(|c| format!("{:x}", c)).collect(),
            bytes,
        })
    }

    async fn update(&self, arg: Self::Arguments, _prev: Self::State) -> io::Result<Self::State> {
        self.create(arg).await
    }
}
