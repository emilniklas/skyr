use std::io;

use serde::{Deserialize, Serialize};
use skyr::{export_plugin, Collection, ResourceState, TypeOf};

export_plugin!(Random);

use rand::RngCore;

use skyr::analyze::Type;
use skyr::execute::{ExecutionContext, RuntimeValue};
use skyr::{Plugin, Resource};

pub struct Random;

#[async_trait::async_trait]
impl Plugin for Random {
    fn import_name(&self) -> &str {
        "Random"
    }

    fn module_type(&self) -> Type {
        Type::named(
            "Random",
            Type::record([("Identifier", IdentifierConstructor::type_of())]),
        )
    }

    fn module_value<'a>(&self, ctx: ExecutionContext<'a>) -> RuntimeValue<'a> {
        RuntimeValue::Collection(Collection::record([(
            "Identifier",
            RuntimeValue::resource(ctx, IdentifierConstructor),
        )]))
    }

    async fn delete_matching_resource(&self, resource: &ResourceState) -> io::Result<Option<()>> {
        if resource.has_type(&Identifier::type_of()) {
            Ok(Some(
                IdentifierConstructor
                    .delete(resource.state.deserialize().unwrap())
                    .await?,
            ))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone)]
struct IdentifierConstructor;

impl TypeOf for IdentifierConstructor {
    fn type_of() -> Type {
        Type::function([IdentifierArgs::type_of()], Identifier::type_of())
    }
}

#[derive(Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct IdentifierArgs {
    name: String,
    byte_length: usize,
}

impl TypeOf for IdentifierArgs {
    fn type_of() -> Type {
        Type::record([("name", Type::String), ("byteLength", Type::Integer)])
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Identifier {
    name: String,
    hex: String,
    bytes: Vec<u8>,
}

impl TypeOf for Identifier {
    fn type_of() -> Type {
        Type::named(
            "Random.Identifier",
            Type::record([
                ("name", Type::String),
                ("hex", Type::String),
                ("bytes", Type::list(Type::Integer)),
            ]),
        )
    }
}

#[async_trait::async_trait]
impl Resource for IdentifierConstructor {
    type Arguments = IdentifierArgs;
    type State = Identifier;

    fn id(&self, arg: &Self::Arguments) -> String {
        arg.name.clone()
    }

    async fn create(&self, arg: Self::Arguments) -> io::Result<Self::State> {
        let mut bytes = vec![0u8; arg.byte_length];
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
