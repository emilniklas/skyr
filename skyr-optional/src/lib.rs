use skyr::{export_plugin, known, Collection, Primitive};

export_plugin!(Optional);

use skyr::analyze::Type;
use skyr::execute::{ExecutionContext, RuntimeValue};
use skyr::Plugin;

pub struct Optional;

#[async_trait::async_trait]
impl Plugin for Optional {
    fn import_name(&self) -> &str {
        "Optional"
    }

    fn module_type(&self) -> Type {
        Type::named(
            "Optional",
            Type::record([
                ("otherwise", {
                    let a = Type::open();
                    Type::function([Type::optional(a.clone()), a.clone()], a)
                }),
                ("map", {
                    let a = Type::open();
                    let b = Type::open();
                    Type::function(
                        [Type::optional(a.clone()), Type::function([a], b.clone())],
                        Type::optional(b),
                    )
                }),
            ]),
        )
    }

    fn module_value<'a>(&self, _ctx: ExecutionContext<'a>) -> RuntimeValue<'a> {
        RuntimeValue::Collection(Collection::record([
            (
                "otherwise",
                RuntimeValue::function_sync(|_, mut args| {
                    let optional = known!(args.remove(0));
                    let otherwise = args.remove(0);
                    if let RuntimeValue::Primitive(Primitive::Nil) = optional {
                        otherwise
                    } else {
                        optional.into()
                    }
                }),
            ),
            (
                "map",
                RuntimeValue::function_async(|e, mut args| {
                    Box::pin(async move {
                        let optional = known!(args.remove(0));
                        let f = args.remove(0);

                        if let RuntimeValue::Primitive(Primitive::Nil) = optional {
                            optional.into()
                        } else {
                            let f = known!(f);
                            f.as_func()(e, vec![optional.into()]).await
                        }
                    })
                }),
            ),
        ]))
    }
}
