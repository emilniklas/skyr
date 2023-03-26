use futures::stream::FuturesOrdered;
use futures::StreamExt;
use skyr::{export_plugin, known, Collection, Primitive};

export_plugin!(List);

use skyr::analyze::Type;
use skyr::execute::{ExecutionContext, RuntimeValue};
use skyr::Plugin;

pub struct List;

#[async_trait::async_trait]
impl Plugin for List {
    fn import_name(&self) -> &str {
        "List"
    }

    fn module_type(&self) -> Type {
        Type::named(
            "List",
            Type::record([
                ("first", {
                    let t = Type::open();
                    Type::function([Type::list(t.clone())], Type::optional(t))
                }),
                ("range", {
                    Type::function([Type::INTEGER], Type::list(Type::INTEGER))
                }),
                ("map", {
                    let a = Type::open();
                    let b = Type::open();
                    Type::function(
                        [Type::list(a.clone()), Type::function([a], b.clone())],
                        Type::list(b),
                    )
                }),
            ]),
        )
    }

    fn module_value<'a>(&self, _ctx: ExecutionContext<'a>) -> RuntimeValue<'a> {
        RuntimeValue::Collection(Collection::record([
            (
                "first",
                RuntimeValue::function_sync(|_, mut args| {
                    let list = known!(args.remove(0));
                    let list = list.as_collection().as_vec();
                    list.first()
                        .cloned()
                        .unwrap_or(RuntimeValue::Primitive(Primitive::Nil).into())
                }),
            ),
            (
                "range",
                RuntimeValue::function_sync(|_, mut args| {
                    let list = known!(args.remove(0));
                    let length = list.as_primitive().as_usize();
                    RuntimeValue::Collection(Collection::List(
                        (0..length)
                            .map(|i| RuntimeValue::Primitive(i.into()).into())
                            .collect(),
                    ))
                    .into()
                }),
            ),
            (
                "map",
                RuntimeValue::function_async(|e, mut args| {
                    Box::pin(async move {
                        let list = known!(args.remove(0));
                        let f = known!(args.remove(0));

                        let list = list.as_collection().as_vec();
                        let f = f.as_func();

                        let mut fo = FuturesOrdered::new();
                        for element in list {
                            fo.push_back(f(e, vec![element.clone()]));
                        }
                        RuntimeValue::Collection(Collection::List(fo.collect().await)).into()
                    })
                }),
            ),
        ]))
    }
}
