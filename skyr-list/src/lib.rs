use futures::stream::FuturesOrdered;
use futures::StreamExt;
use skyr::{export_plugin, known};

export_plugin!(List);

use skyr::analyze::Type;
use skyr::execute::{ExecutionContext, Value};
use skyr::Plugin;

pub struct List;

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
                    Type::function([Type::list(t.clone())], t)
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

    fn module_value<'a>(&self, _ctx: ExecutionContext<'a>) -> Value<'a> {
        Value::record([
            (
                "first",
                Value::function_sync(|_, args| {
                    let list = args[0].as_vec();
                    list.first().cloned().unwrap_or(Value::Nil)
                }),
            ),
            (
                "map",
                Value::function_async(|e, args| {
                    Box::pin(async move {
                        let list = known!(&args[0]).as_vec();
                        let f = known!(&args[1]).as_func();

                        let mut fo = FuturesOrdered::new();
                        for element in list {
                            fo.push_back(f(e, vec![element.clone()]));
                        }
                        Value::List(fo.collect().await)
                    })
                }),
            ),
        ])
    }
}
