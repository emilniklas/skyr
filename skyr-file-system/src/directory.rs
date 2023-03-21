use serde::{Deserialize, Serialize};
use skyr::Resource;
use skyr::TypeOf;
use std::io;

#[derive(Serialize, Deserialize, PartialEq, TypeOf)]
#[serde(rename_all = "camelCase")]
pub struct DirectoryArgs {
    path: String,
}

#[derive(Serialize, Deserialize, TypeOf)]
#[serde(rename_all = "camelCase")]
pub struct Directory {
    path: String,
}

#[derive(Clone)]
pub struct DirectoryResource;

#[async_trait::async_trait]
impl Resource for DirectoryResource {
    type Arguments = DirectoryArgs;
    type State = Directory;

    fn id(&self, arg: &Self::Arguments) -> String {
        arg.path.clone()
    }

    async fn create(&self, arg: Self::Arguments) -> io::Result<Self::State> {
        async_std::fs::create_dir(&arg.path).await?;

        Ok(Directory { path: arg.path })
    }

    async fn read(
        &self,
        prev: Self::State,
        _arg: &mut Self::Arguments,
    ) -> io::Result<Option<Self::State>> {
        match async_std::fs::read_dir(&prev.path).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };

        Ok(Some(prev))
    }

    async fn update(&self, _arg: Self::Arguments, prev: Self::State) -> io::Result<Self::State> {
        Ok(prev)
    }

    async fn delete(prev: Self::State) -> io::Result<()> {
        async_std::fs::remove_dir(&prev.path).await
    }
}
