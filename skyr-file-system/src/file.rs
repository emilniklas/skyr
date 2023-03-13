use async_std::fs::OpenOptions;
use async_std::io::{ReadExt, WriteExt};
use serde::{Deserialize, Serialize};
use skyr::Resource;
use skyr::TypeOf;
use std::io;

#[derive(Serialize, Deserialize, PartialEq, TypeOf)]
#[serde(rename_all = "camelCase")]
pub struct FileArgs {
    path: String,
    content: String,
}

#[derive(Serialize, Deserialize, TypeOf)]
#[serde(rename_all = "camelCase")]
pub struct File {
    path: String,
    content: String,
    bytes: Vec<u8>,
}

#[derive(Clone)]
pub struct FileResource;

#[async_trait::async_trait]
impl Resource for FileResource {
    type Arguments = FileArgs;
    type State = File;

    fn id(&self, arg: &Self::Arguments) -> String {
        arg.path.clone()
    }

    async fn create(&self, arg: Self::Arguments) -> io::Result<Self::State> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&arg.path)
            .await?;

        let bytes = arg.content.as_bytes();
        file.write_all(bytes).await?;

        Ok(File {
            path: arg.path,
            bytes: arg.content.as_bytes().to_vec(),
            content: arg.content,
        })
    }

    async fn read(
        &self,
        prev: Self::State,
        arg: &mut Self::Arguments,
    ) -> io::Result<Option<Self::State>> {
        let mut file = match OpenOptions::new().read(true).open(&prev.path).await {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };

        let mut bytes = vec![];
        file.read_to_end(&mut bytes).await?;

        let content = String::from_utf8_lossy(&bytes).to_string();

        arg.path = prev.path.clone();
        arg.content = content.clone();

        Ok(Some(File {
            path: prev.path,
            content,
            bytes,
        }))
    }

    async fn update(&self, arg: Self::Arguments, _prev: Self::State) -> io::Result<Self::State> {
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&arg.path)
            .await?;

        let bytes = arg.content.as_bytes();
        file.write_all(bytes).await?;

        Ok(File {
            path: arg.path,
            bytes: bytes.to_vec(),
            content: arg.content,
        })
    }

    async fn delete(&self, prev: Self::State) -> io::Result<()> {
        async_std::fs::remove_file(&prev.path).await
    }
}
