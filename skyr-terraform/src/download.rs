use std::io;
use std::os::unix::prelude::OpenOptionsExt;
use std::path::PathBuf;
use std::process::Stdio;

use async_compat::CompatExt;
use async_std::stream::StreamExt;
use futures_util::TryStreamExt;
use serde::Deserialize;

pub fn download_binary(
    registry: &str,
    publisher: &str,
    provider: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    async_std::task::block_on(
        async move {
            println!("cargo:rustc-env=PROVIDER_NAME={}", provider);

            let mut out_file: PathBuf = std::env::var("OUT_DIR")?.into();
            out_file.push("provider");

            let os = if cfg!(target_os = "linux") {
                "linux"
            } else if cfg!(target_os = "macos") {
                "darwin"
            } else {
                return Err("unsupported operating system".into());
            };

            let arch = if cfg!(target_arch = "x86_64") {
                "amd64"
            } else if cfg!(target_arch = "aarch64") {
                "arm64"
            } else {
                return Err("unsupported system architecture".into());
            };

            let version = std::env::var("CARGO_PKG_VERSION")?;

            let url: hyper::Uri = format!(
                "https://{}/v1/providers/{}/{}/{}/download/{}/{}",
                registry, publisher, provider, version, os, arch,
            )
            .parse()?;

            let client =
                hyper::Client::builder().build::<_, hyper::Body>(hyper_tls::HttpsConnector::new());

            let res = client
                .get(url)
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            let body: hyper::body::Bytes = res
                .into_body()
                .collect::<hyper::Result<Vec<hyper::body::Bytes>>>()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                .into_iter()
                .flatten()
                .collect();

            let download: DownloadEndpointResponse = serde_json::from_slice(&body)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            let res = client
                .get(download.download_url.parse()?)
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            let stream = res
                .into_body()
                .map(|r| r.map_err(|e| io::Error::new(io::ErrorKind::Other, e)))
                .into_async_read();

            let mut file = async_tempfile::TempFile::new().await?;

            async_std::io::copy(stream, (&mut file).compat()).await?;

            let mut command = async_std::process::Command::new("unzip")
                .arg("-p")
                .arg(file.file_path())
                .stdout(Stdio::piped())
                .spawn()?;

            let stdout = command.stdout.take().unwrap();

            let out = async_std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .mode(0o755)
                .open(&out_file)
                .await?;

            async_std::io::copy(stdout, out).await?;

            Ok(())
        }
        .compat(),
    )
}

#[derive(Deserialize)]
struct DownloadEndpointResponse {
    download_url: String,
}
