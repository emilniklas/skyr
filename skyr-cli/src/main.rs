use futures::FutureExt;
use std::io;

use clap::Parser;
use glob;

#[derive(Parser)]
struct SkyrCli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Parser)]
enum Command {
    Plan,
}

#[async_std::main]
async fn main() -> io::Result<()> {
    let SkyrCli { command } = Parser::parse();

    match command {
        Command::Plan => plan().await,
    }
}

async fn plan() -> io::Result<()> {
    let files = glob::glob("**/*.skyr").map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let files = files
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    let (sources, statefile) = futures::future::join(
        futures::future::join_all(files.into_iter().map(|path| async move {
            let code = async_std::fs::read_to_string(&path).await?;
            Ok(skyr::Source::new(path.display().to_string(), code))
        })),
        async_std::fs::read(".skyr.state").map(|r| r.unwrap_or_default()),
    )
    .await;

    let sources = sources.into_iter().collect::<Result<Vec<_>, io::Error>>()?;

    let program = skyr::Program::new(sources)
        .compile()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let mut program = program
        .analyze()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    let state = skyr::State::open(statefile.as_slice())?;

    println!("{:#?}", program.plan(&state).await);

    Ok(())
}
