use std::io::{self, BufRead, Write};
use std::process::ExitCode;

use async_std::stream::StreamExt;
use clap::Parser;
use glob;
use skyr::{stdlib, Plugin, Source, State};

#[derive(Parser)]
struct SkyrCli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Parser)]
enum Command {
    Apply {
        #[clap(long = "approve", short = 'y')]
        approve: bool,
    },
    Plan,
}

#[async_std::main]
async fn main() -> io::Result<ExitCode> {
    let SkyrCli { command } = Parser::parse();

    match command {
        Command::Apply { approve } => apply(approve).await,
        Command::Plan => plan().await,
    }
}

async fn sources() -> io::Result<Vec<Source>> {
    let files = glob::glob("**/*.skyr").map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let files = files
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    let sources = futures::future::join_all(files.into_iter().map(|path| async move {
        let code = async_std::fs::read_to_string(&path).await?;
        Ok(skyr::Source::new(path.display().to_string(), code))
    }))
    .await;

    sources.into_iter().collect::<io::Result<Vec<_>>>()
}

async fn state() -> State {
    async_std::fs::read(".skyr.state")
        .await
        .and_then(|s| skyr::State::open(s.as_slice()))
        .unwrap_or_default()
}

async fn plan() -> io::Result<ExitCode> {
    let (sources, state) = futures::future::join(sources(), state()).await;

    let plugins: Vec<Box<dyn Plugin>> = vec![Box::new(stdlib::Random)];

    let program = skyr::Program::new(sources?)
        .compile(plugins)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let program = program
        .analyze()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    let plan = program.plan(&state).await;
    println!("{:?}", plan);

    if !plan.is_empty() {
        Ok(ExitCode::FAILURE)
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

async fn apply(approve: bool) -> io::Result<ExitCode> {
    let (sources, state) = futures::future::join(sources(), state()).await;

    let plugins: Vec<Box<dyn Plugin>> = vec![Box::new(stdlib::Random)];

    let program = skyr::Program::new(sources?)
        .compile(plugins)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let program = program
        .analyze()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    let mut exit_code = ExitCode::SUCCESS;
    {
        let mut stdin = std::io::BufReader::new(std::io::stdin().lock());
        let mut plan = program.plan(&state).await;
        while !plan.is_empty() {
            if !approve {
                print!("{:?}\n\nContinue? ", plan);
                io::stdout().flush()?;
                let mut line = String::new();
                stdin.read_line(&mut line)?;

                if line != "yes\n" {
                    exit_code = ExitCode::FAILURE;
                    break;
                }
            } else {
                println!("{:?}", plan);
            }

            let (tx, mut rx) = async_std::channel::unbounded();

            async_std::task::spawn(async move {
                while let Some(event) = rx.next().await {
                    println!("{:?}", event);
                }
            });

            plan = plan.execute(&program, &state, tx).await;
        }
        println!("{:?}", plan);
    }

    state.save(
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(".skyr.state")?,
    )?;

    Ok(exit_code)
}