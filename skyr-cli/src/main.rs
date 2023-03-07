use std::io::{self, BufRead, Write};

use clap::Parser;
use glob;

#[derive(Parser)]
struct SkyrCli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Parser)]
enum Command {
    Apply,
}

#[async_std::main]
async fn main() -> io::Result<()> {
    let SkyrCli { command } = Parser::parse();

    match command {
        Command::Apply => apply().await,
    }
}

async fn apply() -> io::Result<()> {
    let files = glob::glob("**/*.skyr").map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let files = files
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    let (sources, statefile) = futures::future::join(
        futures::future::join_all(files.into_iter().map(|path| async move {
            let code = async_std::fs::read_to_string(&path).await?;
            Ok(skyr::Source::new(path.display().to_string(), code))
        })),
        async_std::fs::read(".skyr.state"),
    )
    .await;

    let sources = sources.into_iter().collect::<Result<Vec<_>, io::Error>>()?;

    let program = skyr::Program::new(sources)
        .compile()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let program = program
        .analyze()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    let state = statefile
        .and_then(|s| skyr::State::open(s.as_slice()))
        .unwrap_or_default();

    {
        let mut stdin = std::io::BufReader::new(std::io::stdin().lock());
        let mut plan = program.plan(&state).await;
        while !plan.is_empty() {
            print!("{:?}\n\nContinue? ", plan);
            io::stdout().flush()?;

            let mut line = String::new();
            stdin.read_line(&mut line)?;

            if line != "yes\n" {
                break;
            }

            plan = plan.execute(&program, &state).await;
        }
    }

    state.save(
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(".skyr.state")?,
    )?;

    Ok(())
}
