use std::io::{self, BufRead, Write};
use std::process::ExitCode;

use async_std::stream::StreamExt;
use clap::Parser;
use glob;
use skyr::{Plan, Plugin, Source, State};

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
    Teardown {
        #[clap(long = "approve", short = 'y')]
        approve: bool,
    },
    Plan,
    #[clap(subcommand)]
    State(StateCommand),
}

#[derive(Parser)]
enum StateCommand {
    Inspect,
}

#[async_std::main]
async fn main() -> io::Result<ExitCode> {
    let SkyrCli { command } = Parser::parse();

    let plugins = unsafe {
        glob::glob("**/libstd_skyr_*.dylib")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .into_iter()
            .filter_map(|d| d.ok())
            .enumerate()
            .map(|(idx, dylib)| {
                let lib = libloading::Library::new(dylib).unwrap();
                let func: libloading::Symbol<unsafe extern "C" fn(u64) -> *mut dyn Plugin> =
                    lib.get(b"skyr_plugin").unwrap();
                Box::from_raw(func(idx as _))
            })
            .collect()
    };

    match command {
        Command::Apply { approve } => apply(approve, plugins).await,
        Command::Teardown { approve } => teardown(approve, plugins).await,
        Command::Plan => plan(plugins).await,
        Command::State(StateCommand::Inspect) => inspect_state().await,
    }
}

async fn teardown(approve: bool, plugins: Vec<Box<dyn Plugin>>) -> io::Result<ExitCode> {
    let state = state().await;

    let mut plan = Plan::new();

    for resource in state.all_not_in(&Default::default()) {
        let plugins = &plugins;
        plan.register_delete(resource.id.clone(), move |_, _| {
            Box::pin(async move {
                for plugin in plugins.iter() {
                    if let Some(()) = plugin.delete_matching_resource(&resource).await? {
                        return Ok(());
                    }
                }

                Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("no plugin took ownership of resource {:?}", resource.id),
                ))
            })
        });
    }

    println!("{:?}", plan);

    let mut stdin = std::io::BufReader::new(std::io::stdin().lock());
    if !approve {
        print!("\nContinue? ");
        io::stdout().flush()?;
        let mut line = String::new();
        stdin.read_line(&mut line)?;

        if line != "yes\n" {
            return Ok(ExitCode::FAILURE);
        }
    }

    let (tx, mut rx) = async_std::channel::unbounded();

    let join = async_std::task::spawn(async move {
        while let Some(event) = rx.next().await {
            println!("{:?}", event);
        }
    });

    if let Err(e) = plan.execute_once(&state, tx).await {
        println!("{:?}", e);
        join.await;
        return Ok(ExitCode::FAILURE);
    }

    join.await;

    save_state(state).await?;

    Ok(ExitCode::SUCCESS)
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

const STATEFILE: &'static str = ".skyr.state";

async fn state() -> State {
    async_std::fs::read(STATEFILE)
        .await
        .and_then(|s| skyr::State::open(s.as_slice()))
        .unwrap_or_default()
}

async fn save_state(state: State) -> io::Result<()> {
    if state.is_empty() {
        std::fs::remove_file(STATEFILE)
    } else {
        state.save(
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(STATEFILE)?,
        )
    }
}

async fn inspect_state() -> io::Result<ExitCode> {
    let state = state().await;
    let resources = state.into_resources();
    for (id, resource) in resources {
        println!("{:?} {:#?}", id, resource.state);
    }
    Ok(ExitCode::SUCCESS)
}

async fn plan(plugins: Vec<Box<dyn Plugin>>) -> io::Result<ExitCode> {
    let (sources, state) = futures::future::join(sources(), state()).await;

    let program = skyr::Program::new(sources?)
        .compile(plugins)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let program = program
        .analyze()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    let plan = match program.plan(&state).await {
        Ok(p) => p,
        Err(errors) => {
            println!("{:?}", errors);
            return Ok(ExitCode::FAILURE);
        }
    };
    println!("{:?}", plan);

    if !plan.is_empty() {
        Ok(ExitCode::FAILURE)
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

async fn apply(approve: bool, plugins: Vec<Box<dyn Plugin>>) -> io::Result<ExitCode> {
    let (sources, state) = futures::future::join(sources(), state()).await;

    let program = skyr::Program::new(sources?)
        .compile(plugins)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let program = program
        .analyze()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    let mut exit_code = ExitCode::SUCCESS;
    {
        let mut stdin = std::io::BufReader::new(std::io::stdin().lock());
        let mut plan = match program.plan(&state).await {
            Ok(p) => p,
            Err(e) => {
                println!("{:?}", e);
                return Ok(ExitCode::FAILURE);
            }
        };

        println!("{:?}", plan);

        while !plan.is_empty() {
            if !approve {
                print!("\nContinue? ");
                io::stdout().flush()?;
                let mut line = String::new();
                stdin.read_line(&mut line)?;

                if line != "yes\n" {
                    exit_code = ExitCode::FAILURE;
                    break;
                }
            }

            let (tx, mut rx) = async_std::channel::unbounded();

            let join = async_std::task::spawn(async move {
                while let Some(event) = rx.next().await {
                    println!("{:?}", event);
                }
            });

            plan = match plan.execute(&program, &state, tx).await {
                Ok(p) => p,
                Err(e) => {
                    println!("{:?}", e);
                    exit_code = ExitCode::FAILURE;
                    break;
                }
            };

            join.await;

            println!("{:?}", plan);
        }
    }

    save_state(state).await?;

    Ok(exit_code)
}
