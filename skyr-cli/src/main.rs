use std::io::{self, Read, Write};
use std::process::ExitCode;

use async_std::stream::StreamExt;
use clap::Parser;
use glob;
use skyr::{Plan, PluginFactory, Source, State};

use self::gui::Gui;

mod gui;

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
    let gui = Gui::new(io::stdin(), io::stdout());

    match do_main(gui.clone()).await {
        Err(e) => {
            gui.print_io_error(e).await?;
            Ok(ExitCode::FAILURE)
        }
        Ok(c) => Ok(c),
    }
}

async fn do_main(
    gui: Gui<impl 'static + Read + Send, impl 'static + Write + Send>,
) -> io::Result<ExitCode> {
    let SkyrCli { command } = Parser::parse();

    let plugins = unsafe {
        #[cfg(target_os = "macos")]
        let pattern = "**/libskyr_plugin_*.dylib";

        #[cfg(target_os = "linux")]
        let pattern = "**/libskyr_plugin_*.so";

        #[cfg(target_os = "window")]
        let pattern = "**/libskyr_plugin_*.dll";

        glob::glob(pattern)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .into_iter()
            .filter_map(|d| d.ok())
            .enumerate()
            .map(|(idx, dylib)| {
                let lib = libloading::Library::new(dylib).unwrap();
                let func: libloading::Symbol<unsafe extern "C" fn(u64) -> *mut PluginFactory> =
                    lib.get(b"skyr_plugin").unwrap();
                Box::from_raw(func(idx as _))
            })
            .collect()
    };

    match command {
        Command::Apply { approve } => apply(gui, approve, plugins).await,
        Command::Teardown { approve } => teardown(gui, approve, plugins).await,
        Command::Plan => plan(gui, plugins).await,
        Command::State(StateCommand::Inspect) => inspect_state(gui).await,
    }
}

async fn teardown(
    gui: Gui<impl 'static + Read + Send, impl 'static + Write + Send>,
    approve: bool,
    plugins: Vec<Box<PluginFactory>>,
) -> io::Result<ExitCode> {
    let state = state().await;

    let mut plan = Plan::new();

    for resource in state.all_not_in(&Default::default()) {
        let plugins = &plugins;
        plan.register_delete(
            resource.id.clone(),
            resource.dependencies.clone(),
            move |_, _| {
                Box::pin(async move {
                    for plugin in plugins.iter() {
                        if let Some(()) = plugin().await.delete_matching_resource(&resource).await?
                        {
                            return Ok(());
                        }
                    }

                    Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("no plugin took ownership of resource {:?}", resource.id),
                    ))
                })
            },
        );
    }

    gui.print_plan(&plan).await?;

    if plan.is_empty() {
        return Ok(ExitCode::SUCCESS);
    }

    if !(approve || gui.confirm().await?) {
        return Ok(ExitCode::FAILURE);
    }

    let (tx, mut rx) = async_std::channel::unbounded();

    let join = async_std::task::spawn({
        let gui = gui.clone();
        async move {
            while let Some(event) = rx.next().await {
                gui.print_plan_execution_event(event).await.unwrap_or(());
            }
        }
    });

    if let Err(e) = plan.execute_once(&state, tx).await {
        gui.print_resource_errors(e).await?;
        join.await;
        return Ok(ExitCode::FAILURE);
    }

    join.await;

    save_state(&state).await?;

    Ok(ExitCode::SUCCESS)
}

async fn sources() -> io::Result<Vec<Source>> {
    let files = glob::glob("**/*.skyr").map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let files = files
        .filter(|r| !matches!(r, Ok(p) if p.as_os_str() == ".skyr"))
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

async fn save_state(state: &State) -> io::Result<()> {
    if state.is_empty() {
        match std::fs::remove_file(STATEFILE) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
            Ok(()) => Ok(()),
        }
    } else {
        state.save(
            &mut std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(STATEFILE)?,
        )
    }
}

async fn inspect_state(gui: Gui<impl Read, impl Write>) -> io::Result<ExitCode> {
    let state = state().await;
    let resources = state.into_resources();
    for resource in resources.values() {
        gui.print_resource_state(resource).await?;
    }
    Ok(ExitCode::SUCCESS)
}

async fn plan(
    gui: Gui<impl Read, impl Write>,
    plugins: Vec<Box<PluginFactory>>,
) -> io::Result<ExitCode> {
    let (sources, state) = futures::future::join(sources(), state()).await;
    let sources = sources?;

    let program = compile(&gui, &sources, plugins).await?;
    let program = analyze(&gui, &sources, &program).await?;

    let plan = match program.plan(&state).await {
        Ok(p) => p,
        Err(errors) => {
            gui.print_resource_errors(errors).await?;
            return Ok(ExitCode::FAILURE);
        }
    };
    gui.print_plan(&plan).await?;

    if !plan.is_empty() {
        Ok(ExitCode::FAILURE)
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

async fn compile(
    gui: &Gui<impl Read, impl Write>,
    sources: &Vec<Source>,
    plugins: Vec<Box<PluginFactory>>,
) -> io::Result<skyr::ParsedProgram> {
    match skyr::Program::new(&sources).compile(plugins) {
        Ok(p) => Ok(p),
        Err(e) => {
            gui.print_compile_error(e, sources).await?;
            Err(io::Error::new(io::ErrorKind::Other, "Compilation failed"))
        }
    }
}

async fn analyze<'a>(
    gui: &Gui<impl Read, impl Write>,
    sources: &Vec<Source>,
    program: &'a skyr::ParsedProgram,
) -> io::Result<skyr::AnalyzedProgram<'a>> {
    match program.analyze().await {
        Ok(p) => Ok(p),
        Err(e) => {
            gui.print_compile_error(e, sources).await?;
            Err(io::Error::new(io::ErrorKind::Other, "Compilation failed"))
        }
    }
}

async fn apply(
    gui: Gui<impl 'static + Read + Send, impl 'static + Write + Send>,
    approve: bool,
    plugins: Vec<Box<PluginFactory>>,
) -> io::Result<ExitCode> {
    let (sources, state) = futures::future::join(sources(), state()).await;
    let sources = sources?;

    let program = compile(&gui, &sources, plugins).await?;
    let program = analyze(&gui, &sources, &program).await?;

    let mut exit_code = ExitCode::SUCCESS;
    {
        let mut plan = match program.plan(&state).await {
            Ok(p) => p,
            Err(e) => {
                gui.print_resource_errors(e).await?;
                return Ok(ExitCode::FAILURE);
            }
        };

        gui.print_plan(&plan).await?;

        while !plan.is_empty() {
            if !(approve || gui.confirm().await?) {
                exit_code = ExitCode::FAILURE;
                break;
            }

            let (tx, mut rx) = async_std::channel::unbounded();

            let join = async_std::task::spawn({
                let gui = gui.clone();
                async move {
                    while let Some(event) = rx.next().await {
                        gui.print_plan_execution_event(event).await.unwrap_or(());
                    }
                }
            });

            plan = match plan.execute(&program, &state, tx).await {
                Ok(p) => p,
                Err(e) => {
                    gui.print_resource_errors(e).await?;
                    exit_code = ExitCode::FAILURE;
                    break;
                }
            };

            join.await;

            save_state(&state).await?;

            gui.print_plan(&plan).await?;
        }
    }

    save_state(&state).await?;

    Ok(exit_code)
}
