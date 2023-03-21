use std::io::{self, BufRead, BufReader, Read, Write};
use std::sync::Arc;
use std::time::Duration;

use async_std::sync::Mutex;
use colored::{Color, Colorize};
use skyr::execute::RuntimeValue;
use skyr::{
    Collection, Diff, Plan, PlanExecutionEvent, Primitive, ResourceError, ResourceState, Value,
};

pub struct Gui<R, W> {
    s: Arc<Mutex<GuiState<R, W>>>,
}

impl<R, W> Clone for Gui<R, W> {
    fn clone(&self) -> Self {
        Gui { s: self.s.clone() }
    }
}

impl<R: Read, W: Write> Gui<R, W> {
    pub fn new(r: R, w: W) -> Self {
        Self {
            s: Arc::new(Mutex::new(GuiState {
                r: BufReader::new(r),
                w,
                indentation: 0,
                value_color: None,
            })),
        }
    }

    pub async fn print_io_error(&self, error: io::Error) -> io::Result<()> {
        self.s.lock().await.print_io_error(error)
    }

    pub async fn print_plan<'a>(&self, plan: &Plan<'a>) -> io::Result<()> {
        self.s.lock().await.print_plan(plan)
    }

    pub async fn print_resource_state(&self, state: &ResourceState) -> io::Result<()> {
        self.s.lock().await.print_resource_state(state)
    }

    pub async fn print_plan_execution_event(&self, event: PlanExecutionEvent) -> io::Result<()> {
        self.s.lock().await.print_plan_execution_event(event)
    }

    pub async fn confirm(&self) -> io::Result<bool> {
        let confirmed = self.s.lock().await.confirm()?;
        if confirmed {
            async_std::task::sleep(Duration::from_millis(400)).await;
        }
        Ok(confirmed)
    }

    pub async fn print_resource_errors(
        &self,
        errors: impl IntoIterator<Item = ResourceError>,
    ) -> io::Result<()> {
        self.s
            .lock()
            .await
            .print_resource_errors(errors.into_iter().collect())
    }
}

struct GuiState<R, W> {
    r: BufReader<R>,
    w: W,
    indentation: usize,
    value_color: Option<Color>,
}

impl<R: Read, W: Write> GuiState<R, W> {
    pub fn print_io_error(&mut self, error: io::Error) -> io::Result<()> {
        write!(self.w, "{:?}", error)
    }

    pub fn print_runtime_value<'a>(&mut self, value: &RuntimeValue<'a>) -> io::Result<()> {
        match value {
            RuntimeValue::Primitive(p) => self.print_primitive(p),
            RuntimeValue::Collection(c) => self.print_collection(c, |s, v| {
                if let Some(v) = v.as_resolved() {
                    s.print_runtime_value(v)
                } else {
                    Ok(())
                }
            }),
            RuntimeValue::Deferred(_) => write!(self.w, "{}", "<deferred>".bright_purple()),
            RuntimeValue::Function(_) => write!(self.w, "{}", "<fn>".bright_magenta()),
        }
    }

    pub fn print_plan<'a>(&mut self, plan: &Plan<'a>) -> io::Result<()> {
        for (span, value) in plan.debug_messages() {
            write!(self.w, "{} ", format!("{:?}", span).bright_black())?;
            self.print_runtime_value(value)?;
            self.print_newline()?;
        }

        if plan.is_empty() {
            if plan.is_continuation() {
                writeln!(self.w, "{}", "Nothing more to do".bright_green().bold())?;
            } else {
                writeln!(self.w, "{}", "Nothing to do".bright_green().bold())?;
            }
            return Ok(());
        }

        if !plan.is_continuation() {
            writeln!(
                self.w,
                "{}",
                "═══════════════════════════════════════".bright_black()
            )?;
        }

        writeln!(
            self.w,
            "{}",
            format!("PHASE {}", plan.phase_index() + 1)
                .bright_yellow()
                .bold()
        )?;
        writeln!(
            self.w,
            "{}",
            "———————————————————————————————————————".bright_black()
        )?;
        self.print_newline()?;

        let mut num_to_be_created = 0;
        let mut num_to_be_updated = 0;
        let mut num_to_be_deleted = 0;

        for (id, args) in plan.to_be_created() {
            num_to_be_created += 1;

            write!(
                self.w,
                "{} {} ",
                "+".green().bold(),
                format!("{:?}", id.type_).bold()
            )?;
            self.indentation += 1;
            self.print_value(args)?;
            self.indentation -= 1;
            self.print_newline()?;
        }

        for (id, diff) in plan.to_be_updated() {
            num_to_be_updated += 1;

            write!(
                self.w,
                "{} {} ",
                "↑".yellow().bold(),
                format!("{:?}", id.type_).bold()
            )?;
            self.indentation += 1;
            self.print_diff(diff)?;
            self.indentation -= 1;
            self.print_newline()?;
        }

        for id in plan.to_be_deleted() {
            num_to_be_deleted += 1;

            writeln!(
                self.w,
                "{} {} {}",
                "-".red().bold(),
                format!("{:?}", id.type_).bold(),
                format!("{:?}", id.id).bright_blue()
            )?;
        }

        self.print_newline()?;
        writeln!(
            self.w,
            "{}\n{}",
            "———————————————————————————————————————".bright_black(),
            "SUMMARY".bright_yellow().bold()
        )?;

        if num_to_be_created > 0 {
            writeln!(
                self.w,
                "{} {} resource(s) will be created",
                "+".green().bold(),
                num_to_be_created
            )?;
        }
        if num_to_be_updated > 0 {
            writeln!(
                self.w,
                "{} {} resource(s) will be updated",
                "↑".yellow().bold(),
                num_to_be_updated
            )?;
        }
        if num_to_be_deleted > 0 {
            writeln!(
                self.w,
                "{} {} resource(s) will be deleted",
                "-".red().bold(),
                num_to_be_deleted
            )?;
        }

        Ok(())
    }

    fn print_newline(&mut self) -> io::Result<()> {
        write!(self.w, "\n{:width$}", "", width = self.indentation * 2)
    }

    fn print_diff<'a>(&mut self, diff: Diff<'a>) -> io::Result<()> {
        match diff {
            Diff::Unchanged(v) => self.print_value(v),
            Diff::Added(v) => {
                write!(self.w, "{} ", "+".green())?;
                self.value_color = Some(Color::BrightGreen);
                self.print_value(v)?;
                self.value_color = None;
                Ok(())
            }
            Diff::Removed(v) => {
                write!(self.w, "{} ", "-".red())?;
                self.value_color = Some(Color::BrightRed);
                self.print_value(v)?;
                self.value_color = None;
                Ok(())
            }
            Diff::Changed(o, n) => {
                self.value_color = Some(Color::BrightRed);
                self.print_value(o)?;
                write!(self.w, " {} ", "=>".bright_yellow().bold())?;
                self.value_color = Some(Color::BrightGreen);
                self.print_value(n)?;
                self.value_color = None;
                Ok(())
            }
            Diff::List(l) => {
                write!(self.w, "[")?;
                self.indentation += 1;
                for element in l {
                    self.print_newline()?;
                    self.print_diff(element)?;
                }
                self.indentation -= 1;
                self.print_newline()?;
                write!(self.w, "]")
            }
            Diff::Tuple(l) => {
                write!(self.w, "(")?;
                self.indentation += 1;
                for element in l {
                    self.print_newline()?;
                    self.print_diff(element)?;
                }
                self.indentation -= 1;
                self.print_newline()?;
                write!(self.w, ")")
            }
            Diff::Record(r) => {
                write!(self.w, "{{")?;
                self.indentation += 1;
                for (name, element) in r {
                    self.print_newline()?;
                    write!(self.w, "{}: ", name.italic())?;
                    self.print_diff(element)?;
                }
                self.indentation -= 1;
                self.print_newline()?;
                write!(self.w, "}}")
            }
            Diff::Dict(d) => {
                write!(self.w, "{{")?;
                self.indentation += 1;
                for (key, value) in d {
                    self.print_newline()?;
                    self.print_value(key)?;
                    write!(self.w, ": ")?;
                    self.print_diff(value)?;
                }
                self.indentation -= 1;
                self.print_newline()?;
                write!(self.w, "}}")
            }
        }
    }

    fn print_value(&mut self, value: &Value) -> io::Result<()> {
        match value {
            Value::Primitive(p) => self.print_primitive(p),
            Value::Collection(p) => self.print_collection(p, |s, v| s.print_value(v)),
        }
    }

    fn print_primitive(&mut self, primitive: &Primitive) -> io::Result<()> {
        let output = format!("{:?}", primitive);
        match self.value_color.or_else(|| match primitive {
            Primitive::String(_) => Some(Color::BrightBlue),
            Primitive::Integer(_) => Some(Color::BrightCyan),
            _ => None,
        }) {
            None => write!(self.w, "{}", output),
            Some(c) => write!(self.w, "{}", output.color(c)),
        }
    }

    fn print_collection<T, F>(&mut self, collection: &Collection<T>, mut f: F) -> io::Result<()>
    where
        F: FnMut(&mut Self, &T) -> io::Result<()>,
    {
        match collection {
            Collection::Tuple(l) => {
                write!(self.w, "(")?;
                self.indentation += 1;
                for element in l {
                    self.print_newline()?;
                    f(self, element)?;
                }
                self.indentation -= 1;
                self.print_newline()?;
                write!(self.w, ")")
            }
            Collection::List(l) => {
                write!(self.w, "[")?;
                self.indentation += 1;
                for element in l {
                    self.print_newline()?;
                    f(self, element)?;
                }
                self.indentation -= 1;
                self.print_newline()?;
                write!(self.w, "]")
            }
            Collection::Record(r) => {
                write!(self.w, "{{")?;
                self.indentation += 1;
                for (name, element) in r {
                    self.print_newline()?;
                    write!(self.w, "{}: ", name.italic())?;
                    f(self, element)?;
                }
                self.indentation -= 1;
                self.print_newline()?;
                write!(self.w, "}}")
            }
            Collection::Dict(d) => {
                write!(self.w, "{{")?;
                self.indentation += 1;
                for (key, value) in d {
                    self.print_newline()?;
                    f(self, key)?;
                    write!(self.w, ": ")?;
                    f(self, value)?;
                }
                self.indentation -= 1;
                self.print_newline()?;
                write!(self.w, "}}")
            }
        }
    }

    pub fn print_resource_state(&mut self, state: &ResourceState) -> io::Result<()> {
        write!(self.w, "{} ", format!("{:?}", state.id.type_).bold())?;

        if !state.dependencies.is_empty() {
            self.indentation += 1;

            self.print_newline()?;
            write!(self.w, "{}", "depends on".blue().bold())?;
            self.indentation += 1;
            for dep in state.dependencies.iter() {
                self.print_newline()?;
                write!(
                    self.w,
                    "{} {}",
                    format!("{:?}", dep.type_).bold(),
                    format!("{:?}", dep.id).bright_blue()
                )?;
            }
            self.indentation -= 1;
            self.print_newline()?;
        }

        self.print_value(&state.state)?;

        if !state.dependencies.is_empty() {
            self.indentation -= 1;
        }
        self.print_newline()
    }

    pub fn confirm(&mut self) -> io::Result<bool> {
        write!(self.w, "{} ", "\n  Continue?".bold())?;
        self.w.flush()?;
        let mut line = String::new();
        self.r.read_line(&mut line)?;
        self.print_newline()?;
        Ok(line == "yes\n")
    }

    pub fn print_plan_execution_event(&mut self, event: PlanExecutionEvent) -> io::Result<()> {
        match event {
            PlanExecutionEvent::Done(d) => writeln!(
                self.w,
                "{}\n{} in {}\n{}",
                "———————————————————————————————————————".bright_black(),
                "Done".bold(),
                format!("{:?}", d).bold().bright_yellow(),
                "═══════════════════════════════════════".bright_black()
            ),

            PlanExecutionEvent::Created(id, d) => writeln!(
                self.w,
                "{} {} {} in {}",
                "Created".bold().bright_green(),
                format!("{:?}", id.type_).bold(),
                format!("{:?}", id.id).bright_blue(),
                format!("{:?}", d).bold().bright_yellow(),
            ),

            PlanExecutionEvent::Updated(id, d) => writeln!(
                self.w,
                "{} {} {} in {}",
                "Updated".bold().bright_yellow(),
                format!("{:?}", id.type_).bold(),
                format!("{:?}", id.id).bright_blue(),
                format!("{:?}", d).bold().bright_yellow(),
            ),

            PlanExecutionEvent::Deleted(id, d) => writeln!(
                self.w,
                "{} {} {} in {}",
                "Deleted".bold().bright_red(),
                format!("{:?}", id.type_).bold(),
                format!("{:?}", id.id).bright_blue(),
                format!("{:?}", d).bold().bright_yellow(),
            ),

            PlanExecutionEvent::Creating(id, d) => writeln!(
                self.w,
                "  {} {} {} {} after {}",
                "-".bright_black(),
                "Still creating".bright_green(),
                format!("{:?}", id.type_).bold(),
                format!("{:?}", id.id).bright_blue(),
                format!("{:?}", d).bold().bright_yellow(),
            ),

            PlanExecutionEvent::Updating(id, d) => writeln!(
                self.w,
                "  {} {} {} {} after {}",
                "-".bright_black(),
                "Still updating".bright_yellow(),
                format!("{:?}", id.type_).bold(),
                format!("{:?}", id.id).bright_blue(),
                format!("{:?}", d).bold().bright_yellow(),
            ),

            PlanExecutionEvent::Deleting(id, d) => writeln!(
                self.w,
                "  {} {} {} {} after {}",
                "-".bright_black(),
                "Still deleting".bright_red(),
                format!("{:?}", id.type_).bold(),
                format!("{:?}", id.id).bright_blue(),
                format!("{:?}", d).bold().bright_yellow(),
            ),
        }
    }

    pub fn print_resource_error(
        &mut self,
        ResourceError(resource_id, error): ResourceError,
    ) -> io::Result<()> {
        write!(self.w, "{} ", format!("{:?}", resource_id.type_).bold(),)?;
        self.print_io_error(error)
    }

    pub fn print_resource_errors(&mut self, errors: Vec<ResourceError>) -> io::Result<()> {
        for error in errors {
            self.print_resource_error(error)?;
        }
        Ok(())
    }
}
