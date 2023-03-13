use std::io::{self, Write};
use std::sync::Arc;

use async_std::sync::Mutex;
use colored::{Color, Colorize};
use skyr::{Collection, Diff, Plan, Primitive, ResourceState, Value};

pub struct Gui<W> {
    w: Arc<Mutex<GuiState<W>>>,
}

impl<W> Clone for Gui<W> {
    fn clone(&self) -> Self {
        Gui { w: self.w.clone() }
    }
}

impl<W: Write> Gui<W> {
    pub fn new(w: W) -> Self {
        Self {
            w: Arc::new(Mutex::new(GuiState {
                w,
                indentation: 0,
                value_color: None,
            })),
        }
    }

    pub async fn print_io_error(&self, error: io::Error) -> io::Result<()> {
        self.w.lock().await.print_io_error(error)
    }

    pub async fn print_plan<'a>(&self, plan: &Plan<'a>) -> io::Result<()> {
        self.w.lock().await.print_plan(plan)
    }

    pub async fn print_resource_state(&self, state: &ResourceState) -> io::Result<()> {
        self.w.lock().await.print_resource_state(state)
    }
}

struct GuiState<W> {
    w: W,
    indentation: usize,
    value_color: Option<Color>,
}

impl<W: Write> GuiState<W> {
    pub fn print_io_error(&mut self, error: io::Error) -> io::Result<()> {
        write!(self.w, "{:?}", error)
    }

    pub fn print_plan<'a>(&mut self, plan: &Plan<'a>) -> io::Result<()> {
        if plan.is_empty() {
            if plan.is_continuation() {
                writeln!(self.w, "{}", "Nothing more to do".bright_green().bold())?;
            } else {
                writeln!(self.w, "{}", "Nothing to do".bright_green().bold())?;
            }
            return Ok(());
        }

        if plan.is_continuation() {
            writeln!(self.w, "{}", "===============".bright_black())?;
        }

        writeln!(
            self.w,
            "{}",
            format!("PHASE {}", plan.phase_index() + 1)
                .bright_yellow()
                .bold()
        )?;
        writeln!(self.w, "{}", "---------------".bright_black())?;

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

        writeln!(
            self.w,
            "{}\n{}",
            "---------------".bright_black(),
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
        }
    }

    pub fn print_resource_state(&mut self, state: &ResourceState) -> io::Result<()> {
        write!(self.w, "{} ", format!("{:?}", state.id.type_).bold(),)?;
        self.print_value(&state.state)?;
        self.print_newline()
    }
}
