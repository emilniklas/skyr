use std::collections::BTreeMap;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::sync::Arc;
use std::time::Duration;

use async_std::sync::Mutex;
use colored::{Color, ColoredString, Colorize};
use skyr::analyze::{CompositeType, PrimitiveType, Type, TypeError, TypeId};
use skyr::compile::{CompileError, Location, ParseError, Span, Token, TokenKind};
use skyr::execute::RuntimeValue;
use skyr::{
    Collection, Diff, Plan, PlanExecutionEvent, Primitive, ResourceError, ResourceState, Source,
    Value,
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

    pub async fn print_compile_error(
        &self,
        error: CompileError,
        sources: &Vec<Source>,
    ) -> io::Result<()> {
        self.s.lock().await.print_compile_error(error, sources)
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
        writeln!(self.w, "{}", error.to_string().red())
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

    fn indent(&mut self) {
        self.indentation += 2;
    }

    fn unindent(&mut self) {
        self.indentation -= 2;
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
            self.indent();
            self.print_value(args)?;
            self.unindent();
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
            self.indent();
            self.print_diff(diff)?;
            self.unindent();
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
        write!(self.w, "\n{:width$}", "", width = self.indentation)
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
                self.indent();
                for element in l {
                    self.print_newline()?;
                    self.print_diff(element)?;
                }
                self.unindent();
                self.print_newline()?;
                write!(self.w, "]")
            }
            Diff::Tuple(l) => {
                write!(self.w, "(")?;
                self.indent();
                for element in l {
                    self.print_newline()?;
                    self.print_diff(element)?;
                }
                self.unindent();
                self.print_newline()?;
                write!(self.w, ")")
            }
            Diff::Record(r) => {
                write!(self.w, "{{")?;
                self.indent();
                for (name, element) in r {
                    self.print_newline()?;
                    write!(self.w, "{}: ", name.italic())?;
                    self.print_diff(element)?;
                }
                self.unindent();
                self.print_newline()?;
                write!(self.w, "}}")
            }
            Diff::Dict(d) => {
                write!(self.w, "{{")?;
                self.indent();
                for (key, value) in d {
                    self.print_newline()?;
                    self.print_value(key)?;
                    write!(self.w, ": ")?;
                    self.print_diff(value)?;
                }
                self.unindent();
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
                self.indent();
                for element in l {
                    self.print_newline()?;
                    f(self, element)?;
                }
                self.unindent();
                self.print_newline()?;
                write!(self.w, ")")
            }
            Collection::List(l) => {
                write!(self.w, "[")?;
                self.indent();
                for element in l {
                    self.print_newline()?;
                    f(self, element)?;
                }
                self.unindent();
                self.print_newline()?;
                write!(self.w, "]")
            }
            Collection::Record(r) => {
                write!(self.w, "{{")?;
                self.indent();
                for (name, element) in r {
                    self.print_newline()?;
                    write!(self.w, "{}: ", name.italic())?;
                    f(self, element)?;
                }
                self.unindent();
                self.print_newline()?;
                write!(self.w, "}}")
            }
            Collection::Dict(d) => {
                write!(self.w, "{{")?;
                self.indent();
                for (key, value) in d {
                    self.print_newline()?;
                    f(self, key)?;
                    write!(self.w, ": ")?;
                    f(self, value)?;
                }
                self.unindent();
                self.print_newline()?;
                write!(self.w, "}}")
            }
        }
    }

    pub fn print_resource_state(&mut self, state: &ResourceState) -> io::Result<()> {
        write!(self.w, "{} ", format!("{:?}", state.id.type_).bold())?;

        if !state.dependencies.is_empty() {
            self.indent();

            self.print_newline()?;
            write!(self.w, "{}", "depends on".blue().bold())?;
            self.indent();
            for dep in state.dependencies.iter() {
                self.print_newline()?;
                write!(
                    self.w,
                    "{} {}",
                    format!("{:?}", dep.type_).bold(),
                    format!("{:?}", dep.id).bright_blue()
                )?;
            }
            self.unindent();
            self.print_newline()?;
        }

        self.print_value(&state.state)?;

        if !state.dependencies.is_empty() {
            self.unindent();
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

    const TYPE_VAR_CHARS: [char; 26] = [
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
        's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    ];

    pub fn print_type(&mut self, type_: &Type, open_types: &mut Vec<TypeId>) -> io::Result<()> {
        match type_ {
            Type::Primitive(PrimitiveType::Void) => {
                write!(self.w, "{}", "Void".bright_black().bold())
            }
            Type::Primitive(PrimitiveType::String) => write!(self.w, "{}", "String".bold()),
            Type::Primitive(PrimitiveType::Integer) => write!(self.w, "{}", "Integer".bold()),
            Type::Primitive(PrimitiveType::Float) => write!(self.w, "{}", "Float".bold()),
            Type::Primitive(PrimitiveType::Boolean) => write!(self.w, "{}", "Boolean".bold()),
            Type::Open(id) => {
                let idx = open_types
                    .iter()
                    .enumerate()
                    .find(|(_, oid)| *oid == id)
                    .map(|(idx, _)| idx)
                    .unwrap_or_else(|| {
                        let idx = open_types.len();
                        open_types.push(*id);
                        idx
                    });
                let idx = idx % Self::TYPE_VAR_CHARS.len();
                let ch = Self::TYPE_VAR_CHARS[idx];
                write!(self.w, "{}", format!("{}", ch).bold().italic())
            }
            Type::Named(n, _) => write!(self.w, "{}", n.bold()),
            Type::Composite(CompositeType::Function(p, r)) => {
                let is_simple_single_arg =
                    p.len() == 1 && matches!(p[0], Type::Primitive(_) | Type::Open(_));

                if !is_simple_single_arg {
                    write!(self.w, "{}", "(".bright_black())?;
                }
                for param in p {
                    self.print_type(param, open_types)?;
                }
                if !is_simple_single_arg {
                    write!(self.w, "{}", ")".bright_black())?;
                }

                write!(self.w, "{}", " -> ".bright_black())?;

                self.print_type(r, open_types)
            }
            Type::Composite(CompositeType::Tuple(t)) => {
                write!(self.w, "{}", "(".bright_black())?;
                for el in t {
                    self.print_type(el, open_types)?;
                }
                write!(self.w, "{}", ")".bright_black())
            }
            Type::Composite(CompositeType::Optional(t)) => {
                self.print_type(t, open_types)?;
                write!(self.w, "{}", "?".bold())
            }
            Type::Composite(CompositeType::Record(r)) => self
                .print_collection(&Collection::record(r.clone()), |s, t| {
                    s.print_type(t, open_types)
                }),
            Type::Composite(CompositeType::List(element)) => {
                write!(self.w, "{}", "[".bright_black())?;
                self.print_type(element, open_types)?;
                write!(self.w, "{}", "]".bright_black())
            }
            Type::Composite(CompositeType::Dict(key, value)) => {
                write!(self.w, "{}", "{".bright_black())?;
                self.print_type(key, open_types)?;
                write!(self.w, "{}", ": ".bright_black())?;
                self.print_type(value, open_types)?;
                write!(self.w, "{}", "}".bright_black())
            }
        }
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

    pub fn print_compile_error(
        &mut self,
        error: CompileError,
        sources: &Vec<Source>,
    ) -> io::Result<()> {
        let sources = sources.iter().map(|s| (s.name(), s)).collect();

        self.do_print_compile_error(error, &sources)
    }

    fn do_print_compile_error(
        &mut self,
        error: CompileError,
        sources: &BTreeMap<&str, &Source>,
    ) -> io::Result<()> {
        match error {
            CompileError::Cons(a, b) => {
                self.do_print_compile_error(*a, sources)?;
                self.do_print_compile_error(*b, sources)
            }

            CompileError::ParseError(ParseError::Expected(expectation, span)) => self
                .do_print_string_error(
                    expectation,
                    span.as_ref().and_then(|span| {
                        sources
                            .get(span.source_name.as_str())
                            .copied()
                            .map(|s| (span, s))
                    }),
                ),

            CompileError::UndefinedReference(reference_name, span) => {
                let source = sources
                    .get(span.source_name.as_str())
                    .copied()
                    .map(|s| (&span, s));
                self.do_print_string_error(&format!("`{}` is not defined", reference_name), source)
            }

            CompileError::TypeError(TypeError::Mismatch {
                expected: lhs,
                assigned: rhs,
                span,
            }) => {
                let source = sources
                    .get(span.source_name.as_str())
                    .copied()
                    .map(|s| (&span, s));
                self.do_print_error(
                    |s| {
                        write!(s.w, "{}", "Type mismatch:".red())?;

                        let mut open_types = vec![];

                        s.indent();
                        s.print_newline()?;
                        s.print_type(&rhs, &mut open_types)?;
                        s.unindent();
                        s.print_newline()?;

                        write!(s.w, "{}", "is not assignable to".red())?;

                        s.indent();
                        s.print_newline()?;
                        s.print_type(&lhs, &mut open_types)?;
                        s.unindent();

                        Ok(())
                    },
                    source,
                )
            } /*
              CompileError::TypeError(TypeError::MissingField {
                  lhs: _,
                  rhs,
                  field,
                  span,
              }) => {
                  let source = sources
                      .get(span.source_name.as_str())
                      .copied()
                      .map(|s| (&span, s));
                  self.do_print_error(
                      |s| {
                          write!(
                              s.w,
                              "{} {} {} ",
                              "Field".red(),
                              field.italic(),
                              "is missing in".red()
                          )?;
                          s.indent();
                          s.print_type(&rhs, &mut vec![])?;
                          s.unindent();

                          Ok(())
                      },
                      source,
                  )
              }

              CompileError::TypeError(TypeError::UnresolvedImport { name, span }) => {
                  let source = sources
                      .get(span.source_name.as_str())
                      .copied()
                      .map(|s| (&span, s));
                  self.do_print_string_error(&format!("No module named `{}` is loaded", name), source)
              }

              CompileError::TypeError(TypeError::WrongNumberOfArguments { lhs, rhs, span }) => {
                  let source = sources
                      .get(span.source_name.as_str())
                      .copied()
                      .map(|s| (&span, s));
                  self.do_print_string_error(
                      &format!(
                          "Function has {} parameter(s) but was given {} arguments",
                          lhs, rhs
                      ),
                      source,
                  )
              }
              */
        }
    }

    fn do_print_string_error(
        &mut self,
        error: &str,
        source: Option<(&Span, &Source)>,
    ) -> io::Result<()> {
        self.do_print_error(|f| write!(f.w, "{}", error.red()), source)
    }

    fn do_print_error(
        &mut self,
        error: impl FnOnce(&mut Self) -> io::Result<()>,
        source: Option<(&Span, &Source)>,
    ) -> io::Result<()> {
        match source {
            None => error(self),

            Some((span, source)) => {
                let start_line = span.range.start.line.checked_sub(3).unwrap_or(0);
                let end_line = span.range.end.line + 3;

                let mut gutter_width = 2;
                let mut rest = end_line;
                while rest > 0 {
                    gutter_width += 1;
                    rest /= 10;
                }

                let tokens = source
                    .lex()
                    .filter(|t| t.span.range.end.line >= start_line)
                    .filter(|t| t.span.range.start.line <= end_line);

                let mut error = Some(error);

                let mut location = Location {
                    line: start_line,
                    ..Default::default()
                };

                writeln!(
                    self.w,
                    "{} ",
                    format!("{}", span.source_name).bright_black()
                )?;
                write!(
                    self.w,
                    "{:>gutter_width$}",
                    format!("{}  ", location.line,).bright_black(),
                    gutter_width = gutter_width
                )?;
                for token in tokens {
                    if token.span.range.start.line > span.range.end.line {
                        if let Some(error) = error.take() {
                            let before = self.indentation;
                            self.indentation +=
                                gutter_width + span.range.start.character as usize - 1;
                            self.print_newline()?;
                            error(self)?;
                            self.indentation = before;
                        }
                    }

                    while token.span.range.start.line > location.line {
                        location.increment_line();
                        self.print_newline()?;
                        write!(
                            self.w,
                            "{}",
                            format!(
                                "{:>gutter_width$}",
                                format!("{}  ", location.line),
                                gutter_width = gutter_width
                            )
                            .bright_black()
                        )?;
                    }

                    while token.span.range.start.character > location.character {
                        location.increment_character();
                        write!(self.w, " ")?;
                    }

                    self.print_token(&token, &mut location, |c| {
                        if span.includes(&token.span) {
                            c.underline().bright_red()
                        } else {
                            c
                        }
                    })?;
                }

                if let Some(error) = error.take() {
                    let before = self.indentation;
                    self.indentation += gutter_width + span.range.start.character as usize - 1;
                    self.print_newline()?;
                    error(self)?;
                    self.indentation = before;
                }

                self.print_newline()?;

                Ok(())
            }
        }
    }

    fn print_token(
        &mut self,
        token: &Token,
        location: &mut Location,
        f: impl FnOnce(ColoredString) -> ColoredString,
    ) -> io::Result<()> {
        let lexeme = token.kind.lexeme();
        location.character += lexeme.len() as u64;
        use TokenKind::*;
        let colorized = match &token.kind {
            Plus
            | OpenCurly
            | CloseCurly
            | OpenParen
            | CloseParen
            | OpenAngle
            | CloseAngle
            | OpenSquare
            | CloseSquare
            | Arrow
            | FatArrow
            | Colon
            | Comma
            | Period
            | EqualSign
            | QuestionMark
            | LessThanOrEqualSign
            | GreaterThanOrEqualSign
            | DoubleEqualSign
            | NotEqualSign
            | Minus
            | Slash
            | Asterisk => lexeme.bright_black(),

            TypeKeyword | FnKeyword | ReturnKeyword | DebugKeyword | ImportKeyword
            | TrueKeyword | FalseKeyword | IfKeyword | ElseKeyword | AndKeyword | OrKeyword
            | NilKeyword => lexeme.blue().bold(),

            Symbol(s) => {
                if s.chars().next().map(|c| c.is_uppercase()).unwrap_or(false) {
                    lexeme.bold()
                } else {
                    lexeme.italic()
                }
            }
            StringLiteral(_, _) => lexeme.bright_green(),
            Integer(_) => lexeme.purple(),
            Unknown(_) => lexeme.red(),
        };
        write!(self.w, "{}", f(colorized))
    }
}
