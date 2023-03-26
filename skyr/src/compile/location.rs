use std::fmt;
use std::sync::Arc;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Location {
    pub line: u64,
    pub character: u64,
}

impl fmt::Debug for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.line, self.character)
    }
}

impl Default for Location {
    fn default() -> Self {
        Self {
            line: 1,
            character: 1,
        }
    }
}

impl From<(u64, u64)> for Location {
    fn from((line, character): (u64, u64)) -> Self {
        Self { line, character }
    }
}

impl Location {
    pub fn increment_character(&mut self) {
        self.character += 1;
    }

    pub fn increment_line(&mut self) {
        self.character = 1;
        self.line += 1;
    }
}

#[derive(Clone)]
pub struct Span {
    pub source_name: Arc<String>,
    pub range: std::ops::Range<Location>,
}

impl PartialEq for Span {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.source_name, &other.source_name) && self.range == other.range
    }
}

impl Span {
    pub fn through(&self, other: &Self) -> Self {
        if !Arc::ptr_eq(&self.source_name, &other.source_name) {
            panic!("cannot make a span across modules");
        }

        Self {
            source_name: self.source_name.clone(),
            range: self.range.start.min(other.range.start)..self.range.end.max(other.range.end),
        }
    }

    pub fn includes(&self, other: &Self) -> bool {
        self.range.start <= other.range.start && self.range.end >= other.range.end
    }
}

impl fmt::Debug for Span {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{:?}->{:?}", self.source_name, self.range.start, self.range.end)
    }
}

pub trait HasSpan {
    fn span(&self) -> Span;
}
