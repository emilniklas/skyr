use std::fmt;

#[derive(Clone, Copy)]
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

impl Location {
    pub fn increment_character(&mut self) {
        self.character += 1;
    }

    pub fn increment_line(&mut self) {
        self.character = 1;
        self.line += 1;
    }
}

pub type Span = std::ops::Range<Location>;

pub trait HasSpan {
    fn span(&self) -> Span;
}
