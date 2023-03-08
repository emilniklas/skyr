use std::borrow::Cow;

use super::{Location, Span};

#[derive(Debug)]
pub struct Token<'a> {
    pub span: Span,
    pub kind: TokenKind<'a>,
}

#[derive(Debug)]
pub enum TokenKind<'a> {
    OpenCurly,
    CloseCurly,
    OpenParen,
    CloseParen,

    Arrow,
    Colon,
    Comma,
    Period,
    EqualSign,

    TypeKeyword,
    FnKeyword,
    ReturnKeyword,
    DebugKeyword,
    ImportKeyword,

    Symbol(&'a str),
    StringLiteral(Cow<'a, str>, bool),
    Integer(i128),

    Unknown(char),
}

pub struct Lexer<'a> {
    code: &'a str,
    location: Location,
}

impl<'a> Lexer<'a> {
    pub fn new(code: &'a str) -> Self {
        Self {
            code,
            location: Default::default(),
        }
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.code.chars().next() {
            if !c.is_whitespace() {
                break;
            }
            if c == '\n' {
                self.location.increment_line();
            } else {
                self.location.increment_character();
            }
            self.code = &self.code[1..];
        }
    }

    fn next_token(&mut self) -> Token<'a> {
        let start = self.location;
        let kind = self.next_token_kind();
        let end = self.location;
        Token {
            span: start..end,
            kind,
        }
    }

    fn next_token_kind(&mut self) -> TokenKind<'a> {
        let mut chars = self.code.chars();
        let c = chars.next();
        match c {
            Some('-') if chars.next() == Some('>') => {
                self.location.increment_character();
                self.location.increment_character();
                self.code = &self.code[2..];
                return TokenKind::Arrow;
            }

            Some('"') => return self.string_literal(),

            Some(c) if c >= '0' && c <= '9' => return self.integer_literal(),

            Some(c) if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' => {
                return self.symbol_or_keyword()
            }

            _ => {}
        }

        self.location.increment_character();
        self.code = &self.code[1..];

        match c {
            Some('{') => TokenKind::OpenCurly,
            Some('}') => TokenKind::CloseCurly,

            Some('(') => TokenKind::OpenParen,
            Some(')') => TokenKind::CloseParen,

            Some(',') => TokenKind::Comma,
            Some(':') => TokenKind::Colon,
            Some('.') => TokenKind::Period,
            Some('=') => TokenKind::EqualSign,

            Some(c) => TokenKind::Unknown(c),

            None => TokenKind::Unknown('\0'),
        }
    }

    fn integer_literal(&mut self) -> TokenKind<'a> {
        let mut len = 0;
        let mut chars = self.code.chars();
        while let Some(c) = chars.next() {
            if c >= '0' && c <= '9' {
                self.location.increment_character();
                len += 1;
            } else {
                break;
            }
        }
        let integer = &self.code[..len];
        self.code = &self.code[len..];

        TokenKind::Integer(integer.parse().unwrap_or(i128::MAX))
    }

    fn symbol_or_keyword(&mut self) -> TokenKind<'a> {
        let mut len = 0;
        let mut chars = self.code.chars();
        while let Some(c) = chars.next() {
            if (c >= 'a' && c <= 'z')
                || (c >= 'A' && c <= 'Z')
                || (c >= '0' && c <= '9')
                || c == '_'
            {
                self.location.increment_character();
                len += 1;
            } else {
                break;
            }
        }
        let symbol = &self.code[..len];
        self.code = &self.code[len..];

        match symbol {
            "type" => TokenKind::TypeKeyword,
            "fn" => TokenKind::FnKeyword,
            "return" => TokenKind::ReturnKeyword,
            "debug" => TokenKind::DebugKeyword,
            "import" => TokenKind::ImportKeyword,
            _ => TokenKind::Symbol(symbol),
        }
    }

    fn string_literal(&mut self) -> TokenKind<'a> {
        // Skip quote
        self.code = &self.code[1..];

        let mut chars = self.code.chars();
        let mut len = 0;

        let mut owned = String::new();

        loop {
            match chars.next() {
                None | Some('\n') => {
                    let kind = TokenKind::StringLiteral(Cow::Borrowed(&self.code[..len]), false);
                    self.code = &self.code[len..];
                    return kind;
                }

                Some('"') => {
                    let kind = TokenKind::StringLiteral(
                        if owned.is_empty() {
                            Cow::Borrowed(&self.code[..len])
                        } else {
                            Cow::Owned(owned)
                        },
                        true,
                    );
                    self.location.increment_character();
                    self.code = &self.code[len + 1..];
                    return kind;
                }

                Some('\\') => {
                    if owned.is_empty() {
                        owned.push_str(&self.code[..len]);
                    }
                    len += 1;
                    self.location.increment_character();
                    match chars.next() {
                        None => {
                            // Continue, which will end the string
                        }

                        Some('\\') => {
                            len += 1;
                            self.location.increment_character();
                            owned.push('\\');
                        }

                        Some('n') => {
                            len += 1;
                            self.location.increment_character();
                            owned.push('\n');
                        }

                        Some('r') => {
                            len += 1;
                            self.location.increment_character();
                            owned.push('\r');
                        }

                        Some('t') => {
                            len += 1;
                            self.location.increment_character();
                            owned.push('\t');
                        }

                        Some(c) => {
                            len += 1;
                            self.location.increment_character();
                            owned.push(c);
                        }
                    }
                }

                Some(c) => {
                    len += 1;
                    self.location.increment_character();
                    if !owned.is_empty() {
                        owned.push(c);
                    }
                }
            }
        }
    }
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Token<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.skip_whitespace();

        if self.code.len() == 0 {
            None
        } else {
            Some(self.next_token())
        }
    }
}

#[derive(Debug)]
pub enum ParseError {
    Expected(&'static str, Option<Span>),
}

pub type ParseResult<'a, T> = Result<(T, &'a [Token<'a>]), ParseError>;

pub trait Parser {
    type Output;

    fn parse<'a>(tokens: &'a [Token<'a>]) -> ParseResult<'a, Self::Output>;
}
