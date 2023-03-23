use std::borrow::Cow;
use std::sync::Arc;

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
    OpenAngle,
    CloseAngle,
    OpenSquare,
    CloseSquare,

    Arrow,
    FatArrow,
    Colon,
    Comma,
    Period,
    EqualSign,
    QuestionMark,

    LessThanOrEqualSign,
    GreaterThanOrEqualSign,
    DoubleEqualSign,
    NotEqualSign,

    Plus,
    Minus,
    Slash,
    Asterisk,

    TypeKeyword,
    FnKeyword,
    ReturnKeyword,
    DebugKeyword,
    ImportKeyword,
    TrueKeyword,
    FalseKeyword,
    IfKeyword,
    ElseKeyword,
    AndKeyword,
    OrKeyword,
    NilKeyword,

    Symbol(&'a str),
    StringLiteral(Cow<'a, str>, bool),
    Integer(i64),

    Unknown(char),
}

impl<'a> TokenKind<'a> {
    pub fn lexeme(&self) -> Cow<str> {
        match self {
            TokenKind::Plus => "+",
            TokenKind::OpenCurly => "{",
            TokenKind::CloseCurly => "}",
            TokenKind::OpenParen => "(",
            TokenKind::CloseParen => ")",
            TokenKind::OpenAngle => "<",
            TokenKind::CloseAngle => ">",
            TokenKind::OpenSquare => "[",
            TokenKind::CloseSquare => "]",
            TokenKind::Arrow => "->",
            TokenKind::FatArrow => "=>",
            TokenKind::Colon => ":",
            TokenKind::Comma => ",",
            TokenKind::Period => ".",
            TokenKind::EqualSign => "=",
            TokenKind::QuestionMark => "?",
            TokenKind::LessThanOrEqualSign => "<=",
            TokenKind::GreaterThanOrEqualSign => ">=",
            TokenKind::DoubleEqualSign => "==",
            TokenKind::NotEqualSign => "!=",
            TokenKind::Minus => "-",
            TokenKind::Slash => "/",
            TokenKind::Asterisk => "*",
            TokenKind::TypeKeyword => "type",
            TokenKind::FnKeyword => "fn",
            TokenKind::ReturnKeyword => "return",
            TokenKind::DebugKeyword => "debug",
            TokenKind::ImportKeyword => "import",
            TokenKind::TrueKeyword => "true",
            TokenKind::FalseKeyword => "false",
            TokenKind::IfKeyword => "if",
            TokenKind::ElseKeyword => "else",
            TokenKind::AndKeyword => "and",
            TokenKind::OrKeyword => "or",
            TokenKind::NilKeyword => "nil",
            TokenKind::Symbol(s) => s,
            TokenKind::StringLiteral(s, _) => return format!("{:?}", s).into(),
            TokenKind::Integer(i) => return format!("{}", i).into(),
            TokenKind::Unknown(c) => return format!("{}", c).into(),
        }
        .into()
    }
}

pub struct Lexer<'a> {
    source_name: &'a Arc<String>,
    code: &'a str,
    location: Location,
}

impl<'a> Lexer<'a> {
    pub fn new(source_name: &'a Arc<String>, code: &'a str) -> Self {
        Self {
            source_name,
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
            span: Span {
                source_name: self.source_name.clone(),
                range: start..end,
            },
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

            Some('=') if chars.next() == Some('>') => {
                self.location.increment_character();
                self.location.increment_character();
                self.code = &self.code[2..];
                return TokenKind::FatArrow;
            }

            Some('<') if chars.next() == Some('=') => {
                self.location.increment_character();
                self.location.increment_character();
                self.code = &self.code[2..];
                return TokenKind::LessThanOrEqualSign;
            }

            Some('>') if chars.next() == Some('=') => {
                self.location.increment_character();
                self.location.increment_character();
                self.code = &self.code[2..];
                return TokenKind::GreaterThanOrEqualSign;
            }

            Some('!') if chars.next() == Some('=') => {
                self.location.increment_character();
                self.location.increment_character();
                self.code = &self.code[2..];
                return TokenKind::NotEqualSign;
            }

            Some('=') if chars.next() == Some('=') => {
                self.location.increment_character();
                self.location.increment_character();
                self.code = &self.code[2..];
                return TokenKind::DoubleEqualSign;
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
            Some('<') => TokenKind::OpenAngle,
            Some('>') => TokenKind::CloseAngle,
            Some('[') => TokenKind::OpenSquare,
            Some(']') => TokenKind::CloseSquare,

            Some(',') => TokenKind::Comma,
            Some(':') => TokenKind::Colon,
            Some('.') => TokenKind::Period,
            Some('=') => TokenKind::EqualSign,
            Some('?') => TokenKind::QuestionMark,

            Some('+') => TokenKind::Plus,
            Some('-') => TokenKind::Minus,
            Some('/') => TokenKind::Slash,
            Some('*') => TokenKind::Asterisk,

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

        TokenKind::Integer(integer.parse().unwrap_or(i64::MAX))
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
            "true" => TokenKind::TrueKeyword,
            "false" => TokenKind::FalseKeyword,
            "if" => TokenKind::IfKeyword,
            "else" => TokenKind::ElseKeyword,
            "and" => TokenKind::AndKeyword,
            "or" => TokenKind::OrKeyword,
            "nil" => TokenKind::NilKeyword,
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
