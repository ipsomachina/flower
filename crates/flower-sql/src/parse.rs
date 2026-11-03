use super::invalid;
use alloc::{boxed::Box, collections::BTreeSet, string::String, vec, vec::Vec};
use flower_sdk::{Result, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Part {
    Field(String),
    Index(usize),
}
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Expr {
    Path(Vec<Part>),
    Param(String),
    Literal(Value),
}
impl Expr {
    pub fn reads_value(&self) -> bool {
        matches!(self, Self::Path(p) if p.first() != Some(&Part::Field("_key".into())))
    }
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Compare {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Predicate {
    Compare(Expr, Compare, Expr),
    IsNull(Expr, bool),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
}
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Ast {
    pub collection: String,
    pub projection: Vec<(String, Expr)>,
    pub star: bool,
    pub predicate: Option<Predicate>,
    pub order: Vec<(Expr, bool)>,
    pub limit: Expr,
}
#[derive(Clone, Debug, PartialEq)]
enum Token {
    Word(String),
    Text(String),
    Number(String),
    Symbol(char),
    Op(Compare),
}

type Lexed = (Vec<Token>, Vec<(usize, usize)>);
fn lex(sql: &str) -> Result<Lexed> {
    if sql.len() > flower_sdk::sql::MAX_SQL_BYTES {
        return Err(invalid("SQL statement exceeds 16KiB"));
    }
    let mut chars = sql.char_indices().peekable();
    let mut tokens = Vec::new();
    let mut spans = Vec::new();
    while let Some((start, c)) = chars.next() {
        if c.is_whitespace() {
            continue;
        }
        let token = match c {
            '\'' => {
                let mut text = String::new();
                loop {
                    match chars.next() {
                        Some((_, '\'')) if chars.peek().is_some_and(|(_, c)| *c == '\'') => {
                            chars.next();
                            text.push('\'');
                        }
                        Some((_, '\'')) => break,
                        Some((_, c)) => text.push(c),
                        None => {
                            return Err(at_span(
                                invalid("unterminated SQL string"),
                                start,
                                sql.len(),
                            ));
                        }
                    }
                }
                Token::Text(text)
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let mut word = String::from(c);
                while chars
                    .peek()
                    .is_some_and(|(_, c)| c.is_ascii_alphanumeric() || *c == '_')
                {
                    word.push(chars.next().unwrap().1);
                }
                Token::Word(word)
            }
            c if c.is_ascii_digit() || c == '-' => {
                let mut number = String::from(c);
                while chars.peek().is_some_and(|(_, c)| {
                    c.is_ascii_digit() || matches!(c, '.' | 'e' | 'E' | '+' | '-')
                }) {
                    number.push(chars.next().unwrap().1);
                }
                Token::Number(number)
            }
            '=' => Token::Op(Compare::Eq),
            '<' => Token::Op(match chars.peek() {
                Some((_, '=')) => {
                    chars.next();
                    Compare::Le
                }
                Some((_, '>')) => {
                    chars.next();
                    Compare::Ne
                }
                _ => Compare::Lt,
            }),
            '>' => Token::Op(if chars.peek().is_some_and(|(_, c)| *c == '=') {
                chars.next();
                Compare::Ge
            } else {
                Compare::Gt
            }),
            '!' if chars.next().is_some_and(|(_, c)| c == '=') => Token::Op(Compare::Ne),
            ',' | '.' | ':' | '[' | ']' | '(' | ')' | '*' | ';' => Token::Symbol(c),
            _ => {
                return Err(at_span(
                    invalid("unsupported SQL token"),
                    start,
                    start + c.len_utf8(),
                ));
            }
        };
        spans.push((start, chars.peek().map_or(sql.len(), |(offset, _)| *offset)));
        tokens.push(token);
        if tokens.len() > 2048 {
            return Err(invalid("SQL has more than 2048 tokens"));
        }
    }
    Ok((tokens, spans))
}

fn at_span(mut error: flower_sdk::Failure, start: usize, end: usize) -> flower_sdk::Failure {
    error.details = Some(Value::Object(alloc::collections::BTreeMap::from([
        (String::from("start_byte"), Value::Number(start as f64)),
        (String::from("end_byte"), Value::Number(end as f64)),
    ])));
    error
}

struct Parser {
    tokens: Vec<Token>,
    at: usize,
    nodes: usize,
}
impl Parser {
    fn word(&mut self, word: &str) -> bool {
        if matches!(self.tokens.get(self.at), Some(Token::Word(w)) if w.eq_ignore_ascii_case(word))
        {
            self.at += 1;
            true
        } else {
            false
        }
    }
    fn need(&mut self, word: &str) -> Result<()> {
        if self.word(word) {
            Ok(())
        } else {
            Err(invalid(alloc::format!("expected `{word}`")))
        }
    }
    fn symbol(&mut self, c: char) -> bool {
        if self.tokens.get(self.at) == Some(&Token::Symbol(c)) {
            self.at += 1;
            true
        } else {
            false
        }
    }
    fn ident(&mut self) -> Result<String> {
        match self.tokens.get(self.at).cloned() {
            Some(Token::Word(w)) => {
                self.at += 1;
                Ok(w)
            }
            _ => Err(invalid("expected an identifier")),
        }
    }
    fn expr(&mut self) -> Result<Expr> {
        if self.symbol(':') {
            return Ok(Expr::Param(self.ident()?));
        }
        if self.word("NULL") {
            return Ok(Expr::Literal(Value::Null));
        }
        if self.word("TRUE") {
            return Ok(Expr::Literal(Value::Bool(true)));
        }
        if self.word("FALSE") {
            return Ok(Expr::Literal(Value::Bool(false)));
        }
        match self.tokens.get(self.at).cloned() {
            Some(Token::Text(text)) => {
                self.at += 1;
                return Ok(Expr::Literal(Value::String(text)));
            }
            Some(Token::Number(text)) => {
                self.at += 1;
                let n: f64 = text.parse().map_err(|_| invalid("invalid number"))?;
                if !n.is_finite() {
                    return Err(invalid("SQL numbers must be finite"));
                }
                return Ok(Expr::Literal(Value::Number(n)));
            }
            _ => {}
        }
        let mut path = vec![Part::Field(self.ident()?)];
        loop {
            if self.symbol('.') {
                path.push(Part::Field(self.ident()?));
            } else if self.symbol('[') {
                let Some(Token::Number(n)) = self.tokens.get(self.at) else {
                    return Err(invalid("tuple index requires an integer"));
                };
                let index = n.parse().map_err(|_| invalid("invalid tuple index"))?;
                self.at += 1;
                if !self.symbol(']') {
                    return Err(invalid("expected ]"));
                }
                path.push(Part::Index(index));
            } else {
                break;
            }
            if path.len() > 32 {
                return Err(invalid("SQL path exceeds depth 32"));
            }
        }
        Ok(Expr::Path(path))
    }
    fn predicate(&mut self, depth: usize, precedence: u8) -> Result<Predicate> {
        self.nodes += 1;
        if depth > 32 || self.nodes > 128 {
            return Err(invalid(
                "SQL predicate complexity exceeds 128 nodes/depth 32",
            ));
        }
        let mut left = if self.word("NOT") {
            Predicate::Not(Box::new(self.predicate(depth + 1, 3)?))
        } else if self.symbol('(') {
            let p = self.predicate(depth + 1, 0)?;
            if !self.symbol(')') {
                return Err(invalid("expected )"));
            }
            p
        } else {
            let left = self.expr()?;
            if self.word("IS") {
                let not = self.word("NOT");
                self.need("NULL")?;
                Predicate::IsNull(left, not)
            } else {
                let Some(Token::Op(op)) = self.tokens.get(self.at).cloned() else {
                    return Err(invalid("expected comparison or IS NULL"));
                };
                self.at += 1;
                Predicate::Compare(left, op, self.expr()?)
            }
        };
        loop {
            if precedence < 2 && self.word("AND") {
                left = Predicate::And(Box::new(left), Box::new(self.predicate(depth + 1, 2)?));
            } else if precedence < 1 && self.word("OR") {
                left = Predicate::Or(Box::new(left), Box::new(self.predicate(depth + 1, 1)?));
            } else {
                break;
            }
            self.nodes += 1;
            if self.nodes > 128 {
                return Err(invalid("SQL predicate exceeds 128 nodes"));
            }
        }
        Ok(left)
    }
}

pub(crate) fn parse(sql: &str) -> Result<Ast> {
    let (tokens, spans) = lex(sql)?;
    let mut p = Parser {
        tokens,
        at: 0,
        nodes: 0,
    };
    let result = (|| {
        p.need("SELECT")?;
        let star = p.symbol('*');
        let mut projection = Vec::new();
        let mut names = BTreeSet::new();
        if !star {
            loop {
                let expr = p.expr()?;
                let name = if p.word("AS") {
                    p.ident()?
                } else {
                    match &expr {
                        Expr::Path(parts) => match parts.last() {
                            Some(Part::Field(name)) => name.clone(),
                            _ => return Err(invalid("tuple projections require AS")),
                        },
                        _ => return Err(invalid("literal and parameter projections require AS")),
                    }
                };
                if !names.insert(name.clone()) {
                    return Err(invalid("duplicate projected column"));
                }
                projection.push((name, expr));
                if projection.len() > 64 {
                    return Err(invalid("at most 64 projected columns"));
                }
                if !p.symbol(',') {
                    break;
                }
            }
        }
        p.need("FROM")?;
        let collection = p.ident()?;
        let predicate = if p.word("WHERE") {
            Some(p.predicate(0, 0)?)
        } else {
            None
        };
        let mut order = Vec::new();
        if p.word("ORDER") {
            p.need("BY")?;
            loop {
                let expr = p.expr()?;
                let desc = p.word("DESC");
                if !desc {
                    p.word("ASC");
                }
                order.push((expr, desc));
                if order.len() > 8 {
                    return Err(invalid("at most 8 sort expressions"));
                }
                if !p.symbol(',') {
                    break;
                }
            }
        }
        p.need("LIMIT")?;
        let limit = p.expr()?;
        p.symbol(';');
        if p.at != p.tokens.len() {
            return Err(invalid("unsupported SQL clause or additional statement"));
        }
        Ok(Ast {
            collection,
            projection,
            star,
            predicate,
            order,
            limit,
        })
    })();
    result.map_err(|error| {
        let (start, end) = spans.get(p.at).copied().unwrap_or((sql.len(), sql.len()));
        at_span(error, start, end)
    })
}
