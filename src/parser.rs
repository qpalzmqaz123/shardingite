use crate::Result;
use sqlparser::dialect::Dialect;

const RUSQLITE_DIALECT: RusqliteDialect = RusqliteDialect;

pub struct Parser {}

impl Parser {
    pub fn parse(sql: &str) -> Result<sqlparser::ast::Statement> {
        let mut asts = sqlparser::parser::Parser::parse_sql(&RUSQLITE_DIALECT, sql)?;
        if asts.len() == 0 {
            Err("Empty sql".into())
        } else if asts.len() == 1 {
            Ok(asts.pop().unwrap())
        } else {
            Err("Expect single sql, not list".into())
        }
    }
}

#[derive(Debug, Default)]
struct RusqliteDialect;

impl Dialect for RusqliteDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ('a'..='z').contains(&ch) || ('A'..='Z').contains(&ch) || ch == '_' || ch == '?'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ('a'..='z').contains(&ch)
            || ('A'..='Z').contains(&ch)
            || ('0'..='9').contains(&ch)
            || ch == '_'
            || ch == '?'
    }
}
