use crate::Result;
use sqlparser::{
    ast::{Expr, FunctionArg, Select, SelectItem, SetExpr},
    dialect::Dialect,
};

const RUSQLITE_DIALECT: RusqliteDialect = RusqliteDialect;

#[derive(Debug, Clone)]
pub struct Limit {
    pub limit: u32,
    pub offset: u32,
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    pub column_index: u8,
    pub is_asc: bool,
}

#[derive(Debug, Clone)]
pub enum Aggregate {
    Count(String),
}

#[derive(Debug, Clone)]
pub struct Query {
    pub limit: Option<Limit>,
    pub order_by: Option<OrderBy>,
    pub aggregate: Option<Aggregate>,
}

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

    pub fn get_query_from_ast(ast: &sqlparser::ast::Statement) -> Result<Query> {
        if let sqlparser::ast::Statement::Query(query) = ast {
            let order_by = match query.order_by.get(0) {
                Some(expr) => {
                    Some(OrderBy {
                        column_index: {
                            // Get column name
                            let column_name = match &expr.expr {
                                sqlparser::ast::Expr::Identifier(ident) => ident.value.to_string(),
                                _ => return Err("Expect ident in order expr".into()),
                            };

                            // Search column index
                            let column_index = if let sqlparser::ast::SetExpr::Select(select) =
                                &query.body
                            {
                                Self::find_column_index_in_select(select, &column_name)?
                            } else {
                                return Err("Currently only supports the combination of order by and select".into());
                            };

                            column_index
                        },
                        is_asc: expr.asc.unwrap_or(true),
                    })
                }
                None => None,
            };

            let limit_number: Option<u32> = match &query.limit {
                Some(e) => match e {
                    sqlparser::ast::Expr::Value(v) => match v {
                        sqlparser::ast::Value::Number(n, _) => Some(n.parse()?),
                        _ => return Err("Expect number value in limit expr".into()),
                    },
                    _ => return Err("Expect value in limit expr".into()),
                },
                None => None,
            };

            let offset_number: Option<u32> = match &query.offset {
                Some(sqlparser::ast::Offset { value, .. }) => match value {
                    sqlparser::ast::Expr::Value(v) => match v {
                        sqlparser::ast::Value::Number(n, _) => Some(n.parse()?),
                        _ => return Err("Expect number value in offset expr".into()),
                    },
                    _ => return Err("Expect value in offset expr".into()),
                },
                None => None,
            };

            let limit = match limit_number {
                Some(l) => Some(Limit {
                    limit: l,
                    offset: offset_number.unwrap_or(0),
                }),
                None => None,
            };

            let aggregate = Self::get_aggregate(query)?;

            return Ok(Query {
                limit,
                order_by,
                aggregate,
            });
        }

        Err("Not a query".into())
    }

    fn get_aggregate(query: &sqlparser::ast::Query) -> Result<Option<Aggregate>> {
        match &query.body {
            SetExpr::Select(sel) => match sel.as_ref() {
                Select { projection, .. } => {
                    for prj in projection {
                        match prj {
                            SelectItem::UnnamedExpr(expr) => {
                                if let Expr::Function(f) = expr {
                                    let name = f.name.0[0].value.clone();
                                    match name.as_str().to_lowercase().as_str() {
                                        "count" => {
                                            if projection.len() > 1 {
                                                return Err("Currently only supports count function alone in query function".into());
                                            }

                                            if let Some(FunctionArg::Unnamed(Expr::Wildcard)) =
                                                &f.args.get(0)
                                            {
                                                return Ok(Some(Aggregate::Count("*".to_string())));
                                            }

                                            return Err("Currently only supports * with count function in query function".into());
                                        }
                                        "json_extract" => {}
                                        _ => {
                                            return {
                                                Err("Currently only supports ['count', 'json_extract'] filed in query function"
                                                    .into())
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {
                                return Err("Currently only supports unname expr in query".into());
                            }
                        }
                    }

                    Ok(None)
                }
            },
            _ => Err("Currently only supports select query".into()),
        }
    }

    fn find_column_index_in_select(select: &Select, column: &String) -> Result<u8> {
        for (index, item) in select.projection.iter().enumerate() {
            if let SelectItem::UnnamedExpr(e) = item {
                if let Expr::Identifier(id) = e {
                    if &id.value == column {
                        return Ok(index as u8);
                    }
                } else {
                    return Err("Currently only supports the unnamed id expr in select".into());
                }
            } else {
                return Err("Currently only supports the unnamed expr in select".into());
            }
        }

        Err(format!("Column '{}' not found in select", column).into())
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
