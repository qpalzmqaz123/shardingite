use crate::{Result, ShardingIteConfig, SqlParam};
use sqlparser::ast::{Expr, SetExpr, Statement};

pub struct Router {}

impl Router {
    pub fn get_indexes_with_params(
        config: &ShardingIteConfig,
        ast: &Statement,
        params: &Vec<SqlParam>,
    ) -> Result<Vec<u32>> {
        if let Statement::Insert {
            columns,
            source,
            table_name,
            ..
        } = ast
        {
            // Check table name
            if table_name.0.len() == 0
                || table_name.0.first().unwrap().value != config.sharding_table
            {
                log::trace!("Table mismatch, use full match");
                return Ok((0..config.sharding_count).collect());
            }

            let sharding_column_index = columns
                .iter()
                .position(|s| s.value == config.sharding_column);

            if let Some(index) = sharding_column_index {
                if let SetExpr::Values(values) = &source.body {
                    if values.0.len() != 1 {
                        return Err("Values lenght is not 1".into());
                    }

                    if let Some(sharding_column_value) = values.0[0].get(index) {
                        match sharding_column_value {
                            Expr::Identifier(id) => {
                                if id.value.starts_with("?") {
                                    match params.get(index) {
                                        Some(param) => {
                                            let routes = vec![config.sharding_index.as_ref()(
                                                param,
                                            )?];
                                            log::trace!("Param routes: {:?}", routes);
                                            return Ok(routes)
                                        }
                                        None => return Err("Sharding param not found".into()),
                                    }
                                } else {
                                    return Err(format!(
                                        "Ident must starts with ?, received {:?}",
                                        id
                                    )
                                    .into());
                                }
                            }
                            Expr::Value(v) => {
                                match v {
                                    sqlparser::ast::Value::Number(n, ..) => {
                                        let n: i64 = n.parse().map_err(|e| format!("Parse number error in sql router: {}", e))?;
                                        let param = SqlParam::I64(n);
                                        let routes = vec![config.sharding_index.as_ref()(&param)?];
                                        log::trace!("Sql values routes: {:?}", routes);
                                        return Ok(routes);
                                    },
                                    e @ _ => return Err(format!("Expect number type of sharding column in VALUES, received {:?}", e).into()),
                                }
                            }
                            e @ _ => return Err(format!("Expect id or value, received {:?}", e).into()),
                        }
                    }
                }
            }
        }

        log::trace!("Not insert, use full match");
        Ok((0..config.sharding_count).collect())
    }
}
