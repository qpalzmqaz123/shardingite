use crate::{Result, ShardingIteConfig, SqlParam};
use sqlparser::ast::{Expr, SetExpr, Statement};

pub struct Router {}

impl Router {
    pub fn get_indexes(config: &ShardingIteConfig, ast: &Statement) -> Vec<u32> {
        match ast {
            _ => (0..config.sharding_count).collect(),
        }
    }

    pub fn get_indexes_with_params(
        config: &ShardingIteConfig,
        ast: &Statement,
        params: &Vec<SqlParam>,
    ) -> Result<Vec<u32>> {
        if let Statement::Insert {
            columns, source, ..
        } = ast
        {
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
                                            return Ok(vec![config.sharding_index.as_ref()(
                                                param,
                                            )?]);
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
                            e @ _ => return Err(format!("Expect id, received {:?}", e).into()),
                        }
                    }
                }
            }
        }

        Ok((0..config.sharding_count).collect())
    }
}
