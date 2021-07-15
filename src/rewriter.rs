use sqlparser::ast::Statement;

pub struct ReWriter;

impl ReWriter {
    pub fn rewrite(ast: &Statement) -> String {
        let mut new_ast = ast.clone();
        if let Statement::Query(query) = &mut new_ast {
            query.limit = None;
            query.offset = None;
            log::trace!(
                "Rewrite query '{}' -> '{}'",
                ast.to_string(),
                new_ast.to_string()
            );
        }

        new_ast.to_string()
    }
}
