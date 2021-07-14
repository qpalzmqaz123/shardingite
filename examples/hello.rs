use shardingite::{ShardingIte, ShardingIteConfig, SqlParam};

const INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS user (
    id INTEGER NOT NULL,
    name VARCHAR NOT NULL,
    age INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS user_id_index ON user (
    id
);
"#;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let conn = ShardingIte::new(ShardingIteConfig {
        sharding_count: 2,
        sharding_path: Box::new(|i| format!("/tmp/shardingite/{}.sqlite", i)),
        sharding_table: "user".to_string(),
        sharding_column: "id".to_string(),
        sharding_index: Box::new(|param| match param {
            SqlParam::I64(n) => Ok(*n as u32 % 2),
            SqlParam::U32(n) => Ok(n % 2),
            p @ _ => Err(format!("Invalid param: {:?}", p).into()),
        }),
    })?;

    for sql in INIT_SQL
        .split(";")
        .map(|s| s.trim())
        .filter(|s| s.len() > 0)
    {
        conn.execute(sql, vec![])?;
    }

    let tx = conn.transaction()?;
    let stmt = tx.prepare(
        r#"
        INSERT INTO user
            (id, name, age)
        VALUES
            (?1, ?2, ?3)
        "#,
    )?;
    for i in 0..10 {
        stmt.execute(vec![
            SqlParam::U32(i),
            SqlParam::String(format!("name{}", i)),
            SqlParam::U16(i as u16),
        ])?;
    }
    tx.commit()?;

    Ok(())
}
