use shardingite::{ShardingIte, ShardingIteConfig, SqlParam, NO_PARAMS};

const INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS user (
    id INTEGER NOT NULL,
    name VARCHAR NOT NULL,
    age INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS index1 ON user (
    id, name, age
);

CREATE INDEX IF NOT EXISTS index2 ON user (
    id, age, name
);

CREATE INDEX IF NOT EXISTS index3 ON user (
    name, id, age
);

CREATE INDEX IF NOT EXISTS index4 ON user (
    name, age, id
);

CREATE INDEX IF NOT EXISTS index5 ON user (
    age, id, name
);

CREATE INDEX IF NOT EXISTS index6 ON user (
    age, name, id
);
"#;
const SHARDING_COUNT: u32 = 2;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let conn = ShardingIte::new(ShardingIteConfig {
        sharding_count: SHARDING_COUNT,
        sharding_path: Box::new(|i| format!("/tmp/shardingite/{}.sqlite", i)),
        sharding_table: "user".to_string(),
        sharding_column: "id".to_string(),
        sharding_index: Box::new(|param| match param {
            SqlParam::I64(n) => Ok(*n as u32 % SHARDING_COUNT),
            SqlParam::U32(n) => Ok(n % SHARDING_COUNT),
            p @ _ => Err(format!("Invalid param: {:?}", p).into()),
        }),
    })?;

    conn.execute_batch(INIT_SQL)?;

    let start_time = std::time::SystemTime::now();
    let tx = conn.transaction()?;
    let mut stmt = tx.prepare(
        r#"
        INSERT INTO user
            (id, name, age)
        VALUES
            (?1, ?2, ?3)
        "#,
    )?;
    for i in 0..10000 {
        stmt.execute(vec![
            SqlParam::U32(i),
            SqlParam::String(format!("name{}", i)),
            SqlParam::U16(i as u16),
        ])?;
    }
    drop(stmt);
    tx.commit()?;
    log::info!("time: {}ms", start_time.elapsed().unwrap().as_millis());

    let start_time = std::time::SystemTime::now();
    let stmt = conn.prepare(
        r#"
        SELECT id, name, age
        FROM user
        ORDER BY id ASC
        LIMIT 10
        OFFSET 9990
    "#,
    )?;
    let mut rows = stmt.query(NO_PARAMS)?;
    while let Some(row) = rows.next()? {
        println!("{:?}", row);
    }
    log::info!("time: {}ms", start_time.elapsed().unwrap().as_millis());

    Ok(())
}
