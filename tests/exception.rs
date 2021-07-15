#[cfg(test)]
mod exception {
    use shardingite::{ShardingIte, ShardingIteConfig, SqlParam, NO_PARAMS};

    #[test]
    fn test1() {
        let mut env = setup();
        let conn = &mut env.conn;

        // tc
        {
            let _ = conn.transaction().unwrap();
            query(conn);
        }

        // tc prepare
        {
            let tc = conn.transaction().unwrap();
            let _ = tc.prepare("xxxx").ok();
            query(conn);
        }

        // prepare
        {
            let _ = conn.prepare("xxxxx").ok();
            query(conn);
        }

        // exec
        {
            conn.execute(
                "INSERT INTO test (value) VALUES (?1)",
                vec![SqlParam::String("abc".to_string())],
            )
            .ok();
            query(conn);
        }

        teardown(env);
    }

    fn query(conn: &ShardingIte) {
        let count = conn
            .query_row("SELECT count(*) from test", NO_PARAMS, |row| {
                row.get::<u32>(0)
            })
            .unwrap();
        assert_eq!(count, 1);
    }

    struct Env {
        pub conn: ShardingIte,
    }

    fn setup() -> Env {
        env_logger::init();

        std::fs::remove_dir_all("/tmp/shardingite_test").unwrap();

        let conn = ShardingIte::new(ShardingIteConfig {
            sharding_count: 2,
            sharding_table: "test".to_string(),
            sharding_column: "value".to_string(),
            sharding_path: Box::new(|index| format!("/tmp/shardingite_test/{}.sqlite", index)),
            sharding_index: Box::new(|param| match param {
                SqlParam::I64(n) => Ok(*n as u32 % 2),
                SqlParam::U32(n) => Ok(n % 2),
                p @ _ => Err(format!("Invalid param: {:?}", p).into()),
            }),
        })
        .unwrap();

        conn.execute_batch(
            r#"
        CREATE TABLE IF NOT EXISTS test (
            value INTEGER NOT NULL
        );

        INSERT INTO test (value) VALUES (0);
        "#,
        )
        .unwrap();

        Env { conn }
    }

    fn teardown(_env: Env) {}
}
