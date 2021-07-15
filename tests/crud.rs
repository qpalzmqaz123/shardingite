#[cfg(test)]
mod crud {
    use shardingite::{ShardingIte, ShardingIteConfig, SqlParam, NO_PARAMS};

    #[test]
    fn test1() {
        let mut env = setup();
        let conn = &mut env.conn;

        // Write test data
        {
            let tx = conn.transaction().unwrap();
            let mut stmt = tx
                .prepare(
                    r#"
                INSERT INTO user
                    (id, name, age)
                VALUES
                    (?1, ?2, ?3)
                "#,
                )
                .unwrap();
            for i in 0..10 {
                stmt.execute(vec![
                    SqlParam::U32(i),
                    SqlParam::String(format!("name{}", i)),
                    SqlParam::U16(i as u16),
                ])
                .unwrap();
            }
            drop(stmt);
            tx.commit().unwrap();
        }

        // Check count
        {
            let count1: u32 = conn
                .query_row("SELECT count(*) from user", NO_PARAMS, |row| row.get(0))
                .unwrap();
            assert_eq!(count1, 10);

            let count2: u32 = conn
                .query_row("SELECT count(*) from user WHERE id > 5", NO_PARAMS, |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count2, 4);

            let count3: u32 = conn
                .query_row(
                    "SELECT count(*) from user WHERE age > 4",
                    NO_PARAMS,
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count3, 5);
        }

        // Query exact 1
        {
            let info = conn
                .query_row(
                    "SELECT id, name, age FROM user WHERE id = 2",
                    NO_PARAMS,
                    |row| Ok((row.get::<u32>(0)?, row.get::<String>(1)?, row.get::<u8>(2)?)),
                )
                .unwrap();
            assert_eq!(info, (2, "name2".to_string(), 2));
        }

        // Query exact 2
        {
            let stmt = conn
                .prepare("SELECT id, name, age FROM user WHERE id = 2")
                .unwrap();
            let rows: Vec<(u32, String, u8)> = stmt
                .query_map(NO_PARAMS, |row| {
                    Ok((row.get::<u32>(0)?, row.get::<String>(1)?, row.get::<u8>(2)?))
                })
                .unwrap()
                .map(|v| v.unwrap())
                .collect();
            assert_eq!(rows, vec![(2 as u32, "name2".to_string(), 2 as u8)]);
        }

        // Query range asc
        {
            let stmt = conn
                .prepare("SELECT id, name, age FROM user WHERE id >= 6 ORDER BY id ASC")
                .unwrap();
            let rows: Vec<(u32, String, u8)> = stmt
                .query_map(NO_PARAMS, |row| {
                    Ok((row.get::<u32>(0)?, row.get::<String>(1)?, row.get::<u8>(2)?))
                })
                .unwrap()
                .map(|v| v.unwrap())
                .collect();
            assert_eq!(
                rows,
                vec![
                    (6 as u32, "name6".to_string(), 6 as u8),
                    (7, "name7".to_string(), 7),
                    (8, "name8".to_string(), 8),
                    (9, "name9".to_string(), 9),
                ]
            );
        }

        // Query range desc
        {
            let stmt = conn
                .prepare("SELECT id, name, age FROM user WHERE id >= 6 ORDER BY id DESC")
                .unwrap();
            let rows: Vec<(u32, String, u8)> = stmt
                .query_map(NO_PARAMS, |row| {
                    Ok((row.get::<u32>(0)?, row.get::<String>(1)?, row.get::<u8>(2)?))
                })
                .unwrap()
                .map(|v| v.unwrap())
                .collect();
            assert_eq!(
                rows,
                vec![
                    (9, "name9".to_string(), 9),
                    (8, "name8".to_string(), 8),
                    (7, "name7".to_string(), 7),
                    (6 as u32, "name6".to_string(), 6 as u8),
                ]
            );
        }

        // Query limit
        {
            let stmt = conn
                .prepare("SELECT id, name, age FROM user WHERE id >= 2 ORDER BY id ASC LIMIT 3")
                .unwrap();
            let rows: Vec<(u32, String, u8)> = stmt
                .query_map(NO_PARAMS, |row| {
                    Ok((row.get::<u32>(0)?, row.get::<String>(1)?, row.get::<u8>(2)?))
                })
                .unwrap()
                .map(|v| v.unwrap())
                .collect();
            assert_eq!(
                rows,
                vec![
                    (2 as u32, "name2".to_string(), 2 as u8),
                    (3, "name3".to_string(), 3),
                    (4, "name4".to_string(), 4),
                ]
            );
        }

        // Query offset
        {
            let stmt = conn
                .prepare(
                    "SELECT id, name, age FROM user WHERE id >= 2 ORDER BY id ASC LIMIT 3 OFFSET 1",
                )
                .unwrap();
            let rows: Vec<(u32, String, u8)> = stmt
                .query_map(NO_PARAMS, |row| {
                    Ok((row.get::<u32>(0)?, row.get::<String>(1)?, row.get::<u8>(2)?))
                })
                .unwrap()
                .map(|v| v.unwrap())
                .collect();
            assert_eq!(
                rows,
                vec![
                    (3 as u32, "name3".to_string(), 3 as u8),
                    (4, "name4".to_string(), 4),
                    (5, "name5".to_string(), 5),
                ]
            );
        }

        teardown(env);
    }

    struct Env {
        pub conn: ShardingIte,
    }

    fn setup() -> Env {
        env_logger::init();

        std::fs::remove_dir_all("/tmp/shardingite_test").unwrap();

        let conn = ShardingIte::new(ShardingIteConfig {
            sharding_count: 2,
            sharding_table: "user".to_string(),
            sharding_column: "id".to_string(),
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
        "#,
        )
        .unwrap();

        Env { conn }
    }

    fn teardown(_env: Env) {}
}
