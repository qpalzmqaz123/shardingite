mod parser;
mod router;
mod sql_daemon;

pub use sql_daemon::SqlParam;

use crate::{router::Router, sql_daemon::SqlDaemon};
use parser::Parser;
use sql_daemon::{DataCall, DataRet};
use std::{
    collections::HashMap,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    time::Duration,
};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub struct ShardingIteConfig {
    pub sharding_count: u32,
    pub sharding_path: Box<dyn Fn(u32) -> String>,
    pub sharding_table: String,
    pub sharding_column: String,
    pub sharding_index: Box<dyn Fn(&SqlParam) -> Result<u32>>,
}

pub struct ShardingIte {
    config: ShardingIteConfig,
    call_map: HashMap<u32, Sender<DataCall>>,
    ret_rx: Receiver<(u32, DataRet)>,
}

impl ShardingIte {
    pub fn new(config: ShardingIteConfig) -> Result<Self> {
        let mut call_map: HashMap<u32, Sender<DataCall>> = HashMap::new();
        let (ret_tx, ret_rx) = mpsc::channel::<(u32, DataRet)>();

        // Init connections
        for i in 0..config.sharding_count {
            let file = config.sharding_path.as_ref()(i);
            let basepath = std::path::Path::new(&file)
                .parent()
                .ok_or(format!("Parent path is empty of file '{}'", file))?;
            if !basepath.exists() {
                log::debug!("Create db path: {:?}", basepath);
                std::fs::create_dir_all(basepath)?;
            }

            // Create daemon
            let (call_tx, call_rx) = mpsc::channel::<DataCall>();
            let mut daemon = SqlDaemon::new(i, &file, call_rx, ret_tx.clone())?;
            std::thread::spawn(move || {
                while let Err(e) = daemon.run() {
                    log::error!("[{}] Daemon error: {}, retry...", i, e);
                    std::thread::sleep(Duration::from_secs(3));
                }
            });
            call_map.insert(i, call_tx);
        }

        Ok(Self {
            config,
            call_map,
            ret_rx,
        })
    }

    pub fn execute(&self, sql: &str, params: Vec<SqlParam>) -> Result<()> {
        let ast = Parser::parse(sql)?;
        let indexes = Router::get_indexes(&self.config, &ast);
        let params = Arc::new(params);

        // Call
        for index in &indexes {
            self.execute_index(*index, sql.to_string(), params.clone())?;
        }

        // Wait result
        for _ in &indexes {
            match self.ret_rx.recv()?.1 {
                DataRet::Execute(ret) => ret?,
                e @ _ => {
                    return Err(format!("Message mismatch in wait execute: {:?}", e).into());
                }
            }
        }

        Ok(())
    }

    pub fn transaction(&self) -> Result<Transaction<'_>> {
        let tc: Transaction = Transaction::new(self)?;

        Ok(tc)
    }

    fn execute_index(&self, index: u32, sql: String, params: Arc<Vec<SqlParam>>) -> Result<()> {
        self.send_data(index, DataCall::Execute(sql, params))?;

        Ok(())
    }

    fn send_data(&self, index: u32, data: DataCall) -> Result<()> {
        let call_tx = self
            .call_map
            .get(&index)
            .ok_or(format!("Connection of index {} not found", index))?;

        call_tx.send(data)?;

        Ok(())
    }
}

impl Drop for ShardingIte {
    fn drop(&mut self) {
        // Send exit message
        for i in 0..self.config.sharding_count {
            self.send_data(i, DataCall::Exit).ok();
        }
    }
}

pub struct Transaction<'a> {
    sharding_ite: &'a ShardingIte,
}

impl<'a> Transaction<'a> {
    pub fn new(si: &'a ShardingIte) -> Result<Self> {
        // Start transaction
        for i in 0..si.config.sharding_count {
            si.send_data(i, DataCall::Transaction)?;
        }

        // Wait transaction
        for _ in 0..si.config.sharding_count {
            match si.ret_rx.recv()?.1 {
                DataRet::Transaction(ret) => ret?,
                e @ _ => {
                    return Err(format!("Message mismatch in wait transaction: {:?}", e).into());
                }
            }
        }

        Ok(Self { sharding_ite: si })
    }

    pub fn prepare(&self, sql: &str) -> Result<Statement<'_>> {
        let ast = Parser::parse(sql)?;
        log::trace!("Transaction prepare ast: {:#?}", ast);

        // Start prepare
        for i in 0..self.sharding_ite.config.sharding_count {
            self.sharding_ite
                .send_data(i, DataCall::TransactionPrepare(sql.to_string()))?;
        }

        // Wait prepare
        for _ in 0..self.sharding_ite.config.sharding_count {
            match self.sharding_ite.ret_rx.recv()?.1 {
                DataRet::TransactionPrepare(ret) => ret?,
                e @ _ => {
                    return Err(
                        format!("Message mismatch in wait transaction prepare: {:?}", e).into(),
                    );
                }
            }
        }

        Ok(Statement::new(self, ast)?)
    }

    pub fn commit(self) -> Result<()> {
        log::trace!("Transaction commit");

        // Start commit
        for i in 0..self.sharding_ite.config.sharding_count {
            self.sharding_ite
                .send_data(i, DataCall::TransactionCommit)?;
        }

        // Wait commit
        for _ in 0..self.sharding_ite.config.sharding_count {
            match self.sharding_ite.ret_rx.recv()?.1 {
                DataRet::TransactionCommit(ret) => ret?,
                e @ _ => {
                    return Err(
                        format!("Message mismatch in wait transaction commit: {:?}", e).into(),
                    );
                }
            }
        }

        Ok(())
    }
}

pub struct Statement<'a> {
    transaction: &'a Transaction<'a>,
    ast: sqlparser::ast::Statement,
    exec_counter: usize,
}

impl<'a> Statement<'a> {
    pub fn new(tc: &'a Transaction<'a>, ast: sqlparser::ast::Statement) -> Result<Self> {
        Ok(Self {
            transaction: tc,
            ast,
            exec_counter: 0,
        })
    }

    pub fn execute(&mut self, params: Vec<SqlParam>) -> Result<()> {
        let list = Router::get_indexes_with_params(
            &self.transaction.sharding_ite.config,
            &self.ast,
            &params,
        )?;

        let params = Arc::new(params);

        // Execute
        for i in &list {
            self.transaction
                .sharding_ite
                .send_data(*i, DataCall::TransactionExecute(params.clone()))?;
            self.exec_counter += 1;
        }

        Ok(())
    }
}

impl<'a> Drop for Statement<'a> {
    fn drop(&mut self) {
        // Wait all exec are consumed
        for _ in 0..self.exec_counter {
            if self.transaction.sharding_ite.ret_rx.recv().is_err() {
                break;
            }
        }
    }
}
