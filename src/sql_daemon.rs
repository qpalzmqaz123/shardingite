use crate::Result;
use rusqlite::Connection;
use std::sync::{
    mpsc::{Receiver, Sender},
    Arc,
};

#[derive(Debug, Clone)]
pub enum SqlParam {
    String(String),
    I64(i64),
    U32(u32),
    U16(u16),
}

impl rusqlite::ToSql for SqlParam {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            SqlParam::I64(v) => Ok(rusqlite::types::ToSqlOutput::Owned((*v).into())),
            SqlParam::U32(v) => Ok(rusqlite::types::ToSqlOutput::Owned((*v).into())),
            SqlParam::U16(v) => Ok(rusqlite::types::ToSqlOutput::Owned((*v).into())),
            SqlParam::String(v) => Ok(rusqlite::types::ToSqlOutput::Owned(v.to_string().into())),
        }
    }
}

#[derive(Debug)]
pub enum DataCall {
    Exit,
    Execute(String, Arc<Vec<SqlParam>>),
    Transaction,
    TransactionPrepare(String),
    TransactionExecute(Arc<Vec<SqlParam>>),
    TransactionCommit,
}

#[derive(Debug)]
pub enum DataRet {
    Execute(rusqlite::Result<()>),
    Transaction(rusqlite::Result<()>),
    TransactionPrepare(rusqlite::Result<()>),
    TransactionExecute(rusqlite::Result<()>),
    TransactionCommit(rusqlite::Result<()>),
}

pub struct SqlDaemon {
    index: u32,
    conn: Connection,
    rx: Receiver<DataCall>,
    tx: Sender<(u32, DataRet)>,
}

impl SqlDaemon {
    pub fn new(
        index: u32,
        path: &str,
        rx: Receiver<DataCall>,
        tx: Sender<(u32, DataRet)>,
    ) -> Result<Self> {
        Ok(Self {
            index,
            conn: Connection::open(path)?,
            tx,
            rx,
        })
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            let call = self.rx.recv()?;
            match call {
                DataCall::Exit => {
                    log::debug!("[{}] Exit", self.index);
                    return Ok(());
                }
                DataCall::Execute(sql, params) => {
                    log::trace!("[{}] Execute sql: {}", self.index, sql);
                    let res = self
                        .conn
                        .execute(&sql, &*param_vec_to_tosql_vec(&params))
                        .map(|_| ());
                    self.tx.send((self.index, DataRet::Execute(res)))?;
                }
                DataCall::Transaction => {
                    log::trace!("[{}] Start transaction", self.index);
                    let tc = match self.conn.transaction() {
                        Ok(tc) => {
                            self.tx.send((self.index, DataRet::Transaction(Ok(()))))?;
                            tc
                        }
                        Err(e) => {
                            self.tx.send((self.index, DataRet::Transaction(Err(e))))?;
                            continue;
                        }
                    };

                    // Wait for prepare
                    let sql = match self.rx.recv()? {
                        DataCall::TransactionPrepare(sql) => sql,
                        d @ _ => {
                            log::error!("Expect prepare, received: {:?}", d);
                            continue;
                        }
                    };

                    log::trace!("[{}] Transaction prepare {}", self.index, sql);
                    let mut stmt = match tc.prepare(&sql) {
                        Ok(stmt) => {
                            self.tx
                                .send((self.index, DataRet::TransactionPrepare(Ok(()))))?;
                            stmt
                        }
                        Err(e) => {
                            self.tx
                                .send((self.index, DataRet::TransactionPrepare(Err(e))))?;
                            continue;
                        }
                    };

                    // Wait for execute or commit
                    loop {
                        match self.rx.recv()? {
                            DataCall::TransactionExecute(params) => {
                                log::trace!("[{}] Transaction execute: {:?}", self.index, params);
                                match stmt.execute(&*param_vec_to_tosql_vec(&params)) {
                                    Ok(_) => {
                                        self.tx.send((
                                            self.index,
                                            DataRet::TransactionExecute(Ok(())),
                                        ))?;
                                    }
                                    Err(e) => {
                                        self.tx.send((
                                            self.index,
                                            DataRet::TransactionExecute(Err(e)),
                                        ))?;
                                    }
                                }
                            }
                            DataCall::TransactionCommit => {
                                log::trace!("[{}] Transaction commit", self.index);
                                drop(stmt);
                                match tc.commit() {
                                    Ok(_) => {
                                        self.tx.send((
                                            self.index,
                                            DataRet::TransactionCommit(Ok(())),
                                        ))?;
                                    }
                                    Err(e) => {
                                        self.tx.send((
                                            self.index,
                                            DataRet::TransactionCommit(Err(e)),
                                        ))?;
                                    }
                                }
                                break;
                            }
                            d @ _ => {
                                log::error!("Expect execute or commit, received: {:?}", d)
                            }
                        }
                    }
                }
                d @ _ => log::error!("Expect transaction, received: {:?}", d),
            }
        }
    }
}

fn param_vec_to_tosql_vec(v: &Vec<SqlParam>) -> Vec<&dyn rusqlite::ToSql> {
    v.iter().map(|p| p as &dyn rusqlite::ToSql).collect()
}
