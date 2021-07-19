use crate::Result;
use rusqlite::{types::ValueRef, Connection};
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

#[derive(Debug, Clone)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl From<rusqlite::types::ValueRef<'_>> for SqlValue {
    fn from(val: rusqlite::types::ValueRef) -> Self {
        match val {
            rusqlite::types::ValueRef::Null => Self::Null,
            rusqlite::types::ValueRef::Integer(v) => Self::Integer(v),
            rusqlite::types::ValueRef::Real(v) => Self::Real(v),
            rusqlite::types::ValueRef::Text(v) => Self::Text(v.to_vec()),
            rusqlite::types::ValueRef::Blob(v) => Self::Blob(v.to_vec()),
        }
    }
}

impl<'a> Into<ValueRef<'a>> for &'a SqlValue {
    fn into(self) -> ValueRef<'a> {
        match &self {
            SqlValue::Null => ValueRef::Null,
            SqlValue::Integer(v) => ValueRef::Integer(*v),
            SqlValue::Real(v) => ValueRef::Real(*v),
            SqlValue::Text(v) => ValueRef::Text(v),
            SqlValue::Blob(v) => ValueRef::Blob(v),
        }
    }
}

#[derive(Debug)]
pub enum DataCall {
    Exit,
    Prepare(String),
    StatementExecute(Arc<Vec<SqlParam>>),
    StatementQuery(Arc<Vec<SqlParam>>),
    StatementEnd,
    RowsNext,
    RowsEnd,
    Transaction,
    TransactionCommit,
    TransactionRollback,
    LastInsertRowId,
}

#[derive(Debug)]
pub enum DataRet {
    Prepare(rusqlite::Result<()>),
    StatementExecute(rusqlite::Result<()>),
    StatementQuery(rusqlite::Result<()>),
    Next(rusqlite::Result<Option<Vec<SqlValue>>>),
    Transaction(rusqlite::Result<()>),
    TransactionCommit(rusqlite::Result<()>),
    TransactionRollback(rusqlite::Result<()>),
    LastInsertRowId(i64),
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
                DataCall::Transaction => {
                    log::trace!("[{}] Start transaction", self.index);
                    process_transcation(self.index, &self.tx, &self.rx, &mut self.conn)?;
                }
                DataCall::Prepare(sql) => {
                    log::trace!("[{}] Prepare: {}", self.index, sql);
                    process_prepare(self.index, &self.tx, &self.rx, &mut self.conn, sql)?;
                }
                DataCall::LastInsertRowId => {
                    log::trace!("[{}] Last insert row id", self.index);
                    process_last_insert_row_id(self.index, &self.tx, &mut self.conn)?;
                }
                d @ _ => log::error!("DataCall mismatch, received: {:?}", d),
            }
        }
    }
}

fn process_transcation(
    index: u32,
    tx: &Sender<(u32, DataRet)>,
    rx: &Receiver<DataCall>,
    conn: &mut Connection,
) -> Result<()> {
    let mut tc = match conn.transaction() {
        Ok(tc) => {
            tx.send((index, DataRet::Transaction(Ok(()))))?;
            tc
        }
        Err(e) => {
            tx.send((index, DataRet::Transaction(Err(e))))?;
            return Ok(());
        }
    };

    loop {
        match rx.recv()? {
            DataCall::Prepare(sql) => {
                log::trace!("[{}] Transaction prepare {}", index, sql);
                process_transcation_prepare(index, tx, rx, &mut tc, sql)?;
            }
            DataCall::TransactionCommit => {
                log::trace!("[{}] Transaction commit", index);
                match tc.commit() {
                    Ok(_) => {
                        tx.send((index, DataRet::TransactionCommit(Ok(()))))?;
                    }
                    Err(e) => {
                        tx.send((index, DataRet::TransactionCommit(Err(e))))?;
                    }
                }
                break;
            }
            DataCall::TransactionRollback => {
                log::trace!("[{}] Transaction rollback", index);
                match tc.rollback() {
                    Ok(_) => {
                        tx.send((index, DataRet::TransactionRollback(Ok(()))))?;
                    }
                    Err(e) => {
                        tx.send((index, DataRet::TransactionRollback(Err(e))))?;
                    }
                }
                break;
            }
            d @ _ => {
                log::error!("DataCall mismatch in transcation, received: {:?}", d);
            }
        }
    }

    Ok(())
}

fn process_transcation_prepare(
    index: u32,
    tx: &Sender<(u32, DataRet)>,
    rx: &Receiver<DataCall>,
    tc: &mut rusqlite::Transaction,
    sql: String,
) -> Result<()> {
    let mut stmt = match tc.prepare(&sql) {
        Ok(stmt) => {
            tx.send((index, DataRet::Prepare(Ok(()))))?;
            stmt
        }
        Err(e) => {
            tx.send((index, DataRet::Prepare(Err(e))))?;
            return Ok(());
        }
    };

    process_statement(index, tx, rx, &mut stmt)?;

    Ok(())
}

fn process_prepare(
    index: u32,
    tx: &Sender<(u32, DataRet)>,
    rx: &Receiver<DataCall>,
    conn: &mut rusqlite::Connection,
    sql: String,
) -> Result<()> {
    let mut stmt = match conn.prepare(&sql) {
        Ok(stmt) => {
            tx.send((index, DataRet::Prepare(Ok(()))))?;
            stmt
        }
        Err(e) => {
            tx.send((index, DataRet::Prepare(Err(e))))?;
            return Ok(());
        }
    };

    process_statement(index, tx, rx, &mut stmt)?;

    Ok(())
}

fn process_statement(
    index: u32,
    tx: &Sender<(u32, DataRet)>,
    rx: &Receiver<DataCall>,
    stmt: &mut rusqlite::Statement,
) -> Result<()> {
    loop {
        match rx.recv()? {
            DataCall::StatementExecute(params) => {
                log::trace!("[{}] Statement execute: {:?}", index, params);
                match stmt.execute(&*param_vec_to_tosql_vec(&params)) {
                    Ok(_) => {
                        tx.send((index, DataRet::StatementExecute(Ok(()))))?;
                    }
                    Err(e) => {
                        tx.send((index, DataRet::StatementExecute(Err(e))))?;
                    }
                }
            }
            DataCall::StatementQuery(params) => {
                log::trace!("[{}] Statement query: {:?}", index, params);
                let mut rows = match stmt.query(&*param_vec_to_tosql_vec(&params)) {
                    Ok(rows) => {
                        tx.send((index, DataRet::StatementQuery(Ok(()))))?;
                        rows
                    }
                    Err(e) => {
                        tx.send((index, DataRet::StatementQuery(Err(e))))?;
                        continue;
                    }
                };

                process_rows(index, tx, rx, &mut rows)?;
            }
            DataCall::StatementEnd => {
                log::trace!("[{}] Statement end", index);
                break;
            }
            d @ _ => {
                log::error!("DataCall mismatch in statement, received: {:?}", d);
            }
        }
    }

    Ok(())
}

fn process_rows(
    index: u32,
    tx: &Sender<(u32, DataRet)>,
    rx: &Receiver<DataCall>,
    rows: &mut rusqlite::Rows,
) -> Result<()> {
    loop {
        match rx.recv()? {
            DataCall::RowsNext => {
                log::trace!("[{}] Rows next", index);
                let row = match rows.next() {
                    Ok(row) => row,
                    Err(e) => {
                        tx.send((index, DataRet::Next(Err(e))))?;
                        continue;
                    }
                };

                let row = match row {
                    Some(row) => row,
                    None => {
                        tx.send((index, DataRet::Next(Ok(None))))?;
                        continue;
                    }
                };
                let values: Vec<SqlValue> = (0..row.column_count())
                    .map(|i| row.get_ref_unwrap(i).into())
                    .collect();

                tx.send((index, DataRet::Next(Ok(Some(values)))))?;
            }
            DataCall::RowsEnd => {
                log::trace!("[{}] Rows end", index);
                break;
            }
            d @ _ => {
                log::error!("DataCall mismatch in rows, received: {:?}", d);
            }
        }
    }

    Ok(())
}

fn process_last_insert_row_id(
    index: u32,
    tx: &Sender<(u32, DataRet)>,
    conn: &mut rusqlite::Connection,
) -> Result<()> {
    let id = conn.last_insert_rowid();
    tx.send((index, DataRet::LastInsertRowId(id)))?;

    Ok(())
}

fn param_vec_to_tosql_vec(v: &Vec<SqlParam>) -> Vec<&dyn rusqlite::ToSql> {
    v.iter().map(|p| p as &dyn rusqlite::ToSql).collect()
}
