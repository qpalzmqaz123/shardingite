mod parser;
mod rewriter;
mod router;
mod sql_daemon;

pub use rusqlite;
pub use sql_daemon::SqlParam;

use crate::{rewriter::ReWriter, router::Router, sql_daemon::SqlDaemon};
use parser::{Aggregate, OrderBy, Parser, Query};
use sql_daemon::{DataCall, DataRet, SqlValue};
use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap},
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    time::Duration,
};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub const NO_PARAMS: Vec<SqlParam> = Vec::new();

pub struct ShardingIteConfig {
    pub sharding_count: u32,
    pub sharding_path: Box<dyn Fn(u32) -> String + Send>,
    pub sharding_table: String,
    pub sharding_column: String,
    pub sharding_index: Box<dyn Fn(&SqlParam) -> Result<u32> + Send>,
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
        let tc = self.transaction()?;
        let mut stmt = tc.prepare(sql)?;
        stmt.execute(params)?;
        drop(stmt);
        tc.commit()?;

        Ok(())
    }

    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        for sql in sql.split(";").map(|s| s.trim()).filter(|s| s.len() > 0) {
            self.execute(sql, NO_PARAMS)?;
        }

        Ok(())
    }

    pub fn transaction(&self) -> Result<Transaction<'_>> {
        let tc: Transaction = Transaction::new(self)?;

        Ok(tc)
    }

    pub fn prepare(&self, sql: &str) -> Result<Statement> {
        let ast = Parser::parse(sql)?;

        let sql = ReWriter::rewrite(&ast);

        // Send prepare
        for i in 0..self.config.sharding_count {
            self.send_data(i, DataCall::Prepare(sql.to_string()))?;
        }

        // Wait prepare
        for _ in 0..self.config.sharding_count {
            match self.ret_rx.recv()?.1 {
                DataRet::Prepare(ret) => ret?,
                e @ _ => {
                    return Err(format!("Message mismatch in wait prepare: {:?}", e).into());
                }
            }
        }

        Ok(Statement::new(self, ast)?)
    }

    pub fn query_row<T, F>(&self, sql: &str, params: Vec<SqlParam>, f: F) -> Result<T>
    where
        F: FnOnce(Row) -> Result<T>,
    {
        let stmt = self.prepare(sql)?;
        if let Some(row) = stmt.query(params)?.next()? {
            return f(row);
        }

        return Err("Query is empty".into());
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

impl Default for ShardingIte {
    fn default() -> Self {
        Self::new(ShardingIteConfig {
            sharding_count: 0,
            sharding_table: "".to_string(),
            sharding_column: "".to_string(),
            sharding_path: Box::new(|_| unreachable!()),
            sharding_index: Box::new(|_| unreachable!()),
        })
        .unwrap()
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
    committed: bool,
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

        Ok(Self {
            sharding_ite: si,
            committed: false,
        })
    }

    pub fn prepare(&self, sql: &str) -> Result<Statement<'_>> {
        let ast = Parser::parse(sql)?;

        // Start prepare
        for i in 0..self.sharding_ite.config.sharding_count {
            self.sharding_ite
                .send_data(i, DataCall::Prepare(sql.to_string()))?;
        }

        // Wait prepare
        for _ in 0..self.sharding_ite.config.sharding_count {
            match self.sharding_ite.ret_rx.recv()?.1 {
                DataRet::Prepare(ret) => ret?,
                e @ _ => {
                    return Err(
                        format!("Message mismatch in wait transaction prepare: {:?}", e).into(),
                    );
                }
            }
        }

        Ok(Statement::new(self.sharding_ite, ast)?)
    }

    pub fn commit(mut self) -> Result<()> {
        // Start commit
        for i in 0..self.sharding_ite.config.sharding_count {
            self.sharding_ite
                .send_data(i, DataCall::TransactionCommit)?;
        }

        self.committed = true;

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

    pub fn rollback(self) -> Result<()> {
        // Start rollback
        for i in 0..self.sharding_ite.config.sharding_count {
            self.sharding_ite
                .send_data(i, DataCall::TransactionRollback)?;
        }

        // Wait rollback
        for _ in 0..self.sharding_ite.config.sharding_count {
            match self.sharding_ite.ret_rx.recv()?.1 {
                DataRet::TransactionRollback(ret) => ret?,
                e @ _ => {
                    return Err(
                        format!("Message mismatch in wait transaction rollback: {:?}", e).into(),
                    );
                }
            }
        }

        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            // Rollback
            for i in 0..self.sharding_ite.config.sharding_count {
                self.sharding_ite
                    .send_data(i, DataCall::TransactionRollback)
                    .ok();
            }

            // Wait rollback
            for _ in 0..self.sharding_ite.config.sharding_count {
                // TODO: Verify message
                self.sharding_ite.ret_rx.recv().ok();
            }
        }
    }
}

pub struct Statement<'a> {
    sharding_ite: &'a ShardingIte,
    ast: sqlparser::ast::Statement,
    exec_counter: usize,
}

impl<'a> Statement<'a> {
    pub fn new(sdi: &'a ShardingIte, ast: sqlparser::ast::Statement) -> Result<Self> {
        Ok(Self {
            sharding_ite: sdi,
            ast,
            exec_counter: 0,
        })
    }

    pub fn execute(&mut self, params: Vec<SqlParam>) -> Result<()> {
        let list = Router::get_indexes_with_params(&self.sharding_ite.config, &self.ast, &params)?;

        let params = Arc::new(params);

        // Execute
        for i in &list {
            self.sharding_ite
                .send_data(*i, DataCall::StatementExecute(params.clone()))?;
            self.exec_counter += 1;
        }

        Ok(())
    }

    pub fn query(&self, params: Vec<SqlParam>) -> Result<Rows> {
        let list = Router::get_indexes_with_params(&self.sharding_ite.config, &self.ast, &params)?;
        let params = Arc::new(params);
        let query = Parser::get_query_from_ast(&self.ast)?;

        // Send query
        for i in &list {
            self.sharding_ite
                .send_data(*i, DataCall::StatementQuery(params.clone()))?;
        }

        // Wait query
        for _ in &list {
            match self.sharding_ite.ret_rx.recv()?.1 {
                DataRet::StatementQuery(ret) => ret?,
                e @ _ => {
                    return Err(format!("Message mismatch in wait statement query: {:?}", e).into());
                }
            }
        }

        Ok(Rows::new(self.sharding_ite, list, query)?)
    }

    pub fn query_map<F, T>(&self, params: Vec<SqlParam>, map: F) -> Result<MappedRows<'_, F>>
    where
        F: FnMut(&Row) -> Result<T>,
    {
        let rows = self.query(params)?;
        Ok(MappedRows { rows, map })
    }
}

impl<'a> Drop for Statement<'a> {
    fn drop(&mut self) {
        // Wait all exec are consumed
        for _ in 0..self.exec_counter {
            if self.sharding_ite.ret_rx.recv().is_err() {
                break;
            }
        }

        // Send end
        for i in 0..self.sharding_ite.config.sharding_count {
            self.sharding_ite.send_data(i, DataCall::StatementEnd).ok();
        }
    }
}

#[derive(Debug)]
struct HeapData(u32, i64, Vec<SqlValue>); // (index, column, data)

impl PartialEq for HeapData {
    fn eq(&self, other: &Self) -> bool {
        self.1.eq(&other.1)
    }
}

impl Eq for HeapData {}

impl PartialOrd for HeapData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.1.partial_cmp(&other.1)
    }
}

impl Ord for HeapData {
    fn cmp(&self, other: &Self) -> Ordering {
        self.1.cmp(&other.1)
    }
}

pub struct Rows<'a> {
    sharding_ite: &'a ShardingIte,
    sharding_index_list: Vec<u32>,
    query: Query,
    counter: u32,
    skipped: bool,
    max_heap: Option<BinaryHeap<HeapData>>,
    min_heap: Option<BinaryHeap<Reverse<HeapData>>>,
}

impl<'a> Rows<'a> {
    pub fn new(sdi: &'a ShardingIte, list: Vec<u32>, query: Query) -> Result<Self> {
        Ok(Self {
            sharding_ite: sdi,
            sharding_index_list: list,
            query,
            counter: 0,
            skipped: false,
            max_heap: None,
            min_heap: None,
        })
    }

    pub fn next(&mut self) -> Result<Option<Row>> {
        // Check limit
        if let Some(limit) = &self.query.limit {
            if self.counter >= limit.limit {
                return Ok(None);
            }

            // Skip offset
            if !self.skipped {
                for _ in 0..limit.offset {
                    self._next()?;
                }
            }

            self.skipped = true;
        }

        if let Some(aggregate) = self.query.aggregate.clone() {
            match aggregate {
                Aggregate::Count(col) => {
                    if col.as_str() != "*" {
                        return Err(
                            "Currently only supports * with count function in query function"
                                .into(),
                        );
                    }

                    let mut sum: i64 = 0;
                    while let Some(row) = self._next()? {
                        sum += row.get::<i64>(0)?;
                    }

                    if sum == 0 {
                        Ok(None)
                    } else {
                        Ok(Some(Row::new(vec![SqlValue::Integer(sum)])))
                    }
                }
            }
        } else {
            let next = self._next()?;
            self.counter += 1;

            Ok(next)
        }
    }

    fn _next(&mut self) -> Result<Option<Row>> {
        let next = if let Some(order_by) = self.query.order_by.clone() {
            self.next_with_order(order_by)?
        } else {
            self.next_without_order()?
        };

        Ok(next.map(|v| Row::new(v)))
    }

    fn next_without_order(&self) -> Result<Option<Vec<SqlValue>>> {
        for i in &self.sharding_index_list {
            loop {
                self.sharding_ite.send_data(*i, DataCall::RowsNext)?;
                match self.sharding_ite.ret_rx.recv()?.1 {
                    DataRet::Next(v) => match v {
                        Ok(v) => {
                            if let Some(v) = v {
                                return Ok(Some(v));
                            } else {
                                break;
                            }
                        }
                        Err(e) => return Err(e.into()),
                    },
                    m @ _ => {
                        return Err(
                            format!("Message mismatch in wait next_without_order: {:?}", m).into(),
                        )
                    }
                }
            }
        }

        Ok(None)
    }

    fn next_with_order(&mut self, order_by: OrderBy) -> Result<Option<Vec<SqlValue>>> {
        if order_by.is_asc {
            // Use min-heap
            if self.min_heap.is_none() {
                // Init heap
                let mut heap = BinaryHeap::new();
                for i in &self.sharding_index_list {
                    if let Some(data) = self.next_heap_data(*i, &order_by)? {
                        heap.push(Reverse(data));
                    }
                }
                self.min_heap = Some(heap);
            }

            if let Some(data) = self.min_heap.as_mut().unwrap().pop() {
                if let Some(data) = self.next_heap_data(data.0 .0, &order_by)? {
                    self.min_heap.as_mut().unwrap().push(Reverse(data));
                }

                return Ok(Some(data.0 .2));
            }
        } else {
            // Use max-heap
            if self.max_heap.is_none() {
                // Init heap
                let mut heap = BinaryHeap::new();
                for i in &self.sharding_index_list {
                    if let Some(data) = self.next_heap_data(*i, &order_by)? {
                        heap.push(data);
                    }
                }
                self.max_heap = Some(heap);
            }

            if let Some(data) = self.max_heap.as_mut().unwrap().pop() {
                if let Some(data) = self.next_heap_data(data.0, &order_by)? {
                    self.max_heap.as_mut().unwrap().push(data);
                }

                return Ok(Some(data.2));
            }
        }

        Ok(None)
    }

    fn next_index(&self, index: u32) -> Result<Option<Vec<SqlValue>>> {
        self.sharding_ite.send_data(index, DataCall::RowsNext)?;
        match self.sharding_ite.ret_rx.recv()?.1 {
            DataRet::Next(v) => match v {
                Ok(v) => {
                    if let Some(v) = v {
                        return Ok(Some(v));
                    } else {
                        return Ok(None);
                    }
                }
                Err(e) => return Err(e.into()),
            },
            m @ _ => {
                return Err(format!("Message mismatch in wait next_without_order: {:?}", m).into())
            }
        }
    }

    fn next_heap_data(&self, index: u32, order_by: &OrderBy) -> Result<Option<HeapData>> {
        if let Some(val) = self.next_index(index)? {
            let column_val =
                if let Some(SqlValue::Integer(n)) = val.get(order_by.column_index as usize) {
                    *n
                } else {
                    return Err("Order by column type must be integer".into());
                };
            return Ok(Some(HeapData(index, column_val, val)));
        }

        Ok(None)
    }
}

impl Drop for Rows<'_> {
    fn drop(&mut self) {
        for i in &self.sharding_index_list {
            self.sharding_ite.send_data(*i, DataCall::RowsEnd).ok();
        }
    }
}

pub struct MappedRows<'a, F> {
    rows: Rows<'a>,
    map: F,
}

impl<T, F> Iterator for MappedRows<'_, F>
where
    F: FnMut(&Row) -> Result<T>,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows
            .next()
            .transpose()
            .map(|row| Ok((self.map)(&row?)?))
    }
}

#[derive(Debug)]
pub struct Row {
    v: Vec<SqlValue>,
}

impl Row {
    fn new(v: Vec<SqlValue>) -> Self {
        Self { v }
    }

    pub fn get<T: rusqlite::types::FromSql>(&self, index: usize) -> Result<T> {
        let value = match self.v.get(index) {
            Some(v) => v,
            None => return Err(format!("Value of index {} not found", index).into()),
        };

        Ok(T::column_result(value.into())?)
    }
}
