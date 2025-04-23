use crate::page::tuple::Tuple;
use crate::query::err::{QueryError, QueryResult};
use crate::query::planner::{QueryPlanner, TableOp};
use crate::query::Query;
use crate::table::heap::scan_table;
use crate::table::{Table, TableCatalog};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_stream::Stream;

pub type TupleStream = Pin<Box<dyn Stream<Item = Tuple> + Send>>;

pub struct QueryExecutor {
    catalog: Arc<RwLock<TableCatalog>>,
    planner: Box<dyn QueryPlanner>
}

impl QueryExecutor {
    pub fn new(
        catalog: Arc<RwLock<TableCatalog>>,
        planner: Box<dyn QueryPlanner>
    ) -> Self {
        Self {catalog, planner}
    }

    pub async fn execute_ops(
        &self,
        table: &Table,
        ops: Vec<TableOp>,
    ) -> QueryResult<TupleStream> {
        let mut stream_ops = vec![];

        for op in ops {
            match op {
                TableOp::Insert(values) => {
                    let tuple = Tuple { values };
                    let heap = table.heap.clone();
                    let mut heap_lock = heap.write().await;
                    heap_lock.insert_tuple(&tuple).await.unwrap();
                }
                _ => stream_ops.push(op),
            }
        }

        let iter = scan_table(table.heap.clone()).await;
        let initial_stream: TupleStream = Box::pin(iter);
        let final_stream = stream_ops.into_iter().fold(initial_stream, |stream, op| op.apply(stream));
        Ok(final_stream)
    }

    pub async fn execute(&self, query: Query) -> QueryResult<impl Stream<Item = Tuple> + Send + '_> {
        let catalog = self.catalog.read().await;
        let table = catalog
            .get_table(&query.table)
            .await
            .ok_or(QueryError::TableNotFound(query.table.clone()))?;
        drop(catalog);
        let ops = self.planner.plan(&table, &query)?;
        self.execute_ops(&table, ops).await
    }
}