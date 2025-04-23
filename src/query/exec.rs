use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::query::planner::{QueryPlanner, TableOp};
use crate::query::Query;
use crate::table::heap::{scan_table, TableHeapIterator};
use crate::table::TableCatalog;
use tokio_stream::Stream;
use crate::page::tuple::Tuple;
use crate::query::err::{QueryError, QueryResult};

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


    pub fn execute_ops(
        &self,
        table_iter: TableHeapIterator,
        ops: Vec<TableOp>,
    ) -> QueryResult<TupleStream> {
        let initial_stream: Pin<Box<dyn Stream<Item = Tuple> + Send>> = Box::pin(table_iter);
        let final_stream = ops.into_iter().fold(initial_stream, |stream, op| op.apply(stream));
        Ok(final_stream)
    }

    pub async fn execute(&self, query: Query) -> QueryResult<impl Stream<Item = Tuple> + Send + '_> {
        let catalog = self.catalog.read().await;
        let table = catalog
            .get_table(&query.table)
            .await
            .ok_or(QueryError::TableNotFound(query.table.clone()))?;
        drop(catalog);
        let ops = self.planner.plan(&table, &query);
        let iter = scan_table(table.heap.clone());

        self.execute_ops(iter, ops)
    }
}