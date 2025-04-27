use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use futures::Stream;
use crate::page::tuple::Tuple;
use crate::query::err::{QueryError, QueryResult};
use crate::query::Query;
use crate::table::{Table, TableCatalog};
use crate::query::plan::{QueryPlanner, TableOp, PlanResult};
use crate::table::heap::scan_table;

pub type TupleStream = Pin<Box<dyn Stream<Item = Tuple> + Send + 'static>>;

pub struct QueryExecutor {
    catalog: Arc<RwLock<TableCatalog>>,
    planner: Box<dyn QueryPlanner>,
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
        plan_result: PlanResult,
    ) -> QueryResult<TupleStream> {
        match plan_result {
            PlanResult::Stream(ops) => {
                let iter = scan_table(table.heap.clone()).await;
                let initial_stream: TupleStream = Box::pin(iter);
                let final_stream = ops.into_iter().fold(initial_stream, |stream, op| op.apply(stream));
                Ok(final_stream)
            },
            PlanResult::ModifyData { ops, returning } => {
                let mut inserted_tuples = Vec::new();

                for op in ops {
                    match op {
                        TableOp::Insert(values) => {
                            let tuple = Tuple { values };
                            let heap = table.heap.clone();
                            let mut heap_lock = heap.write().await;
                            heap_lock.insert_tuple(&tuple).await.unwrap();

                            if returning.is_some() {
                                inserted_tuples.push(tuple);
                            }
                        },
                        _ => return Err(QueryError::InvalidOperation(
                            "Only INSERT operations are supported in ModifyData".to_string()
                        )),
                    }
                }

                if let Some(columns) = returning {
                    if !columns.is_empty() {
                        let tuples = inserted_tuples
                            .into_iter()
                            .map(move |tuple| {
                                let projected_values = columns.iter()
                                    .map(|&idx| tuple.values[idx].clone())
                                    .collect();
                                Tuple { values: projected_values }
                            });
                        Ok(Box::pin(futures::stream::iter(tuples)))
                    } else {
                        Ok(Box::pin(futures::stream::iter(inserted_tuples)))
                    }
                } else {
                    Ok(Box::pin(futures::stream::empty()))
                }
            }
        }
    }

    pub async fn execute(&self, query: Query) -> QueryResult<impl Stream<Item = Tuple> + Send + '_> {
        let table_name = match &query {
            Query::Select(select) => &select.from,
            Query::Insert(insert) => &insert.into,
            Query::Update(update) => &update.table,
            Query::Delete(delete) => &delete.table,
        };

        let catalog = self.catalog.read().await;
        let table = catalog
            .get_table(table_name)
            .await
            .ok_or(QueryError::TableNotFound(table_name.clone()))?;
        drop(catalog);

        let plan_result = self.planner.plan(&table, &query)?;
        self.execute_ops(&table, plan_result).await
    }
}