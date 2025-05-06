use std::pin::Pin;
use std::sync::Arc;
use futures::Stream;
use tokio::sync::RwLock;
use crate::page::tuple::Tuple;
use crate::query::err::{QueryError, QueryResult};
use crate::query::new_plan::compiler::{PlanCompiler, PlanExecutable};
use crate::query::new_plan::PlanNode;
use crate::table::heap::scan_table;
use crate::table::TableCatalog;
use tokio_stream::StreamExt;

pub type TupleStream = Pin<Box<dyn Stream<Item = Tuple> + Send + 'static>>;

pub struct QueryExecutor {
    catalog: Arc<RwLock<TableCatalog>>,
    plan_compiler: PlanCompiler,
}

impl QueryExecutor {
    pub fn new(catalog: Arc<RwLock<TableCatalog>>) -> Self {
        Self {
            catalog,
            plan_compiler: PlanCompiler::new(),
        }
    }

    pub async fn execute_plan(&mut self, plan: PlanNode) -> QueryResult<TupleStream> {
        let executable = self.plan_compiler.compile(&plan, Arc::new(self.catalog.read().await))?;
        self.execute_executable(executable).await
    }

    pub fn execute_executable(&self, executable: PlanExecutable) -> Pin<Box<dyn Future<Output = QueryResult<TupleStream>> + Send + '_>> {
        Box::pin(async move {
            match executable {
                PlanExecutable::TableScan { table, ops } => {
                    let table = self
                        .catalog
                        .read()
                        .await
                        .get_table(&table)
                        .await
                        .ok_or(QueryError::TableNotFound(table))?;
                    let iter = scan_table(table.heap).await;
                    let initial_stream: TupleStream = Box::pin(iter);

                    let final_stream = ops.into_iter().fold(initial_stream, |stream, op| op.apply(stream));
                    Ok(final_stream)
                },
                PlanExecutable::Filter { input, ops } => {
                    let input_stream = self.execute_executable(*input).await?;
                    let final_stream = ops.into_iter().fold(input_stream, |stream, op| op.apply(stream));
                    Ok(final_stream)
                },
                PlanExecutable::Projection { input, op } => {
                    let input_stream = self.execute_executable(*input).await?;
                    Ok(op.apply(input_stream))
                },
                PlanExecutable::Limit { input, ops } => {
                    let input_stream = self.execute_executable(*input).await?;
                    let final_stream = ops.into_iter().fold(input_stream, |stream, op| op.apply(stream));
                    Ok(final_stream)
                },
                PlanExecutable::Values(values) => {
                    let tuples = values.chunks(values.len())
                        .map(|chunk| Tuple { values: chunk.to_vec() })
                        .collect::<Vec<_>>();

                    let stream = futures::stream::iter(tuples);
                    let boxed_stream: Pin<Box<dyn Stream<Item = Tuple> + Send>> = Box::pin(stream);

                    Ok(boxed_stream)
                }
            }
        })
    }
}