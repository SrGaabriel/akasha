use crate::page::tuple::{Tuple, Value};
use crate::query::Transaction;
use crate::query::op::TableOp;
use crate::query::stream::{apply_ops, map_inner};
use crate::table::heap::{scan_table, IndexedTuple};
use crate::table::{ColumnInfo, TableCatalog, TableInfo};
use futures::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::StreamExt;
use crate::util::trees::IndexedList;

pub type TupleStream = Pin<Box<dyn Stream<Item = IndexedTuple> + Send + 'static>>;

pub struct QueryExecutor {
    catalog: Arc<TableCatalog>,
}

pub enum PlanResult {
    Stream(Vec<TableOp>),
    ModifyData {
        ops: Vec<TableOp>,
        returning: Option<Vec<usize>>,
    },
}

impl QueryExecutor {
    pub fn new(catalog: Arc<TableCatalog>) -> Self {
        Self { catalog }
    }

    pub async fn execute(
        &self,
        transaction: Transaction,
    ) -> Result<Pin<Box<dyn Stream<Item = Tuple> + Send>>, String> {
        match transaction {
            Transaction::Select { table, ops } => {
                let physical_table = self
                    .catalog
                    .get_table(&table)
                    .ok_or_else(|| format!("Table '{}' not found", table))?;
                let heap = physical_table.heap.clone();
                let base_stream = scan_table(heap).await;
                Ok(map_inner(apply_ops(base_stream, ops)))
            }
            Transaction::Insert {
                table,
                values,
                pos_ops: ops,
                returning,
            } => {
                let physical_table = self
                    .catalog
                    .get_table(&table)
                    .ok_or_else(|| format!("Table '{}' not found", table))?;
                let mut tuple = Self::build_tuple(&physical_table.info, values)?;
                let heap = physical_table.heap.clone();
                heap.insert_tuple(&tuple)
                    .await
                    .map_err(|e| format!("Insert failed: {}", e))?;

                if returning.is_empty() {
                    Ok(Box::pin(futures::stream::iter(vec![])))
                } else {
                    let tuple_values: Vec<Value> = returning
                        .iter()
                        .map(|idx| std::mem::replace(&mut tuple.0[*idx], Value::Null))
                        .collect();
                    let base_stream = Box::pin(futures::stream::iter(vec![
                        IndexedTuple {
                            inner: Tuple(tuple_values),
                            slot_id: 0,
                            page_id: 0
                        }
                    ]));
                    Ok(map_inner(apply_ops(base_stream, ops)))
                }
            },
            Transaction::Update {
                table,
                values,
                pre_ops,
            } => {
                let physical_table = self
                    .catalog
                    .get_table(&table)
                    .ok_or_else(|| format!("Table '{}' not found", table))?;
                let heap = physical_table.heap.clone();
                let base_stream = scan_table(heap.clone()).await;
                let filtered_stream = apply_ops(base_stream, pre_ops);

                let indexed_values = IndexedList::from_vec(values
                    .into_iter()
                    .filter_map(|(col_id, val)| {
                        physical_table
                            .info
                            .columns
                            .values()
                            .find(|col| col.id == col_id)
                            .map(|col| (col.id, val))
                    })
                    .collect()); // todo: optimize

                let mut update_futures = Vec::new();
                let mut idx = 0;
                let mut stream = filtered_stream;
                while let Some(tuple) = stream.next().await {
                    let heap_clone = heap.clone();
                    let values = indexed_values.clone();
                    update_futures.push(tokio::spawn(async move {
                        heap_clone
                            .update_tuple(tuple.page_id, tuple.slot_id, values)
                            .await
                    }));
                    idx += 1;
                }

                for fut in update_futures {
                    fut.await.map_err(|e| format!("Update task failed: {}", e))??;
                }

                Ok(Box::pin(futures::stream::iter(vec![])))
            }
        }
    }

    fn build_tuple(table_info: &TableInfo, values: Vec<(u32, Value)>) -> Result<Tuple, String> {
        let mut value_map: HashMap<u32, Value> = values.into_iter().collect();
        let mut columns: Vec<&ColumnInfo> = table_info.columns.values().collect();
        columns.sort_by_key(|col| col.id);

        let mut tuple_values = Vec::new();
        for col in columns.drain(..) {
            if let Some(val) = value_map.remove(&col.id) {
                tuple_values.push(Value::from(val));
            } else if let Some(default) = &col.default {
                tuple_values.push(default.clone());
            } else if col.nullable {
                tuple_values.push(Value::Null);
            } else {
                return Err(format!(
                    "Missing value for column without defaults '{}'",
                    col.name
                ));
            }
        }
        Ok(Tuple(tuple_values))
    }
}
