use crate::page::tuple::{Tuple, Value};
use crate::query::Transaction;
use crate::query::op::TableOp;
use crate::query::stream::apply_ops;
use crate::table::heap::scan_table;
use crate::table::{ColumnInfo, TableCatalog, TableInfo};
use futures::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

pub type TupleStream = Pin<Box<dyn Stream<Item = Tuple> + Send + 'static>>;

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
                Ok(apply_ops(base_stream, ops))
            }
            Transaction::Insert {
                table,
                values,
                ops,
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

                if let Some(returning_columns) = returning {
                    let tuple_values: Vec<Value> = returning_columns
                        .iter()
                        .map(|idx| std::mem::replace(&mut tuple.0[*idx], Value::Null))
                        .collect();
                    let base_stream = Box::pin(futures::stream::iter(vec![Tuple(tuple_values)]));
                    Ok(apply_ops(base_stream, ops))
                } else {
                    Ok(Box::pin(futures::stream::iter(vec![])))
                }
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
