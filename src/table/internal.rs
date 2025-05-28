use std::collections::HashMap;
use std::sync::Arc;
use crate::page::io::IoManager;
use crate::page::pool::BufferPool;
use crate::page::tuple::{DataType, Tuple, Value};
use crate::table::heap::{scan_table, TableHeap};
use crate::table::{ColumnInfo, PhysicalTable, TableInfo};
use futures::StreamExt;

pub const RELATIONS_TABLE_ID: u32 = 0;
pub const COLUMNS_TABLE_ID: u32 = 1;

pub const RELATIONS_TABLE_ID_INDEX: usize = 0;
pub const RELATIONS_TABLE_NAME_INDEX: usize = 1;

pub const COLUMNS_TABLE_ID_INDEX: usize = 0;
pub const COLUMNS_TABLE_TABLE_ID_INDEX: usize = 1;
pub const COLUMNS_TABLE_NAME_INDEX: usize = 2;
pub const COLUMNS_TABLE_TYPE_INDEX: usize = 3;
pub const COLUMNS_TABLE_NULLABLE_INDEX: usize = 4;
pub const COLUMNS_TABLE_DEFAULT_INDEX: usize = 5;

pub struct InternalTableInterface {
    pub pool: Arc<BufferPool>,
    pub io: Arc<IoManager>,
    relations_table: Arc<TableHeap>,
    columns_table: Arc<TableHeap>,
}

impl InternalTableInterface {
    pub async fn from_disk(pool: Arc<BufferPool>, io: Arc<IoManager>) -> std::io::Result<Self> {
        let relations_table = load_table_heap(RELATIONS_TABLE_ID, io.clone(), pool.clone()).await?;
        let columns_table = load_table_heap(COLUMNS_TABLE_ID, io.clone(), pool.clone()).await?;

        Ok(InternalTableInterface { pool, io, relations_table, columns_table })
    }

    pub async fn init_internals(
        pool: Arc<BufferPool>,
        io: Arc<IoManager>,
    ) {
        let relations_table = TableHeap::new(RELATIONS_TABLE_ID, pool.clone());
        let columns_table = TableHeap::new(COLUMNS_TABLE_ID, pool.clone());

        let interface = InternalTableInterface {
            pool,
            io,
            relations_table: relations_table.clone(),
            columns_table: columns_table.clone(),
        };

        println!("Creating internal tables...");
        interface.save_table(relations_table, "akasha.relations".to_string(), relations_table_columns()).await.expect("Failed to save relations table");
        interface.save_table(columns_table, "akasha.columns".to_string(), columns_table_columns()).await.expect("Failed to save columns table");
    }

    pub async fn load_tables(&self) -> std::io::Result<HashMap<String, PhysicalTable>> {
        let table_iterator = scan_table(self.relations_table.clone()).await;
        let column_iterator = scan_table(self.columns_table.clone()).await;

        let column_tuples: Vec<(u32, ColumnInfo)> = column_iterator
            .filter_map(|tuple| async move {
                let column_id = tuple.0[COLUMNS_TABLE_ID_INDEX].as_int().unwrap() as u32;
                let table_id: u32 = tuple.0[COLUMNS_TABLE_TABLE_ID_INDEX].as_int().unwrap() as u32;
                let name: String = tuple.0[COLUMNS_TABLE_NAME_INDEX].as_string().unwrap();
                let data_type: DataType = DataType::from_id(tuple.0[COLUMNS_TABLE_TYPE_INDEX].as_byte().unwrap())
                    .expect("Invalid data type");
                let nullable: bool = tuple.0[COLUMNS_TABLE_NULLABLE_INDEX].as_boolean().unwrap();
                let default = tuple.0.get(COLUMNS_TABLE_DEFAULT_INDEX).cloned();

                let column_info = ColumnInfo {
                    id: column_id,
                    name,
                    data_type,
                    nullable,
                    default
                };

                Some((table_id, column_info))
            })
            .collect()
            .await;

        let mut columns: HashMap<u32, Vec<ColumnInfo>> = HashMap::new();
        for (table_id, column_info) in column_tuples {
            columns.entry(table_id).or_insert_with(Vec::new).push(column_info);
        }

        let table_tuples: Vec<Tuple> = table_iterator.collect().await;
        let mut tables = HashMap::new();

        for tuple in table_tuples {
            let id: u32 = tuple.0[RELATIONS_TABLE_ID_INDEX].as_int().unwrap() as u32;
            let name: String = tuple.0[RELATIONS_TABLE_NAME_INDEX].as_string().unwrap();
            let heap = self.load_table_heap(id).await?;

            let columns_map: HashMap<String, ColumnInfo> = columns
                .get(&id)
                .map(|cols| cols.iter().map(|col| (col.name.clone(), col.clone())).collect())
                .unwrap_or_default();

            let physical = PhysicalTable {
                file_id: id,
                name: name.clone(),
                heap,
                info: TableInfo {
                    columns: columns_map,
                },
            };

            tables.insert(name, physical);
        }

        Ok(tables)
    }

    pub async fn save_table(&self, heap: Arc<TableHeap>, name: String, columns: HashMap<String, ColumnInfo>) -> std::io::Result<PhysicalTable> {
        heap.init().await;
        let mut column_rows: Vec<Tuple> = Vec::new();
        for column in columns.values() {
            let tuple = Tuple(vec![
                Value::Int(column.id as i32), // COLUMNS_TABLE_ID_INDEX
                Value::Int(heap.file_id as i32), // COLUMNS_TABLE_TABLE_ID_INDEX
                Value::Text(column.name.clone()), // COLUMNS_TABLE_NAME_INDEX
                Value::Byte(column.data_type.id()), // COLUMNS_TABLE_TYPE_INDEX
                Value::Boolean(column.nullable), // COLUMNS_TABLE_NULLABLE_INDEX
            ]);
            column_rows.push(tuple);
        }

        let column_heap = self.columns_table.clone();
        for tuple in column_rows {
            column_heap.insert_tuple(&tuple).await.expect("Failed to insert column tuple");
        }

        let relation_tuple = Tuple(vec![
            Value::Int(heap.file_id as i32), // RELATIONS_TABLE_ID_INDEX
            Value::Text(name.clone()), // RELATIONS_TABLE_NAME_INDEX
        ]);
        self.relations_table.insert_tuple(&relation_tuple).await.expect("Failed to insert relation tuple");

        Ok(PhysicalTable {
            file_id: heap.file_id,
            name,
            heap,
            info: TableInfo { columns },
        })
    }

    async fn load_table_heap(&self, file_id: u32) -> std::io::Result<Arc<TableHeap>> {
        load_table_heap(file_id, self.io.clone(), self.pool.clone()).await
    }
}

async fn load_table_heap(file_id: u32, io: Arc<IoManager>, buffer_pool: Arc<BufferPool>) -> std::io::Result<Arc<TableHeap>> {
    TableHeap::from_existing(file_id, buffer_pool, io)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
}

fn relations_table_columns() -> HashMap<String, ColumnInfo> {
    HashMap::from([
        ("id".to_string(), ColumnInfo {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            default: None,
        }),
        ("name".to_string(), ColumnInfo {
            id: 1,
            name: "name".to_string(),
            data_type: DataType::Text,
            nullable: false,
            default: None,
        }),
    ])
}

fn columns_table_columns() -> HashMap<String, ColumnInfo> {
    HashMap::from([
        ("id".to_string(), ColumnInfo {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            default: None,
        }),
        ("table_id".to_string(), ColumnInfo {
            id: 1,
            name: "table_id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            default: None,
        }),
        ("name".to_string(), ColumnInfo {
            id: 2,
            name: "name".to_string(),
            data_type: DataType::Text,
            nullable: false,
            default: None,
        }),
        ("type".to_string(), ColumnInfo {
            id: 3,
            name: "type".to_string(),
            data_type: DataType::Byte,
            nullable: false,
            default: None,
        }),
        ("nullable".to_string(), ColumnInfo {
            id: 4,
            name: "nullable".to_string(),
            data_type: DataType::Boolean,
            nullable: false,
            default: None,
        }),
    ])
}