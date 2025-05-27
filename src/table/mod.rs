use crate::page::io::IoManager;
use crate::page::pool::BufferPool;
use crate::page::tuple::{DataType, Value};
use crate::table::heap::TableHeap;
use crate::table::interface::InternalTableInterface;
use std::collections::HashMap;
use std::sync::Arc;

pub mod heap;
mod interface;

#[derive(Clone, Debug)]
pub struct TableInfo {
    pub columns: HashMap<String, ColumnInfo>,
}

impl TableInfo {
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.keys().position(|k| k == name)
    }
}

#[derive(Clone, Debug)]
pub struct ColumnInfo {
    pub id: u32,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
}

#[derive(Clone, Debug)]
pub struct PhysicalTable {
    pub file_id: u32,
    pub name: String,
    pub heap: Arc<TableHeap>,
    pub info: TableInfo,
}

pub struct TableCatalog {
    pub internals: InternalTableInterface,
    pub buffer_pool: Arc<BufferPool>,
    pub tables: HashMap<String, PhysicalTable>,
}

impl TableCatalog {
    pub fn new(internals: InternalTableInterface, buffer_pool: Arc<BufferPool>) -> Self {
        TableCatalog {
            internals,
            buffer_pool,
            tables: HashMap::new(),
        }
    }

    pub async fn create_blank(
        io: Arc<IoManager>,
        buffer_pool: Arc<BufferPool>,
    ) -> Self {
        let internals = InternalTableInterface::create(buffer_pool.clone(), io).await;
        let internal_tables = internals.generate_physicals().await;

        let mut catalog = TableCatalog::new(internals, buffer_pool);
        for table in internal_tables {
            catalog.tables.insert(table.name.clone(), table);
        }

        catalog
    }

    pub async fn create_table(&mut self, name: String, info: TableInfo) -> std::io::Result<()> {
        if self.tables.contains_key(&name) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("Table '{}' already exists", name),
            ));
        }
        let file_id = self.tables.len() as u32;
        let heap = TableHeap::new(file_id, self.buffer_pool.clone());
        let physical = self.internals.save_table(heap.clone(), name.clone(), info.columns).await?;

        self.tables.insert(name, physical);
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&PhysicalTable> {
        self.tables.get(name)
    }

    pub async fn load(
        io: Arc<IoManager>,
        pool: Arc<BufferPool>,
    ) -> std::io::Result<Self> {
        let internals = InternalTableInterface::from_disk(pool.clone(), io).await?;
        let tables = internals.load_tables().await?;
        let internal_tables = internals.generate_physicals().await;

        let mut catalog = TableCatalog::new(internals, pool);
        for table in internal_tables {
            catalog.tables.insert(table.name.clone(), table);
        }
        catalog.tables.extend(tables);
        Ok(catalog)
    }
}