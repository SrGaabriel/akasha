use crate::page::Page;
use crate::page::io::IoManager;
use crate::page::pool::BufferPool;
use crate::page::tuple::{DataType, Value};
use crate::table::heap::TableHeap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

pub mod heap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableInfo {
    pub columns: HashMap<String, ColumnInfo>,
}

impl TableInfo {
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.keys().position(|k| k == name)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub data_type: DataType,
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
}

#[derive(Clone)]
pub struct PhysicalTable {
    pub file_id: u32,
    pub name: String,
    pub heap: Arc<TableHeap>,
    pub info: TableInfo,
}

pub struct TableCatalog {
    pub buffer_pool: Arc<BufferPool>,
    pub tables: HashMap<String, PhysicalTable>,
}

impl TableCatalog {
    pub fn new(buffer_pool: Arc<BufferPool>) -> Self {
        TableCatalog {
            buffer_pool,
            tables: HashMap::new(),
        }
    }

    pub async fn create_table(&mut self, name: String, info: TableInfo) -> Result<(), String> {
        if self.tables.contains_key(&name) {
            return Err("exists".into());
        }
        let file_id = self.tables.len() as u32;
        let heap = TableHeap::new(file_id, self.buffer_pool.clone());
        let ptr = self.buffer_pool.get_page(file_id, 0).await;
        let mut page = unsafe { Page::from_raw(0, ptr) };
        page.init_new();
        self.buffer_pool.unpin_and_flush(file_id, 0, true).await;
        self.tables.insert(name.clone(), PhysicalTable {
            file_id,
            name,
            heap,
            info,
        });
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&PhysicalTable> {
        self.tables.get(name)
    }

    pub async fn persist(&self, base: &str) -> std::io::Result<()> {
        let meta: Vec<_> = self
            .tables
            .values()
            .map(|t| TableMetadata {
                name: t.name.clone(),
                file_id: t.file_id,
                info: t.info.clone(),
            })
            .collect();
        let s = serde_json::to_string_pretty(&meta)?;
        let mut f = tokio::fs::File::create(format!("{}/catalog.json", base)).await?;
        f.write_all(s.as_bytes()).await?;
        f.flush().await
    }

    pub async fn load(
        base: &str,
        pool: Arc<BufferPool>,
        io: Arc<IoManager>,
    ) -> std::io::Result<Self> {
        println!("Loading catalog from {}", base);
        let s = tokio::fs::read_to_string(format!("{}/catalog.json", base)).await?;
        let entries: Vec<TableMetadata> = serde_json::from_str(&s)?;
        let mut cat = TableCatalog::new(pool.clone());
        for entry in entries {
            let file_id = entry.file_id;
            let heap = TableHeap::from_existing(file_id, pool.clone(), io.clone())
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            let table = PhysicalTable {
                file_id,
                name: entry.name.clone(),
                heap,
                info: entry.info,
            };
            cat.tables.insert(entry.name, table);
        }
        Ok(cat)
    }
}

// todo: make this a table
#[derive(Serialize, Deserialize)]
struct TableMetadata {
    name: String,
    file_id: u32,
    info: TableInfo,
}
