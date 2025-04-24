use std::collections::HashMap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use crate::page::Page;
use crate::page::pool::BufferPool;
use crate::page::tuple::Value;
use crate::table::heap::TableHeap;

pub mod heap;

#[derive(Clone)]
pub struct Schema {
    pub column_names: Vec<String>,
    pub column_defaults: Vec<Option<Value>>
}

impl Schema {
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.column_names.iter().position(|n| n == name)
    }
}

#[derive(Clone)]
pub struct Table {
    pub file_id: u32,
    pub name: String,
    pub schema: Schema,
    pub heap: Arc<RwLock<TableHeap>>,
}

pub struct TableCatalog {
    pub(crate) tables: HashMap<String, Table>,
}

impl TableCatalog {
    pub fn new() -> Self {
        Self { tables: HashMap::new() }
    }

    pub async fn create_table(
        &mut self,
        name: String,
        columns: Vec<String>,
        defaults: Vec<Option<Value>>,
        buffer_pool: Arc<RwLock<BufferPool>>,
    ) -> Result<(), String> {
        if self.tables.contains_key(&name) {
            return Err("Table already exists".to_string());
        }

        let file_id = self.tables.len() as u32;

        let pool = buffer_pool.read().await;
        let new_page_id = pool.frames.len() as u32; // naive strategy
        let new_page = Page::new(new_page_id);
        pool.file_accessor.write_page(file_id, &new_page).await.unwrap();
        drop(pool);

        let heap = Arc::new(RwLock::new(TableHeap {
            file_id,
            buffer_pool: Arc::clone(&buffer_pool),
            page_ids: vec![new_page_id],
        }));

        let schema = Schema {
            column_names: columns.clone(),
            column_defaults: defaults
        };

        let entry = Table {
            file_id,
            name: name.clone(),
            schema,
            heap: Arc::clone(&heap),
        };

        self.tables.insert(name.clone(), entry);

        Ok(())
    }

    pub fn register_table(&mut self, name: &str, table: Table) {
        self.tables.insert(name.to_string(), table);
    }

    pub async fn get_table(&self, name: &str) -> Option<Table> {
        self.tables.get(name).cloned()
    }

    pub async fn persist(&self, base_path: String) -> Result<(), String> {
        let tables: Vec<TableMetadata> = self.tables.values().map(|t| TableMetadata {
            name: t.name.clone(),
            file_id: t.file_id,
            column_names: t.schema.column_names.clone(),
            column_defaults: t.schema.column_defaults.clone(),
        }).collect();

        let json = serde_json::to_string_pretty(&tables).map_err(|e| e.to_string())?;
        let path = format!("{}/catalog.json", base_path);
        let mut file = tokio::fs::File::create(path).await.map_err(|e| e.to_string())?;
        file.write_all(json.as_bytes()).await.map_err(|e| e.to_string())
    }

    pub async fn load(base_path: &str, buffer_pool: Arc<RwLock<BufferPool>>) -> Result<Self, String> {
        let path = format!("{}/catalog.json", base_path);
        let content = tokio::fs::read_to_string(path).await.map_err(|e| e.to_string())?;
        let entries: Vec<TableMetadata> = serde_json::from_str(&content).map_err(|e| e.to_string())?;

        let mut catalog = TableCatalog::new();

        for entry in entries {
            let heap = Arc::new(RwLock::new(TableHeap {
                file_id: entry.file_id,
                buffer_pool: Arc::clone(&buffer_pool),
                page_ids: vec![0],
            }));

            let table = Table {
                file_id: entry.file_id,
                name: entry.name.clone(),
                schema: Schema {
                    column_names: entry.column_names,
                    column_defaults: entry.column_defaults,
                },
                heap,
            };

            catalog.register_table(&entry.name, table);
        }

        Ok(catalog)
    }
}

// todo: make this a table
#[derive(Serialize, Deserialize)]
struct TableMetadata {
    name: String,
    file_id: u32,
    column_names: Vec<String>,
    column_defaults: Vec<Option<Value>>,
}