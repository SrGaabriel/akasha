use std::collections::HashMap;
use std::sync::Arc;
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
    pub name: String,
    pub schema: Schema,
    pub heap: Arc<RwLock<TableHeap>>,
}

pub struct TableCatalog {
    tables: HashMap<String, Table>,
}

impl TableCatalog {
    pub fn new() -> Self {
        Self { tables: HashMap::new() }
    }

    pub async fn create_table(
        &mut self,
        name: String,
        columns: Vec<String>, // todo: add types
        defaults: Vec<Option<Value>>,
        buffer_pool: Arc<RwLock<BufferPool>>,
    ) -> Result<(), String> {
        if self.tables.contains_key(&name) {
            return Err("Table already exists".to_string());
        }

        let file_id = self.tables.len() as u32;

        let mut pool = buffer_pool.write().await;
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
}