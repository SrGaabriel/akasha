use std::collections::HashMap;
use std::sync::Arc;
use crate::table::heap::TableHeap;

pub mod heap;

#[derive(Clone)]
pub struct Schema {
    pub column_names: Vec<String>
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
    pub heap: Arc<TableHeap>,
}

pub struct TableCatalog {
    tables: HashMap<String, Table>,
}

impl TableCatalog {
    pub fn new() -> Self {
        Self { tables: HashMap::new() }
    }

    pub fn register_table(&mut self, name: &str, table: Table) {
        self.tables.insert(name.to_string(), table);
    }

    pub async fn get_table(&self, name: &str) -> Option<Table> {
        self.tables.get(name).cloned()
    }
}