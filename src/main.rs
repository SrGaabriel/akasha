use tokio_stream::StreamExt;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::page::file::PageFileIO;
use crate::page::Page;
use crate::page::pool::BufferPool;
use crate::page::tuple::{Tuple, Value};
use crate::query::{Filter, Query};
use crate::query::exec::QueryExecutor;
use crate::query::planner::TemporaryQueryPlanner;
use crate::table::{Table, TableCatalog};

pub mod page;
pub mod query;
pub mod table;

#[tokio::main]
async fn main() {
    let select_query = Query {
        table: "users".to_string(),
        filter: Some(Filter::Eq("age".to_string(), Value::Int(30))),
        insert: None
    };

    let insert_query = Query {
        table: "users".to_string(),
        filter: None,
        insert: Some(vec![
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(30)),
        ].into_iter().collect())
    };

    let file_io = Arc::new(PageFileIO::new("akasha_dbs".to_string()));
    file_io.create_home().await.expect("Could not create home directory");
    let buffer_pool = Arc::new(RwLock::new(BufferPool::new(8, file_io)));
    let mut catalog = TableCatalog::new();
    catalog.create_table(
        "users".to_string(),
        vec!["name".to_string(), "age".to_string()],
        vec![None],
        buffer_pool
    ).await.unwrap();
    let catalog = Arc::new(RwLock::new(catalog));

    let planner = Box::new(TemporaryQueryPlanner);
    let executor = QueryExecutor::new(catalog, planner);

    let _ = executor.execute(insert_query).await.unwrap();
    let mut stream = executor.execute(select_query).await.unwrap();

    while let Some(tuple) = stream.next().await {
        println!("{:?}", tuple);
    }
}
