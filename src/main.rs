use crate::page::file::PageFileIO;
use crate::page::pool::BufferPool;
use crate::page::tuple::Value;
use crate::query::exec::QueryExecutor;
use crate::query::planner::TemporaryQueryPlanner;
use crate::query::{Filter, Query};
use crate::table::TableCatalog;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;

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

    let home_dir = "akasha_dbs".to_string();
    let file_io = Arc::new(PageFileIO::new(home_dir.clone()));
    file_io.create_home().await.expect("Could not create home directory");
    let buffer_pool = Arc::new(RwLock::new(BufferPool::new(8, file_io)));
    let catalog = match TableCatalog::load("akasha_dbs", Arc::clone(&buffer_pool)).await {
        Ok(cat) => {
            println!("Loaded catalog with {} tables", cat.tables.len());
            cat
        },
        Err(_) => {
            let mut cat = TableCatalog::new();
            cat.persist(home_dir).await.unwrap();
            cat
        }
    };
    let catalog = Arc::new(RwLock::new(catalog));

    let planner = Box::new(TemporaryQueryPlanner);
    let executor = QueryExecutor::new(catalog, planner);

    let _ = executor.execute(insert_query).await.unwrap();
    let mut stream = executor.execute(select_query).await.unwrap();

    while let Some(tuple) = stream.next().await {
        println!("Found one: {:?}", tuple);
    }
}
