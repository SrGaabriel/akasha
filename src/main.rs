#![feature(let_chains)]

use crate::page::file::PageFileIO;
use crate::page::pool::BufferPool;
use crate::page::tuple::{Tuple, Value};
use crate::query::exec::QueryExecutor;
use crate::table::TableCatalog;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use crate::query::{Columns, InsertQuery, Query, SelectQuery};
use crate::query::plan::planners::DefaultQueryPlanner;

pub mod page;
pub mod query;
pub mod table;

#[tokio::main]
async fn main() {
    let select_query = Query::Select(SelectQuery {
        from: "users".to_string(),
        columns: Columns::List(vec!["name".to_string(), "age".to_string()]),
        conditions: None,
        order_by: None,
        limit: None,
        offset: None
    });

    let insert_query = Query::Insert(InsertQuery {
        into: "users".to_string(),
        columns: vec!["name".to_string(), "age".to_string()],
        values: vec![Value::Text("Alice".to_string()), Value::Int(30)],
        returning: None,
    });

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
            let cat = TableCatalog::new();
            cat.persist(home_dir).await.unwrap();
            cat
        }
    };
    let catalog = Arc::new(RwLock::new(catalog));

    let planner = Box::new(DefaultQueryPlanner);
    let executor = QueryExecutor::new(catalog, planner);

    let _ = executor.execute(insert_query.clone()).await.unwrap();
    let _ = executor.execute(insert_query.clone()).await.unwrap();
    let _ = executor.execute(insert_query.clone()).await.unwrap();
    let _ = executor.execute(insert_query.clone()).await.unwrap();
    let _ = executor.execute(insert_query.clone()).await.unwrap();
    let _ = executor.execute(insert_query.clone()).await.unwrap();
    let _ = executor.execute(insert_query.clone()).await.unwrap();
    let _ = executor.execute(insert_query.clone()).await.unwrap();
    let _ = executor.execute(insert_query.clone()).await.unwrap();
    let _ = executor.execute(insert_query.clone()).await.unwrap();
    let _ = executor.execute(insert_query.clone()).await.unwrap();
    let _ = executor.execute(insert_query.clone()).await.unwrap();

    let stream = executor.execute(select_query).await.unwrap();
    let result: Vec<Tuple> = stream.collect().await;
    println!("Result: {:?}", result);
}
