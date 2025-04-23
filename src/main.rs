use tokio_stream::StreamExt;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::page::Page;
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
    let mut page = Page::new(0);

    let tup = Tuple {
        values: vec![
            Value::Int(42),
            Value::Bool(true),
            Value::String("hello".into()),
        ],
    };

    let id = page.insert_tuple(&tup).unwrap();
    let out = page.get_tuple(id).unwrap();

    let query = Query {
        table: "users".to_string(),
        filter: Some(Filter::Eq("age".to_string(), Value::Int(30))),
    };

    let catalog = Arc::new(RwLock::new(TableCatalog::new()));

    let planner = Box::new(TemporaryQueryPlanner);
    let executor = QueryExecutor::new(catalog, planner);
    let mut stream = executor.execute(query).await.unwrap();

    while let Some(tuple) = stream.next().await {
        println!("{:?}", tuple);
    }


    dbg!(out);
}
