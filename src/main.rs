#![feature(let_chains)]

use crate::frontend::ast::{Arena, AstTraversal};
use crate::frontend::lexer::Lexer;
use crate::frontend::parser::parse_expression;
use crate::frontend::print::PrettyPrinter;
use crate::page::file::PageFileIO;
use crate::page::pool::BufferPool;
use crate::query::exec::QueryExecutor;
use crate::query::plan::planners::DefaultQueryPlanner;
use crate::table::TableCatalog;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;

pub mod page;
pub mod query;
pub mod table;
pub mod frontend;

#[tokio::main]
async fn main() {
    let home_dir = "db".to_string();
    let file_io = Arc::new(PageFileIO::new(home_dir.clone()));
    file_io.create_home().await.expect("Could not create home directory");
    let buffer_pool = Arc::new(RwLock::new(BufferPool::new(8, file_io)));
    let catalog = match TableCatalog::load("db", Arc::clone(&buffer_pool)).await {
        Ok(cat) => {
            println!("Loaded catalog with {} tables", cat.tables.len());
            cat
        },
        Err(_) => {
            let mut cat = TableCatalog::new();
            cat.create_table(
                "users".to_string(),
                vec!["name".to_string(), "age".to_string()],
                vec![],
                buffer_pool.clone(),
            ).await.unwrap();
            cat.persist(home_dir).await.unwrap();
            cat
        }
    };
    let catalog = Arc::new(RwLock::new(catalog));

    let planner = Box::new(DefaultQueryPlanner);
    let executor = QueryExecutor::new(catalog, planner);

    let query_file = tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("queries/test.aka")
        .await
        .expect("Failed to open query file");

    let mut buffer = tokio::io::BufReader::new(query_file);
    let mut text = String::new();
    buffer.read_to_string(&mut text).await.expect("Failed to read query file");

    let mut lexer = Lexer::new(text.as_str());
    let lexed = lexer.tokenize().expect("Failed to lex query");

    let mut arena = Arena::with_capacity(1000, 100);
    let root_id = parse_expression(&*lexed, &mut arena).expect("Failed to parse expression");

    let traversal = AstTraversal::new(&arena);
    let mut printer = PrettyPrinter::new();

    traversal.visit(&mut printer, root_id);
}
