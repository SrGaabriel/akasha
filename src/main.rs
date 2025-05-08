#![feature(let_chains)]

use crate::frontend::ast::{Arena, AstTraversal};
use crate::frontend::lexer::Lexer;
use crate::frontend::parser::parse_expression;
use crate::frontend::print::PrettyPrinter;
use crate::page::file::PageFileIO;
use crate::page::pool::BufferPool;
use crate::query::compiler::PlanCompiler;
use crate::query::exec::QueryExecutor;
use crate::query::optimizer::IdentityOptimizer;
use crate::query::transformer::AstToQueryTransformer;
use crate::table::{ColumnInfo, TableCatalog, TableInfo};
use std::collections::HashMap;
use std::sync::Arc;
use futures::StreamExt;
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
            let mut columns = HashMap::new();
            columns.insert("name".to_string(), ColumnInfo {
                data_type: page::tuple::DataType::Int,
                default: None,
                nullable: false,
            });
            columns.insert("age".to_string(), ColumnInfo {
                data_type: page::tuple::DataType::Int,
                default: None,
                nullable: false,
            });
            cat.create_table(
                "users".to_string(),
                TableInfo {
                    columns
                },
                buffer_pool.clone()
            ).await.expect("Failed to create table");

            cat.persist(home_dir).await.unwrap();
            cat
        }
    };
    let catalog = Arc::new(RwLock::new(catalog));
    let query_file = tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("queries/select.aka")
        .await
        .expect("Failed to open query file");

    let mut buffer = tokio::io::BufReader::new(query_file);
    let mut text = String::new();
    buffer.read_to_string(&mut text).await.expect("Failed to read query file");

    let mut lexer = Lexer::new(text.as_str());
    let lexed = lexer.tokenize().expect("Failed to lex query");
    println!("Lexed tokens: {:?}", lexed);

    let mut arena = Arena::with_capacity(1000, 100);
    let root_id = parse_expression(&*lexed, &mut arena).expect("Failed to parse expression");

    let traversal = AstTraversal::new(&arena);
    let mut printer = PrettyPrinter::new();
    traversal.visit(&mut printer, root_id);

    let mut transformer = AstToQueryTransformer::new(
        &arena,
        Box::new(IdentityOptimizer),
    );
    let transformed = transformer.transform(root_id).expect("Failed to transform AST");
    println!("Transformed AST: {:?}", transformed);

    let mut compiler = PlanCompiler::new();
    let catalog_lock = Arc::new(catalog.read().await);
    let compiled = compiler.compile(&transformed, catalog_lock).expect("Failed to compile plan");
    println!("Compiled: {:?}", compiled);

    let executor = QueryExecutor::new(catalog.clone());
    let mut plan = executor.execute(compiled).await.expect("Failed to execute plan");

    println!("Plan results: ");
    while let Some(tuple) = plan.next().await {
        println!("{:?}", tuple);
    }
}
