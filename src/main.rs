#![feature(let_chains)]

use crate::frontend::ast::Arena;
use crate::frontend::lexer::Lexer;
use crate::frontend::parser::parse_expression;
use crate::page::pool::BufferPool;
use crate::query::compiler::PlanCompiler;
use crate::query::exec::QueryExecutor;
use crate::query::optimizer::IdentityOptimizer;
use crate::query::transformer::AstToQueryTransformer;
use crate::table::{ColumnInfo, TableCatalog, TableInfo};
use futures::StreamExt;
use std::collections::HashMap;
use std::env;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;
use crate::page::io::{IoManager, FileSystemManager};
use crate::page::tuple::Tuple;

pub mod page;
pub mod query;
pub mod table;
pub mod frontend;

struct DebugTimer {
    name: String,
    start: Instant,
    debug_mode: bool,
}

impl DebugTimer {
    #[inline(always)]
    fn new(name: &str, debug_mode: bool) -> Self {
        let timer = Self {
            name: name.to_string(),
            start: Instant::now(),
            debug_mode,
        };

        if debug_mode {
            println!("Starting: {}", name);
        }

        timer
    }

    #[inline(always)]
    fn elapsed(&self) -> String {
        format!("{:.4?}", self.start.elapsed())
    }
}

impl Drop for DebugTimer {
    fn drop(&mut self) {
        if self.debug_mode {
            println!("Completed: {} in {}", self.name, self.elapsed());
        }
    }
}

struct QueryEngine {
    buffer_pool: Arc<BufferPool>,
    catalog: Arc<RwLock<TableCatalog>>,
    compiler: PlanCompiler,
    arena: Arena,
    executor: QueryExecutor,
    debug_mode: bool,
}

impl QueryEngine {
    async fn new(debug_mode: bool) -> Result<Self, Box<dyn std::error::Error>> {
        let init_timer = DebugTimer::new("Database initialization", debug_mode);

        let home_dir = "db".to_string();
        let file_io = Arc::new(FileSystemManager::new(home_dir.clone()));
        file_io.create_home().await?;

        let io = Arc::new(IoManager::new(file_io.clone()));
        let buffer_pool = BufferPool::new(io.clone());

        let catalog = match TableCatalog::load("db", Arc::clone(&buffer_pool), io).await {
            Ok(cat) => {
                println!("Loaded catalog with {} tables", cat.tables.len());
                cat
            },
            Err(err) => {
                eprintln!("Error loading catalog: {}", err);
                let mut cat = TableCatalog::new(buffer_pool.clone());
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
                    TableInfo { columns }
                ).await?;
                cat.persist(&*home_dir).await?;
                cat
            }
        };

        let catalog = Arc::new(RwLock::new(catalog));
        drop(init_timer);

        Ok(Self {
            buffer_pool,
            catalog: catalog.clone(),
            compiler: PlanCompiler::new(),
            arena: Arena::with_capacity(10000, 1000),
            executor: QueryExecutor::new(catalog),
            debug_mode,
        })
    }

    async fn execute_query_file(&mut self, file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        println!("Executing query from file: {}", file_path);

        let file_timer = DebugTimer::new("File loading", self.debug_mode);
        let query_file = tokio::fs::OpenOptions::new()
            .read(true)
            .open(file_path)
            .await?;

        let mut buffer = tokio::io::BufReader::new(query_file);
        let mut text = String::new();
        buffer.read_to_string(&mut text).await?;
        drop(file_timer);

        let total_timer = DebugTimer::new("Total query execution", self.debug_mode);

        let lexer_timer = DebugTimer::new("Lexical analysis", self.debug_mode);
        let mut lexer = Lexer::new(text.as_str());
        let lexed = lexer.tokenize()?;
        drop(lexer_timer);

        self.arena.clear();

        let parse_timer = DebugTimer::new("Parsing", self.debug_mode);
        let root_id = parse_expression(&*lexed, &mut self.arena).unwrap();
        drop(parse_timer);

        let transform_timer = DebugTimer::new("AST transformation", self.debug_mode);
        let mut transformer = AstToQueryTransformer::new(
            &self.arena,
            Box::new(IdentityOptimizer),
        );
        let transformed = transformer.transform(root_id)?;
        drop(transform_timer);

        let compile_timer = DebugTimer::new("Query compilation", self.debug_mode);
        let catalog_lock = Arc::new(self.catalog.read().await);
        let compiled = self.compiler.compile(&transformed, catalog_lock).unwrap();
        drop(compile_timer);

        let execute_timer = DebugTimer::new("Query execution", self.debug_mode);
        let mut plan = self.executor.execute(&compiled).await?;
        let tuples = plan.collect::<Vec<Tuple>>().await;

        let execution_elapsed = execute_timer.elapsed();
        let total_elapsed = total_timer.elapsed();

        println!("Executed: {:?}", compiled);
        println!("{:?} Query executed in {} and completed in {}", tuples.len(), execution_elapsed, total_elapsed);

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let debug_mode = args.len() > 1 && args[1] == "--debug";

    if debug_mode {
        println!("Debug mode enabled - detailed timing information will be shown");
    }

    let mut engine = QueryEngine::new(debug_mode).await?;

    let queries_dir = Path::new("queries");
    if !queries_dir.exists() {
        std::fs::create_dir_all(queries_dir)?;
    }

    println!("Query CLI started");
    println!("Available commands:");
    println!("  <filename>  - Execute a query file from /queries");
    println!("  :list       - List all available query files");
    println!("  :q          - Quit the program");

    let mut input = String::with_capacity(100);

    loop {
        input.clear();

        print!("> ");
        io::stdout().flush()?;

        io::stdin().read_line(&mut input)?;
        let input_str = input.trim();

        match input_str {
            ":q" => break,
            ":list" => {
                let list_timer = DebugTimer::new("Listing files", debug_mode);
                println!("Available query files:");

                if let Ok(entries) = std::fs::read_dir(queries_dir) {
                    let mut found = false;

                    for entry in entries.filter_map(Result::ok) {
                        if let Some(file_name) = entry.file_name().to_str() {
                            if file_name.ends_with(".aka") {
                                println!("  {}", file_name);
                                found = true;
                            }
                        }
                    }

                    if !found {
                        println!("No query files found in /queries directory");
                    }
                }
                drop(list_timer);
            },
            _ => {
                let file_path = if input_str.ends_with(".aka") {
                    format!("queries/{}", input_str)
                } else {
                    format!("queries/{}.aka", input_str)
                };

                if !Path::new(&file_path).exists() {
                    println!("Error: File '{}' not found", file_path);
                    continue;
                }

                if let Err(e) = engine.execute_query_file(&file_path).await {
                    println!("Error: {}", e);
                }
            }
        }
    }
    Ok(())
}