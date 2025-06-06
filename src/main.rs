#![feature(let_chains)]

use crate::frontend::ast::{Arena, Visitor};
use crate::frontend::lexer::Lexer;
use crate::frontend::parser::parse_expression;
use crate::frontend::print::PrettyPrinter;
use crate::page::io::{FileSystemManager, IoManager};
use crate::page::pool::BufferPool;
use crate::page::tuple::Tuple;
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

pub mod frontend;
pub mod page;
pub mod query;
pub mod table;

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
    compiler: PlanCompiler,
    arena: Arena,
    executor: QueryExecutor,
    debug_mode: bool,
}

impl QueryEngine {
    async fn new(debug_mode: bool) -> Result<Self, Box<dyn std::error::Error>> {
        let init_timer = DebugTimer::new("Database initialization", debug_mode);

        let file_io = Arc::new(FileSystemManager::new("database".to_string()));
        file_io.create_home().await?;

        let io = Arc::new(IoManager::new(Arc::clone(&file_io)));
        let buffer_pool = BufferPool::new(Arc::clone(&io));

        let catalog = match TableCatalog::load(Arc::clone(&io), Arc::clone(&buffer_pool)).await {
            Ok(cat) => {
                println!("Loaded catalog with {} tables", cat.tables.len());
                cat
            }
            Err(err) => {
                eprintln!("Error loading catalog: {}", err);
                let mut cat = TableCatalog::init_then_load(io, buffer_pool).await;
                let mut columns = HashMap::new();
                columns.insert("name".to_string(), ColumnInfo {
                    id: 0,
                    name: "name".to_string(),
                    data_type: page::tuple::DataType::Text,
                    default: None,
                    nullable: false,
                });
                columns.insert("age".to_string(), ColumnInfo {
                    id: 1,
                    name: "age".to_string(),
                    data_type: page::tuple::DataType::Int,
                    default: None,
                    nullable: false,
                });
                cat.create_table("users".to_string(), TableInfo { columns })
                    .await?;
                cat
            }
        };
        drop(init_timer);
        let catalog = Arc::new(catalog);

        Ok(Self {
            compiler: PlanCompiler::new(Arc::clone(&catalog)),
            arena: Arena::with_capacity(10000, 1000),
            executor: QueryExecutor::new(catalog),
            debug_mode,
        })
    }

    async fn execute_query_file(
        &mut self,
        file_path: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
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
        if self.debug_mode {
            let mut printer = PrettyPrinter::new();
            printer.visit(&self.arena, root_id);
        }
        drop(parse_timer);

        let transform_timer = DebugTimer::new("AST transformation", self.debug_mode);
        let mut transformer = AstToQueryTransformer::new(&self.arena, Box::new(IdentityOptimizer));
        let transformed = transformer.transform(root_id)?;
        if self.debug_mode {
            println!("\nTransformed query: {:#?}", transformed);
        }
        drop(transform_timer);

        let compile_timer = DebugTimer::new("Query compilation", self.debug_mode);
        let compiled = self.compiler.compile(&transformed).unwrap();
        if self.debug_mode {
            println!("\nCompiled query: {:#?}", compiled);
        }
        drop(transformed);
        drop(compile_timer);

        let execute_timer = DebugTimer::new("Query execution", self.debug_mode);
        let plan = self.executor.execute(compiled).await?;
        let tuples = plan.collect::<Vec<Tuple>>().await;

        let execution_elapsed = execute_timer.elapsed();
        let total_elapsed = total_timer.elapsed();

        println!(
            "{:?} Query executed in {} and completed in {}",
            tuples.len(),
            execution_elapsed,
            total_elapsed
        );
        println!("\nResults:");
        if tuples.is_empty() {
            println!("No results found.");
        } else {
            for tuple in tuples {
                println!("{:?}", tuple);
            }
        }
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
            }
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
