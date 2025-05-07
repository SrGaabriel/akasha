use std::pin::Pin;
use std::sync::Arc;
use futures::Stream;
use tokio::sync::RwLock;
use crate::page::tuple::Tuple;
use crate::query::err::{QueryError, QueryResult};
use crate::query::new_plan::compiler::PlanCompiler;
use crate::query::new_plan::op::TableOp;
use crate::query::new_plan::{QueryExpr, Transaction};
use crate::table::heap::scan_table;
use crate::table::TableCatalog;

pub type TupleStream = Pin<Box<dyn Stream<Item = Tuple> + Send + 'static>>;

pub struct QueryExecutor {
    catalog: Arc<RwLock<TableCatalog>>,
    plan_compiler: PlanCompiler,
}

pub enum PlanResult {
    Stream(Vec<TableOp>),
    ModifyData {
        ops: Vec<TableOp>,
        returning: Option<Vec<usize>>
    },
}

impl QueryExecutor {
    pub fn new(catalog: Arc<RwLock<TableCatalog>>) -> Self {
        Self {
            catalog,
            plan_compiler: PlanCompiler::new(),
        }
    }

    pub async fn execute_plan(&mut self, plan: QueryExpr) -> QueryResult<PlanResult> {
        let executable = self.plan_compiler.compile(&plan, Arc::new(self.catalog.read().await))?;
        self.execute_transaction(executable).await
    }

    pub async fn execute_transaction(&self, executable: Transaction) -> QueryResult<PlanResult> {
        todo!()
    }
}