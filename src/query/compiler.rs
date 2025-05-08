use crate::page::tuple::{Tuple, Value};
use crate::query::err::{QueryError, QueryResult};
use crate::query::op::TableOp;
use crate::query::{PredicateExpr, ProjectionExpr, QueryExpr, SymbolInfo, Transaction, TransactionOp, TransactionType};
use crate::table::TableCatalog;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLockReadGuard;

#[derive(Debug, Clone)]
pub enum TransactionValue {
    Row(Vec<(String, Value)>),
    Literal(Value)
}

pub struct PlanCompiler {
    symbol_table_stack: Vec<HashMap<String, SymbolInfo>>,
}

impl PlanCompiler {
    pub fn new() -> Self {
        Self {
            symbol_table_stack: vec![HashMap::new()],
        }
    }

    pub fn compile(
        &mut self,
        expr: &QueryExpr,
        catalog: Arc<RwLockReadGuard<TableCatalog>>,
    ) -> QueryResult<Transaction> {
        match expr {
            QueryExpr::Binding { name, value, body } => {
                self.push_scope();

                self.add_symbol(name.clone(), *value.clone());
                let result = self.compile(body, catalog.clone())?;
                self.pop_scope();

                Ok(result)
            },
            QueryExpr::Reference(name) => {
                if let Some(info) = self.lookup_symbol(name) {
                    self.compile(&info.clone(), catalog.clone())
                } else {
                    Err(QueryError::SymbolNotFound(name.clone()))
                }
            },
            QueryExpr::Transaction { operations, typ } => {
                let mut ops = vec![];
                for op in operations {
                    let compiled_op = self.compile_transaction_ops(catalog.clone(), op)?;
                    ops.extend(compiled_op);
                }

                match &typ {
                    TransactionType::Scan { table_name } => {
                        Ok(Transaction::Select {
                            table: table_name.clone(),
                            ops
                        })
                    },
                    TransactionType::Insert { table, value } => {
                        let value = self.compile_expr(catalog.clone(), value)?;

                        match value {
                            TransactionValue::Row(values) => {
                                Ok(Transaction::Insert {
                                    table: table.clone(),
                                    values,
                                    ops,
                                    returning: false // todo: implement returning
                                })
                            },
                            _ => Err(QueryError::ExpectedRow)
                        }
                    },
                }
            },
            _ => Err(QueryError::NotATransaction),
        }
    }

    pub fn compile_expr(
        &mut self,
        catalog: Arc<RwLockReadGuard<TableCatalog>>,
        expr: &QueryExpr,
    ) -> QueryResult<TransactionValue> {
        match expr {
            QueryExpr::Instance(values) => {
                let mut compiled_values = vec![];
                for (name, value) in values {
                    let compiled_value = match self.compile_expr(catalog.clone(), value)? {
                        TransactionValue::Literal(value) => Ok(value),
                        TransactionValue::Row(_) => Err(QueryError::RowCannotBeEmbeddedIntoAnotherRow)
                    }?;
                    compiled_values.push((name.clone(), compiled_value));
                }
                Ok(TransactionValue::Row(compiled_values))
            }
            QueryExpr::Literal(value) => {
                Ok(TransactionValue::Literal(value.clone()))
            }
            u => todo!("Unimplemented expression: {:?}", u)
        }
    }

    pub fn compile_transaction_ops(
        &mut self,
        catalog: Arc<RwLockReadGuard<TableCatalog>>,
        transaction: &TransactionOp,
    ) -> QueryResult<Vec<TableOp>> {
        match transaction {
            TransactionOp::Filter { predicate } => {
                match &**predicate {
                    PredicateExpr::Comparison { left, op, right } => {
                        if let (QueryExpr::Column(col_name), QueryExpr::Literal(value)) = (&left, &right) {
                            let col_idx = self.resolve_column_index(col_name)?;

                            return Ok(vec![TableOp::Filter {
                                column_index: col_idx,
                                operator: op.clone(),
                                value: value.clone(),
                            }]);
                        }

                        let filter_fn = self.create_predicate_function(&*predicate)?;
                        Ok(vec![TableOp::PredicativeFilter(filter_fn)])
                    },
                    _ => {
                        let filter_fn = self.create_predicate_function(&*predicate)?;
                        Ok(vec![TableOp::PredicativeFilter(filter_fn)])
                    }
                }
            },
            TransactionOp::Limit { count, offset } => {
                Ok(vec![TableOp::Limit {
                    count: *count,
                    offset: offset.unwrap_or(0),
                }])
            },
        }
    }

    fn compile_projection(&self, catalog: Arc<RwLockReadGuard<'_, TableCatalog>>, projections: &[ProjectionExpr]) -> QueryResult<TableOp> {
        todo!("Implement projection compilation");
    }

    // TODO: implement
    fn resolve_column_index(&self, name: &str) -> QueryResult<usize> {
        Ok(1)
    }

    // TODO: implement
    fn create_predicate_function(&self, predicate: &PredicateExpr) -> QueryResult<Arc<dyn Fn(&Tuple) -> bool + Send + Sync>> {
        let pred = predicate.clone();

        let filter_fn = Arc::new(move |tuple: &Tuple| -> bool {
            true
        });

        Ok(filter_fn)
    }


    fn push_scope(&mut self) {
        self.symbol_table_stack.push(HashMap::new());
    }

    fn pop_scope(&mut self) {
        if self.symbol_table_stack.len() > 1 {
            self.symbol_table_stack.pop();
        }
    }

    fn add_symbol(&mut self, name: String, info: SymbolInfo) {
        if let Some(current_scope) = self.symbol_table_stack.last_mut() {
            current_scope.insert(name, info);
        }
    }

    fn lookup_symbol(&self, name: &str) -> Option<&SymbolInfo> {
        for scope in self.symbol_table_stack.iter().rev() {
            if let Some(info) = scope.get(name) {
                return Some(info);
            }
        }
        None
    }
}