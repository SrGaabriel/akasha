use crate::page::tuple::{Tuple, Value};
use crate::query::err::{QueryError, QueryResult};
use crate::query::op::TableOp;
use crate::query::{
    PredicateExpr, QueryExpr, SymbolInfo, Transaction, TransactionOp, TransactionType,
};
use crate::table::TableCatalog;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Debug)]
pub enum TransactionValue {
    Row(Vec<(String, Value)>),
    Literal(Value),
}

pub struct PlanCompiler {
    table_catalog: Arc<TableCatalog>,
    symbol_table_stack: Vec<HashMap<String, SymbolInfo>>,
}

impl PlanCompiler {
    pub fn new(table_catalog: Arc<TableCatalog>) -> Self {
        Self {
            table_catalog,
            symbol_table_stack: vec![HashMap::new()],
        }
    }

    pub fn compile(&mut self, expr: &QueryExpr) -> QueryResult<Transaction> {
        match expr {
            QueryExpr::Binding { name, value, body } => {
                self.push_scope();

                self.add_symbol(name.clone(), Rc::new((**value).clone()));
                let result = self.compile(body)?;
                self.pop_scope();

                Ok(result)
            }
            QueryExpr::Reference(name) => {
                if let Some(info) = self.lookup_symbol(name) {
                    let info = info.clone();
                    self.compile(&info)
                } else {
                    Err(QueryError::SymbolNotFound(name.clone()))
                }
            }
            QueryExpr::Transaction { operations, typ } => match &typ {
                TransactionType::Scan { table_name } => {
                    let ops = self.build_ops(table_name, operations)?;
                    Ok(Transaction::Select {
                        table: table_name.clone(),
                        ops,
                    })
                }
                TransactionType::Insert {
                    table_name,
                    value,
                    returning,
                } => {
                    let ops = self.build_ops(table_name, operations)?;
                    let value = self.compile_expr(value)?;
                    let returning_indices = returning
                        .as_ref()
                        .map(|returning_columns| {
                            returning_columns
                                .iter()
                                .map(|col| self.resolve_column_index(table_name, col))
                                .collect::<QueryResult<Vec<usize>>>()
                        })
                        .transpose()?;

                    match value {
                        TransactionValue::Row(mut values) => {
                            let indexed_values = values
                                .drain(..)
                                .map(|(name, value)| {
                                    self.resolve_column_index(table_name, &name)
                                        .map(|index| (index as u32, value))
                                })
                                .collect::<QueryResult<Vec<_>>>()?;

                            Ok(Transaction::Insert {
                                table: table_name.clone(),
                                values: indexed_values,
                                ops,
                                returning: returning_indices,
                            })
                        }
                        _ => Err(QueryError::ExpectedRow),
                    }
                }
            },
            _ => Err(QueryError::NotATransaction),
        }
    }

    pub fn compile_expr(&mut self, expr: &QueryExpr) -> QueryResult<TransactionValue> {
        match expr {
            QueryExpr::Instance(values) => {
                let mut compiled_values = vec![];
                for (name, value) in values {
                    let compiled_value = match self.compile_expr(value)? {
                        TransactionValue::Literal(value) => Ok(value),
                        TransactionValue::Row(_) => {
                            Err(QueryError::RowCannotBeEmbeddedIntoAnotherRow)
                        }
                    }?;
                    compiled_values.push((name.clone(), compiled_value));
                }
                Ok(TransactionValue::Row(compiled_values))
            }
            QueryExpr::Literal(value) => Ok(TransactionValue::Literal(value.clone())),
            u => todo!("Unimplemented expression: {:?}", u),
        }
    }

    pub fn compile_transaction_ops(
        &mut self,
        table: &str,
        transaction: &TransactionOp,
    ) -> QueryResult<Vec<TableOp>> {
        match transaction {
            TransactionOp::Filter { predicate } => match &**predicate {
                PredicateExpr::Comparison { left, op, right } => {
                    if let (QueryExpr::Column(col_name), QueryExpr::Literal(value)) =
                        (&left, &right)
                    {
                        let col_idx = self.resolve_column_index(table, col_name)?;

                        return Ok(vec![TableOp::Filter {
                            column_index: col_idx,
                            operator: op.clone(),
                            value: value.clone(),
                        }]);
                    }

                    let filter_fn = self.create_predicate_function(&*predicate)?;
                    Ok(vec![TableOp::PredicativeFilter(filter_fn)])
                }
                _ => {
                    let filter_fn = self.create_predicate_function(&*predicate)?;
                    Ok(vec![TableOp::PredicativeFilter(filter_fn)])
                }
            },
            TransactionOp::Limit { count } => Ok(vec![TableOp::Limit(*count)]),
            TransactionOp::Offset { offset } => Ok(vec![TableOp::Offset(*offset)]),
            TransactionOp::Project { columns } => {
                let mut indices = vec![];
                for column in columns {
                    let index = self.resolve_column_index(table, column)?;
                    indices.push(index);
                }
                Ok(vec![TableOp::Project(indices)])
            }
        }
    }

    fn resolve_column_index(&self, table: &str, column: &str) -> QueryResult<usize> {
        self.table_catalog
            .get_table(table)
            .ok_or_else(|| QueryError::TableNotFound(table.to_string()))?
            .info
            .get_column_index(column)
            .ok_or_else(|| QueryError::ColumnNotFound(column.to_string(), table.to_string()))
    }

    // TODO: implement
    fn create_predicate_function(
        &self,
        _predicate: &PredicateExpr,
    ) -> QueryResult<Arc<dyn Fn(&Tuple) -> bool + Send + Sync>> {
        let filter_fn = Arc::new(move |_tuple: &Tuple| -> bool { true });

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

    fn build_ops(
        &mut self,
        table: &str,
        operations: &[TransactionOp],
    ) -> QueryResult<Vec<TableOp>> {
        let mut ops = vec![];
        for op in operations {
            let compiled_op = self.compile_transaction_ops(table, op)?;
            ops.extend(compiled_op);
        }
        Ok(ops)
    }
}
