use crate::page::tuple::{Tuple, Value};
use crate::query::err::QueryResult;
use crate::query::new_plan::{PlanExpr, PlanNode, Predicate, ProjectionExpr, SymbolInfo};
use crate::query::ComparisonOperator;
use crate::table::TableCatalog;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tokio::sync::RwLockReadGuard;
use crate::query::new_plan::op::TableOp;

#[derive(Debug)]
pub enum PlanExecutable {
    TableScan {
        table: String,
        ops: Vec<TableOp>,
    },
    Filter {
        input: Box<PlanExecutable>,
        ops: Vec<TableOp>,
    },
    Projection {
        input: Box<PlanExecutable>,
        op: TableOp,
    },
    Limit {
        input: Box<PlanExecutable>,
        ops: Vec<TableOp>,
    },
    Values(Vec<Value>),
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
        plan: &PlanNode,
        catalog: Arc<RwLockReadGuard<TableCatalog>>,
    ) -> QueryResult<PlanExecutable> {
        match plan {
            PlanNode::TableScan { table_name, filter } => {
                let mut ops = Vec::new();

                if let Some(predicate) = filter {
                    let filter_ops = self.compile_predicate(predicate)?;
                    ops.extend(filter_ops);
                }

                Ok(PlanExecutable::TableScan {
                    table: table_name.clone(),
                    ops,
                })
            },
            PlanNode::Filter { predicate, input } => {
                let input_exe = self.compile(input, catalog)?;
                let filter_ops = self.compile_predicate(predicate)?;

                Ok(PlanExecutable::Filter {
                    input: Box::new(input_exe),
                    ops: filter_ops,
                })
            },
            PlanNode::Map { projection, input } => {
                let input_exe = self.compile(input, catalog.clone())?;
                let projection_op = self.compile_projection(catalog, &projection)?;

                Ok(PlanExecutable::Projection {
                    input: Box::new(input_exe),
                    op: projection_op,
                })
            },
            PlanNode::Limit { count, offset, input } => {
                let input_exe = self.compile(input, catalog)?;

                let mut ops = Vec::new();
                if let Some(offset_val) = offset {
                    ops.push(TableOp::Offset(*offset_val));
                }
                ops.push(TableOp::Limit(*count));

                Ok(PlanExecutable::Limit {
                    input: Box::new(input_exe),
                    ops,
                })
            },
            PlanNode::Values(values) => {
                Ok(PlanExecutable::Values(values.clone()))
            },
            PlanNode::Binding { name, value, body } => {
                self.push_scope();

                self.add_symbol(name.clone(), SymbolInfo {
                    node_id: None,
                    plan_node: Some(*value.clone()),
                });

                let result = self.compile(body, catalog.clone())?;

                self.pop_scope();

                Ok(result)
            },
            u => panic!("Unsupported plan node type: {:?}", u),
        }
    }

    fn compile_predicate(&self, predicate: &Predicate) -> QueryResult<Vec<TableOp>> {
        match predicate {
            Predicate::Comparison { left, op, right } => {
                if let (PlanExpr::Column(col_name), PlanExpr::Literal(value)) = (&left, &right) {
                    let col_idx = self.resolve_column_index(col_name)?;

                    return Ok(vec![TableOp::Filter {
                        column_index: col_idx,
                        operator: op.clone(),
                        value: value.clone(),
                    }]);
                }

                let filter_fn = self.create_predicate_function(predicate)?;
                Ok(vec![TableOp::PredicativeFilter(filter_fn)])
            },
            Predicate::And(predicates) => {
                let mut all_ops = Vec::new();
                for pred in predicates {
                    let ops = self.compile_predicate(pred)?;
                    all_ops.extend(ops);
                }
                Ok(all_ops)
            },
            _ => {
                let filter_fn = self.create_predicate_function(predicate)?;
                Ok(vec![TableOp::PredicativeFilter(filter_fn)])
            }
        }
    }

    fn compile_projection(&self, catalog: Arc<RwLockReadGuard<'_, TableCatalog>>, projections: &[ProjectionExpr]) -> QueryResult<TableOp> {
        todo!("Implement projection compilation");
    }

    // TODO: implement
    fn resolve_column_index(&self, name: &str) -> QueryResult<usize> {
        Ok(0)
    }

    // TODO: implement
    fn create_predicate_function(&self, predicate: &Predicate) -> QueryResult<Arc<dyn Fn(&Tuple) -> bool + Send + Sync>> {
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

impl Debug for TableOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TableOp::Filter { column_index, operator, value } => {
                write!(f, "Filter(column_index: {}, operator: {:?}, value: {:?})", column_index, operator, value)
            },
            TableOp::Insert(values) => {
                write!(f, "Insert(values: {:?})", values)
            },
            TableOp::Project(columns) => {
                write!(f, "Project(columns: {:?})", columns)
            },
            TableOp::Offset(offset) => {
                write!(f, "Offset(offset: {})", offset)
            },
            TableOp::Limit(limit) => {
                write!(f, "Limit(limit: {})", limit)
            },
            TableOp::PredicativeFilter(_) => {
                write!(f, "PredicativeFilter")
            },
            TableOp::Map(_) => {
                write!(f, "Map")
            }
        }
    }
}