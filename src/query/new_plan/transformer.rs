use crate::frontend::ast::{Arena, Expr, NodeId};
use crate::frontend::lexer::TokenKind;
use crate::page::tuple::Value;
use crate::query::new_plan::err::TransformError;
use crate::query::new_plan::optimizer::QueryOptimizer;
use crate::query::new_plan::{PlanExpr, PlanNode, Predicate};
use crate::query::{BinaryOperator, ComparisonOperator};
use crate::table::TableCatalog;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

struct BuiltInFunction {
    name: String,
    arity: usize,
    apply: fn(&mut AstToQueryTransformer, Vec<PlanNode>) -> Result<PlanNode, TransformError>,
}

pub struct AstToQueryTransformer<'a> {
    arena: &'a Arena,
    catalog: Arc<RwLock<TableCatalog>>,
    optimizer: Box<dyn QueryOptimizer>,
    table_aliases: HashMap<String, String>,
    current_scope: Vec<SymbolTable>,
    current_row_variable: Option<String>,
    built_in_functions: HashMap<String, BuiltInFunction>,
}

struct SymbolTable {
    symbols: HashMap<String, SymbolInfo>,
}

pub enum SymbolInfo {
    Value(PlanNode),
    Function(PlanNode),
}

impl<'a> AstToQueryTransformer<'a> {
    pub fn new(arena: &'a Arena, catalog: Arc<RwLock<TableCatalog>>, optimizer: Box<dyn QueryOptimizer>) -> Self {
        let mut built_in_functions = HashMap::new();

        built_in_functions.insert(
            "scan".to_string(),
            BuiltInFunction {
                name: "scan".to_string(),
                arity: 1,
                apply: |_, args| {
                    if let PlanNode::Reference(table_name) = &args[0] {
                        Ok(PlanNode::TableScan {
                            table_name: table_name.clone(),
                            filter: None,
                        })
                    } else {
                        Err(TransformError::InvalidArgument)
                    }
                },
            },
        );

        built_in_functions.insert(
            "filter".to_string(),
            BuiltInFunction {
                name: "filter".to_string(),
                arity: 2,
                apply: |transformer, args| {
                    if let PlanNode::Lambda { params, body } = &args[0] {
                        if params.len() == 1 {
                            transformer.push_scope();
                            transformer.set_row_variable(&params[0]);
                            let predicate = transformer.transform_predicate(*body)?;
                            transformer.pop_scope();
                            transformer.clear_row_variable();
                            Ok(PlanNode::Filter {
                                input: Box::new(args[1].clone()),
                                predicate: Box::new(predicate),
                            })
                        } else {
                            Err(TransformError::InvalidLambdaParams)
                        }
                    } else {
                        Err(TransformError::ExpectedLambda)
                    }
                },
            },
        );

        Self {
            arena,
            catalog,
            optimizer,
            table_aliases: HashMap::new(),
            current_scope: vec![SymbolTable { symbols: HashMap::new() }],
            current_row_variable: None,
            built_in_functions,
        }
    }

    pub fn transform(&mut self, root_node: NodeId) -> Result<PlanNode, TransformError> {
        let unoptimized = self.transform_node(root_node)?;
        Ok(self.optimizer.optimize(unoptimized))
    }

    fn transform_node(&mut self, node_id: NodeId) -> Result<PlanNode, TransformError> {
        match self.arena.get(node_id) {
            Expr::Reference(name_id) => {
                let name = self.arena.resolve_str(*name_id).to_string();
                self.resolve_reference(&name)
            }
            Expr::Lambda { params, body } => {
                let param_names = params.iter().map(|&p| self.arena.resolve_str(p).to_string()).collect();
                Ok(PlanNode::Lambda {
                    params: param_names,
                    body: *body,
                })
            }
            Expr::FunctionCall { func, args } => {
                let func_plan = self.transform_node(*func)?;
                let arg_plans = self.transform_args(*args)?;
                match func_plan {
                    PlanNode::BuiltInFunction { name } => {
                        if let Some(built_in) = self.built_in_functions.get(&name) {
                            if arg_plans.len() < built_in.arity {
                                Ok(PlanNode::PartiallyApplied {
                                    func: name,
                                    args: arg_plans,
                                })
                            } else if arg_plans.len() == built_in.arity {
                                (built_in.apply)(self, arg_plans)
                            } else {
                                Err(TransformError::TooManyArguments)
                            }
                        } else {
                            Err(TransformError::UnknownFunction)
                        }
                    }
                    PlanNode::PartiallyApplied { func, args: prev_args } => {
                        let total_args = prev_args.into_iter().chain(arg_plans.into_iter()).collect::<Vec<_>>();
                        if let Some(built_in) = self.built_in_functions.get(&func) {
                            if total_args.len() == built_in.arity {
                                (built_in.apply)(self, total_args)
                            } else if total_args.len() < built_in.arity {
                                Ok(PlanNode::PartiallyApplied {
                                    func,
                                    args: total_args,
                                })
                            } else {
                                Err(TransformError::TooManyArguments)
                            }
                        } else {
                            Err(TransformError::UnknownFunction)
                        }
                    }
                    _ => Ok(PlanNode::Apply {
                        func: Box::new(func_plan),
                        args: arg_plans,
                    }),
                }
            }
            Expr::Let { name, value, body } => {
                let value_plan = self.transform_node(*value)?;
                let name_str = self.arena.resolve_str(*name).to_string();
                self.push_scope();
                self.add_symbol(&name_str, SymbolInfo::Value(value_plan.clone()));
                let body_plan = self.transform_node(*body)?;
                self.pop_scope();
                Ok(PlanNode::Binding {
                    name: name_str,
                    value: Box::new(value_plan),
                    body: Box::new(body_plan),
                })
            }
            u => Err(TransformError::UnsupportedExpression(u.clone())),
        }
    }

    fn transform_args(&mut self, args_node: NodeId) -> Result<Vec<PlanNode>, TransformError> {
        match self.arena.get(args_node) {
            Expr::Tuple(items) => items.iter().map(|&item| self.transform_node(item)).collect(),
            _ => {
                let arg_plan = self.transform_node(args_node)?;
                Ok(vec![arg_plan])
            }
        }
    }

    fn resolve_reference(&self, name: &str) -> Result<PlanNode, TransformError> {
        if let Some(built_in) = self.built_in_functions.get(name) {
            Ok(PlanNode::BuiltInFunction {
                name: built_in.name.clone(),
            })
        } else {
            for scope in self.current_scope.iter().rev() {
                if let Some(info) = scope.symbols.get(name) {
                    return match info {
                        SymbolInfo::Value(plan) => Ok(plan.clone()),
                        SymbolInfo::Function(func) => Ok(func.clone()),
                    };
                }
            }
            Ok(PlanNode::Reference(name.to_string()))
        }
    }

    fn transform_predicate(&mut self, node_id: NodeId) -> Result<Predicate, TransformError> {
        match self.arena.get(node_id) {
            Expr::BinaryOp { op, left, right } => {
                let left_expr = self.transform_expr(*left)?;
                let right_expr = self.transform_expr(*right)?;
                let operator = match op {
                    TokenKind::GreaterThan => ComparisonOperator::Gt,
                    TokenKind::LessThan => ComparisonOperator::Lt,
                    _ => return Err(TransformError::UnsupportedOperator(op.clone())),
                };
                Ok(Predicate::Comparison {
                    left: left_expr,
                    op: operator,
                    right: right_expr,
                })
            }
            u => Err(TransformError::UnsupportedExpression(u.clone())),
        }
    }

    fn transform_expr(&mut self, node_id: NodeId) -> Result<PlanExpr, TransformError> {
        match self.arena.get(node_id) {
            Expr::FieldAccess { base, field } => {
                let base_name = match self.arena.get(*base) {
                    Expr::Reference(name_id) => self.arena.resolve_str(*name_id).to_string(),
                    _ => return Err(TransformError::InvalidFieldAccess),
                };
                if let Some(row_var) = &self.current_row_variable {
                    if base_name == *row_var {
                        let field_name = self.arena.resolve_str(*field).to_string();
                        Ok(PlanExpr::Column(field_name))
                    } else {
                        Err(TransformError::InvalidFieldAccess)
                    }
                } else {
                    Err(TransformError::InvalidFieldAccess)
                }
            }
            Expr::Number(num_str) => {
                let num_str = self.arena.resolve_str(*num_str);
                if let Ok(n) = num_str.parse::<i32>() {
                    Ok(PlanExpr::Literal(Value::Int(n)))
                } else {
                    Err(TransformError::InvalidNumber)
                }
            }
            Expr::Instance(values) => {
                let mut fields = vec![];
                for (field, value) in values.iter() {
                    let field_name = self.arena.resolve_str(*field).to_string();
                    let field_value = self.transform_expr(*value)?;
                    fields.push((field_name, Box::new(field_value)));
                }
                Ok(PlanExpr::Struct(fields))
            }
            Expr::BinaryOp { op, left, right } => {
                let left_expr = self.transform_expr(*left)?;
                let right_expr = self.transform_expr(*right)?;
                let operator = match op {
                    TokenKind::Plus => BinaryOperator::Add,
                    TokenKind::Minus => BinaryOperator::Subtract,
                    TokenKind::Asterisk => BinaryOperator::Multiply,
                    TokenKind::Slash => BinaryOperator::Divide,
                    _ => return Err(TransformError::UnsupportedOperator(op.clone())),
                };
                Ok(PlanExpr::BinaryOp {
                    left: Box::new(left_expr),
                    op: operator,
                    right: Box::new(right_expr),
                })
            }
            u => Err(TransformError::UnsupportedExpression(u.clone())),
        }
    }

    fn push_scope(&mut self) {
        self.current_scope.push(SymbolTable { symbols: HashMap::new() });
    }

    fn pop_scope(&mut self) {
        self.current_scope.pop();
    }

    fn add_symbol(&mut self, name: &str, info: SymbolInfo) {
        if let Some(scope) = self.current_scope.last_mut() {
            scope.symbols.insert(name.to_string(), info);
        }
    }

    fn set_row_variable(&mut self, name: &str) {
        self.current_row_variable = Some(name.to_string());
    }

    fn clear_row_variable(&mut self) {
        self.current_row_variable = None;
    }
}