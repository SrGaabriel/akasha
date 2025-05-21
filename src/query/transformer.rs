use crate::frontend::ast::{Arena, Expr, NodeId};
use crate::frontend::lexer::TokenKind;
use crate::page::tuple::Value;
use crate::query::err::TransformError;
use crate::query::optimizer::QueryOptimizer;
use crate::query::{PredicateExpr, QueryExpr, TransactionOp, TransactionType};
use crate::query::{BinaryOperator, ComparisonOperator};
use std::collections::HashMap;
use std::rc::Rc;

struct BuiltInTransactionFunction {
    name: String,
    arity: usize,
    apply: fn(&mut AstToQueryTransformer, Vec<QueryExpr>) -> Result<QueryExpr, TransformError>,
}

pub struct AstToQueryTransformer<'a> {
    arena: &'a Arena,
    optimizer: Box<dyn QueryOptimizer>,
    current_scope: Vec<SymbolTable>,
    current_row_variable: Option<String>,
    built_in_functions: HashMap<String, BuiltInTransactionFunction>,
}

struct SymbolTable {
    symbols: HashMap<String, SymbolInfo>,
}

#[derive(Clone)]
pub enum SymbolInfo {
    Value(QueryExpr),
    Function(QueryExpr),
}

impl<'a> AstToQueryTransformer<'a> {
    pub fn new(arena: &'a Arena, optimizer: Box<dyn QueryOptimizer>) -> Self {
        let mut built_in_functions = HashMap::new();

        built_in_functions.insert(
            "scan".to_string(),
            BuiltInTransactionFunction {
                name: "scan".to_string(),
                arity: 1,
                apply: |_, args| {
                    if let QueryExpr::Literal(Value::Text(table_name)) = &args[0] {
                        Ok(QueryExpr::Transaction {
                            typ: TransactionType::Scan {
                                table_name: table_name.clone(),
                            },
                            operations: vec![]
                        })
                    } else if let QueryExpr::Reference(table_name) = &args[0] {
                        Ok(QueryExpr::Transaction {
                            typ: TransactionType::Scan {
                                table_name: table_name.clone(),
                            },
                            operations: vec![]
                        })
                    } else {
                        Err(TransformError::InvalidArgument("scan".to_string()))
                    }
                },
            },
        );

        built_in_functions.insert(
            "filter".to_string(),
            BuiltInTransactionFunction {
                name: "filter".to_string(),
                arity: 2,
                apply: |transformer, mut args| {
                    if let QueryExpr::Lambda { params, body, .. } = &args[0] {
                        if params.len() == 1 {
                            transformer.push_scope();
                            transformer.set_row_variable(&params[0]);

                            let predicate = match transformer.transform_to_predicate(*body) {
                                Ok(pred) => pred,
                                Err(e) => return Err(e),
                            };

                            transformer.pop_scope();
                            transformer.clear_row_variable();

                            let input = args.get_mut(1).ok_or_else(|| TransformError::InvalidArgument("filter".to_string()))?;
                            match input {
                                QueryExpr::Transaction { operations, .. } => {
                                    operations.push(TransactionOp::Filter {
                                        predicate: Rc::new(predicate),
                                    });
                                }
                                _ => return Err(TransformError::InvalidArgument("filter".to_string())),
                            }
                            Ok(input.clone())
                        } else {
                            Err(TransformError::InvalidLambdaParams)
                        }
                    } else {
                        Err(TransformError::ExpectedLambda)
                    }
                },
            },
        );

        built_in_functions.insert(
            "insert".to_string(),
            BuiltInTransactionFunction {
                name: "insert".to_string(),
                arity: 2,
                apply: |_, args| {
                    if let QueryExpr::Reference(table_name) = &args[0] {
                        let value = args[1].clone();

                        Ok(QueryExpr::Transaction {
                            typ: TransactionType::Insert {
                                table_name: table_name.clone(),
                                value: Rc::new(value)
                            },
                            operations: vec![]
                        })
                    } else {
                        Err(TransformError::InvalidArgument("insert".to_string()))
                    }
                },
            },
        );

        Self {
            arena,
            optimizer,
            current_scope: vec![SymbolTable { symbols: HashMap::new() }],
            current_row_variable: None,
            built_in_functions,
        }
    }

    pub fn transform(&mut self, root_node: NodeId) -> Result<QueryExpr, TransformError> {
        let unoptimized = self.transform_node(root_node)?;
        Ok(self.optimizer.optimize(unoptimized))
    }

    fn transform_node(&mut self, node_id: NodeId) -> Result<QueryExpr, TransformError> {
        match self.arena.get(node_id) {
            Expr::Reference(name_id) => {
                let name = self.arena.resolve_str(*name_id).to_string();
                self.resolve_reference(&name)
            }
            Expr::Lambda { params, body } => {
                let param_names = params.iter().map(|&p| self.arena.resolve_str(p).to_string()).collect();
                Ok(QueryExpr::Lambda {
                    params: param_names,
                    body: *body
                })
            }
            Expr::FunctionCall { func, args } => {
                let func_expr = self.transform_node(*func)?;
                let arg_exprs = self.transform_args(args.to_vec())?;

                if let QueryExpr::Reference(op_name) = &func_expr {
                    if op_name == "|>" && arg_exprs.len() == 2 {
                        return Ok(QueryExpr::Bind {
                            input: Rc::new(arg_exprs[0].clone()),
                            func: Rc::new(arg_exprs[1].clone()),
                        });
                    }
                }

                match func_expr {
                    QueryExpr::BuiltInFunction { name } => {
                        if let Some(built_in) = self.built_in_functions.get(&name) {
                            if arg_exprs.len() == built_in.arity {
                                (built_in.apply)(self, arg_exprs)
                            } else {
                                Err(TransformError::WrongNumberOfArguments {
                                    name: name.clone(),
                                    expected: built_in.arity,
                                    found: arg_exprs.len(),
                                })
                            }
                        } else {
                            Err(TransformError::UnknownFunction)
                        }
                    }
                    _ => Ok(QueryExpr::Apply {
                        func: Rc::new(func_expr),
                        args: arg_exprs,
                    }),
                }
            }
            Expr::Let { name, value, body } => {
                let value_expr = self.transform_node(*value)?;
                let name_str = self.arena.resolve_str(*name).to_string();
                self.push_scope();
                self.add_symbol(&name_str, SymbolInfo::Value(value_expr.clone()));
                let body_expr = self.transform_node(*body)?;
                self.pop_scope();
                Ok(QueryExpr::Binding {
                    name: name_str,
                    value: Rc::new(value_expr),
                    body: Rc::new(body_expr),
                })
            }
            Expr::BinaryOp { op, left, right } => {
                let left_expr = self.transform_node(*left)?;
                let right_expr = self.transform_node(*right)?;

                let operator = match op {
                    TokenKind::Plus => BinaryOperator::Add,
                    TokenKind::Minus => BinaryOperator::Subtract,
                    TokenKind::Asterisk => BinaryOperator::Multiply,
                    TokenKind::Slash => BinaryOperator::Divide,
                    TokenKind::Percent => BinaryOperator::Modulus,
                    _ => return Err(TransformError::UnsupportedOperator(op.clone())),
                };

                Ok(QueryExpr::BinaryOp {
                    left: Rc::new(left_expr),
                    op: operator,
                    right: Rc::new(right_expr),
                })
            }
            Expr::Number(num_str) => {
                let num_str = self.arena.resolve_str(*num_str);
                if let Ok(n) = num_str.parse::<i32>() {
                    Ok(QueryExpr::Literal(Value::Int(n)))
                } else if let Ok(f) = num_str.parse::<f64>() {
                    Ok(QueryExpr::Literal(Value::Double(f)))
                } else {
                    Err(TransformError::InvalidNumber)
                }
            }
            Expr::StringLit(str_id) => {
                let string_value = self.arena.resolve_str(*str_id).to_string();
                Ok(QueryExpr::Literal(Value::Text(string_value)))
            }
            Expr::Bool(value) => {
                Ok(QueryExpr::Literal(Value::Boolean(*value)))
            }
            Expr::Instance(values) => {
                let mut fields = Vec::new();
                for (name_id, value) in values {
                    let name = self.arena.resolve_str(*name_id).to_string();
                    let value_expr = self.transform_node(*value)?;
                    fields.push((name, value_expr));
                }
                Ok(QueryExpr::Instance(fields))
            }
            Expr::FieldAccess { base, field } => {
                let base_name = match self.arena.get(*base) {
                    Expr::Reference(name_id) => self.arena.resolve_str(*name_id).to_string(),
                    _ => return Err(TransformError::InvalidFieldAccess),
                };

                if let Some(row_var) = &self.current_row_variable {
                    if base_name == *row_var {
                        let field_name = self.arena.resolve_str(*field).to_string();
                        Ok(QueryExpr::Column(field_name))
                    } else {
                        Err(TransformError::InvalidFieldAccess)
                    }
                } else {
                    Err(TransformError::InvalidFieldAccess)
                }
            }
            u => Err(TransformError::UnsupportedExpression(u.clone())),
        }
    }

    fn transform_args(&mut self, args_nodes: Vec<NodeId>) -> Result<Vec<QueryExpr>, TransformError> {
        let mut args = Vec::new();
        for arg_node in args_nodes {
            let arg_expr = self.transform_node(arg_node)?;
            args.push(arg_expr);
        }
        Ok(args)
    }

    fn resolve_reference(&self, name: &str) -> Result<QueryExpr, TransformError> {
        if let Some(built_in) = self.built_in_functions.get(name) {
            return Ok(QueryExpr::BuiltInFunction {
                name: built_in.name.clone(),
            });
        }

        for scope in self.current_scope.iter().rev() {
            if let Some(info) = scope.symbols.get(name) {
                return match info {
                    SymbolInfo::Value(expr) => Ok(expr.clone()),
                    SymbolInfo::Function(expr) => Ok(expr.clone()),
                };
            }
        }

        Ok(QueryExpr::Reference(name.to_string()))
    }

    fn transform_to_predicate(&mut self, node_id: NodeId) -> Result<PredicateExpr, TransformError> {
        match self.arena.get(node_id) {
            Expr::BinaryOp { op, left, right } => {
                let left_expr = self.transform_node(*left)?;
                let right_expr = self.transform_node(*right)?;

                let operator = match op {
                    TokenKind::Equals => ComparisonOperator::Eq,
                    TokenKind::NotEquals => ComparisonOperator::Neq,
                    TokenKind::GreaterThan => ComparisonOperator::Gt,
                    TokenKind::GreaterThanEquals => ComparisonOperator::GtEq,
                    TokenKind::LessThan => ComparisonOperator::Lt,
                    TokenKind::LessThanEquals => ComparisonOperator::LtEq,
                    TokenKind::And => {
                        let left_pred = self.transform_to_predicate(*left)?;
                        let right_pred = self.transform_to_predicate(*right)?;
                        return Ok(PredicateExpr::And(Rc::new(left_pred), Rc::new(right_pred)));
                    }
                    TokenKind::Or => {
                        let left_pred = self.transform_to_predicate(*left)?;
                        let right_pred = self.transform_to_predicate(*right)?;
                        return Ok(PredicateExpr::Or(Rc::new(left_pred), Rc::new(right_pred)));
                    }
                    _ => return Err(TransformError::UnsupportedOperator(op.clone())),
                };

                Ok(PredicateExpr::Comparison {
                    left: left_expr,
                    op: operator,
                    right: right_expr,
                })
            }
            Expr::UnaryOp { op, operand } => {
                if *op == TokenKind::Not {
                    let pred = self.transform_to_predicate(*operand)?;
                    Ok(PredicateExpr::Not(Rc::new(pred)))
                } else {
                    Err(TransformError::UnsupportedOperator(op.clone()))
                }
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