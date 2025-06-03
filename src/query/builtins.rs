use crate::page::tuple::Value;
use crate::query::err::TransformError;
use crate::query::transformer::AstToQueryTransformer;
use crate::query::{QueryExpr, TransactionOp, TransactionType};
use std::rc::Rc;

pub struct BuiltInTransactionFunction {
    pub name: String,
    pub arity: usize,
    pub apply: fn(&mut AstToQueryTransformer, Vec<QueryExpr>) -> Result<QueryExpr, TransformError>,
}

pub fn scan_impl(
    _transformer: &mut AstToQueryTransformer,
    args: Vec<QueryExpr>,
) -> Result<QueryExpr, TransformError> {
    if let QueryExpr::Literal(Value::Text(table_name)) = &args[0] {
        Ok(QueryExpr::Transaction {
            typ: TransactionType::Scan {
                table_name: table_name.clone(),
            },
            operations: vec![],
        })
    } else if let QueryExpr::Reference(table_name) = &args[0] {
        Ok(QueryExpr::Transaction {
            typ: TransactionType::Scan {
                table_name: table_name.clone(),
            },
            operations: vec![],
        })
    } else {
        Err(TransformError::InvalidArgument("scan".to_string()))
    }
}

pub fn filter_impl(
    transformer: &mut AstToQueryTransformer,
    mut args: Vec<QueryExpr>,
) -> Result<QueryExpr, TransformError> {
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

            let input = args
                .get_mut(1)
                .ok_or_else(|| TransformError::InvalidArgument("filter".to_string()))?;
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
}

pub fn insert_impl(
    _transformer: &mut AstToQueryTransformer,
    args: Vec<QueryExpr>,
) -> Result<QueryExpr, TransformError> {
    if let QueryExpr::Reference(table_name) = &args[0] {
        let value = args[1].clone();

        Ok(QueryExpr::Transaction {
            typ: TransactionType::Insert {
                table_name: table_name.clone(),
                value: Rc::new(value),
                returning: None,
            },
            operations: vec![],
        })
    } else {
        Err(TransformError::InvalidArgument("insert".to_string()))
    }
}

pub fn insert_r_impl(
    _transformer: &mut AstToQueryTransformer,
    args: Vec<QueryExpr>,
) -> Result<QueryExpr, TransformError> {
    if let QueryExpr::Reference(table_name) = &args[0] {
        let value = args[1].clone();
        let columns = match &args[2] {
            QueryExpr::Tuple(cols) => cols.clone(),
            QueryExpr::Reference(name) => vec![name.clone()],
            _ => return Err(TransformError::InvalidArgument("insertR".to_string())),
        };

        Ok(QueryExpr::Transaction {
            typ: TransactionType::Insert {
                table_name: table_name.clone(),
                value: Rc::new(value),
                returning: Some(columns),
            },
            operations: vec![],
        })
    } else {
        Err(TransformError::InvalidArgument("insertR".to_string()))
    }
}

pub fn project_impl(
    _transformer: &mut AstToQueryTransformer,
    mut args: Vec<QueryExpr>,
) -> Result<QueryExpr, TransformError> {
    let columns = match args.get(0) {
        Some(QueryExpr::Tuple(cols)) => cols.clone(),
        Some(QueryExpr::Reference(name)) => vec![name.clone()],
        _ => return Err(TransformError::ExpectedLambda),
    };

    let input = args
        .get_mut(1)
        .ok_or_else(|| TransformError::InvalidArgument("project".to_string()))?;

    match input {
        QueryExpr::Transaction { operations, .. } => {
            operations.push(TransactionOp::Project { columns });
        }
        _ => return Err(TransformError::InvalidArgument("project".to_string())),
    }

    Ok(input.clone())
}

pub fn limit_impl(
    _transformer: &mut AstToQueryTransformer,
    mut args: Vec<QueryExpr>,
) -> Result<QueryExpr, TransformError> {
    let limit_value = match args.get(0) {
        Some(QueryExpr::Literal(Value::Int(limit))) => *limit,
        _ => return Err(TransformError::ExpectedNumber),
    };

    let input = args
        .get_mut(1)
        .ok_or_else(|| TransformError::InvalidArgument("limit".to_string()))?;

    match input {
        QueryExpr::Transaction { operations, .. } => {
            operations.push(TransactionOp::Limit { count: limit_value });
        }
        _ => return Err(TransformError::InvalidArgument("limit".to_string())),
    }

    Ok(input.clone())
}

pub fn offset_impl(
    _transformer: &mut AstToQueryTransformer,
    mut args: Vec<QueryExpr>,
) -> Result<QueryExpr, TransformError> {
    let offset_value = match args.get(0) {
        Some(QueryExpr::Literal(Value::Int(offset))) => *offset,
        _ => return Err(TransformError::ExpectedNumber),
    };

    let input = args
        .get_mut(1)
        .ok_or_else(|| TransformError::InvalidArgument("offset".to_string()))?;

    match input {
        QueryExpr::Transaction { operations, .. } => {
            operations.push(TransactionOp::Offset {
                offset: offset_value,
            });
        }
        _ => return Err(TransformError::InvalidArgument("offset".to_string())),
    }

    Ok(input.clone())
}
