use crate::page::tuple::{Tuple, Value};
use crate::query::ComparisonOperator;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub enum TableOp {
    Filter {
        column_index: usize,
        operator: ComparisonOperator,
        value: Value,
    },
    Project(Vec<usize>),
    Limit {
        count: usize,
        offset: usize,
    },
    PredicativeFilter(Arc<dyn Fn(&Tuple) -> bool + Send + Sync>),
    Map(Arc<dyn Fn(&Tuple) -> Tuple + Send + Sync>),
}

impl Debug for TableOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TableOp::Filter {
                column_index,
                operator,
                value,
            } => {
                write!(
                    f,
                    "Filter(column_index: {}, operator: {:?}, value: {:?})",
                    column_index, operator, value
                )
            }
            TableOp::Project(indices) => {
                write!(f, "Project(indices: {:?})", indices)
            }
            TableOp::Limit { count, offset } => {
                write!(f, "Limit(count: {}, offset: {})", count, offset)
            }
            TableOp::PredicativeFilter(_) => {
                write!(f, "PredicativeFilter")
            }
            TableOp::Map(_) => {
                write!(f, "Map")
            }
        }
    }
}
