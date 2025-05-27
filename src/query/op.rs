use crate::page::tuple::{Tuple, Value};
use crate::query::ComparisonOperator;
use futures::Stream;
use futures::StreamExt;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;

#[derive(Clone)]
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

impl TableOp {
    pub fn apply<S>(&self, stream: S) -> Pin<Box<dyn Stream<Item = Tuple> + Send + 'static>>
    where
        S: Stream<Item = Tuple> + Send + 'static,
    {
        let op = self.clone();
        match op {
            TableOp::Filter {
                column_index,
                operator,
                value,
            } => {
                let filter_fn = move |Tuple(tuple_values): &Tuple| {
                    let column_value = &tuple_values[column_index];
                    futures::future::ready(match (column_value, &operator, &value) {
                        (a, ComparisonOperator::Eq, b) => a == b,
                        (a, ComparisonOperator::Neq, b) => a != b,
                        (a, ComparisonOperator::Gt, b) => a > b,
                        (a, ComparisonOperator::GtEq, b) => a >= b,
                        (a, ComparisonOperator::Lt, b) => a < b,
                        (a, ComparisonOperator::LtEq, b) => a <= b,
                        (a, ComparisonOperator::Like, b) => {
                            if let (Value::Text(a), Value::Text(b)) = (a, b) {
                                a.contains(b)
                            } else {
                                false
                            }
                        }
                        (a, ComparisonOperator::NotLike, b) => {
                            if let (Value::Text(a), Value::Text(b)) = (a, b) {
                                !a.contains(b)
                            } else {
                                false
                            }
                        }
                    })
                };
                Box::pin(stream.filter(filter_fn))
            }
            TableOp::PredicativeFilter(filter_fn) => {
                Box::pin(stream.filter(move |tuple| futures::future::ready(filter_fn(tuple))))
            }
            TableOp::Project(indices) => Box::pin(stream.map(move |Tuple(tuple_values)| {
                let projected_values = indices
                    .iter()
                    .map(|&idx| tuple_values[idx].clone())
                    .collect();
                Tuple(projected_values)
            })),
            TableOp::Map(map_fn) => Box::pin(stream.map(move |tuple| map_fn(&tuple))),
            TableOp::Limit { count, offset } => Box::pin(stream.skip(offset).take(count)), // todo: fix
        }
    }
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
