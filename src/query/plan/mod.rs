pub mod planners;

use crate::page::tuple::{Tuple, Value};
use crate::query::err::QueryResult;
use crate::query::{Operator, Query};
use crate::table::Table;
use futures::StreamExt;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::Stream;

#[derive(Clone)]
pub enum TableOp {
    Filter {
        column_index: usize,
        operator: Operator,
        value: Value,
    },
    Insert(Vec<Value>),
    // Projection using only specified column indices
    Project(Vec<usize>),
    Offset(usize),
    Limit(usize),
    PredicativeFilter(Arc<dyn Fn(&Tuple) -> bool + Send + Sync>)
}

pub trait QueryPlanner {
    fn plan(&self, table: &Table, query: &Query) -> QueryResult<PlanResult>;
}

pub enum PlanResult {
    Stream(Vec<TableOp>),
    ModifyData {
        ops: Vec<TableOp>,
        returning: Option<Vec<usize>>
    },
}

impl TableOp {
    pub fn apply<S>(
        &self,
        stream: S,
    ) -> Pin<Box<dyn Stream<Item = Tuple> + Send + 'static>>
    where
        S: Stream<Item = Tuple> + Send + 'static,
    {
        let op = self.clone();
        match op {
            TableOp::Filter { column_index, operator, value } => {
                let filter_fn = move |tuple: &Tuple| {
                    let column_value = &tuple.values[column_index];
                    futures::future::ready(match (column_value, &operator, &value) {
                        (a, Operator::Eq, b) => a == b,
                        (a, Operator::NotEq, b) => a != b,
                        (a, Operator::Gt, b) => a > b,
                        (a, Operator::GtEq, b) => a >= b,
                        (a, Operator::Lt, b) => a < b,
                        (a, Operator::LtEq, b) => a <= b,
                        (a, Operator::Like, b) => {
                            if let Value::Text(a) = a && let Value::Text(b) = b {
                                a.contains(b)
                            } else {
                                false
                            }
                        },
                        (a, Operator::NotLike, b) => {
                            if let Value::Text(a) = a && let Value::Text(b) = b {
                                !a.contains(b)
                            } else {
                                false
                            }
                        },
                    })
                };
                Box::pin(stream.filter(filter_fn))
            }
            TableOp::Limit(limit) => Box::pin(stream.take(limit)),
            _ => Box::pin(stream)
        }
    }
}

