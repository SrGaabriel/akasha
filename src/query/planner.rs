use std::pin::Pin;
use std::sync::Arc;
use futures::StreamExt;
use tokio_stream::Stream;
use crate::page::tuple::{Tuple, Value};
use crate::query::Query;
use crate::table::Table;

#[derive(Clone)]
pub enum TableOp {
    Filter(usize, Comparison, Value),
    Limit(usize)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte
}

pub trait QueryPlanner {
    fn plan(&self, table: &Table, query: &Query) -> Vec<TableOp>;
}

pub struct TemporaryQueryPlanner;

impl QueryPlanner for TemporaryQueryPlanner {
    fn plan(&self, table: &Table, query: &Query) -> Vec<TableOp> {
        query.filter.as_ref().map_or(vec![], |x| {
            let column_index = table.schema.get_column_index(&x.reference()).unwrap();
            vec![TableOp::Filter(column_index, Comparison::Eq, x.value())]
        })
    }
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
            TableOp::Filter(column, comparison, value) => {
                let filter_fn = move |tuple: &Tuple| {
                    let column_value = &tuple.values[column];
                    futures::future::ready(match (column_value, &comparison, &value) {
                        (a, Comparison::Eq, b) => a == b,
                        (a, Comparison::Ne, b) => a != b,
                        (a, Comparison::Gt, b) => a > b,
                        (a, Comparison::Gte, b) => a >= b,
                        (a, Comparison::Lt, b) => a < b,
                        (a, Comparison::Lte, b) => a <= b
                    })
                };
                Box::pin(stream.filter(filter_fn))
            }
            TableOp::Limit(limit) => Box::pin(stream.take(limit)),
        }
    }
}