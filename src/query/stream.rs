use crate::page::tuple::{Tuple, Value};
use crate::query::op::TableOp;
use crate::query::ComparisonOperator;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_stream::Stream;

struct CombinedOpsStream<S> {
    inner: Pin<Box<S>>,
    ops: Vec<TableOp>,
    offset_remaining: usize,
    taken: usize,
    limit: Option<usize>,
}

impl<S> CombinedOpsStream<S>
where
    S: Stream<Item = Tuple> + Send,
{
    fn new(stream: S, ops: Vec<TableOp>) -> Self {
        let (offset, limit) = ops.iter().fold((0, None), |(acc_offset, acc_limit), op| {
            match op {
                TableOp::Limit { count, offset } => {
                    (acc_offset + offset, Some(acc_limit.unwrap_or(usize::MAX).min(*count)))
                }
                _ => (acc_offset, acc_limit),
            }
        });

        Self {
            inner: Box::pin(stream),
            ops,
            offset_remaining: offset,
            taken: 0,
            limit,
        }
    }

    fn apply_ops_to_tuple(&self, mut tuple: Tuple) -> Option<Tuple> {
        for op in &self.ops {
            match op {
                TableOp::Filter { column_index, operator, value } => {
                    let Tuple(ref tuple_values) = tuple;
                    let column_value = &tuple_values[*column_index];
                    let matches = match (column_value, operator, value) {
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
                    };
                    if !matches {
                        return None;
                    }
                }
                TableOp::PredicativeFilter(filter_fn) => {
                    if !filter_fn(&tuple) {
                        return None;
                    }
                }
                TableOp::Project(indices) => {
                    let Tuple(tuple_values) = tuple;
                    let projected_values = indices
                        .iter()
                        .map(|&idx| tuple_values[idx].clone())
                        .collect();
                    tuple = Tuple(projected_values);
                }
                TableOp::Map(map_fn) => {
                    tuple = map_fn(&tuple);
                }
                TableOp::Limit { .. } => {
                }
            }
        }
        Some(tuple)
    }
}

impl<S> Stream for CombinedOpsStream<S>
where
    S: Stream<Item = Tuple> + Send,
{
    type Item = Tuple;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(limit) = self.limit {
                if self.taken >= limit {
                    return Poll::Ready(None);
                }
            }

            match futures::ready!(self.inner.as_mut().poll_next(cx)) {
                Some(tuple) => {
                    if self.offset_remaining > 0 {
                        self.offset_remaining -= 1;
                        continue;
                    }

                    if let Some(processed_tuple) = self.apply_ops_to_tuple(tuple) {
                        self.taken += 1;
                        return Poll::Ready(Some(processed_tuple));
                    }
                }
                None => return Poll::Ready(None),
            }
        }
    }
}

pub fn apply_ops<S>(
    stream: S,
    ops: &Vec<TableOp>,
) -> Pin<Box<dyn Stream<Item = Tuple> + Send + 'static>>
where
    S: Stream<Item = Tuple> + Send + 'static,
{
    Box::pin(CombinedOpsStream::new(stream, ops.clone()))
}