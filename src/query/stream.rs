use crate::page::tuple::{Tuple, Value};
use crate::query::op::{compare_values, TableOp};
use std::pin::Pin;
use std::task::{Context, Poll};
use futures::StreamExt;
use tokio_stream::Stream;
use crate::table::heap::IndexedTuple;

struct CombinedOpsStream<S> {
    inner: Pin<Box<S>>,
    ops: Vec<TableOp>,
    offset_remaining: usize,
    taken: usize,
    limit: Option<usize>,
}

impl<S> CombinedOpsStream<S>
where
    S: Stream<Item = IndexedTuple> + Send
{
    fn new(stream: S, ops: Vec<TableOp>) -> Self {
        let (offset, limit) = ops
            .iter()
            .fold((0, None), |(acc_offset, acc_limit), op| match op {
                TableOp::Limit(count) => (
                    acc_offset,
                    Some(acc_limit.unwrap_or(usize::MAX).min(*count as usize)),
                ),
                TableOp::Offset(offset_value) => (acc_offset + *offset_value as usize, acc_limit),
                _ => (acc_offset, acc_limit),
            });

        Self {
            inner: Box::pin(stream),
            ops,
            offset_remaining: offset,
            taken: 0,
            limit,
        }
    }

    fn apply_ops_to_tuple(&self, mut tuple: IndexedTuple) -> Option<IndexedTuple> {
        for op in &self.ops {
            match op {
                TableOp::Filter {
                    column_index,
                    operator,
                    value,
                } => {
                    let Tuple(ref tuple_values) = tuple.inner;
                    let column_value = &tuple_values[*column_index];
                    let matches = compare_values(column_value, operator, value);
                    if !matches {
                        return None;
                    }
                }
                TableOp::PredicativeFilter(filter_fn) => {
                    if !filter_fn(&tuple.inner) {
                        return None;
                    }
                }
                TableOp::Project(indices) => {
                    let Tuple(ref mut tuple_values) = tuple.inner;
                    let projected_values = indices
                        .iter()
                        .map(|&idx| std::mem::replace(&mut tuple_values[idx], Value::Null))
                        .collect();
                    tuple = tuple.swap(Tuple(projected_values));
                }
                TableOp::Map(map_fn) => {
                    let mapped_tuple = map_fn(&tuple.inner);
                    tuple = tuple.swap(mapped_tuple);
                }
                TableOp::Limit { .. } => {}
                TableOp::Offset { .. } => {}
            }
        }
        Some(tuple)
    }
}

impl<S> Stream for CombinedOpsStream<S>
where
    S: Stream<Item = IndexedTuple> + Send
{
    type Item = IndexedTuple;

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
    ops: Vec<TableOp>,
) -> Pin<Box<dyn Stream<Item = IndexedTuple> + Send + 'static>>
where
    S: Stream<Item = IndexedTuple> + Send + 'static,
{
    Box::pin(CombinedOpsStream::new(stream, ops))
}

pub fn map_inner<S>(stream: Pin<Box<S>>) -> Pin<Box<dyn Stream<Item = Tuple> + Send + 'static>>
where
    S: Stream<Item = IndexedTuple> + Send + 'static + ?Sized,
{
    Box::pin(stream.map(|tuples| tuples.inner))
}
