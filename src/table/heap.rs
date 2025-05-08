use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use futures::FutureExt;
use tokio::sync::RwLock;
use tokio_stream::Stream;
use crate::page::pool::BufferPool;
use crate::page::tuple::Tuple;

pub struct TableHeap {
    pub file_id: u32,
    pub buffer_pool: Arc<RwLock<BufferPool>>,
    pub page_ids: Vec<u32>,
}

impl TableHeap {
    pub async fn insert_tuple(&mut self, tuple: &Tuple) -> Result<(u32, usize), String> {
        for &page_id in &self.page_ids {
            let frame_arc = self.buffer_pool.write().await.get_page(self.file_id, page_id).await.ok_or("Failed to get page")?;
            let mut frame = frame_arc.write().await;

            let result = frame.page.insert_tuple(tuple);
            if let Ok(slot_id) = result {
                frame.is_dirty = true;
                return Ok((page_id, slot_id));
            }
        }

        let mut pool = self.buffer_pool.write().await;
        let new_page_id = pool.allocate_new_page(self.file_id).await;
        self.page_ids.push(new_page_id);

        let frame_arc = pool.get_page(self.file_id, new_page_id).await.ok_or("Failed to get new page")?;
        let mut frame = frame_arc.write().await;

        let slot_id = frame.page.insert_tuple(tuple)?;
        frame.is_dirty = true;

        Ok((new_page_id, slot_id))
    }

    pub async fn get_tuple(&self, page_id: u32, slot_id: usize) -> Option<Tuple> {
        let mut pool = self.buffer_pool.write().await;
        let frame_arc = pool.get_page(self.file_id, page_id).await?;
        let frame = frame_arc.read().await;

        frame.page.get_tuple(slot_id)
    }
}

pub struct TableHeapIterator {
    table_heap: Arc<RwLock<TableHeap>>,
    buffer_pool: Arc<RwLock<BufferPool>>,
    current_page_index: usize,
    current_slot_index: usize,
    current_future: Option<Pin<Box<dyn Future<Output = Option<(Tuple, usize, usize)>> + Send>>>,
}

impl TableHeapIterator {
    pub async fn create(table_heap: Arc<RwLock<TableHeap>>) -> Self {
        let buffer_pool = {
            let table_heap_lock = table_heap.read().await;
            table_heap_lock.buffer_pool.clone()
        };
        Self {
            buffer_pool,
            table_heap,
            current_page_index: 0,
            current_slot_index: 0,
            current_future: None,
        }
    }

    pub fn reset(&mut self) {
        self.current_page_index = 0;
        self.current_slot_index = 0;
    }
}

async fn next_tuple_helper(
    table_heap: Arc<RwLock<TableHeap>>,
    buffer_pool: Arc<RwLock<BufferPool>>,
    mut page_index: usize,
    mut slot_index: usize,
) -> Option<(Tuple, usize, usize)> {
    let table_heap = table_heap.read().await;
    while page_index < table_heap.page_ids.len() {
        let page_id = table_heap.page_ids[page_index];

        let mut pool = buffer_pool.write().await;
        let frame_arc = pool.get_page(table_heap.file_id, page_id).await?;
        let frame = frame_arc.read().await;

        while slot_index < frame.page.slot_count {
            let tuple_opt = frame.page.get_tuple(slot_index);
            slot_index += 1;

            if let Some(tuple) = tuple_opt {
                return Some((tuple, page_index, slot_index));
            }
        }

        page_index += 1;
        slot_index = 0;
    }
    None
}

impl Stream for TableHeapIterator {
    type Item = Tuple;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.current_future.is_none() {
            let table_heap = self.table_heap.clone();
            let buffer_pool = self.buffer_pool.clone();
            let page_index = self.current_page_index;
            let slot_index = self.current_slot_index;
            let fut = next_tuple_helper(table_heap, buffer_pool, page_index, slot_index);
            self.current_future = Some(Box::pin(fut));
        }

        let fut = self.current_future.as_mut().unwrap();
        match fut.poll_unpin(cx) {
            Poll::Ready(res) => {
                self.current_future = None;
                if let Some((tuple, page_index, slot_index)) = res {
                    self.current_page_index = page_index;
                    self.current_slot_index = slot_index;
                    Poll::Ready(Some(tuple))
                } else {
                    Poll::Ready(None)
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pub async fn scan_table(table_ref: Arc<RwLock<TableHeap>>) -> TableHeapIterator {
    TableHeapIterator::create(table_ref).await
}