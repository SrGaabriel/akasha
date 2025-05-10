use crate::page::Page;
use crate::page::pool::BufferPool;
use crate::page::tuple::Tuple;
use tokio::sync::Mutex;
use std::sync::Arc;
use futures::{Stream, task::{Context, Poll}};
use std::pin::Pin;

pub struct TableHeap {
    pub file_id: u32,
    pub buffer_pool: Arc<BufferPool>,
    pub page_ids: Mutex<Vec<u32>>,
}

impl TableHeap {
    pub fn new(file_id: u32, buffer_pool: Arc<BufferPool>) -> Arc<Self> {
        Arc::new(TableHeap { file_id, buffer_pool, page_ids: Mutex::new(vec![0]) })
    }

    pub async fn insert_tuple(&self, tuple: &Tuple) -> Result<(), String> {
        let mut pages = self.page_ids.lock().await;
        for pid in pages.iter() {
            let ptr = self.buffer_pool.get_page(self.file_id, *pid).await;
            let mut page = unsafe { Page::from_raw(*pid, ptr) };
            if page.insert_tuple(&tuple).is_ok() {
                self.buffer_pool.unpin(self.file_id, *pid, true);
                return Ok(());
            }
            self.buffer_pool.unpin(self.file_id, *pid, false);
        }
        let new_pid = pages.len() as u32;
        let ptr = self.buffer_pool.get_page(self.file_id, new_pid).await;
        let mut page = unsafe { Page::from_raw(new_pid, ptr) };
        page.init_new();
        page.insert_tuple(&tuple)?;
        pages.push(new_pid);
        self.buffer_pool.unpin(self.file_id, new_pid, true);
        Ok(())
    }

    pub async fn get_tuple(&self, page_id: u32, slot_id: usize) -> Option<Tuple> {
        let ptr = self.buffer_pool.get_page(self.file_id, page_id).await;
        let page = unsafe { Page::from_raw(page_id, ptr) };
        let t = page.get_tuple(slot_id);
        self.buffer_pool.unpin(self.file_id, page_id, false);
        t
    }
}

pub struct TableIterator {
    heap: Arc<TableHeap>,
    page_idx: usize,
    slot_idx: usize,
}

impl TableIterator {
    pub fn new(heap: Arc<TableHeap>) -> Self {
        TableIterator { heap, page_idx: 0, slot_idx: 0 }
    }
}

impl Stream for TableIterator {
    type Item = Tuple;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let heap = this.heap.clone();
        let mut fut = Box::pin(async move {
            let pages = heap.page_ids.lock().await;
            while this.page_idx < pages.len() {
                let pid = pages[this.page_idx];
                let ptr = heap.buffer_pool.get_page(heap.file_id, pid).await;
                let page = unsafe { Page::from_raw(pid, ptr) };
                if let Some(t) = page.get_tuple(this.slot_idx) {
                    this.slot_idx += 1;
                    heap.buffer_pool.unpin(heap.file_id, pid, false);
                    return Some(t);
                }
                this.slot_idx = 0;
                this.page_idx += 1;
            }
            None
        });
        fut.as_mut().poll(cx)
    }
}

pub async fn scan_table(table_ref: Arc<TableHeap>) -> TableIterator {
    TableIterator::new(table_ref)
}