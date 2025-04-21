
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::page::pool::BufferPool;
use crate::page::tuple::Tuple;

pub struct TableHeap {
    file_id: u32,
    buffer_pool: Arc<RwLock<BufferPool>>,
    page_ids: Vec<u32>,
}

impl TableHeap {
    pub async fn insert_tuple(&mut self, tuple: &Tuple) -> Option<(u32, usize)> {
        for &page_id in &self.page_ids {
            let frame_arc = self.buffer_pool.write().await.get_page(self.file_id, page_id).await?;
            let mut frame = frame_arc.write().await;

            let result = frame.page.insert_tuple(tuple);
            if let Ok(slot_id) = result {
                frame.is_dirty = true;
                return Some((page_id, slot_id));
            }
        }

        let mut pool = self.buffer_pool.write().await;
        let new_page_id = pool.allocate_new_page(self.file_id).await;
        self.page_ids.push(new_page_id);

        let frame_arc = pool.get_page(self.file_id, new_page_id).await?;
        let mut frame = frame_arc.write().await;

        let slot_id = frame.page.insert_tuple(tuple).ok()?;
        frame.is_dirty = true;

        Some((new_page_id, slot_id))
    }

    pub async fn get_tuple(&self, page_id: u32, slot_id: usize) -> Option<Tuple> {
        let mut pool = self.buffer_pool.write().await;
        let frame_arc = pool.get_page(self.file_id, page_id).await?;
        let frame = frame_arc.read().await;

        frame.page.get_tuple(slot_id)
    }
}
