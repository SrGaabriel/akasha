// TODO: optimize this and learn more about buffer pools

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use crate::page::file::PageFileIO;
use crate::page::Page;

#[derive(Clone, Debug)]
pub struct Frame {
    page: Page,
    is_dirty: bool,
    pin_count: usize,
}

pub struct BufferPool {
    frames: Vec<Option<Arc<RwLock<Frame>>>>,
    page_table: HashMap<(u32, u32), usize>,
    lru_list: VecDeque<usize>,
    free_list: VecDeque<usize>,
    file_accessor: Arc<PageFileIO>
}

impl BufferPool {
    pub fn new(file_accessor: Arc<PageFileIO>) -> Self {
        BufferPool {
            frames: vec![],
            page_table: HashMap::new(),
            lru_list: VecDeque::new(),
            free_list: VecDeque::new(),
            file_accessor
        }
    }

    pub async fn get_page(&mut self, file_id: u32, page_id: u32) -> Option<Arc<RwLock<Frame>>> {
        let key = (file_id, page_id);

        // Case 1: Page is already in buffer
        if let Some(&frame_index) = self.page_table.get(&key) {
            if let Some(frame_arc) = &self.frames[frame_index] {
                let mut frame = frame_arc.write().await;
                frame.pin_count += 1;
                drop(frame); // release lock before return
                self.lru_list.retain(|&i| i != frame_index); // move to back
                self.lru_list.push_back(frame_index);
                return Some(Arc::clone(frame_arc));
            }
        }

        // Case 2: Use free frame if available
        if let Some(frame_index) = self.free_list.pop_front() {
            let frame = Frame {
                page: Page::new(page_id),
                is_dirty: false,
                pin_count: 1,
            };
            let frame_arc = Arc::new(RwLock::new(frame));
            self.frames[frame_index] = Some(Arc::clone(&frame_arc));
            self.page_table.insert(key, frame_index);
            self.lru_list.push_back(frame_index);
            return Some(frame_arc);
        }

        // Case 3: Evict a frame from LRU
        while let Some(evicted_index) = self.lru_list.pop_front() {
            // Take out the frame temporarily to avoid borrowing `self.frames` across await
            let evicted_frame_arc = match self.frames[evicted_index].take() {
                Some(arc) => arc,
                None => continue,
            };

            let evicted_arc = Arc::clone(&evicted_frame_arc);
            let mut evicted = evicted_arc.write().await;

            if evicted.pin_count > 0 {
                // Put the frame back and skip
                self.frames[evicted_index] = Some(evicted_frame_arc);
                continue;
            }

            if evicted.is_dirty {
                self.file_accessor.write_page(file_id, &evicted.page).await.unwrap();
            }

            let old_page_id = evicted.page.index;
            self.page_table.remove(&(file_id, old_page_id));

            // Prepare the new frame
            let new_frame = Frame {
                page: Page::new(page_id),
                is_dirty: false,
                pin_count: 1,
            };
            let new_arc = Arc::new(RwLock::new(new_frame));

            // Replace the evicted frame with the new one
            self.frames[evicted_index] = Some(Arc::clone(&new_arc));
            self.page_table.insert(key, evicted_index);
            self.lru_list.push_back(evicted_index);

            return Some(new_arc);
        }

        // All pages are pinned — can't load new page
        None
    }
}