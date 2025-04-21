use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::page::file::PageFileIO;
use crate::page::Page;

#[derive(Clone, Debug)]
pub struct Frame {
    pub(crate) page: Page,
    pub(crate) is_dirty: bool,
    pin_count: usize,
}

pub struct BufferPool {
    pub(crate) frames: Vec<Option<Arc<RwLock<Frame>>>>,
    page_table: HashMap<(u32, u32), usize>,
    lru_list: VecDeque<usize>,
    free_list: VecDeque<usize>,
    pub(crate) file_accessor: Arc<PageFileIO>
}

impl BufferPool {
    pub fn new(capacity: usize, file_accessor: Arc<PageFileIO>) -> Self {
        BufferPool {
            frames: vec![None; capacity],
            page_table: HashMap::new(),
            lru_list: VecDeque::new(),
            free_list: (0..capacity).collect(), // Pre-fill all free frame indices
            file_accessor
        }
    }

    pub async fn get_page(&mut self, file_id: u32, page_id: u32) -> Option<Arc<RwLock<Frame>>> {
        let key = (file_id, page_id);

        if let Some(&frame_index) = self.page_table.get(&key) {
            return self.use_existing_frame(frame_index).await;
        }

        let frame_index = if let Some(index) = self.free_list.pop_front() {
            index
        } else {
            self.evict_frame().await?
        };

        let page = self.file_accessor.read_page(file_id, page_id).await.ok()?;
        let frame = Frame {
            page,
            is_dirty: false,
            pin_count: 1,
        };
        let frame_arc = Arc::new(RwLock::new(frame));

        self.frames[frame_index] = Some(Arc::clone(&frame_arc));
        self.page_table.insert(key, frame_index);
        self.lru_list.push_back(frame_index);

        Some(frame_arc)
    }

    async fn use_existing_frame(&mut self, frame_index: usize) -> Option<Arc<RwLock<Frame>>> {
        if let Some(frame_arc) = &self.frames[frame_index] {
            let mut frame = frame_arc.write().await;
            frame.pin_count += 1;
            drop(frame);

            self.lru_list.retain(|&i| i != frame_index);
            self.lru_list.push_back(frame_index);

            Some(Arc::clone(frame_arc))
        } else {
            None
        }
    }

    async fn evict_frame(&mut self) -> Option<usize> {
        while let Some(index) = self.lru_list.pop_front() {
            let Some(evicted_arc) = self.frames[index].take() else { continue };

            let evicted_arc_clone = Arc::clone(&evicted_arc);
            let evicted = evicted_arc_clone.read().await;
            if evicted.pin_count > 0 {
                self.frames[index] = Some(evicted_arc);
                continue;
            }

            if evicted.is_dirty {
                self.file_accessor.write_page(0, &evicted.page).await.ok()?; // TODO: support multiple file_ids
            }

            self.page_table.remove(&(0, evicted.page.index)); // TODO: use real file_id
            return Some(index);
        }

        None
    }

    pub async fn allocate_new_page(&self, file_id: u32) -> u32 {
        let new_page_id = self.file_accessor.num_pages(file_id).await;
        let new_page = Page::new(new_page_id);
        self.file_accessor.write_page(file_id, &new_page).await.unwrap();
        new_page_id
    }
}