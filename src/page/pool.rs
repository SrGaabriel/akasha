use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::page::io::PageFileIO;
use crate::page::Page;

#[derive(Clone, Debug)]
pub struct Frame {
    pub(crate) page: Page,
    pub(crate) is_dirty: bool,
    pin_count: usize,
}

pub struct BufferPool {
    pub frames: Vec<Option<Arc<RwLock<Frame>>>>,
    pub page_table: HashMap<(u32, u32), usize>,
    pub lru_list: VecDeque<usize>,
    pub free_list: VecDeque<usize>,
    pub file_accessor: Arc<PageFileIO>,
}

impl BufferPool {
    pub fn new(capacity: usize, file_accessor: Arc<PageFileIO>) -> Self {
        BufferPool {
            frames: vec![None; capacity],
            page_table: HashMap::with_capacity(capacity),
            lru_list: VecDeque::with_capacity(capacity),
            free_list: (0..capacity).collect(),
            file_accessor,
        }
    }

    pub async fn get_page(&mut self, file_id: u32, page_id: u32) -> std::io::Result<Arc<RwLock<Frame>>> {
        let key = (file_id, page_id);
        if let Some(&frame_index) = self.page_table.get(&key) {
            return self.use_existing_frame(frame_index).await;
        }

        let frame_index = match self.free_list.pop_front() {
            Some(index) => index,
            None => {
                if let Some(evict_index) = self.evict_frame().await {
                    evict_index
                } else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "No free frames available and unable to evict any",
                    ));
                }
            }
        };
        let page = self.file_accessor.read_page(file_id, page_id).await?;
        let frame = Frame { page, is_dirty: false, pin_count: 1 };
        let frame_arc = Arc::new(RwLock::new(frame));
        self.frames[frame_index] = Some(frame_arc.clone());
        self.page_table.insert(key, frame_index);
        self.lru_list.push_back(frame_index);
        Ok(frame_arc)
    }

    async fn use_existing_frame(&mut self, frame_index: usize) -> std::io::Result<Arc<RwLock<Frame>>> {
        let frame_arc = self.frames[frame_index].as_ref().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "Frame not found")
        })?;
        {
            let mut frame = frame_arc.write().await;
            frame.pin_count += 1;
        }
        self.lru_list.retain(|&i| i != frame_index);
        self.lru_list.push_back(frame_index);
        Ok(frame_arc.clone())
    }

    async fn evict_frame(&mut self) -> Option<usize> {
        while let Some(index) = self.lru_list.pop_front() {
            let frame_opt = self.frames[index].take()?.clone();
            let cloned_frame = frame_opt.clone();
            let frame = cloned_frame.read().await;
            if frame.pin_count > 0 {
                self.frames[index] = Some(frame_opt);
                continue;
            }
            if frame.is_dirty {
                self.file_accessor.write_page(frame.page.index, &frame.page).await.ok()?;
            }
            self.page_table.remove(&(frame.page.index, frame.page.index));
            return Some(index);
        }
        None
    }

    pub async fn allocate_new_page(&self, file_id: u32) -> std::io::Result<u32> {
        let new_page_id = self.file_accessor.num_pages(file_id).await;
        let new_page = Page::new(new_page_id);
        self.file_accessor.write_page(file_id, &new_page).await?;
        Ok(new_page_id)
    }
}