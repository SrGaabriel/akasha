use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use crate::page::file::{PageFile, EXTENSION};
use crate::page::{Page, PAGE_SIZE};

pub struct PageFileIO {
    home_dir: String,
}

impl PageFileIO {
    pub fn new(home_dir: String) -> Self {
        PageFileIO { home_dir }
    }

    pub async fn create_home(&self) -> std::io::Result<()> {
        tokio::fs::create_dir_all(&self.home_dir).await
    }

    pub async fn open_page_file(&self, file_id: u32) -> std::io::Result<PageFile> {
        let path = format!("{}/pg_{}ak.{}", self.home_dir, file_id, EXTENSION);
        PageFile::open(file_id, &path).await
    }

    pub async fn read_page<'a>(&self, file_id: u32, page_index: u32, buffer: &'a mut [u8; PAGE_SIZE]) -> std::io::Result<Page<'a>> {
        let mut page_file = self.open_page_file(file_id).await?;
        page_file.read_page_into_buffer(page_index, buffer).await
    }

    pub async fn write_page(&self, file_id: u32, page: &Page<'_>) -> std::io::Result<()> {
        let mut page_file = self.open_page_file(file_id).await?;
        page_file.write_page(page).await
    }

    pub async fn num_pages(&self, file_id: u32) -> u32 {
        let file = self.open_page_file(file_id).await.unwrap();
        let metadata = file.metadata().await.unwrap();
        (metadata.len() / PAGE_SIZE as u64) as u32
    }
}

struct WriteJob {
    file_id: u32,
    page_id: u32,
    data: Vec<u8>,
}

#[derive(Clone)]
pub struct IoManager {
    inner: Arc<PageFileIO>,
    open_files: Arc<Mutex<HashMap<u32, PageFile>>>,
    tx: mpsc::UnboundedSender<WriteJob>,
}

impl IoManager {
    pub fn new(inner: Arc<PageFileIO>) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<WriteJob>();
        let inner_clone = inner.clone();

        tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                if let Ok(mut pf) = inner_clone.open_page_file(job.file_id).await {
                    let _ = pf.write_page_data(job.page_id, job.data).await;
                }
            }
        });

        IoManager {
            inner,
            open_files: Arc::new(Mutex::new(HashMap::new())),
            tx,
        }
    }

    pub async fn read_into_buf(
        &self,
        file_id: u32,
        page_id: u32,
        buf: &mut [u8; PAGE_SIZE],
    ) -> std::io::Result<()> {
        let mut map = self.open_files.lock().await;
        let pf = match map.get_mut(&file_id) {
            Some(pf) => pf,
            None => {
                let pf = self.inner.open_page_file(file_id).await?;
                map.insert(file_id, pf);
                map.get_mut(&file_id).unwrap()
            }
        };
        pf.read_page_into_buffer(page_id, buf).await?;
        Ok(())
    }

    pub fn schedule_write(&self, file_id: u32, page_id: u32, data: Vec<u8>) {
        let _ = self.tx.send(WriteJob { file_id, page_id, data });
    }
}
