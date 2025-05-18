use crate::page::file::{PageFile, EXTENSION};
use crate::page::PAGE_SIZE;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

pub struct FileSystemManager {
    home_dir: String,
}

impl FileSystemManager {
    pub fn new(home_dir: String) -> Self {
        FileSystemManager { home_dir }
    }

    pub async fn create_home(&self) -> std::io::Result<()> {
        tokio::fs::create_dir_all(&self.home_dir).await
    }

    pub async fn open_page_file(&self, file_id: u32) -> std::io::Result<PageFile> {
        let path = format!("{}/pg_{}ak.{}", self.home_dir, file_id, EXTENSION);
        PageFile::open(file_id, &path).await
    }
}

struct WriteJob {
    file_id: u32,
    page_id: u32,
    data: Vec<u8>,
}

#[derive(Clone)]
pub struct IoManager {
    inner: Arc<FileSystemManager>,
    open_files: Arc<Mutex<HashMap<u32, PageFile>>>,
    tx: mpsc::UnboundedSender<WriteJob>,
}

impl IoManager {
    pub fn new(inner: Arc<FileSystemManager>) -> Self {
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

    pub async fn get_page_count(&self, file_id: u32) -> std::io::Result<u32> {
        let mut map = self.open_files.lock().await;
        let pf = map.entry(file_id).or_insert(self.inner.open_page_file(file_id).await?);
        pf.get_page_count().await
    }

    pub fn schedule_write(&self, file_id: u32, page_id: u32, data: Vec<u8>) {
        let _ = self.tx.send(WriteJob { file_id, page_id, data });
    }
}
