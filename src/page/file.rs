use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use crate::page::{Page, PAGE_SIZE};

pub const EXTENSION: &str = "record";

pub struct PageFile {
    id: u32,
    pub(crate) file: File
}

impl PageFile {
    pub async fn open(id: u32, path: &str) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .await?;
        Ok(PageFile { id, file })
    }

    pub async fn read_page(&mut self, page_index: u32) -> std::io::Result<Page> {
        let mut buffer = [0u8; PAGE_SIZE];
        let offset = (page_index as usize) * PAGE_SIZE;
        self.file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
        self.file.read_exact(&mut buffer).await?;
        Ok(Page::from_bytes(page_index, buffer))
    }

    pub async fn write_page(&mut self, page: &Page) -> std::io::Result<()> {
        let offset = (page.index as usize) * PAGE_SIZE;
        self.file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
        self.file.write_all(&page.to_bytes()).await?;
        Ok(())
    }

    pub async fn metadata(&self) -> std::io::Result<std::fs::Metadata> {
        self.file.metadata().await
    }
}