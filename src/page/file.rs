use crate::page::{PAGE_SIZE, Page};
use std::io::SeekFrom;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub const EXTENSION: &str = "record";

pub struct RelationFile {
    #[allow(dead_code)]
    id: u32,
    pub(crate) file: File,
}

impl RelationFile {
    pub async fn open(id: u32, path: &str) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .await?;
        Ok(RelationFile { id, file })
    }

    pub async fn read_page_into_buffer<'a>(
        &mut self,
        page_index: u32,
        buffer: &'a mut [u8; PAGE_SIZE],
    ) -> std::io::Result<Page<'a>> {
        let offset = (page_index as u64) * (PAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset)).await?;
        self.file.read_exact(buffer).await?;
        Ok(Page::from_bytes(page_index, buffer))
    }

    pub async fn write_page(&mut self, page: &Page<'_>) -> std::io::Result<()> {
        let offset = (page.index as u64) * (PAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset)).await?;
        self.file.write_all(&page.to_bytes()).await?;
        self.file.sync_data().await?;
        Ok(())
    }

    pub async fn write_page_data(&mut self, page_id: u32, data: Vec<u8>) -> std::io::Result<()> {
        assert_eq!(data.len(), PAGE_SIZE, "data must be exactly one page");
        let offset = (page_id as u64) * (PAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset)).await?;
        self.file.write_all(&data).await?;
        self.file.sync_data().await?;
        Ok(())
    }

    pub async fn get_page_count(&self) -> std::io::Result<u32> {
        let size = self.file.metadata().await?.len();
        Ok((size / PAGE_SIZE as u64) as u32)
    }
}
