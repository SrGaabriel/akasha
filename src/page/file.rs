use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use crate::page::{Page, PAGE_SIZE};

pub struct PageFile {
    id: u32,
    file: File
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
        self.file.write_all(&page.data).await?;
        Ok(())
    }
}

pub struct PageFileIO {
    home_dir: String,
}

impl PageFileIO {
    pub fn new(home_dir: String) -> Self {
        PageFileIO { home_dir }
    }

    pub async fn open_page_file(&self, file_id: u32) -> std::io::Result<PageFile> {
        let path = format!("{}/tb_{}.aka", self.home_dir, file_id);
        PageFile::open(file_id, &path).await
    }

    pub async fn read_page(&self, file_id: u32, page_index: u32) -> std::io::Result<Page> {
        let mut page_file = self.open_page_file(file_id).await?;
        page_file.read_page(page_index).await
    }

    pub async fn write_page(&self, file_id: u32, page: &Page) -> std::io::Result<()> {
        let mut page_file = self.open_page_file(file_id).await?;
        page_file.write_page(page).await
    }
}