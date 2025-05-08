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

    pub async fn read_page(&self, file_id: u32, page_index: u32) -> std::io::Result<Page> {
        let mut page_file = self.open_page_file(file_id).await?;
        page_file.read_page(page_index).await
    }

    pub async fn write_page(&self, file_id: u32, page: &Page) -> std::io::Result<()> {
        let mut page_file = self.open_page_file(file_id).await?;
        page_file.write_page(page).await
    }

    pub async fn num_pages(&self, file_id: u32) -> u32 {
        let file = self.open_page_file(file_id).await.unwrap();
        let metadata = file.metadata().await.unwrap();
        (metadata.len() / PAGE_SIZE as u64) as u32
    }
}