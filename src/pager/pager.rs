use anyhow::Result;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    sync::Arc,
};
use thiserror::Error;

use crate::config::DbConfig;

pub struct Pager {
    page_count: usize,
    file: File,
}

#[derive(Debug, Clone)]
pub struct Page {
    pub content: [u8; Pager::PAGE_SIZE],
    pub page_id: usize,
    pub is_dirty: bool,
}

#[derive(Error, Debug, PartialEq)]
pub enum PageError {
    #[error("Invalid page id {0}")]
    InvalidPageId(usize),

    #[error("Invalid page content size {0}")]
    InvalidPageSize(usize),

    #[error("Corrupted database file {0}")]
    CorruptedDatabaseFile(String),
}

impl Pager {
    pub const PAGE_SIZE: usize = 4096;

    pub fn new(config: Arc<DbConfig>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&config.file_path)?;

        if !(file.metadata()?.len() as usize).is_multiple_of(config.page_size) {
            return Err(PageError::CorruptedDatabaseFile((&config.file_path).into()).into());
        }

        let page_count = file.metadata()?.len() as usize / Self::PAGE_SIZE;
        Ok(Self { page_count, file })
    }

    pub fn page_count(&self) -> usize {
        self.page_count
    }

    fn is_page_id_valid(&self, page_id: usize) -> Result<()> {
        if page_id >= self.page_count {
            return Err(PageError::InvalidPageId(page_id).into());
        }
        Ok(())
    }
    fn page_offset(page_id: usize) -> usize {
        Self::PAGE_SIZE * page_id
    }

    fn is_page_content_valid(&self, content: &[u8]) -> Result<()> {
        if content.len() != Self::PAGE_SIZE {
            return Err(PageError::InvalidPageSize(content.len()).into());
        }

        Ok(())
    }

    pub fn read_page(&mut self, page_id: usize) -> Result<Page> {
        self.is_page_id_valid(page_id)?;
        let mut content = [0u8; Self::PAGE_SIZE];

        let offset = Self::page_offset(page_id);
        self.file.seek(SeekFrom::Start(offset as u64))?;
        self.file.read_exact(&mut content)?;

        Ok(Page {
            content,
            page_id,
            is_dirty: false,
        })
    }

    pub fn allocate_page(&mut self) -> Result<Page> {
        let content = [0u8; Self::PAGE_SIZE];
        self.write_content(self.page_count, &content)?;
        let page_id = self.page_count;
        self.page_count += 1;
        Ok(Page {
            content,
            page_id,
            is_dirty: false,
        })
    }

    pub fn write_page(&mut self, page_id: usize, content: &[u8]) -> Result<()> {
        self.is_page_id_valid(page_id)?;
        self.write_content(page_id, content)?;
        Ok(())
    }

    fn write_content(&mut self, page_id: usize, content: &[u8]) -> Result<(), anyhow::Error> {
        self.is_page_content_valid(content)?;
        self.file
            .seek(SeekFrom::Start(Self::page_offset(page_id) as u64))?;
        self.file.write_all(content)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    // Helper to create a dummy config for tests
    fn create_test_config(path: &str) -> Arc<DbConfig> {
        Arc::new(DbConfig {
            file_path: path.to_string(),
            page_size: Pager::PAGE_SIZE,
            cache_size: 10,
            port: 8080,
        })
    }

    #[test]
    fn test_pager_initialization() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        let config = create_test_config(tmp.path().to_str().unwrap());

        let pager = Pager::new(config)?;
        assert_eq!(pager.page_count(), 0);
        Ok(())
    }

    #[test]
    fn test_allocate_and_read_page() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        let config = create_test_config(tmp.path().to_str().unwrap());
        let mut pager = Pager::new(config)?;

        // Allocate first page
        let page = pager.allocate_page()?;
        assert_eq!(page.page_id, 0);
        assert_eq!(pager.page_count(), 1);

        // Read it back
        let read_page = pager.read_page(0)?;
        assert_eq!(read_page.page_id, 0);
        assert_eq!(read_page.content, [0u8; Pager::PAGE_SIZE]);
        Ok(())
    }

    #[test]
    fn test_write_and_persistence() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        let path = tmp.path().to_str().unwrap().to_string();
        let config = create_test_config(&path);

        let mut pager = Pager::new(config.clone())?;
        pager.allocate_page()?;

        // Write specific data
        let mut data = [0u8; Pager::PAGE_SIZE];
        data[0] = 42;
        data[Pager::PAGE_SIZE - 1] = 99;
        pager.write_page(0, &data)?;

        // Drop original pager to flush/close file
        drop(pager);

        // Re-open pager and check if data persisted
        let mut new_pager = Pager::new(config)?;
        let read_page = new_pager.read_page(0)?;
        assert_eq!(read_page.content[0], 42);
        assert_eq!(read_page.content[Pager::PAGE_SIZE - 1], 99);
        Ok(())
    }

    #[test]
    fn test_invalid_page_id() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        let config = create_test_config(tmp.path().to_str().unwrap());
        let mut pager = Pager::new(config)?;

        let result = pager.read_page(999);
        assert!(result.is_err());

        // Downcast to check specific error type if needed
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Invalid page id 999"));
        Ok(())
    }

    #[test]
    fn test_corrupted_file_size() -> Result<()> {
        let mut tmp = NamedTempFile::new()?;
        // Write 100 bytes (not a multiple of 4096)
        tmp.write_all(&[0u8; 100])?;

        let config = create_test_config(tmp.path().to_str().unwrap());
        let result = Pager::new(config);

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_invalid_content_size_write() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        let config = create_test_config(tmp.path().to_str().unwrap());
        let mut pager = Pager::new(config)?;

        pager.allocate_page()?;
        let short_data = vec![1, 2, 3];
        let result = pager.write_page(0, &short_data);

        assert!(result.is_err());
        Ok(())
    }
}
