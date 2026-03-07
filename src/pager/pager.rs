use anyhow::Result;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
};
use thiserror::Error;
pub struct Pager {
    page_count: usize,
    file: File,
    buffer_pool: HashMap<usize, Page>,
}

#[derive(Debug)]
pub struct Page {
    pub content: [u8; Pager::PAGE_SIZE],
    pub page_id: usize,
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
    pub const CACHE_SIZE: usize = 100;

    pub fn new(file_path: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(file_path)?;

        if !(file.metadata()?.len() as usize).is_multiple_of(Self::PAGE_SIZE) {
            return Err(PageError::CorruptedDatabaseFile(file_path.into()).into());
        }

        let page_count = file.metadata()?.len() as usize / Self::PAGE_SIZE;
        Ok(Self {
            page_count,
            file,
            buffer_pool: HashMap::new(),
        })
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

        Ok(Page { content, page_id })
    }

    pub fn allocate_page(&mut self) -> Result<Page> {
        let content = [0u8; Self::PAGE_SIZE];
        self.write_content(self.page_count, &content)?;
        let page_id = self.page_count;
        self.page_count += 1;
        Ok(Page { content, page_id })
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

    #[test]
    fn test_pager_write_and_read() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path_str = temp_file.path().to_str().expect("Path not valid UTF-8");
        let mut pager = Pager::new(path_str)?;
        let allocate_page = pager.allocate_page()?;
        println!("Page id - {}", allocate_page.page_id);
        assert!(allocate_page.page_id == 0);
        assert_eq!(allocate_page.content, [0u8; Pager::PAGE_SIZE]);
        let content = [1u8; Pager::PAGE_SIZE];
        pager.write_page(0, &content)?;
        let page = pager.read_page(0)?;
        assert_eq!(page.content, content);
        assert_eq!(page.page_id, 0);
        Ok(())
    }

    #[test]
    fn test_pager_write_and_read_negative() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path_str = temp_file.path().to_str().expect("Path not valid UTF-8");
        let mut pager = Pager::new(path_str)?;
        let allocate_page = pager.allocate_page()?;
        println!("Page id - {}", allocate_page.page_id);
        assert!(allocate_page.page_id == 0);
        assert_eq!(allocate_page.content, [0u8; Pager::PAGE_SIZE]);
        let content = [1u8; Pager::PAGE_SIZE + 2];
        assert_eq!(
            pager.write_page(2, &content).unwrap_err().to_string(),
            PageError::InvalidPageId(2).to_string()
        );
        assert_eq!(
            pager.write_page(0, &content).unwrap_err().to_string(),
            PageError::InvalidPageSize(4098).to_string()
        );
        assert_eq!(
            pager.read_page(2).unwrap_err().to_string(),
            PageError::InvalidPageId(2).to_string()
        );
        Ok(())
    }
}
