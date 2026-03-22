use anyhow::Result;
use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::Arc};
use thiserror::Error;

use crate::{
    cache::LruCache,
    config::DbConfig,
    pager::{Page, Pager},
};

pub type PageId = usize;

#[derive(Error, Debug, PartialEq)]
pub enum BufferPoolError {
    #[error("Page with id {0} not found")]
    PageNotFound(PageId),
}

pub struct BufferPool {
    pool: HashMap<PageId, Rc<RefCell<Page>>>,
    cache_evictor: LruCache<PageId>,
    pager: Pager,
    capacity: usize,
}

// new() DONE
// get_page() DONE
// allocate_page() DONE
// evict_page() DONE
// flush_page()  DONE
// flush_all() DONE

impl BufferPool {
    pub fn new(config: Arc<DbConfig>) -> Result<Self> {
        Ok(Self {
            pool: HashMap::new(),
            cache_evictor: LruCache::new(config.clone()),
            pager: Pager::new(config.clone())?,
            capacity: config.cache_size,
        })
    }

    pub fn page_exists(&self, page_id: PageId) -> bool {
        self.pool.contains_key(&page_id)
    }

    pub fn get_page(&mut self, page_id: PageId) -> Result<Rc<RefCell<Page>>> {
        if let Some(page) = self.pool.get(&page_id) {
            self.cache_evictor.access(page_id);
            return Ok(Rc::clone(page));
        }

        if self.pool.len() >= self.capacity {
            self.evict_page()?;
        }

        let page = Rc::new(RefCell::new(self.pager.read_page(page_id)?));
        self.pool.insert(page_id, page.clone());
        Ok(page.clone())
    }

    pub fn allocate_page(&mut self) -> Result<Rc<RefCell<Page>>> {
        let page = Rc::new(RefCell::new(self.pager.allocate_page()?));
        let key = page.borrow().page_id.clone();
        if self.pool.len() >= self.capacity {
            self.evict_page()?;
        }
        self.cache_evictor.access(key.clone());
        self.pool.insert(key, page.clone());
        Ok(page.clone())
    }

    pub fn evict_page(&mut self) -> Result<()> {
        let page_id = self.cache_evictor.evict()?;
        let page = self.pool.get(&page_id).unwrap();
        if page.borrow().is_dirty {
            self.flush_page(page_id)?;
        }
        self.pool.remove(&page_id);
        self.cache_evictor.remove(page_id)?;
        Ok(())
    }

    pub fn flush_page(&mut self, page_id: PageId) -> Result<()> {
        if let None = self.pool.get(&page_id) {
            return Err(BufferPoolError::PageNotFound(page_id.clone()).into());
        }

        self.cache_evictor.access(page_id.clone());
        let page = self.pool.get(&page_id).unwrap().clone();
        self.pager.write_page(page_id, &page.borrow().content)?;
        page.borrow_mut().is_dirty = false;
        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<()> {
        let page_ids: Vec<_> = self.pool.keys().copied().collect();
        for page_id in page_ids {
            self.flush_page(page_id.clone())?
        }

        Ok(())
    }
}

#[cfg(test)]
mod buffered_pool_tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    fn test_config(cache_size: usize) -> Arc<DbConfig> {
        let file = NamedTempFile::new().unwrap();

        Arc::new(DbConfig {
            file_path: file.path().to_str().unwrap().to_string(),
            page_size: 4096,
            cache_size,
            port: 0,
        })
    }

    #[test]
    fn test_buffer_pool_creation() {
        let config = test_config(8);
        let bp = BufferPool::new(config).unwrap();

        assert_eq!(bp.pool.len(), 0);
        assert_eq!(bp.capacity, 8);
    }

    #[test]
    fn test_page_exists_true_false() {
        let config = test_config(4);
        let mut bp = BufferPool::new(config).unwrap();

        let page = bp.allocate_page().unwrap();
        let id = page.borrow().page_id;

        assert!(bp.page_exists(id));
        assert!(!bp.page_exists(id + 100));
    }

    #[test]
    fn test_allocate_page_inserts_into_pool() {
        let config = test_config(4);
        let mut bp = BufferPool::new(config).unwrap();

        let page = bp.allocate_page().unwrap();
        let id = page.borrow().page_id;

        assert_eq!(bp.pool.len(), 1);
        assert!(bp.page_exists(id));
    }

    #[test]
    fn test_allocate_multiple_pages() {
        let config = test_config(5);
        let mut bp = BufferPool::new(config).unwrap();

        let mut ids = Vec::new();

        for _ in 0..5 {
            let page = bp.allocate_page().unwrap();
            ids.push(page.borrow().page_id);
        }

        assert_eq!(bp.pool.len(), 5);

        for id in ids {
            assert!(bp.page_exists(id));
        }
    }

    #[test]
    fn test_get_page_cache_hit() {
        let config = test_config(4);
        let mut bp = BufferPool::new(config).unwrap();

        let page = bp.allocate_page().unwrap();
        let id = page.borrow().page_id;

        let fetched = bp.get_page(id).unwrap();

        assert_eq!(fetched.borrow().page_id, id);
        assert_eq!(bp.pool.len(), 1);
    }

    #[test]
    fn test_get_page_cache_miss_reads_from_disk() {
        let config = test_config(4);
        let mut bp = BufferPool::new(config.clone()).unwrap();

        let page = bp.allocate_page().unwrap();
        let id = page.borrow().page_id;

        bp.pool.clear();

        let fetched = bp.get_page(id).unwrap();

        assert_eq!(fetched.borrow().page_id, id);
        assert_eq!(bp.pool.len(), 1);
    }

    #[test]
    fn test_eviction_when_capacity_reached() {
        let config = test_config(2);
        let mut bp = BufferPool::new(config).unwrap();

        bp.allocate_page().unwrap();
        bp.allocate_page().unwrap();

        assert_eq!(bp.pool.len(), 2);

        bp.allocate_page().unwrap();

        assert_eq!(bp.pool.len(), 2);
    }

    #[test]
    fn test_eviction_removes_page_from_pool() {
        let config = test_config(1);
        let mut bp = BufferPool::new(config).unwrap();

        let p1 = bp.allocate_page().unwrap();
        let id = p1.borrow().page_id;

        bp.allocate_page().unwrap();

        assert!(!bp.page_exists(id));
    }

    #[test]
    fn test_eviction_flushes_dirty_page() {
        let config = test_config(1);
        let mut bp = BufferPool::new(config).unwrap();

        let p1 = bp.allocate_page().unwrap();
        let id = p1.borrow().page_id;

        p1.borrow_mut().is_dirty = true;

        bp.allocate_page().unwrap();

        assert!(!bp.page_exists(id));
    }

    #[test]
    fn test_flush_page_clears_dirty_flag() {
        let config = test_config(4);
        let mut bp = BufferPool::new(config).unwrap();

        let page = bp.allocate_page().unwrap();
        let id = page.borrow().page_id;

        page.borrow_mut().is_dirty = true;

        bp.flush_page(id).unwrap();

        assert!(!page.borrow().is_dirty);
    }

    #[test]
    fn test_flush_page_error_if_not_found() {
        let config = test_config(4);
        let mut bp = BufferPool::new(config).unwrap();

        let result = bp.flush_page(9999);

        assert!(result.is_err());
    }

    #[test]
    fn test_flush_all_clears_all_dirty_pages() {
        let config = test_config(4);
        let mut bp = BufferPool::new(config).unwrap();

        let p1 = bp.allocate_page().unwrap();
        let p2 = bp.allocate_page().unwrap();

        p1.borrow_mut().is_dirty = true;
        p2.borrow_mut().is_dirty = true;

        bp.flush_all().unwrap();

        assert!(!p1.borrow().is_dirty);
        assert!(!p2.borrow().is_dirty);
    }

    #[test]
    fn test_flush_all_with_empty_pool() {
        let config = test_config(4);
        let mut bp = BufferPool::new(config).unwrap();

        let result = bp.flush_all();

        assert!(result.is_ok());
    }

    #[test]
    fn test_capacity_never_exceeded() {
        let config = test_config(3);
        let mut bp = BufferPool::new(config).unwrap();

        for _ in 0..10 {
            bp.allocate_page().unwrap();
            assert!(bp.pool.len() <= 3);
        }
    }

    #[test]
    fn test_repeated_get_page_updates_cache() {
        let config = test_config(3);
        let mut bp = BufferPool::new(config).unwrap();

        let p1 = bp.allocate_page().unwrap();
        let id = p1.borrow().page_id;

        for _ in 0..10 {
            let fetched = bp.get_page(id).unwrap();
            assert_eq!(fetched.borrow().page_id, id);
        }

        assert_eq!(bp.pool.len(), 1);
    }

    #[test]
    fn test_flush_then_get_page() {
        let config = test_config(3);
        let mut bp = BufferPool::new(config.clone()).unwrap();

        let p1 = bp.allocate_page().unwrap();
        let id = p1.borrow().page_id;

        p1.borrow_mut().is_dirty = true;

        bp.flush_page(id).unwrap();

        let fetched = bp.get_page(id).unwrap();

        assert_eq!(fetched.borrow().page_id, id);
        assert!(!fetched.borrow().is_dirty);
    }

    #[test]
    fn test_allocate_after_flush_all() {
        let config = test_config(2);
        let mut bp = BufferPool::new(config).unwrap();

        let p1 = bp.allocate_page().unwrap();
        p1.borrow_mut().is_dirty = true;

        bp.flush_all().unwrap();

        bp.allocate_page().unwrap();

        assert!(bp.pool.len() <= 2);
        assert!(!p1.borrow().is_dirty);
    }

    #[test]
    fn test_large_sequence_operations() {
        let config = test_config(4);
        let mut bp = BufferPool::new(config).unwrap();

        for _ in 0..20 {
            let page = bp.allocate_page().unwrap();
            let id = page.borrow().page_id;

            let fetched = bp.get_page(id).unwrap();
            assert_eq!(fetched.borrow().page_id, id);
        }

        assert!(bp.pool.len() <= 4);
    }
}
