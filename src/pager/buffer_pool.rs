use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::cache::LruCache;

use super::Page;

type PageId = usize;

struct BufferPool {
    cache: HashMap<PageId, Rc<RefCell<Page>>>,
    cache_limit: usize,
    cache_evictor: LruCache<PageId>,
}

impl BufferPool {
    const DEFAULT_CACHE_LIMIT: usize = 100;

    pub fn init_with_cache_limit(cache_limit: usize) -> Self {
        Self {
            cache: HashMap::new(),
            cache_limit,
        }
    }

    pub fn init() -> Self {
        Self {
            cache: HashMap::new(),
            cache_limit: Self::DEFAULT_CACHE_LIMIT,
        }
    }

    pub fn page_exists(&self, page_id: usize) -> bool {
        self.cache.contains_key(&page_id)
    }

    pub fn get_page(&self, page_id: usize) -> Option<Rc<RefCell<Page>>> {
        if let Some(page) = self.cache.get(&page_id) {
            return Some(Rc::clone(page));
        }
        None
    }

    pub fn put_page(&mut self, page: Page) {
        self.cache.insert(page.page_id, Rc::new(RefCell::new(page)));
    }
}
