use std::{cell::RefCell, collections::HashMap, hash::Hash, rc::Rc, sync::Arc};

use thiserror::Error;

use anyhow::Result;

use crate::config::DbConfig;

use super::{DoublyLinkedList, Node};

#[derive(Error, Debug, PartialEq)]
pub enum CacheError {
    #[error("Key not found")]
    KeyNotFound,

    #[error("Eviction not possible as there are no keys to evict")]
    EvictionNotPossible,
}

pub struct LruCache<T> {
    map: HashMap<T, Rc<RefCell<Node<T>>>>,
    list: DoublyLinkedList<T>,
    capacity: usize,
}

impl<T: Default + Hash + Eq + Clone> LruCache<T> {
    pub fn new(config: Arc<DbConfig>) -> Self {
        Self {
            map: HashMap::new(),
            list: DoublyLinkedList::new(),
            capacity: config.cache_size,
        }
    }

    pub fn access(&mut self, key: T) {
        let node = self.map.get(&key).clone();
        if let None = node {
            let node = self.list.push_front(key.clone());
            self.map.insert(key, node);
            return;
        }
        let internal = node.unwrap();
        self.list.remove_node(internal.clone());
        self.list.push_node_front(internal.clone());
    }

    pub fn remove(&mut self, key: T) -> Result<()> {
        let node = self.map.get(&key).clone();
        if let None = node {
            return Err(CacheError::KeyNotFound.into());
        }
        let internal = node.unwrap();
        self.list.remove_node(internal.clone());
        self.map.remove(&key);
        Ok(())
    }

    pub fn evict(&mut self) -> Result<T> {
        let node = self.list.peek_back();
        if let None = node {
            return Err(CacheError::EvictionNotPossible.into());
        }

        Ok(node.unwrap().borrow().key())
    }

    pub fn len(&mut self) -> usize {
        self.map.len()
    }

    pub fn is_full(&mut self) -> bool {
        self.len() >= self.capacity
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    // Helper to quickly build a config for tests
    fn mock_config(cache_size: usize) -> Arc<DbConfig> {
        Arc::new(DbConfig {
            file_path: "test.db".to_string(),
            page_size: 4096,
            cache_size,
            port: 8080,
        })
    }

    #[test]
    fn test_new_cache() {
        let cache: LruCache<i32> = LruCache::new(mock_config(3));
        assert_eq!(cache.capacity, 3);
        assert_eq!(cache.map.len(), 0);
    }

    #[test]
    fn test_access_inserts_key() {
        let mut cache = LruCache::new(mock_config(3));

        cache.access(1);
        cache.access(2);

        assert_eq!(cache.len(), 2);
        assert!(cache.map.contains_key(&1));
        assert!(cache.map.contains_key(&2));
    }

    #[test]
    fn test_access_updates_recency() {
        let mut cache = LruCache::new(mock_config(3));

        cache.access(1);
        cache.access(2);
        cache.access(3);

        cache.access(1); // 1 is now most recent, 2 is oldest

        let victim = cache.evict().unwrap();
        assert_eq!(victim, 2);
    }

    #[test]
    fn test_remove_existing_key() {
        let mut cache = LruCache::new(mock_config(3));

        cache.access(1);
        cache.access(2);

        cache.remove(1).unwrap();

        assert_eq!(cache.len(), 1);
        assert!(!cache.map.contains_key(&1));
    }

    #[test]
    fn test_remove_nonexistent_key() {
        let mut cache = LruCache::new(mock_config(3));
        let result = cache.remove(42);
        assert!(result.is_err());
    }

    #[test]
    fn test_evict_returns_lru() {
        let mut cache = LruCache::new(mock_config(5));

        cache.access(10);
        cache.access(20);
        cache.access(30);

        let victim = cache.evict().unwrap();
        assert_eq!(victim, 10);
    }

    #[test]
    fn test_evict_empty_cache() {
        let mut cache: LruCache<i32> = LruCache::new(mock_config(3));
        let result = cache.evict();
        assert!(result.is_err());
    }

    #[test]
    fn test_is_full() {
        let mut cache = LruCache::new(mock_config(2));

        cache.access(1);
        assert!(!cache.is_full());

        cache.access(2);
        assert!(cache.is_full());
    }

    #[test]
    fn test_lru_order_complex_sequence() {
        let mut cache = LruCache::new(mock_config(10));

        cache.access(1);
        cache.access(2);
        cache.access(3);
        cache.access(4);

        cache.access(2);
        cache.access(3);

        let victim = cache.evict().unwrap();
        assert_eq!(victim, 1);
    }

    #[test]
    fn test_access_same_key_multiple_times() {
        let mut cache = LruCache::new(mock_config(3));

        cache.access(1);
        cache.access(1);
        cache.access(1);

        assert_eq!(cache.len(), 1);
        let victim = cache.evict().unwrap();
        assert_eq!(victim, 1);
    }
}
