#[derive(Debug, Clone)]
pub struct LeafNode<K, V> {
    pub pairs: Vec<KeyValue<K, V>>,
}

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct KeyValue<K, V> {
    pub key: K,
    pub value: V,
}

impl<K, V> KeyValue<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}

impl<K, V> LeafNode<K, V> {
    pub fn new() -> Self {
        Self { pairs: Vec::new() }
    }
}
