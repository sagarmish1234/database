use super::Node;

#[derive(Debug)]
pub struct InternalNode<K, V> {
    pub keys: Vec<K>,
    pub children: Vec<Box<Node<K, V>>>,
}

impl<K, V> InternalNode<K, V> {
    pub fn new(keys: Vec<K>, children: Vec<Box<Node<K, V>>>) -> Self {
        Self { keys, children }
    }
}
