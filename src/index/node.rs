use super::{internal_node::InternalNode, leaf_node::LeafNode};

#[derive(Debug)]
pub enum Node<K, V> {
    Leaf(LeafNode<K, V>),
    Internal(InternalNode<K, V>),
}
