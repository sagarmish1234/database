use anyhow::Result;
use std::{cell::RefCell, rc::Rc};

use crate::{
    cache::{BufferPool, PageId},
    index,
};

use super::{
    internal_node::InternalNode,
    leaf_node::{KeyValue, LeafNode},
    Node,
};

pub struct BPlustree<K, V: Clone> {
    root: Box<Node<K, V>>,
    order: usize,
}

enum InsertType<K, V> {
    NoSplit,
    Split(K, Box<Node<K, V>>),
}

impl<K: Ord + Clone, V: Clone + Ord> BPlustree<K, V> {
    pub fn new(order: usize) -> Self {
        Self {
            root: Box::new(Node::Leaf(LeafNode { pairs: Vec::new() })),
            order,
        }
    }

    pub fn search(&self, key: K) -> Option<&V> {
        Self::search_node(self.root.as_ref(), &key)
    }

    pub fn search_node<'a>(node: &'a Node<K, V>, key: &K) -> Option<&'a V> {
        match node {
            Node::Leaf(leaf_node) => Self::search_leaf(key, leaf_node),
            Node::Internal(internal_node) => Self::search_internal(key, internal_node),
        }
    }

    fn search_leaf<'a>(key: &K, leaf_node: &'a LeafNode<K, V>) -> Option<&'a V> {
        for (i, k) in leaf_node.pairs.iter().enumerate() {
            if k.key == *key {
                return Some(&leaf_node.pairs[i].value);
            }
        }
        None
    }

    fn search_internal<'a>(key: &K, internal_node: &'a InternalNode<K, V>) -> Option<&'a V> {
        let pos = internal_node
            .keys
            .binary_search(&key)
            .map(|i| i + 1)
            .unwrap_or_else(|x| x);

        Self::search_node(internal_node.children[pos].as_ref(), &key)
    }

    pub fn insert(&mut self, key: K, value: V) {
        let result = Self::insert_into_node(self.root.as_mut(), key, value, self.order.clone());

        if let InsertType::Split(promoted_key, new_root) = result {
            let old_root = std::mem::replace(
                &mut self.root,
                Box::new(Node::Leaf(LeafNode { pairs: vec![] })),
            );
            self.root = Box::new(Node::Internal(InternalNode::new(
                vec![promoted_key],
                vec![old_root, new_root],
            )));
        }
    }

    fn insert_into_leaf(
        leaf: &mut LeafNode<K, V>,
        key: K,
        value: V,
        order: usize,
    ) -> InsertType<K, V> {
        let pair = KeyValue::new(key, value);
        let pos = leaf.pairs.binary_search(&pair).unwrap_or_else(|x| x);
        leaf.pairs.insert(pos.clone(), pair);
        if Self::is_leaf_overflow(leaf, order) {
            return Self::split_leaf(leaf, order);
        }
        InsertType::NoSplit
    }

    fn is_leaf_overflow(node: &LeafNode<K, V>, order: usize) -> bool {
        let limit = ((order as f64 - 1f64) / 2 as f64).ceil();

        node.pairs.len() as f64 > limit
    }

    fn insert_into_node(node: &mut Node<K, V>, key: K, value: V, order: usize) -> InsertType<K, V> {
        match node {
            Node::Leaf(leaf_node) => Self::insert_into_leaf(leaf_node, key, value, order),
            Node::Internal(internal_node) => {
                Self::insert_into_internal(internal_node, key, value, order)
            }
        }
    }

    fn split_leaf(leaf: &mut LeafNode<K, V>, order: usize) -> InsertType<K, V> {
        let pos = ((order as f64 - 1f64) / 2 as f64).ceil();
        let mut remaining = leaf.pairs.split_off(pos as usize);
        let mut new_leaf = LeafNode::new();
        new_leaf.pairs.append(&mut remaining);
        InsertType::Split(
            new_leaf.pairs[0].key.clone(),
            Box::new(Node::Leaf(new_leaf)),
        )
    }

    fn insert_into_internal(
        internal_node: &mut InternalNode<K, V>,
        key: K,
        value: V,
        order: usize,
    ) -> InsertType<K, V> {
        let pos = internal_node.keys.binary_search(&key).unwrap_or_else(|x| x);
        let insert_type =
            Self::insert_into_node(&mut internal_node.children[pos], key, value, order);

        if let InsertType::Split(promoted_key, new_root) = insert_type {
            let key_pos = internal_node
                .keys
                .binary_search(&promoted_key)
                .unwrap_or_else(|x| x);
            internal_node.keys.insert(key_pos, promoted_key);
            internal_node.children.insert(key_pos + 1, new_root);
            if Self::is_internal_overflow(internal_node, order) {
                return Self::split_internal(internal_node, order);
            }
        }
        InsertType::NoSplit
    }

    fn is_internal_overflow(internal_node: &mut InternalNode<K, V>, order: usize) -> bool {
        let limit = ((order as f64 - 1f64) / 2 as f64).ceil();

        internal_node.keys.len() as f64 > limit
    }

    // |16|21|22| ->    |21|
    //               |16|  |22|

    fn split_internal(internal_node: &mut InternalNode<K, V>, order: usize) -> InsertType<K, V> {
        let pos = ((order as f64 - 1f64) / 2 as f64).ceil();
        let mut remaining_keys = internal_node.keys.split_off(pos as usize);
        let remaining_children = internal_node.children.split_off(pos as usize + 1 as usize);
        let promoted_key = remaining_keys[0].clone();
        let new_internal = InternalNode::new(remaining_keys.split_off(1), remaining_children);
        InsertType::Split(promoted_key, Box::new(Node::Internal(new_internal)))
    }
}

#[cfg(test)]
mod b_tree_tests {
    use super::*;

    #[test]
    fn test_internal_node_search() {
        let left = Box::new(Node::Leaf(LeafNode {
            keys: vec![10, 20],
            values: vec!["a", "b"],
        }));

        let right = Box::new(Node::Leaf(LeafNode {
            keys: vec![30, 40],
            values: vec!["c", "d"],
        }));

        let root = Node::Internal(InternalNode {
            keys: vec![30],
            children: vec![left, right],
        });

        let tree = BPlustree {
            root: Box::new(root),
            order: 3,
        };

        assert_eq!(tree.search(20), Some(&"b"));
        assert_eq!(tree.search(40), Some(&"d"));
    }

    #[test]
    fn test_insert_single_value() {
        let mut tree = BPlustree::new(3);

        tree.insert(10, "ten".to_string());

        assert_eq!(tree.search(10), Some(&"ten".to_string()));
    }
    #[test]
    fn test_insert_multiple_sorted() {
        let mut tree = BPlustree::new(3);

        tree.insert(20, "twenty".to_string());
        tree.insert(10, "ten".to_string());
        tree.insert(30, "thirty".to_string());

        match tree.root.as_ref() {
            Node::Leaf(leaf) => {
                assert_eq!(leaf.keys, vec![10, 20, 30]);
            }
            _ => panic!("root should be leaf"),
        }
    }
    #[test]
    fn test_empty_tree_search() {
        let tree: BPlustree<i32, String> = BPlustree::new(3);

        let result = tree.search(10);

        assert_eq!(result, None);
    }

    #[test]
    fn test_search_existing_key() {
        let mut tree: BPlustree<i32, String> = BPlustree::new(3);

        // manually create a leaf node
        tree.root = Box::new(Node::Leaf(LeafNode {
            keys: vec![1, 2, 3],
            values: vec!["one".to_string(), "two".to_string(), "three".to_string()],
        }));

        let result = tree.search(2);

        assert_eq!(result, Some(&"two".to_string()));
    }

    #[test]
    fn test_search_non_existing_key() {
        let mut tree: BPlustree<i32, String> = BPlustree::new(3);

        tree.root = Box::new(Node::Leaf(LeafNode {
            keys: vec![1, 2, 3],
            values: vec!["one".to_string(), "two".to_string(), "three".to_string()],
        }));

        let result = tree.search(4);

        assert_eq!(result, None);
    }

    #[test]
    fn test_single_element_tree() {
        let mut tree: BPlustree<i32, String> = BPlustree::new(3);

        tree.root = Box::new(Node::Leaf(LeafNode {
            keys: vec![10],
            values: vec!["ten".to_string()],
        }));

        let result = tree.search(10);

        assert_eq!(result, Some(&"ten".to_string()));
    }

    #[test]
    fn test_multiple_keys_search() {
        let mut tree: BPlustree<i32, String> = BPlustree::new(4);

        tree.root = Box::new(Node::Leaf(LeafNode {
            keys: vec![5, 10, 15, 20],
            values: vec![
                "five".to_string(),
                "ten".to_string(),
                "fifteen".to_string(),
                "twenty".to_string(),
            ],
        }));

        assert_eq!(tree.search(5), Some(&"five".to_string()));
        assert_eq!(tree.search(15), Some(&"fifteen".to_string()));
        assert_eq!(tree.search(20), Some(&"twenty".to_string()));
    }
}
