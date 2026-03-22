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
        let limit = (order - 1);

        node.pairs.len() > limit
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
        let limit = (order - 1);
        internal_node.keys.len() > limit
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

    // ---------- Helper ----------
    fn validate_node<K: Ord + Clone, V: Clone>(node: &Node<K, V>) {
        match node {
            Node::Leaf(leaf) => {
                for i in 1..leaf.pairs.len() {
                    assert!(leaf.pairs[i - 1].key <= leaf.pairs[i].key);
                }
            }

            Node::Internal(internal) => {
                for i in 1..internal.keys.len() {
                    assert!(internal.keys[i - 1] <= internal.keys[i]);
                }

                assert_eq!(internal.children.len(), internal.keys.len() + 1);

                for child in &internal.children {
                    validate_node(child);
                }
            }
        }
    }

    // ---------- Basic Tests ----------

    #[test]
    fn test_empty_tree_search() {
        let tree: BPlustree<i32, String> = BPlustree::new(3);
        assert_eq!(tree.search(10), None);
    }

    #[test]
    fn test_single_insert() {
        let mut tree = BPlustree::new(3);

        tree.insert(10, "ten".to_string());

        assert_eq!(tree.search(10).map(|v| v.as_str()), Some("ten"));
    }

    #[test]
    fn test_multiple_keys_search() {
        let mut tree: BPlustree<i32, String> = BPlustree::new(4);

        tree.root = Box::new(Node::Leaf(LeafNode {
            pairs: vec![
                KeyValue::new(5, "five".to_string()),
                KeyValue::new(10, "ten".to_string()),
                KeyValue::new(15, "fifteen".to_string()),
                KeyValue::new(20, "twenty".to_string()),
            ],
        }));

        assert_eq!(tree.search(5).map(|v| v.as_str()), Some("five"));
        assert_eq!(tree.search(15).map(|v| v.as_str()), Some("fifteen"));
        assert_eq!(tree.search(20).map(|v| v.as_str()), Some("twenty"));
    }

    // ---------- Insert Behavior ----------

    #[test]
    fn test_insert_preserves_sorted_order() {
        let mut tree = BPlustree::new(4);

        tree.insert(30, "a".to_string());
        tree.insert(10, "b".to_string());
        tree.insert(20, "c".to_string());

        match tree.root.as_ref() {
            Node::Leaf(leaf) => {
                let keys: Vec<_> = leaf.pairs.iter().map(|p| p.key).collect();
                assert_eq!(keys, vec![10, 20, 30]);
            }
            _ => panic!("expected leaf"),
        }
    }

    #[test]
    fn test_leaf_split_trigger() {
        let mut tree = BPlustree::new(3);

        tree.insert(10, "a".to_string());
        tree.insert(20, "b".to_string());
        tree.insert(30, "c".to_string());
        tree.insert(40, "d".to_string());

        match tree.root.as_ref() {
            Node::Internal(_) => {}
            _ => panic!("expected root to be internal after split"),
        }

        validate_node(tree.root.as_ref());
    }

    // ---------- Deep Insert / Split Tests ----------

    #[test]
    fn test_bulk_insert_and_search() {
        let mut tree = BPlustree::new(3);

        for i in 1..100 {
            tree.insert(i, i.to_string());
        }

        for i in 1..100 {
            assert_eq!(
                tree.search(i).map(|v| v.as_str()),
                Some(i.to_string().as_str())
            );
        }

        validate_node(tree.root.as_ref());
    }

    #[test]
    fn test_internal_node_split() {
        let mut tree = BPlustree::new(3);

        for i in 1..100 {
            tree.insert(i, i);
        }

        match tree.root.as_ref() {
            Node::Internal(root) => {
                assert!(root.children.len() >= 2);
            }
            _ => panic!("root should be internal"),
        }

        validate_node(tree.root.as_ref());
    }

    // ---------- Stress Tests ----------

    #[test]
    fn test_sequential_insert_stress() {
        let mut tree = BPlustree::new(3);

        for i in 1..200 {
            tree.insert(i, i);
        }

        for i in 1..200 {
            assert_eq!(tree.search(i), Some(&i));
        }

        validate_node(tree.root.as_ref());
    }

    #[test]
    fn test_reverse_insert() {
        let mut tree = BPlustree::new(3);

        for i in (1..200).rev() {
            tree.insert(i, i);
        }

        for i in 1..200 {
            assert_eq!(tree.search(i), Some(&i));
        }

        validate_node(tree.root.as_ref());
    }

    #[test]
    fn test_random_insert() {
        let mut tree = BPlustree::new(3);

        let keys = vec![50, 10, 70, 30, 20, 90, 40, 60, 80];

        for k in &keys {
            tree.insert(*k, *k);
        }

        for k in &keys {
            assert_eq!(tree.search(*k), Some(k));
        }

        validate_node(tree.root.as_ref());
    }

    // ---------- Edge Cases ----------

    #[test]
    fn test_duplicate_insert() {
        let mut tree = BPlustree::new(3);

        tree.insert(10, 1);
        tree.insert(10, 2);

        assert!(tree.search(10).is_some());
    }

    #[test]
    fn test_missing_keys() {
        let mut tree = BPlustree::new(3);

        for i in 1..50 {
            tree.insert(i * 2, i);
        }

        for i in 1..100 {
            if i % 2 == 0 {
                assert!(tree.search(i).is_some());
            } else {
                assert_eq!(tree.search(i), None);
            }
        }

        validate_node(tree.root.as_ref());
    }
}
