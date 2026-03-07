use std::{cell::RefCell, rc::Rc};

type Link<T> = Option<Rc<RefCell<Node<T>>>>;

// pub struct LruCache<T> {}

struct Node<T> {
    key: T,
    next: Link<T>,
    prev: Link<T>,
}

impl<T: Default> Node<T> {
    fn new(key: T) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self {
            key,
            next: None,
            prev: None,
        }))
    }

    fn new_with_prev_and_next(
        key: T,
        next: Option<Rc<RefCell<Self>>>,
        prev: Option<Rc<RefCell<Self>>>,
    ) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self { key, next, prev }))
    }
}

struct DoublyLinkedList<T> {
    head: Rc<RefCell<Node<T>>>,
    tail: Rc<RefCell<Node<T>>>,
    size: usize,
}

impl<T: Default> DoublyLinkedList<T> {
    fn new() -> Self {
        let head = Node::new(T::default());
        let tail = Node::new(T::default());

        head.borrow_mut().next = Some(Rc::clone(&tail));
        tail.borrow_mut().prev = Some(Rc::clone(&head));

        Self {
            head,
            tail,
            size: 0,
        }
    }

    fn push_front(&mut self, key: T) -> Rc<RefCell<Node<T>>> {
        let old_next_link = self.head.borrow().next.clone();

        let new_node = Node::new_with_prev_and_next(
            key,
            self.head.borrow().next.clone(),
            Some(self.head.clone()),
        );

        if let Some(ref old_next) = old_next_link {
            old_next.borrow_mut().prev = Some(new_node.clone());
        }

        self.head.borrow_mut().next = Some(new_node.clone());

        new_node
    }

    fn push_back(&mut self, key: T) -> Rc<RefCell<Node<T>>> {
        let old_prev = self.tail.borrow().prev.clone();

        let new_node = Node::new_with_prev_and_next(
            key,
            Some(self.tail.clone()),
            self.tail.borrow().prev.clone(),
        );
        if let Some(ref prev) = old_prev {
            prev.borrow_mut().next = Some(new_node.clone());
        }

        self.tail.borrow_mut().prev = Some(new_node.clone());

        new_node
    }

    fn pop_front(&mut self) -> Link<T> {
        let first_node_link = self.head.borrow().next.clone();

        // Use ptr_eq to check if we are looking at the tail sentinel
        if let Some(ref first_node) = first_node_link {
            if Rc::ptr_eq(first_node, &self.tail) {
                return None;
            }
        }

        // 2. Identify the node to remove and the node that will follow it
        let to_remove = first_node_link.unwrap(); // Safe because of the check above
        let new_first = to_remove.borrow().next.clone(); // The node after the one we pop

        // 3. Re-link: Head Sentinel -> New First
        self.head.borrow_mut().next = new_first.clone();

        // 4. Re-link: New First <- Head Sentinel (CRITICAL STEP)
        if let Some(ref node) = new_first {
            node.borrow_mut().prev = Some(self.head.clone());
        }

        // 5. Clean up the popped node's pointers (Prevents leaks/confusion)
        to_remove.borrow_mut().next = None;
        to_remove.borrow_mut().prev = None;

        Some(to_remove)
    }

    fn pop_back(&mut self) -> Link<T> {
        let last_node_link = self.tail.borrow().prev.clone();

        if let Some(ref prev) = last_node_link {
            if Rc::ptr_eq(prev, &self.head) {
                return None;
            }
        }

        let to_remove = last_node_link.unwrap();
        let new_last = to_remove.borrow().prev.clone();

        self.tail.borrow_mut().prev = new_last.clone();

        if let Some(ref node) = new_last {
            node.borrow_mut().next = Some(self.tail.clone());
        }

        to_remove.borrow_mut().next = None;
        to_remove.borrow_mut().prev = None;

        Some(to_remove)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_list_is_empty() {
        let mut list: DoublyLinkedList<i32> = DoublyLinkedList::new();
        assert!(list.pop_front().is_none());
        assert!(list.pop_back().is_none());
    }

    #[test]
    fn test_push_pop_front() {
        let mut list = DoublyLinkedList::new();
        list.push_front(10);
        list.push_front(20);

        // Should be [20, 10]
        let node1 = list.pop_front().unwrap();
        assert_eq!(node1.borrow().key, 20);

        let node2 = list.pop_front().unwrap();
        assert_eq!(node2.borrow().key, 10);

        assert!(list.pop_front().is_none());
    }

    #[test]
    fn test_push_pop_back() {
        let mut list = DoublyLinkedList::new();
        list.push_back(10);
        list.push_back(20);

        // Should be [10, 20]
        let node1 = list.pop_back().unwrap();
        assert_eq!(node1.borrow().key, 20);

        let node2 = list.pop_back().unwrap();
        assert_eq!(node2.borrow().key, 10);

        assert!(list.pop_back().is_none());
    }

    #[test]
    fn test_bidirectional_links() {
        let mut list = DoublyLinkedList::new();
        list.push_back(1);
        list.push_back(2);

        // Structure: Head <-> 1 <-> 2 <-> Tail
        let head_next = list.head.borrow().next.clone().unwrap();
        let first_node_next = head_next.borrow().next.clone().unwrap();

        // Verify Forward: 1's next is 2
        assert_eq!(first_node_next.borrow().key, 2);

        // Verify Backward: 2's prev is 1
        let second_node_prev = first_node_next.borrow().prev.clone().unwrap();
        assert!(Rc::ptr_eq(&second_node_prev, &head_next));
        assert_eq!(second_node_prev.borrow().key, 1);
    }

    #[test]
    fn test_mixed_operations() {
        let mut list = DoublyLinkedList::new();
        list.push_back(1); // [1]
        list.push_front(2); // [2, 1]
        list.push_back(3); // [2, 1, 3]

        assert_eq!(list.pop_front().unwrap().borrow().key, 2);
        assert_eq!(list.pop_back().unwrap().borrow().key, 3);
        assert_eq!(list.pop_front().unwrap().borrow().key, 1);
        assert!(list.pop_front().is_none());
    }
}
