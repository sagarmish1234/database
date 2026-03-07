use std::{cell::RefCell, rc::Rc};

type Link<T> = Option<Rc<RefCell<Node<T>>>>;

pub struct LruCache<T> {}

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
        prev: Option<Rc<RefCell<Self>>>,
        next: Option<Rc<RefCell<Self>>>,
    ) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self { key, next, prev }))
    }
}

struct DoublyLinkedList<T> {
    head: Rc<RefCell<Node<T>>>,
    tail: Rc<RefCell<Node<T>>>,
}

impl<T: Default> DoublyLinkedList<T> {
    fn new() -> Self {
        let head = Node::new(T::default());
        let tail = Node::new(T::default());

        head.borrow_mut().next = Some(Rc::clone(&tail));
        tail.borrow_mut().prev = Some(Rc::clone(&head));

        Self { head, tail }
    }

    fn add_to_front(&mut self, key: T) -> Rc<RefCell<Node<T>>> {
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
}
