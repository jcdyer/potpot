//! BTree index:
//!
//! Node: PageId, (Key, PageId)*
//! Leaf: PageId, (Key, Value)*



use crate::{aligned, record::{self, PageId}};


pub struct BTree {
    root_page: PageId
}

/// A Node consists of an optional pointer to the next sibling Node in the
/// tree and series of child pointers interleaved by Key values that divide
/// the children such that all keys in the child to the left of key k are < k,
/// and all keys in the right child are >= k.
struct Node {
    page_id: PageId,
    buffer: aligned::Buffer
}

impl Node {
    fn find_child(&self, key: &[u8]) -> Result<PageId, anyhow::Error> {
        todo!()
    }

}

struct NodeIter<'a> {
    node: &'a Node,
    offset: usize,
}

pub struct InnerNodePage {

}

pub struct NodeId(u64);
