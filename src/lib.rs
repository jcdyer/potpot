pub mod bufferpool;
pub mod storage;
pub mod aligned;
pub mod page;
pub mod types;
pub mod query;
pub mod record;
pub mod result;
pub mod hashtable;

#[cfg(test)]
mod testutils;

pub(crate) const PAGESIZE: usize = 16384;
pub(crate) type Result<T> = std::result::Result<T, result::Error>;

#[repr(u16)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum PageType {
    MasterRecord = 0x0000,
    DataPage = 0x1000,
    SinglePageHashTable = 0x2000,
    HashTableFixedWidthSlot = 0x2001,
}

impl From<u16> for PageType {
    fn from(val: u16) -> PageType {
        unsafe { std::mem::transmute(val) }
    }
}


#[test]
fn math_check(){
    assert_eq!(16384, 2i64.pow(14));
}
