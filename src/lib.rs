pub mod bufferpool;
pub mod storage;
pub mod aligned;
pub mod page;
pub mod types;
pub mod query;
pub mod record;
pub mod result;
pub mod easyhashtable;
pub mod btree;

#[cfg(test)]
mod testutils;

pub(crate) const PAGESIZE: usize = 16384;

const CRC32: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_CKSUM);

#[repr(u16)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum PageType {
    MasterRecord = 0x0000,
    DataPage = 0x1000,
    SinglePageHashTable = 0x2000,
}

impl From<u16> for PageType {
    fn from(val: u16) -> PageType {
        unsafe { std::mem::transmute(val) }
    }
}
