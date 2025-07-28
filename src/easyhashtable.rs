//! TODO: implement entry marker byte:
//! 0: empty
//! 1: set
//! 2: deleted

use std::hash::BuildHasher;

use anyhow::ensure;
use twox_hash::XxHash64;

use crate::{
    aligned::{self, FromAligned},
    bufferpool::BufferPool,
};

const KEY_SIZE: usize = 16;
const VALUE_SIZE: usize = 47;

#[derive(Debug, thiserror::Error)]
pub enum HashTableError {
    #[error("page error: {0}")]
    Page(#[from] aligned::Error),
    #[error("capacity exceeded")]
    CapacityExceeded,
    #[error("key too large")]
    KeyTooLarge,
    #[error("value too large")]
    ValueTooLarge,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("unknown hash algorithm")]
    UnknownHashAlgorithm,
}

#[repr(u16)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum HashAlgorithm {
    XxHash = 0x0001,
}

struct SeededXxHashBuilder {
    seed: u64,
}
impl SeededXxHashBuilder {
    fn new(seed: u64) -> SeededXxHashBuilder {
        SeededXxHashBuilder { seed }
    }
}

impl BuildHasher for SeededXxHashBuilder {
    type Hasher = XxHash64;

    fn build_hasher(&self) -> Self::Hasher {
        XxHash64::with_seed(self.seed)
    }
}

pub struct EasyHashTable {
    hash_builder: SeededXxHashBuilder,
    page_id: crate::record::PageId,
}

impl EasyHashTable {
    pub fn new(buffer_pool: &mut BufferPool) -> Self {
        let rng = rand::thread_rng();
        EasyHashTable::new_with_rng(buffer_pool, rng)
    }

    pub fn new_with_rng<R: rand::Rng>(buffer_pool: &mut BufferPool, mut rng: R) -> Self {
        let hash_seed = rng.gen();
        let page: page::Page = page::Page::new(HashAlgorithm::XxHash, hash_seed);

        // TODO: Allow shared access to the buffer pool.
        let page_buffer = page.into_aligned();

        let page_id = buffer_pool
            .insert_page(&page_buffer)
            .expect("cannot write page");

        EasyHashTable {
            hash_builder: SeededXxHashBuilder::new(hash_seed),
            page_id,
        }
    }

    /// Create a new hash table at the specified page.  Zeroes out any data already at the page.
    pub fn new_in_page(buffer_pool: &mut BufferPool, page_id: u64) -> Self {
        Self::new_in_page_with_rng(buffer_pool, rand::thread_rng(), page_id)
    }

    pub fn new_in_page_with_rng<R: rand::Rng>(buffer_pool: &mut BufferPool, mut rng: R, page_id: u64) -> Self {
        let hash_seed = rng.gen();
        let page = page::Page::new(HashAlgorithm::XxHash, hash_seed);
        let buffer = page.into_aligned();
        buffer_pool.update_page(page_id, &buffer).expect("cannot write page");
        EasyHashTable {
            hash_builder: SeededXxHashBuilder::new(hash_seed),
            page_id
        }
    }

    pub fn from_page(
        buffer_pool: &mut BufferPool,
        page_id: crate::record::PageId,
    ) -> anyhow::Result<Self> {
        let mut page_buffer = aligned::Buffer::new();
        buffer_pool.read_page(page_id, &mut page_buffer)?;

        let page = page::Page::from_aligned(page_buffer)?;
        ensure!(page.hash_algorithm()? == HashAlgorithm::XxHash);
        let ht = EasyHashTable {
            hash_builder: SeededXxHashBuilder::new(page.hash_seed()),
            page_id,
        };
        Ok(ht)
    }

    pub fn page_id(&self) -> crate::record::PageId {
        self.page_id
    }

    fn entry_size(&self) -> usize {
        1 + KEY_SIZE + VALUE_SIZE
    }

    pub fn capacity(&self) -> usize {
        (crate::PAGESIZE - 0x18) / self.entry_size()
    }

    pub fn insert(
        &mut self,
        buffer_pool: &mut BufferPool,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), HashTableError> {
        if key.len() >= KEY_SIZE {
            return Err(HashTableError::KeyTooLarge)
        }
        if value.len() >= VALUE_SIZE {
            return Err(HashTableError::ValueTooLarge)
        }
        let mut page_buffer = aligned::Buffer::new();
        buffer_pool
            .read_page(self.page_id, &mut page_buffer)
            .expect("cannot read page");
        let mut page = page::Page::from_aligned(page_buffer)?;
        assert_eq!(page.hash_algorithm().unwrap(), HashAlgorithm::XxHash);
        let hash = self.hash_builder.hash_one(key);
        let mut candidate = hash % self.capacity() as u64;
        let start = candidate;
        let slot = loop {
            match self.get_slot(&page, candidate) {
                Some((k, _)) => {
                    if key == k {
                        break candidate;
                    }
                }
                None => break candidate,
            }
            candidate += 1;
            candidate %= self.capacity() as u64;
            if candidate == start {
                return Err(HashTableError::CapacityExceeded);
            }
        };
        let entry = &mut page.as_aligned_mut()[0x18 + self.entry_size() * slot as usize..][..self.entry_size()];
        entry[0] = 1;
        entry[1] = key.len() as u8;
        entry[2..][..key.len()].copy_from_slice(key);
        entry[1 + KEY_SIZE] = value.len() as u8;
        entry[2 + KEY_SIZE..][..value.len()].copy_from_slice(value);

        buffer_pool.update_page(self.page_id, &page.into_aligned())?;
        Ok(())
    }

    // TODO: Allow immutable access.
    pub fn get(&self, buffer_pool: &mut BufferPool, key: &[u8]) -> Result<Option<Vec<u8>>, HashTableError> {
        let mut page_buffer = aligned::Buffer::new();
        buffer_pool
            .read_page(self.page_id, &mut page_buffer)
            .expect("cannot read page");
        let page = page::Page::from_aligned(page_buffer)?;
        let hash = self.hash_builder.hash_one(key);
        let mut candidate = hash % self.capacity() as u64;
        let start = candidate;
        loop {
            match self.get_slot(&page, candidate) {
                Some((k, v)) => {
                    if key == k {
                        break Ok(Some(v.to_vec()));
                    }
                }
                None => break Ok(None),
            }
            candidate += 1;
            candidate %= self.capacity() as u64;
            if candidate == start {
                break Ok(None);
            }
        }
    }

    /// Return the key and value for a given slot, or None if the slot is empty.
    fn get_slot<'a: 'de, 'de>(
        &self,
        page: &'a page::Page,
        id: u64,
    ) -> Option<(&'a [u8], &'a [u8])> {
        let id = id as usize;
        if id > self.capacity() {
            return None;
        }
        let offset = 0x18 + id * self.entry_size();
        let slot_bytes = &page.as_aligned()[offset..][..self.entry_size()];
        let entry_marker = slot_bytes[0];
        match entry_marker {
            0 => None,
            1 => {
                let key_len = slot_bytes[1] as usize;
                if key_len > KEY_SIZE - 1 {
                    panic!("invalid key len {key_len}, must be < {KEY_SIZE}");
                }
                let key = &slot_bytes[2..][..key_len];

                let value_len = slot_bytes[1 + KEY_SIZE] as usize;
                if value_len > VALUE_SIZE - 1 {
                    panic!("invalid value len {value_len}, must be < {VALUE_SIZE}");
                }
                let value = &slot_bytes[1 + KEY_SIZE + 1..][..value_len];
                Some((key, value))
            }
            _ => panic!("unknown entry type {entry_marker}"),
        }
    }
}

mod page {

    struct FieldSpec {
        offset: usize,
        len: usize,
    }

    impl FieldSpec {
        const fn new(offset: usize, len: usize) -> FieldSpec {
            FieldSpec { offset, len }
        }
    }

    #[repr(usize)]
    enum FieldIndex {
        Crc32 = 0,
        PageType = 1,
        ValueSize = 2,
        HashAlgorithm = 3,
        HashSeed = 4,
        Data = 5,
    }

    static FIELDS: &[FieldSpec] = &[
        // FieldIndex::Crc32
        FieldSpec::new(0x0, 4),
        // FieldIndex::PageType
        FieldSpec::new(0x4, 2),
        // FieldIndex::ValueSize
        FieldSpec::new(0x6, 2),
        // FieldIndex::HashAlgorithm
        FieldSpec::new(0x8, 2),
        // FieldIndex::HashSeed
        FieldSpec::new(0x10, 8),
        // FieldIndex::Data
        FieldSpec::new(0x18, crate::PAGESIZE - 0x18),
    ];

    use super::{HashAlgorithm, HashTableError};
    use crate::{aligned, PageType, CRC32};

    // crc32: u32,
    // page_type: PageType,
    // value_size: u16,
    // hash_algorithm: HashAlgorithm,
    // padding: [u8; 6],
    // hash_seed: u64,
    // data: [u8; PAGESIZE - 0x18],
    pub(super) struct Page {
        buffer: Box<aligned::Buffer>,
    }

    /// Reads a slice of two u8s as a u16 using little endian encoding.
    ///
    /// # Panic
    ///
    /// Panics if given a slice of the wrong size.
    fn read_u16(s: &[u8]) -> u16 {
        u16::from_le_bytes(s.try_into().expect("to_u16 expects a slice of two u8s."))
    }

    /// Reads a slice of four u8s as a u32 using little endian encoding.
    ///
    /// # Panic
    ///
    /// Panics if given a slice of the wrong size.
    fn read_u32(s: &[u8]) -> u32 {
        u32::from_le_bytes(s.try_into().expect("to_u32 expects a slice of four u8s."))
    }

    /// Reads a slice of eight u8s as a u64 using little endian encoding.
    ///
    /// # Panic
    ///
    /// Panics if given a slice of the wrong size.
    fn read_u64(s: &[u8]) -> u64 {
        u64::from_le_bytes(s.try_into().expect("to_u32 expects a slice of four u8s."))
    }

    impl Page {
        pub(super) fn page_type(&self) -> PageType {
            PageType::SinglePageHashTable
        }

        fn set_page_type(&mut self) {
            let page_type = &(self.page_type() as u32).to_le_bytes();
            self.buffer[4..8].copy_from_slice(page_type);
        }

        fn set_crc(&mut self) {
            let crc = CRC32.checksum(&self.buffer[4..]);
            self.buffer[..4].copy_from_slice(&crc.to_le_bytes())
        }

        pub(super) fn new(hash_algorithm: HashAlgorithm, hash_seed: u64) -> Page {
            let buffer = aligned::Buffer::new();
            let mut p = Page { buffer };
            p.set_page_type();
            p.set_hash_algorithm(hash_algorithm);
            p.set_hash_seed(hash_seed);

            p
        }

        pub(super) fn as_aligned(&self) -> &aligned::Buffer {
            &self.buffer
        }

        pub(super) fn as_aligned_mut(&mut self) -> &mut aligned::Buffer {
            &mut self.buffer
        }
        pub(super) fn into_aligned(mut self) -> Box<aligned::Buffer> {
            // Calculate and write the checksum for this page
            self.set_crc();
            self.buffer
        }

        pub(super) fn hash_seed(&self) -> u64 {
            read_u64(&self.buffer[0x10..0x18])
        }

        pub(super) fn set_hash_seed(&mut self, hash_seed: u64) {
            self.buffer[0x10..0x18].copy_from_slice(&hash_seed.to_le_bytes())
        }

        pub(super) fn hash_algorithm(&self) -> Result<HashAlgorithm, HashTableError> {
            let algo_val = read_u16(&self.buffer[8..10]);
            match algo_val {
                0x0001 => Ok(HashAlgorithm::XxHash),
                _ => Err(HashTableError::UnknownHashAlgorithm),
            }
        }

        fn set_hash_algorithm(&mut self, algorithm: HashAlgorithm) {
            self.buffer[8..10].copy_from_slice(&(dbg!(algorithm) as u16).to_le_bytes())
        }
    }

    impl aligned::FromAligned for Page {
        fn expected_page_type() -> PageType {
            PageType::SinglePageHashTable
        }

        fn transform(buffer: Box<aligned::Buffer>) -> Self {
            Page { buffer }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bufferpool::BufferPool;
    use crate::storage::PagedFile;
    use crate::testutils::create_test_path;

    use super::*;

    #[test]
    fn simple_access() -> anyhow::Result<()> {
        let path = create_test_path("test-potpotdb::hashtable::new_hashtable.data");
        let storage = PagedFile::from_path(&path)?;
        let mut pool = BufferPool::new(storage, 3);

        let mut ht = EasyHashTable::new(&mut pool);
        dbg!(ht.insert(&mut pool, b"97", b"4")?);
        assert_eq!(dbg!(ht.get(&mut pool, b"97")).unwrap(), Some(vec![b'4']));
        assert!(ht.get(&mut pool, b"25").unwrap().is_none());
        Ok(())
    }

    #[test]
    fn persistence() -> anyhow::Result<()> {
        let path = create_test_path("test-potpotdb::hashtable::new_hashtable.data");
        let page_id = {
            let storage = PagedFile::from_path(&path)?;

            let mut pool = BufferPool::new(storage, 3);

            let mut ht = EasyHashTable::new(&mut pool);
            dbg!();
            ht.insert(&mut pool, b"97", b"4")?;
            dbg!();
            ht.page_id()
            // Old buffer pool is deleted.
        };
            dbg!();
        {
            let storage = PagedFile::from_path(&path)?;
            let mut pool = BufferPool::new(storage, 3);
            let ht = EasyHashTable::from_page(&mut pool, page_id)
                .expect("No hashtable found at that page ID");

            assert_eq!(ht.get(&mut pool, b"97").unwrap(), Some(vec![b'4']));
            assert!(ht.get(&mut pool, b"25").unwrap().is_none());
        }
        Ok(())
    }
}
