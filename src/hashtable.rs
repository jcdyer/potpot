//! A single page hash table
//!
//! Page layouts:
//!
//! Single Page Hash Table - Hashes a byte key to a fixed-size byte value.
//!
//! Emits a CapacityError when trying to insert more elements than we have room for in the page.
//!
//!   0x0000  CRC32 (4 bytes)  // CRC of bytes 4-end
//!   0x0004  Page type (2 bytes) (0x2000)
//!   0x0006  Value size (2 bytes)
//!   0x0008  Hash algorithm (2 bytes)
//!   0x000a  Padding (6 bytes)
//!   0x0010  Hash Seed (8 bytes) (maybe depends on hash algorithm?)
//!   0x0018  Data ((M byte key + N byte value)* up to (PAGE_SIZE - 0x18) / 8)
//!
//!   Capacity: (PAGESIZE - 0x18) / (value size + 8) == 16360 / (8 + valuesize)
//!       So for valuesize=24, capacity == 16360 / 32 == 511
//!
//! Header page:
//!
//!   0x0000  CRC32 (4 bytes)  // CRC of bytes 4-end
//!   0x0004  Page type (2 bytes) (0x2001)
//!   0x0006  Value size (2 bytes)
//!   0x0008  Hash algorithm (4 bytes)
//!   0x000c  Padding (4 bytes)
//!   0x0010  Page count (8 bytes)  // Future optimization: if 0, use page pointers as single-page storage.
//!   0x0018  Hash Seed (8 bytes) (maybe depends on hash algorithm?)
//!   0x0020  Page pointers (8 bytes x page count) (up to (PAGE_SIZE - 0x20) / 8)
//!   0x     End
//!
//! Fixed width slot page
//!
//!   0x0   CRC32 (4 bytes)  // CRC of bytes 4-end
//!   0x4   Page type (2 bytes) (0x2021)
//!   0x6  Value size (2 bytes)
//!   0x8   First slot number (8 bytes)
//!   0x10  Slots (2 byte size + value)
//!
//!

use std::{
    hash::{BuildHasher, Hash, Hasher},
    marker::PhantomData,
};

use twox_hash::XxHash64;

use crate::{
    aligned::{self, FromAligned},
    bufferpool::BufferPool,
};

#[repr(u32)]
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum HashAlgorithm {
    XxHash = 0x0000_0000,
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

pub struct SinglePageHashTable<'bp, V> {
    hash_builder: SeededXxHashBuilder,
    buffer_pool: &'bp mut BufferPool, // TODO: Change to shared reference
    page_id: crate::record::PageId,
    _value_type: PhantomData<V>,
}

impl<'bp, V> SinglePageHashTable<'bp, V> {
    pub fn new(buffer_pool: &'bp mut BufferPool) -> Self {
        let rng = rand::thread_rng();
        SinglePageHashTable::new_with_rng(buffer_pool, rng)
    }

    pub fn new_with_rng<R: rand::Rng>(buffer_pool: &'bp mut BufferPool, mut rng: R) -> Self {
        let hash_seed = rng.gen();
        let header_page: page::Page<V> = page::Page::new(hash_seed);

        // TODO: Allow shared access to the buffer pool.
        let page_buffer = header_page.into_aligned();
        let page_id = buffer_pool
            .append_page(&page_buffer)
            .expect("cannot write page");

        SinglePageHashTable {
            hash_builder: SeededXxHashBuilder::new(hash_seed),
            buffer_pool,
            page_id,
            _value_type: PhantomData,
        }
    }

    pub fn from_page(
        buffer_pool: &'bp mut BufferPool,
        page_id: crate::record::PageId,
    ) -> anyhow::Result<Self> {
        let page_buffer = {
            let mut page_buffer = aligned::Buffer::new();
            buffer_pool.read_page(page_id, &mut page_buffer)?;
            page_buffer
        };

        let page = page::Page::<V>::from_aligned(page_buffer)?;

        // TODO: Validate _value_type
        let ht = SinglePageHashTable {
            hash_builder: SeededXxHashBuilder::new(page.hash_seed()),
            buffer_pool,
            page_id,
            _value_type: PhantomData,
        };
        Ok(ht)
    }

    pub fn page_id(&self) -> crate::record::PageId {
        self.page_id
    }

    pub fn capacity(&self) -> usize {
        (crate::PAGESIZE - 0x18) / (8 + std::mem::size_of::<V>())
    }

    pub fn insert(&mut self, key: u64, value: V) -> anyhow::Result<()>
    where
        V: serde::Serialize + serde::Deserialize<'static>,
    {
        let mut page_buffer = aligned::Buffer::new();
        self.buffer_pool
            .read_page(self.page_id, &mut page_buffer)
            .expect("cannot read page");
        let page: page::Page<V> = page::Page::from_aligned(page_buffer)?;
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let mut candidate = hash % self.capacity() as u64;
        let start = candidate;
        let slot = loop {
            match self.get_slot(&page, candidate) {
                Some((k, _)) => if key == k {break candidate;}
                None => break candidate,
            }
            candidate += 1 % self.capacity() as u64;
            if candidate == start {
                return Err(anyhow::anyhow!("capacity exceeded"));
            }
        };
        todo!("___WRITE(page, slot, key, value)?;");

        // TODO: Need ability to get a slot by ID, check if it's empty, and step through following
        // slots until an empty one is found.  Empty slot bitarray before data slots or interleaved?
        // If before, pre-check next empty slot, then iterate from current to next-empty - 1.  If key
        // not found, insert at next-empty.  Bitarray: 2 bits per slot: 00 - empty, 11 - Full, 01 - Deleted.
        // (bit value xy, x: HasValue, y: ContinueFallthrough)
    }

    // TODO: Allow immutable access.
    pub fn get(&mut self, key: u64) -> Option<&V> {
        None
    }

    /// Return the key and value for a given slot, or None if the slot is empty.
    fn get_slot<'a>(&self, page: &'a page::Page<V>, id: u64) -> Option<(u64, &'a V)> {
        todo!()
    }
}

mod page {

    struct FieldSpec {
        offset: usize,
        len: usize,
    }
    impl FieldSpec {
        const fn new(offset: usize, len: usize) -> FieldSpec {
            FieldSpec {
                offset, len
            }
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
        FieldSpec { // FieldIndex::Crc32
            offset: 0x0,
            len: 4,
        },
        FieldSpec { // FieldIndex::PageType
            offset: 0x4,
            len: 2,
        },
        FieldSpec { // FieldIndex::ValueSize
            offset: 0x6,
            len: 2,
        },

        FieldSpec { // FieldIndex::HashAlgorithm
            offset: 0x8,
            len: 2,
        },
        FieldSpec { // FieldIndex::HashSeed
            offset : 0x10,
            len: 8,
        },
        FieldSpec { // FieldIndex::Data
            offset: 0x18,
            len: crate::PAGESIZE - 0x18,
        },
    ];

    use std::{
        marker::PhantomData,
        mem::size_of,
    };


    use super::HashAlgorithm;
    use crate::{aligned, PageType, CRC32};

    // crc32: u32,
    // page_type: PageType,
    // value_size: u16,
    // hash_algorithm: HashAlgorithm,
    // padding: [u8; 6],
    // hash_seed: u64,
    // data: [u8; PAGESIZE - 0x18],
    pub(super) struct Page<V> {
        buffer: Box<aligned::Buffer>,
        _value_type: PhantomData<V>,
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


    impl<V> Page<V> {
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

        pub(super) fn value_size(&self) -> usize {
            let size = u16::from_le_bytes(self.buffer[8..10].try_into().unwrap());
            size as usize
        }

        /// # Panic
        ///
        /// This method panics if value_size is greater than 4096 bytes;
        fn set_value_size(&mut self, size: usize) {
            assert!(size <= 4096, "value size cannot be greater than 4096 bytes");
            self.buffer[8..10].copy_from_slice(&(size as u16).to_le_bytes())
        }
    }

    impl<V> Page<V> {
        pub(super) fn new(hash_seed: u64) -> Page<V> {
            let buffer = aligned::Buffer::new();
            let mut p = Page {
                buffer,
                _value_type: PhantomData,
            };
            p.set_page_type();
            p.set_value_size(size_of::<V>());
            p.set_hash_algorithm(HashAlgorithm::XxHash);
            p.set_hash_seed(hash_seed);
            p
        }

        pub(super) fn into_aligned(mut self) -> Box<aligned::Buffer> {
            // Calculate and write the checksum for this page
            let checksum = CRC32.checksum(&self.buffer[4..]);
            self.buffer[0..4].clone_from_slice(&checksum.to_le_bytes());
            self.buffer
        }

        pub(super) fn hash_seed(&self) -> u64 {
            read_u64(&self.buffer[0x10..0x18])
        }

        pub(super) fn set_hash_seed(&mut self, hash_seed: u64) {
            self.buffer[0x10..0x18].copy_from_slice(&hash_seed.to_le_bytes())
        }

        pub(super) fn hash_algorithm(&self) -> Result<HashAlgorithm, ()> {
            let algo_val = read_u16(&self.buffer[8..10]);
            match algo_val {
                0x0000 => Ok(HashAlgorithm::XxHash),
                _ => Err(())
            }

        }

        fn set_hash_algorithm(&mut self, algorithm: HashAlgorithm) {
            self.buffer[8..10].copy_from_slice(&(algorithm as u16).to_le_bytes())
        }
    }

    impl<V> aligned::FromAligned for Page<V> {
        fn expected_page_type() -> PageType {
            PageType::SinglePageHashTable
        }

        fn transform(buffer: Box<aligned::Buffer>) -> Self {
            Page {
                buffer,
                _value_type: PhantomData,
            }
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

        let mut ht = SinglePageHashTable::new(&mut pool);
        dbg!(ht.insert(97, (4, 12))?);
        dbg!(ht.get(97));
        assert_eq!(ht.get(97).map(ToOwned::to_owned), Some((4, 12)));
        assert!(ht.get(25).is_none());
        Ok(())
    }

    #[test]
    fn persistence() -> anyhow::Result<()> {
        let path = create_test_path("test-potpotdb::hashtable::new_hashtable.data");
        let page_id = {
            let storage = PagedFile::from_path(&path)?;

            let mut pool = BufferPool::new(storage, 3);

            let mut ht = SinglePageHashTable::new(&mut pool);
            ht.insert(97, (4, 12))?;
            ht.page_id()
            // Old buffer pool is deleted.
        };
        {
            let storage = PagedFile::from_path(&path)?;
            let mut pool = BufferPool::new(storage, 3);
            let mut ht = SinglePageHashTable::<(usize, usize)>::from_page(&mut pool, page_id)
                .expect("No hashtable found at that page ID");

            assert_eq!(ht.get(97).map(ToOwned::to_owned), Some((4, 12)));
            assert!(ht.get(25).is_none());
        }
        Ok(())
    }
}
