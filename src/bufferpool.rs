//! Buffer pool to cache pages from the page file in memory.

// TODO:
// 1.  Write pages to the buffer pool before persisting to the PagedFile
// 2.  Implement delayed persistence.  Writes update buffer pool, and mark
//     entries as dirty.  When a dirty page is marked for eviction, it needs
//     to be persisted before it is written (and any adjacent dirty pages
//     can be written in the same operation).

use crate::{aligned, storage::PagedFile};
use std::collections::{
    HashMap,
};

pub trait CacheManager<T> {
    // Mark the entry at the given slot as updated
    fn update(&mut self, idx: usize);

    // Find an available slot, and return the currently resident value, replacing it with the new value
    fn sweep(&mut self, entry: T) -> (usize, Option<T>);
}

pub struct ClockManager<T> {
    idx: usize,
    clock: Vec<bool>,
    entries: Vec<Option<T>>,
}

impl<T: Copy + Eq> ClockManager<T> {
    pub fn new(size: usize) -> ClockManager<T> {
        ClockManager {
            idx: 0,
            clock: vec![false; size],
            entries: vec![None; size],
        }
    }
}

impl<T> CacheManager<T> for ClockManager<T> {
    fn update(&mut self, idx: usize) {
        self.clock[idx] = true;
    }

    // Find an available slot for the cache
    fn sweep(&mut self, entry: T) -> (usize, Option<T>) {
        let size = self.clock.len();
        let (clock_from_start, clock_to_end) = self.clock.split_at_mut(self.idx);
        let clock_cycle = clock_to_end.iter_mut().chain(clock_from_start);
        let idx = {
            let mut found = None;
            for (i, clockbit) in clock_cycle.enumerate() {
                if *clockbit {
                    *clockbit = false;
                } else {
                    let idx = (self.idx + i) % size;
                    found = Some(idx);
                    break;
                }
            }
            // if nothing was found, return the starting index.
            found.unwrap_or(self.idx)
        };

        // update the clock pointer to the selected slot.
        self.idx = idx;

        // set the reference bit for the selected entry
        self.clock[idx] = true;

        // return the selected index and the replaced entry, if any.
        (idx, self.entries[idx].replace(entry))
    }
}

pub struct BufferPool<CM = ClockManager<u64>>
where
    CM: CacheManager<u64>,
{
    // map page IDs to their location in the buffer pool
    page_table: HashMap<u64, usize>,

    // manager to determine which frames to evict
    manager: CM,

    // cached pages
    frames: Vec<[u8; 4096]>,

    // the managed PagedFile
    storage: PagedFile,
}

impl BufferPool {
    pub fn new(storage: PagedFile, size: usize) -> BufferPool {
        let frames = std::iter::repeat([0; 4096]).take(size).collect();
        BufferPool {
            page_table: HashMap::with_capacity(size),
            manager: ClockManager::new(size),
            frames,
            storage,
        }
    }

    pub fn read_page(&mut self, page_id: u64, buf: &mut aligned::Buffer) -> std::io::Result<()> {

        let entry = self
            .page_table
            .get(&page_id)
            .copied() // Release the borrow of self
            .and_then(|frame_idx| {
                self.manager.update(frame_idx);
                self.frames.get_mut(frame_idx)
            });

        if let Some(val) = entry {
            println!("Got some entry");
            buf.copy_from_slice(val.as_ref());
        } else {
            println!("No entry");
            self.storage.read_page(page_id, buf)?;

            let frame_idx = self.add_to_buffer_pool(page_id, buf);

            self.frames[frame_idx][..].copy_from_slice(&buf);
        }
        Ok(())
    }

    // Write a page and get back a page id.
    pub fn append_page(&mut self, aligned_data: &aligned::Buffer) -> std::io::Result<u64> {
        // TBD: Figure out how to manage page_ids of new pages written to the buffer pool
        // without persisting to disk first. Decouple page_ids from disk order?  Track
        // unwritten page_ids?
        let page_id = self.storage.append_page(aligned_data)?;
        self.add_to_buffer_pool(page_id, aligned_data);
        Ok(page_id)
    }

    // Update an existing page
    pub fn update_page(&mut self, page_id: u64, data: &aligned::Buffer) -> std::io::Result<()> {
        self.add_to_buffer_pool(page_id, data);
        self.storage.write_page(page_id, data)
    }

    fn add_to_buffer_pool(&mut self, page_id: u64, data: &[u8]) -> usize {
        let frame_idx = self.page_table.get(&page_id);
        let frame_idx = match frame_idx {
            Some(&frame_idx) => {
                self.manager.update(frame_idx);
                frame_idx
            }
            None => {
                let (idx, evicted_page) = self.manager.sweep(page_id);

                // If there is a page to evict, remove it now.
                if let Some(page_id) = evicted_page {
                    self.page_table.remove(&page_id);
                }
                self.page_table.insert(page_id, idx);
                idx
            }
        };

        // Review: Is this sometimes not necessary?
        self.frames[frame_idx].copy_from_slice(data);
        frame_idx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{aligned, storage::PagedFile, testutils::create_test_path};
    use std::fmt;

    struct CMDebug<'a, T>(&'a ClockManager<T>);

    impl<'a, T: fmt::Debug> fmt::Debug for CMDebug<'a, T> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.debug_struct("ClockManager")
                .field("idx", &self.0.idx)
                .field("clock", &self.0.clock)
                .field("entries", &self.0.entries)
                .finish()
        }
    }

    #[test]
    fn clock_manager() {
        let mut cm = ClockManager::new(4);

        // Fill the buffer pool
        for (i, val) in (100..104).enumerate() {
            let (idx, replaced) = dbg!(cm.sweep(val));
            assert_eq!(idx, i);
            assert!(replaced.is_none());
            dbg!(CMDebug(&cm));
        }

        // Now we evict the first entry
        let result = cm.sweep(104);
        assert_eq!(result, (0, Some(100)));

        // Some entries get accessed
        cm.update(1);
        cm.update(2);
        dbg!(CMDebug(&cm));

        // The next one after those gets evicted
        let result = cm.sweep(105);
        assert_eq!(result, (3, Some(103)));
        dbg!(CMDebug(&cm));

        // Final state of the ClockManager
        assert_eq!(cm.entries, &[Some(104), Some(101), Some(102), Some(105)]);
    }

    #[test]
    fn append_and_update_pages() -> anyhow::Result<()> {
        let path = create_test_path("test-potpotdb::buffer::append_pages.data");
        let storage = PagedFile::from_path(&path)?;
        let mut pool = BufferPool::new(storage, 3);

        let aligned = aligned::Buffer::with_value(0xff);

        for expected_page in [0, 1, 2, 3, 4].iter() {
            let page_id = pool.append_page(&aligned)?;
            dbg!(page_id);
            assert_eq!(page_id, *expected_page);

            // Verify that the page is written into the buffer pool
            //assert!(pool.page_table.contains_key(&page_id));
        }

        let mut read_aligned = aligned::Buffer::new();

        for page_id in [0, 1, 2, 3, 4].iter() {
            // Assert that reading a page fills the buffer with the appropriate data
            pool.read_page(*page_id, &mut read_aligned)?;
            read_aligned.iter().for_each(|&byte| assert_eq!(byte, 255));

            // Reset to zeros.
            read_aligned.iter_mut().for_each(|loc| *loc = 0)
        }

        // Assert that trying to read a non-existent page results in an error
        pool.read_page(5, &mut read_aligned)
            .expect_err("reading a nonexistent page should error");

        // Try updating a page that is in the buffer pool, and a page that is not in the buffer pool:
        // Verify that the data can be read from the page, and that the page is still in the buffer pool.

        let aligned = aligned::Buffer::with_value(0x80);

        let in_pool = 4;
        assert!(pool.page_table.contains_key(&in_pool));

        let not_in_pool = 0;
        assert!(!pool.page_table.contains_key(&not_in_pool));

        // Test in_pool first, because testing not_in_pool could evict in_pool
        for &page_id in &[in_pool, not_in_pool] {
            pool.update_page(page_id, &aligned)?;
            assert!(pool.page_table.contains_key(&page_id));

            pool.read_page(page_id, &mut read_aligned)?;
            read_aligned.iter().for_each(|byte| assert_eq!(*byte, 128));
        }

        Ok(())
    }

    #[test]
    fn buffer_pool() -> anyhow::Result<()> {
        let path = create_test_path("test-potpotdb::buffer::buffer_pool.data");
        let storage = PagedFile::from_path(&path)?;
        let mut pool = BufferPool::new(storage, 3);
        let mut aligned = aligned::Buffer::new();

        let pages: Vec<_> = [101, 102, 103, 104]
            .iter()
            // Insert enough pages to exceed the capacity of the buffer pool.
            // Pages should get written to disk / evicted from buffer pool.
            .map(|&value| {
                aligned[..].iter_mut().for_each(|loc| *loc = value);
                let page_id = pool.append_page(&aligned).unwrap();
                (page_id, value)
            })
            .collect();

        for (page_id, value) in pages {
            pool.read_page(page_id, &mut aligned)?;
            aligned.iter().for_each(|&byte| assert_eq!(byte, value));
        }
        Ok(())
    }
}
