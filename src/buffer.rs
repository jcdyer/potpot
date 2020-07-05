/// Buffer pool to cache
/// Cache system.  Does this need to
use crate::storage::PagedFile;
use std::{collections::HashMap, sync::Mutex};

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
    // map page IDs to their location in the cache
    catalog: HashMap<u64, usize>,

    // manager to determine which cache entries to evict
    manager: CM,

    // cached pages
    cache: Vec<Mutex<[u8; 4096]>>,

    // the managed PagedFile
    page_file: PagedFile,
}

impl BufferPool {
    pub fn new(page_file: PagedFile, size: usize) -> BufferPool {
        let cache = std::iter::repeat_with(|| Mutex::new([0; 4096]))
            .take(size)
            .collect();
        BufferPool {
            catalog: HashMap::with_capacity(size),
            cache,
            manager: ClockManager::new(size),
            page_file,
        }
    }

    pub fn read_page(&mut self, page_id: u64, buf: &mut [u8]) -> std::io::Result<()> {
        let BufferPool {
            catalog,
            cache,
            manager,
            page_file,
        } = self;

        let entry = catalog
            .get(&page_id)
            .and_then(|cache_idx| {
                manager.update(*cache_idx);
                cache.get(*cache_idx)
            })
            .map(|mtx| mtx.lock().unwrap());

        match entry {
            Some(val) => {
                println!("Got some entry");
                buf.copy_from_slice(val.as_ref());
            }
            None => {
                println!("No entry");
                page_file.read_page(page_id, buf)?;

                let (cache_idx, evicted_page) = manager.sweep(page_id);

                // If there is a page to evict, remove it now.
                evicted_page.and_then(|page_id| catalog.remove(&page_id));

                // Copy the page into the selected cache index
                catalog.insert(page_id, cache_idx);
                cache[cache_idx].lock().unwrap()[..].copy_from_slice(&buf);
            }
        };
        Ok(())
    }

    // Write a page and get back a page id.
    pub fn append_page(&mut self, data: &[u8]) -> std::io::Result<u64> {
        self.page_file.append_page(data)
    }

    // Update an existing page
    pub fn update_page(&mut self, page_id: u64, data: &[u8]) -> std::io::Result<()> {
        self.page_file.write_page(page_id, data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{storage::PagedFile, testutils::create_test_path};
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

        // Fill the cache
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
    fn append_pages() -> anyhow::Result<()> {
        let path = create_test_path("test-potpotdb::buffer::append_pages.data");
        let page_file = PagedFile::from_path(&path, 4096)?;
        let mut pool = BufferPool::new(page_file, 3);

        let buf = [255; 8192];
        let aligned = pool.page_file.aligned_ref(&buf);

        for expected_page in [0, 1, 2, 3, 4].iter() {
            let page_id = pool.append_page(&aligned)?;
            dbg!(page_id);
            assert_eq!(page_id, *expected_page);
        }

        let mut read_buf = [0; 8192];
        let mut read_aligned = pool.page_file.aligned_mut(&mut read_buf);

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

        Ok(())
    }

    #[test]
    fn buffer_cache() -> anyhow::Result<()> {
        let path = create_test_path("test-potpotdb::buffer::buffer_cache.data");
        let page_file = PagedFile::from_path(&path, 4096)?;
        let mut pool = BufferPool::new(page_file, 3);
        let mut buf = [0; 8192];
        let mut aligned = pool.page_file.aligned_mut(&mut buf);

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
