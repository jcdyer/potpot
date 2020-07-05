use std::{
    ops::{Deref, DerefMut},
    fs::{File, OpenOptions},
    io::{self, prelude::*, SeekFrom},
    os::unix::fs::OpenOptionsExt,
    path::Path,
};

use libc::O_DIRECT;

#[allow(unused)]
const PAGE_SIZE: usize = 4096;

#[derive(Debug)]
pub struct PagedFile {
    file: File,
    page_size: usize,
}

impl PagedFile {
    pub fn from_path<P: AsRef<Path>>(filename: P, page_size: usize) -> io::Result<PagedFile> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .custom_flags(O_DIRECT)
            .open(filename)?;
        Ok(PagedFile { file, page_size })
    }

    /// Returns the page size of the PagedFile.
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Reads a single page out of the PagedFile, using direct I/O.
    ///
    /// Direct I/O requires that the provided buffer is properly aligned.
    /// A properly aligned buffer can be acquired with `aligned_mut()`
    ///
    /// ```
    /// # fn main() -> anyhow::Result<()> {
    /// use potpotdb::storage::{PagedFile, aligned_mut};
    ///
    /// let mut path = std::path::PathBuf::from(env!{"CARGO_MANIFEST_DIR"});
    /// path.push("data");
    /// path.push("pagefile");
    /// let mut p = PagedFile::from_path(&path, 4096)?;
    /// let mut buf = [0; 8192]; // One alignment's worth of data will be discarded aligning the buffer
    /// let aligned = aligned_mut(&mut buf, p.page_size());
    /// p.read_page(0, aligned)?;
    /// // Data is now read into `aligned[0..4096]`, which is somewhere in `buf`
    /// assert_eq!(aligned.len(), p.page_size());
    /// assert_eq!(aligned[0], b'1');
    /// assert_eq!(aligned[1], b'a');
    /// # Ok(())
    /// # }
    /// ```
    pub fn read_page(&mut self, page_number: u64, buf: &mut [u8]) -> io::Result<()> {
        (&self.file).seek(SeekFrom::Start(page_number * self.page_size as u64))?;
        (&self.file).read_exact(&mut buf[..self.page_size])?;
        Ok(())
    }

    /// Writes one page from the provided buffer to the specified page of the PagedFile,
    /// using direct I/O.
    ///
    /// Direct I/O requires that the provided buffer is properly aligned.
    pub fn write_page(&mut self, page_number: u64, buf: &[u8]) -> io::Result<()> {
        (&self.file).seek(SeekFrom::Start(page_number * self.page_size as u64))?;
        (&self.file).write_all(&buf[..self.page_size])?;
        (&self.file).sync_data()?;
        Ok(())
    }

    pub fn append_page(&mut self, buf: &[u8]) -> io::Result<u64> {
        let offset = (&self.file).seek(SeekFrom::End(0))?;
        let pageno = offset / self.page_size() as u64;
        (&self.file).write_all(&buf[..self.page_size()])?;
        (&self.file).sync_data()?;
        Ok(pageno)
    }

    pub fn aligned_ref<'a>(&self, buf: &'a [u8]) -> AlignedRef<'a> {
        let slice = aligned_ref(buf, self.page_size());
        AlignedRef::new(slice)
    }

    pub fn aligned_mut<'a>(&self, buf: &'a mut [u8]) -> AlignedMut<'a> {
        let slice = aligned_mut(buf, self.page_size());
        AlignedMut::new(slice)
    }
}

pub struct AlignedRef<'a> {
    slice: &'a [u8]
}

impl<'a> AlignedRef<'a> {
    fn new(slice: &'a [u8]) -> AlignedRef<'a> {
        AlignedRef { slice }
    }

    pub fn as_slice(&self) -> &'a [u8] {
        self.slice
    }
}

impl<'a> Deref for AlignedRef<'a> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.slice
    }
}

pub struct AlignedMut<'a> {
    slice: &'a mut [u8]
}

impl<'a> AlignedMut<'a> {
    fn new(slice: &'a mut [u8]) -> AlignedMut<'a> {
        AlignedMut { slice }
    }

    pub fn into_slice(self) -> &'a mut [u8] {
        self.slice
    }
}

impl<'a> Deref for AlignedMut<'a> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &*self.slice
    }
}

impl<'a> DerefMut for AlignedMut<'a> {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.slice
    }
}


/// Given a slice, returns an aligned slice, sacrificing one alignment's
/// worth of space.
pub fn aligned_ref(slice: &[u8], align: usize) -> &[u8] {
    let addr = slice as *const [u8] as *const u8 as usize;
    let offset = (align - addr % align) % align;
    let len = slice.len();
    &slice[offset..len - align + offset]
}

// Given a mutable slice, returns an aligned mutable slice, sacrificing
// one alignment's worth of space.
pub fn aligned_mut(slice: &mut [u8], align: usize) -> &mut [u8] {
    let addr = slice as *const [u8] as *const u8 as usize;
    let offset = (align - addr % align) % align;
    let len = slice.len();
    &mut slice[offset..len - align + offset]
}
// Page format, will be handled one layer up from this:
//     u8 -> version
//     u8 -> page_type
//
//     [u8; 3]-> [RESERVED]
//     [slots]
//     tuples
//
// slot format:
//     u16 -> SlotId
//     u16 -> offset (of data from start of page)
//     u16 -> length
//     u16 -> reserved?

#[cfg(test)]
mod test {
    use super::*;
    use crate::testutils::create_test_path;


    #[test]
    fn write_then_read() -> anyhow::Result<()> {
        let filepath = create_test_path("test-potpotdb::storage::write_then_read.data");
        let mut f = PagedFile::from_path(&filepath, PAGE_SIZE)?;

        let mut read_buf = [0; PAGE_SIZE * 2];
        let mut read_aligned = f.aligned_mut(&mut read_buf);

        for i in &[b'A', b'B', b'C'] {

            let write_page = [*i; PAGE_SIZE * 2];
            let write_aligned = f.aligned_ref(&write_page);
            let pageno = f.append_page(&write_aligned)?;

            f.read_page(pageno, &mut read_aligned)?;

            for b in &*read_aligned {
                assert_eq!(*b, *i);
            }
        }
        let write_page = [b'z'; PAGE_SIZE * 2];
        let write_aligned = f.aligned_ref(&write_page);

        f.write_page(1, &write_aligned)?;
        f.read_page(1, &mut read_aligned)?;
        for b in &*read_aligned {
            assert_eq!(*b, b'z');
        }
        Ok(())
    }
}
