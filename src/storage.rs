use std::{
    fs::{File, OpenOptions},
    io::{self, prelude::*, SeekFrom},
    os::unix::fs::OpenOptionsExt,
    path::Path,
};

use crate::aligned;
use libc::O_DIRECT;

#[derive(Debug)]
pub struct PagedFile {
    file: File,
}

impl PagedFile {
    pub fn from_path<P: AsRef<Path>>(filename: P) -> io::Result<PagedFile> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .custom_flags(O_DIRECT)
            .open(filename)?;
        Ok(PagedFile { file })
    }

    /// Returns the page size of the PagedFile.
    pub fn page_size(&self) -> usize {
        crate::PAGESIZE
    }

    /// Reads a single page out of the PagedFile, using direct I/O.
    ///
    /// Direct I/O requires that the provided buffer is properly aligned.
    /// A properly aligned buffer can be acquired with `aligned_mut()`
    ///
    /// ```
    /// # fn main() -> anyhow::Result<()> {
    /// use potpotdb::{
    ///     aligned,
    ///     storage::PagedFile,
    /// };
    ///
    /// let mut path = std::path::PathBuf::from(env!{"CARGO_MANIFEST_DIR"});
    /// path.push("data");
    /// path.push("pagefile");
    /// let mut p = PagedFile::from_path(&path)?;
    /// let mut aligned = aligned::Buffer::new();
    /// p.read_page(0, &mut aligned)?;
    /// // Data is now read into aligned, which can be derefed to a &[u8]
    /// assert_eq!(aligned.len(), p.page_size());
    /// assert_eq!(aligned[0], b'1');
    /// assert_eq!(aligned[1], b'a');
    /// # Ok(())
    /// # }
    /// ```
    pub fn read_page(
        &mut self,
        page_number: u64,
        buf: &mut aligned::Buffer,
    ) -> io::Result<()> {
        (&self.file).seek(SeekFrom::Start(page_number * self.page_size() as u64))?;
        (&self.file).read_exact(&mut buf[..self.page_size()])?;
        Ok(())
    }

    /// Writes one page from the provided buffer to the specified page of the PagedFile,
    /// using direct I/O.
    ///
    /// Direct I/O requires that the provided buffer is properly aligned.
    pub fn write_page(&mut self, page_number: u64, buf: &[u8]) -> io::Result<()> {
        (&self.file).seek(SeekFrom::Start(page_number * self.page_size() as u64))?;
        (&self.file).write_all(&buf[..self.page_size()])?;
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
    use crate::aligned;
    use crate::testutils::create_test_path;

    #[test]
    fn write_then_read() -> anyhow::Result<()> {
        let filepath = create_test_path("test-potpotdb::storage::write_then_read.data");
        let mut f = PagedFile::from_path(&filepath)?;

        let mut read_aligned = aligned::Buffer::new();

        for c in [b'A', b'B', b'C'].iter().copied() {
            let write_aligned = aligned::Buffer::with_value(c);
            let pageno = f.append_page(&write_aligned)?;

            f.read_page(pageno, &mut read_aligned)?;

            for b in &*read_aligned {
                assert_eq!(*b, c);
            }
        }
        let write_aligned = aligned::Buffer::with_value(b'z');

        f.write_page(1, &write_aligned)?;
        f.read_page(1, &mut read_aligned)?;
        for b in &*read_aligned {
            assert_eq!(*b, b'z');
        }
        Ok(())
    }
}
