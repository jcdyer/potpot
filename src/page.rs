#![allow(dead_code)]

use crate::aligned;
use std::{convert::TryInto, num::TryFromIntError};

pub(crate) type RecordId = u16;

#[derive(Debug)]
pub(crate) struct TmpError;

impl From<TryFromIntError> for TmpError {
    fn from(_: TryFromIntError) -> TmpError {
        TmpError
    }
}

/// SlottedPage represents a page that holds variable-sized tuples.
/// It comprises a header, followed by free space, followed by data.
/// The header format looks like:
///     * u16: End of free space -- where the most recently data starts
///     * u16: Number of records: [recno]
///     * [(u16, u16); recno]: (offset, size) to records.  (u16::MAX, 0) indicates deleted records?
/// Overall, the file looks like:
///
/// +--------+------------+---------+
/// | Header | Free space | Records |
/// +--------+------------+---------+
///
pub(crate) struct SlottedPage {
    data: Box<aligned::Buffer>,
}

impl Default for SlottedPage {
    fn default() -> SlottedPage {
        let data = aligned::Buffer::new();
        let mut pg = SlottedPage { data };
        pg.write_end_of_free_space(crate::PAGESIZE as u16);
        pg
    }
}

impl SlottedPage {
    pub(crate) fn new(records: &[&[u8]]) -> Result<SlottedPage, TmpError> {
        let mut pg = SlottedPage::default();
        let total_size: usize = records.iter().map(|rec| rec.len() + 4).sum();
        if total_size > pg.free_space() as usize {
            Err(TmpError)
        } else {
            for record in records {
                pg.insert_record(record)?;
            }
            Ok(pg)
        }
    }

    pub(crate) fn insert_record(&mut self, record: &[u8]) -> Result<RecordId, TmpError> {
        let recno = self.record_count();
        let reclen = record.len().try_into()?;


        if dbg!(reclen +4) > dbg!(self.available_bytes()) {
            Err(TmpError)
        } else {
            self.write_record_count(recno + 1);
            let offset = self.end_of_free_space() - reclen;
            let size = reclen;
            self.write_record_header(recno, offset, size);
            self.write_end_of_free_space(offset);
            self.write_record_at(offset, record);
            Ok(recno)
        }
    }

    pub(crate) fn get_record(&self, recno: u16) -> Option<&[u8]> {
        self.record_header(recno)
            .map(|(offset, size)| (offset as usize, size as usize))
            .map(|(offset, size)| &self.data[offset..offset + size])
    }

    pub(crate) fn data(&self) -> &aligned::Buffer {
        &self.data
    }

    pub(crate) fn data_mut(&mut self) -> &mut aligned::Buffer {
        &mut self.data
    }

    pub fn free_space(&self) -> usize {
        self.available_bytes() as usize
    }
}

/// Low-level private methods for properly manipulating the internals of the SlottedPage record
impl SlottedPage {
    fn end_of_free_space(&self) -> u16 {
        u16::from_le_bytes(self.data[0..2].try_into().unwrap())
    }

    fn record_count(&self) -> u16 {
        u16::from_le_bytes(self.data[2..4].try_into().unwrap())
    }

    fn record_header(&self, recno: u16) -> Option<(u16, u16)> {
        if recno < self.record_count() {
            let rho = self.record_header_offset(recno) as usize;
            Some((
                u16::from_le_bytes(self.data[rho..rho + 2].try_into().unwrap()),
                u16::from_le_bytes(self.data[rho + 2..rho + 4].try_into().unwrap()),
            ))
        } else {
            None
        }
    }

    fn available_bytes(&self) -> u16 {
        self.end_of_free_space() - self.header_size()
    }

    fn header_size(&self) -> u16 {
        2 + 2 + 4 * self.record_count()
    }

    fn write_end_of_free_space(&mut self, offset: u16) {
        self.data[0..2].copy_from_slice(&offset.to_le_bytes())
    }

    fn write_record_count(&mut self, new_count: u16) {
        self.data[2..4].copy_from_slice(&new_count.to_le_bytes())
    }

    fn record_header_offset(&self, recno: u16) -> u16 {
        4 + 4 * recno
    }

    fn write_record_header(&mut self, recno: u16, offset: u16, size: u16) {
        let rho = self.record_header_offset(recno) as usize;
        self.data[rho..rho + 2].copy_from_slice(&offset.to_le_bytes());
        self.data[rho + 2..rho + 4].copy_from_slice(&size.to_le_bytes());
    }

    fn write_record_at(&mut self, offset: u16, record: &[u8]) {
        self.data[offset as usize..offset as usize + record.len()].copy_from_slice(record)
    }
}

#[cfg(test)]
mod tests {
    use crate::PAGESIZE;

    use super::*;

    #[test]
    fn empty_slotted_page() {
        let pg = SlottedPage::default();
        assert_eq!(pg.end_of_free_space(), PAGESIZE as u16);
        assert_eq!(pg.record_count(), 0);
        assert_eq!(pg.record_header(0), None);
        assert_eq!(pg.free_space(), PAGESIZE - 4);
    }

    #[test]
    fn insert_record() {
        let mut pg = SlottedPage::default();
        pg.insert_record(b"new record").expect("insert new record");
        pg.insert_record(b"second record").expect("insert second record");
        assert_eq!(pg.end_of_free_space(), (PAGESIZE as u16 - 10 - 13)); // 4096 - 10 - 13
        assert_eq!(pg.record_count(), 2);
        assert_eq!(pg.free_space(), PAGESIZE - 10 - 13 - 12);

        assert_eq!(pg.record_header(0), Some((PAGESIZE as u16 - 10, 10)));
        assert_eq!(pg.get_record(0), Some(b"new record".as_ref()));
        assert_eq!(pg.record_header(1), Some((PAGESIZE as u16 - 10 - 13, 13)));
        assert_eq!(pg.get_record(1), Some(b"second record".as_ref()));
        assert_eq!(pg.record_header(2), None);
        assert_eq!(pg.get_record(2), None);
    }

    #[test]
    fn fill_slotted_page() {
        let mut pg = SlottedPage::default();
        let mut i = 0;
        while i < PAGESIZE / 1028 {
            assert_eq!(pg.insert_record(&[i as u8 + 1; 1024]).unwrap_or_else(|_| panic!("insert {} bytes", i * 1024)), i as u16);
            i += 1;
        }
        pg.insert_record(&[0xee; 1024]).expect_err(&format!("overflow at {} bytes", PAGESIZE));
        assert_eq!(pg.free_space(), 1024 - (i + 1) * 4);
        let available = pg.free_space() - 4;
        assert_eq!(pg.insert_record(&vec![0xff; available]).unwrap_or_else(|_| panic!("insert {} bytes", 1024 * i + available)), i as u16);
        assert_eq!(pg.free_space(), 0); // Full page at 4076 bytes written in four records

        assert_eq!(pg.record_header(0).unwrap(), (PAGESIZE as u16 - 1024, 1024));
        assert_eq!(pg.get_record(0).expect("record 0 not found"), &[1;1024][..], "record 0 not as expected");
        assert_eq!(pg.record_header(1).unwrap(), (PAGESIZE as u16 - 2048, 1024));
        assert_eq!(pg.get_record(1).expect("record 1 not found"), &[2;1024][..], "record 1 not as expected");
        assert_eq!(pg.record_header(14).unwrap(), (1024, 1024));
        assert_eq!(pg.get_record(14).expect("record 2 not found"), &[15;1024][..], "record 2 not as expected");
        assert_eq!(pg.record_header(15).unwrap(), (8 + i as u16 * 4, available as u16));
        assert_eq!(pg.get_record(15).expect("record 3 not found"), &vec![0xff;available][..], "record 3 not as expected");
    }

    #[test]
    fn empty_records() {
        let mut pg = SlottedPage::default();
        assert_eq!(pg.free_space(), PAGESIZE - 4);
        pg.insert_record(&[]).expect("insert empty record");
        assert_eq!(pg.free_space(), PAGESIZE - 8);
        pg.insert_record(&[4,5,6,9]).expect("insert record");
        assert_eq!(pg.free_space(), PAGESIZE - 16);
        pg.insert_record(&[]).expect("insert empty record");
        assert_eq!(pg.free_space(), PAGESIZE - 20);

        assert_eq!(pg.get_record(0), Some([].as_ref()));
        assert_eq!(pg.get_record(1), Some([4u8, 5, 6, 9].as_ref()));
        assert_eq!(pg.get_record(2), Some([].as_ref()));
        assert!(pg.get_record(3).is_none());

    }
}
