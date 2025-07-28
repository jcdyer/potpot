#![allow(unused)]

use crate::{bufferpool, result, page};
use std::collections::BTreeMap;
pub(crate) type PageId = u64;

/// Creating and accessing record
pub struct RecordManager {
    // The ID of the page currently accepting record appends, until it fills up.
    current_page: (PageId, page::SlottedPage),

    // Map of pages with free space
    free_space: BTreeMap<u64, usize>,
}

impl RecordManager {

    /// Write a record into the current
    pub fn append_record(
        &mut self,
        record: &[u8],
        bufpool: &mut bufferpool::BufferPool,
    ) -> Result<(PageId, u16), result::Error> {

        let &mut(pid, ref mut pg) = &mut self.current_page;
        if pg.free_space() >= record.len() + 4 {
            let rid = pg.insert_record(record).map_err(|_| result::Error::Other)?;
            bufpool.update_page(pid, pg.data()).map_err(|_| result::Error::Other)?;
            Ok((pid, rid))
        } else {
            let mut newpg = page::SlottedPage::default();
            match newpg.insert_record(record) {
                Ok(rid) => {
                    let pid = bufpool.insert_page(newpg.data()).map_err(|_| result::Error::Other)?;
                    self.current_page = (pid, newpg);
                    Ok((pid, rid))
                }
                Err(_) => Err(result::Error::Other),
            }
        }
    }

    pub fn get_record(&mut self, page_id: PageId, slot: u16, bufpool: &mut bufferpool::BufferPool) -> Result<Option<Vec<u8>>, result::Error> {
        let mut page = page::SlottedPage::default();
        let buffer = page.data_mut();
        bufpool.read_page(page_id, buffer)?;
        Ok(page.get_record(slot).map(|rec| rec.to_owned()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FieldType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Float16, // Repr?
    Float32,
    Float64,
    Bytes(std::num::NonZeroU16),
    Char(std::num::NonZeroU16),
    Blob,
    Text,
}

#[test]
fn nichy_field_type() {
    assert_eq!(
        std::mem::size_of::<FieldType>(),
        4,
    );
    assert_eq!(
        std::mem::size_of::<FieldType>(),
        std::mem::size_of::<Option<FieldType>>(),
        "FieldType is not nichy"
    );
}
pub struct Schema {
    fields: Vec<FieldType>
}
pub struct Record<'a> {
    data: &'a [u8]
}


