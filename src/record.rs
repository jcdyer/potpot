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
            let res = bufpool.update_page(pid, pg.data()).map_err(|_| result::Error::Other)?;
            Ok((pid, rid))
        } else {
            let mut newpg = page::SlottedPage::default();
            match newpg.insert_record(&record) {
                Ok(rid) => {
                    let pid = bufpool.append_page(newpg.data()).map_err(|_| result::Error::Other)?;
                    self.current_page = (pid, newpg);
                    Ok((pid, rid))
                }
                Err(_) => Err(result::Error::Other),
            }
        }
    }

    pub fn get_record(&mut self, _page_id: PageId) -> Result<(), result::Error> {
        Ok(())
    }
}
