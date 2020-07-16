pub mod bufferpool;
pub mod storage;
pub mod aligned;
pub mod page;
pub mod types;
pub mod query;
pub mod record;
pub mod result;


#[cfg(test)]
mod testutils;

pub const PAGESIZE: usize = 4096;

