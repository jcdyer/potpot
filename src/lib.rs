pub mod buffer;
pub mod storage;
pub mod aligned;
pub mod page;
pub mod types;

#[cfg(test)]
mod testutils;

pub const PAGESIZE: usize = 4096;

