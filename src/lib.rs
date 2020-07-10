pub mod buffer;
pub mod storage;
pub mod aligned;

#[cfg(test)]
mod testutils;

pub const PAGESIZE: usize = 4096;