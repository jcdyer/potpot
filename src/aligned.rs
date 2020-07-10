use std::ops::{Deref, DerefMut};
#[repr(C, align(4096))]
pub struct Buffer {
    data: [u8; crate::PAGESIZE],
}

impl Buffer {
    pub fn new() -> Buffer {
        Buffer::default()
    }

    pub fn with_value(val: u8) -> Buffer {
        Buffer { data: [val; 4096] }
    }

    pub fn copy_from_slice(&mut self, slice: &[u8]) {
        for (loc, i) in self.data.iter_mut().zip(slice) {
            *loc = *i;
        }
    }
}

impl Default for Buffer {
    fn default() -> Buffer {
        Buffer::with_value(Default::default())
    }
}


impl Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.data
    }
}

impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

}