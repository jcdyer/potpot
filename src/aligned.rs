use crc::crc32;
use std::{
    convert::TryInto,
    fmt,
    ops::{Deref, DerefMut},
};

#[repr(C, align(4096))]
#[derive(Clone)]
pub struct Buffer {
    data: [u8; crate::PAGESIZE],
}

impl Buffer {
    pub fn new() -> Box<Buffer> {
        Box::new(Buffer::default())
    }

    pub fn with_value(val: u8) -> Buffer {
        Buffer {
            data: [val; crate::PAGESIZE],
        }
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

fn check_crc(buffer: &Buffer) -> bool {
    let crc: u32 = crc32::checksum_ieee(&buffer[4..]);
    crc == u32::from_le_bytes(buffer[..4].try_into().unwrap())
}

#[derive(Debug, Eq, PartialEq)]
pub enum Error {
    CrcError,
    SizeError,
    PageType,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Error::*;
        write!(f, "{}", match self {
            CrcError => "CRC error",
            SizeError => "Size error",
            PageType => "Page type error",
        })
    }
}

impl std::error::Error for Error {}

pub(crate) trait FromAligned: Sized {
    fn expected_page_type() -> crate::PageType;

    fn extra_constraints(_buffer: &Buffer) -> Result<(), Error> {
        Ok(())
    }

    fn from_aligned(buffer: Box<Buffer>) -> Result<Self, Error> {
        let page_type: crate::PageType = u16::from_le_bytes(buffer[4..6].try_into().unwrap())
            .try_into()
            .or(Err(Error::PageType))?;
        if !check_crc(&buffer) {
            Err(Error::CrcError)
        } else if std::mem::size_of::<Self>() != std::mem::size_of::<Buffer>() {
            Err(Error::SizeError)
        } else if page_type != Self::expected_page_type() {
            Err(Error::PageType)
        } else {
            <Self as FromAligned>::extra_constraints(&buffer)?;
            Ok(<Self as FromAligned>::transform(buffer))
        }
    }

    /// The body of this should just be `unsafe { std::mem::transmute }`,
    /// since conditions have already been checked, but rust's type system
    /// won't let us implement that on the trait since there is no compile-time
    /// guarantee that Self is the right size.
    fn transform(buffer: Box<Buffer>) -> Self;
}
