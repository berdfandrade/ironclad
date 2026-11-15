//! Bounds-checked binary decoding for untrusted bytes (disk and network).
//!
//! `bytes::Buf` panics on underflow; everything that parses external input
//! goes through [`Reader`] instead, which turns short input into an error.

use crate::error::{Error, Result};

pub struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.buf.len() < n {
            return Err(Error::Corruption(format!(
                "truncated input: need {n} bytes, have {}",
                self.buf.len()
            )));
        }
        let (head, tail) = self.buf.split_at(n);
        self.buf = tail;
        Ok(head)
    }

    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    pub fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.bytes(2)?.try_into().unwrap()))
    }

    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.bytes(8)?.try_into().unwrap()))
    }

    /// A `u32` length followed by that many bytes.
    pub fn len_prefixed(&mut self) -> Result<&'a [u8]> {
        let n = self.u32()? as usize;
        self.bytes(n)
    }
}
