//! Bloom filter used by SSTables to skip disk reads for absent keys.
//!
//! Uses double hashing (Kirsch–Mitzenmacher): `h_i = h1 + i * h2`. The hash
//! is implemented here rather than taken from `std` because the filter is
//! persisted and must hash identically across builds and platforms.

use bytes::BufMut;

use crate::codec::Reader;
use crate::error::{Error, Result};

pub struct BloomFilter {
    bits: Vec<u8>,
    k: u32,
}

/// FNV-1a 64-bit followed by a splitmix64 finalizer for better avalanche.
fn hash64(data: &[u8], seed: u64) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325 ^ seed;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

impl BloomFilter {
    pub fn build<'a>(keys: impl ExactSizeIterator<Item = &'a [u8]>, bits_per_key: usize) -> Self {
        let n = keys.len().max(1);
        let nbits = (n * bits_per_key).max(64);
        // Optimal k = ln(2) * bits_per_key.
        let k = ((bits_per_key as f64 * 0.69) as u32).clamp(1, 30);
        let mut filter = Self {
            bits: vec![0; nbits.div_ceil(8)],
            k,
        };
        for key in keys {
            filter.insert(key);
        }
        filter
    }

    fn nbits(&self) -> u64 {
        self.bits.len() as u64 * 8
    }

    fn positions(&self, key: &[u8]) -> impl Iterator<Item = u64> + use<> {
        let h1 = hash64(key, 0);
        let h2 = hash64(key, 0x9e37_79b9_7f4a_7c15) | 1;
        let nbits = self.nbits();
        (0..self.k as u64).map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) % nbits)
    }

    fn insert(&mut self, key: &[u8]) {
        for bit in self.positions(key).collect::<Vec<_>>() {
            self.bits[(bit / 8) as usize] |= 1 << (bit % 8);
        }
    }

    /// False means the key is definitely absent.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.positions(key)
            .all(|bit| self.bits[(bit / 8) as usize] & (1 << (bit % 8)) != 0)
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u32(self.k);
        buf.put_u32(self.bits.len() as u32);
        buf.put_slice(&self.bits);
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut r = Reader::new(data);
        let k = r.u32()?;
        let bits = r.len_prefixed()?.to_vec();
        if k == 0 || k > 30 || bits.is_empty() {
            return Err(Error::Corruption("invalid bloom filter header".into()));
        }
        Ok(Self { bits, k })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives_and_low_false_positive_rate() {
        let keys: Vec<Vec<u8>> = (0..10_000u32)
            .map(|i| format!("key{i}").into_bytes())
            .collect();
        let filter = BloomFilter::build(keys.iter().map(|k| k.as_slice()), 10);
        assert!(keys.iter().all(|k| filter.may_contain(k)));

        let false_positives = (0..10_000u32)
            .filter(|i| filter.may_contain(format!("other{i}").as_bytes()))
            .count();
        // Theoretical rate at 10 bits/key is ~0.8%.
        assert!(false_positives < 200, "fp = {false_positives}");
    }

    #[test]
    fn encode_roundtrip() {
        let filter = BloomFilter::build([b"a".as_slice(), b"b"].into_iter(), 10);
        let mut buf = Vec::new();
        filter.encode(&mut buf);
        let decoded = BloomFilter::decode(&buf).unwrap();
        assert!(decoded.may_contain(b"a") && decoded.may_contain(b"b"));
    }
}
