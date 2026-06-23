use core::ops::{Bound, RangeBounds};

use arrayvec::ArrayVec;
use spacetimedb_runtime_core::io::{ErrorWith, SpacetimeIO};
use zerocopy::{little_endian::U64, IntoBytes as _};

use crate::{
    datafile::Datafile,
    free_set::{FreeSet, Reservation},
    grid::{
        blob::{self, store_blob, PendingBlob},
        BlobReadError, ChainedBlock,
    },
    manifest::BlobRef,
};

pub const WORD_BITS: usize = 64;

pub type ReadResult<'a, IO> = Result<(Bitset<'a>, ChainedBlock), ErrorWith<BlobReadError<IO>, ChainedBlock>>;

pub struct Bitset<'a> {
    bits: &'a mut [U64],
    len: usize,
}

impl<'a> Bitset<'a> {
    pub const fn from_bits(bits: &'a mut [U64]) -> Self {
        let len = bits.len();
        Self { bits, len }
    }

    pub async fn load<IO: SpacetimeIO>(
        df: &Datafile<'_, IO>,
        ptr: &BlobRef,
        buf: ChainedBlock,
        storage: &'a mut [U64],
    ) -> ReadResult<'a, IO> {
        let len = ptr.len.get() as usize;
        assert!(len <= storage.len());

        let buf = blob::load_into(df, ptr, buf, storage.as_mut_bytes()).await?;
        storage.as_mut_bytes()[len..].fill(0);

        Ok((Self::from_bits(storage), buf))
    }

    pub async fn store<const MAX_CONCURRENCY: usize, IO>(
        &self,
        df: &Datafile<'_, IO>,
        free_set: &mut FreeSet<'_>,
        block_memory: &mut ArrayVec<ChainedBlock, MAX_CONCURRENCY>,
        reserved_addresses: Reservation,
    ) -> Result<BlobRef, IO::Error>
    where
        IO: SpacetimeIO,
        IO::Error: Unpin,
    {
        let blob = PendingBlob::new(self.bits.as_bytes());
        store_blob(df, free_set, reserved_addresses, block_memory, blob).await
    }

    pub fn is_set(&self, bit: usize) -> bool {
        assert!(bit < self.len);

        let word = bit / WORD_BITS;
        let mask = 1u64 << (bit % WORD_BITS);

        (self.bits[word].get() & mask) != 0
    }

    pub fn set(&mut self, bit: usize) {
        assert!(bit < self.len);

        let word = bit / WORD_BITS;
        let shift = bit % WORD_BITS;
        let mask = 1u64 << shift;

        let value = self.bits[word].get() | mask;
        self.bits[word].set(value);
    }

    pub fn unset(&mut self, bit: usize) {
        assert!(bit < self.len);
        let word = bit / WORD_BITS;
        let shift = bit % WORD_BITS;
        let mask = 1u64 << shift;

        let value = self.bits[word].get() & !mask;
        self.bits[word].set(value);
    }

    pub fn first_unset(&self, range: impl RangeBounds<usize>) -> Option<usize> {
        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n.checked_add(1)?,
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(&n) => n.checked_add(1)?,
            Bound::Excluded(&n) => n,
            Bound::Unbounded => self.len,
        };

        assert!(start <= end);
        assert!(end <= self.len);

        let mut word_index = start / WORD_BITS;
        let mut bit_index = start;

        while bit_index < end {
            let word_start = word_index * WORD_BITS;
            let word_end = (word_start + WORD_BITS).min(self.len).min(end);

            let first_bit = bit_index - word_start;
            let last_bit = word_end - word_start;

            let mut mask = !self.bits[word_index].get();

            // Clear bits before the range start in the first word.
            mask &= u64::MAX << first_bit;

            // Clear bits after the range end in the last word.
            if last_bit < WORD_BITS {
                mask &= (1u64 << last_bit) - 1;
            }

            if mask != 0 {
                return Some(word_start + mask.trailing_zeros() as usize);
            }

            word_index += 1;
            bit_index = word_index * WORD_BITS;
        }

        None
    }

    pub fn all_set(&self, range: impl RangeBounds<usize>) -> bool {
        self.first_unset(range).is_none()
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_unset() {
        let mut bits = [U64::ZERO; 64];
        let mut bitmap = Bitset::from_bits(&mut bits);
        for i in 0..bitmap.len {
            assert!(!bitmap.is_set(i));
            bitmap.set(i);
            assert!(bitmap.is_set(i));
            bitmap.unset(i);
            assert!(!bitmap.is_set(i));
        }
    }

    #[test]
    #[should_panic]
    fn is_set_out_of_bounds() {
        let mut bits = [U64::ZERO; 64];
        let bitmap = Bitset::from_bits(&mut bits);
        bitmap.is_set(64);
    }

    #[test]
    #[should_panic]
    fn set_out_of_bounds() {
        let mut bits = [U64::ZERO; 64];
        let mut bitmap = Bitset::from_bits(&mut bits);
        bitmap.set(64);
    }

    #[test]
    #[should_panic]
    fn unset_out_of_bounds() {
        let mut bits = [U64::ZERO; 64];
        let mut bitmap = Bitset::from_bits(&mut bits);
        bitmap.unset(64);
    }

    #[test]
    fn first_unset() {
        let mut bits = [U64::ZERO; 512];
        let mut bitmap = Bitset::from_bits(&mut bits);

        bitmap.set(12);
        assert_eq!(bitmap.first_unset(..), Some(0));
        assert_eq!(bitmap.first_unset(12..), Some(13));

        bitmap.set(0);
        assert_eq!(bitmap.first_unset(..), Some(1));

        for i in 0..bitmap.len {
            bitmap.set(i);
        }
        assert_eq!(bitmap.first_unset(..), None);
    }
}
