use arrayvec::ArrayVec;
use core::{
    fmt,
    num::{NonZeroU64, NonZeroUsize},
    ops::Range,
};
use spacetimedb_runtime_core::io::{ErrorWith, SpacetimeIO};
use zerocopy::{little_endian::U64, FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{
    bitset::Bitset,
    checksum::Checksum,
    datafile::Datafile,
    grid::{BlobReadError, BlockRef, ChainedBlock, U32},
    manifest::BlobRef,
};

const SHARD_BITS: usize = 4096;

#[derive(Debug, thiserror::Error)]
pub enum ReadError<IO: SpacetimeIO> {
    #[error("failed to read acquired set")]
    ReadAcquired(BlobReadError<IO>),
    #[error("failed to read released set")]
    ReadReleased(BlobReadError<IO>),
}

#[repr(C)]
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
pub struct FreeSetRef {
    checksum: Checksum,
    len: U32,
    block: BlockRef,
}

pub struct Reservation {
    block_base: usize,
    block_count: usize,
    session: usize,
}

impl Reservation {
    pub fn block_count(&self) -> usize {
        self.block_count
    }

    pub fn block_range(&self) -> Range<usize> {
        Range {
            start: self.block_base,
            end: self.block_base + self.block_count,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReservationState {
    Reserving,
    Forfeiting,
}

#[derive(Debug)]
pub struct AcquiredAddress(NonZeroU64);

impl From<AcquiredAddress> for NonZeroU64 {
    fn from(AcquiredAddress(inner): AcquiredAddress) -> Self {
        inner
    }
}

impl AsRef<NonZeroU64> for AcquiredAddress {
    fn as_ref(&self) -> &NonZeroU64 {
        &self.0
    }
}

impl fmt::Display for AcquiredAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

pub struct FreeSetStorage<'a> {
    pub acquired: &'a mut [U64],
    pub released: &'a mut [U64],
    pub index: &'a mut [U64],
}

pub struct FreeSet<'a> {
    acquired: Bitset<'a>,
    released: Bitset<'a>,
    index: Bitset<'a>,

    reservation_blocks: usize,
    reservation_count: usize,
    reservation_state: ReservationState,
    reservation_session: usize,
}

impl<'a> FreeSet<'a> {
    pub fn new(acquired: Bitset<'a>, released: Bitset<'a>, index: Bitset<'a>) -> Self {
        assert_eq!(acquired.len(), released.len());
        assert!(acquired.len().is_multiple_of(SHARD_BITS));
        assert_eq!(index.len(), acquired.len().div_ceil(SHARD_BITS));

        Self {
            acquired,
            released,
            index,
            reservation_blocks: 0,
            reservation_count: 0,
            reservation_state: ReservationState::Reserving,
            reservation_session: 1,
        }
    }

    pub async fn load<IO: SpacetimeIO>(
        df: &Datafile<'_, IO>,
        acquired_ptr: &BlobRef,
        released_ptr: &BlobRef,
        block_memory: &mut ArrayVec<ChainedBlock, 2>,
        FreeSetStorage {
            acquired,
            released,
            index,
        }: FreeSetStorage<'a>,
    ) -> Result<Self, ReadError<IO>> {
        assert_eq!(acquired.len(), released.len());
        assert!(acquired.len().is_multiple_of(SHARD_BITS));
        assert_eq!(index.len(), acquired.len().div_ceil(SHARD_BITS));

        let acquired = Bitset::load(df, acquired_ptr, block_memory.pop().unwrap(), acquired)
            .await
            .map(|(bitset, block)| {
                block_memory.push(block);
                bitset
            })
            .map_err(|ErrorWith { error, with }| {
                block_memory.push(with);
                ReadError::ReadAcquired(error)
            })?;
        let released = Bitset::load(df, released_ptr, block_memory.pop().unwrap(), released)
            .await
            .map(|(bitset, block)| {
                block_memory.push(block);
                bitset
            })
            .map_err(|ErrorWith { error, with }| {
                block_memory.push(with);
                ReadError::ReadReleased(error)
            })?;

        let mut index = Bitset::from_bits(index);
        for shard in 0..index.len() {
            let start = shard * SHARD_BITS;
            let end = (start + SHARD_BITS).min(acquired.len());

            if acquired.all_set(start..end) {
                index.set(shard);
            } else {
                index.unset(shard);
            }
        }

        Ok(Self {
            acquired,
            released,
            index,
            reservation_blocks: 0,
            reservation_count: 0,
            reservation_state: ReservationState::Reserving,
            reservation_session: 0,
        })
    }

    pub fn reserve(&mut self, num_blocks: NonZeroUsize) -> Option<Reservation> {
        assert_eq!(self.reservation_state, ReservationState::Reserving);

        let shard_start = self.reservation_blocks / SHARD_BITS;
        let first_non_full_shard = self.index.first_unset(shard_start..)?;

        let mut cursor = (first_non_full_shard * SHARD_BITS).max(self.reservation_blocks);

        for _ in 0..num_blocks.get() {
            let free = self.acquired.first_unset(cursor..self.acquired.len())?;
            cursor = free + 1;
        }

        let reservation = Reservation {
            block_base: self.reservation_blocks,
            block_count: cursor - self.reservation_blocks,
            session: self.reservation_session,
        };

        self.reservation_blocks += reservation.block_count;
        self.reservation_count += 1;

        Some(reservation)
    }

    pub fn acquire(&mut self, reservation: &Reservation) -> Option<AcquiredAddress> {
        assert!(self.reservation_count > 0);
        assert_eq!(reservation.session, self.reservation_session);

        let start = reservation.block_base;
        let end = reservation.block_base + reservation.block_count;

        assert!(end <= self.reservation_blocks);
        assert!(end <= self.acquired.len());

        let shard_start = start / SHARD_BITS;
        let shard_end = end.div_ceil(SHARD_BITS);

        let shard = self.index.first_unset(shard_start..shard_end)?;

        let block_start = (shard * SHARD_BITS).max(start);
        let block_end = ((shard + 1) * SHARD_BITS).min(end);

        let block = self.acquired.first_unset(block_start..block_end)?;

        assert!(!self.acquired.is_set(block));
        assert!(!self.released.is_set(block));

        self.acquired.set(block);

        if self.acquired.first_unset(shard_start..shard_end).is_none() {
            self.index.set(shard);
        }

        NonZeroU64::new(block as u64 + 1).map(AcquiredAddress)
    }

    pub fn release(&mut self, address: NonZeroU64) {
        let block = address.get() as usize - 1;

        assert!(block < self.acquired.len());
        assert!(self.acquired.is_set(block));
        assert!(!self.released.is_set(block));

        self.released.set(block);
    }

    pub fn forfeit(&mut self, reservation: Reservation) {
        assert_eq!(reservation.session, self.reservation_session);
        assert!(self.reservation_count > 0);

        self.reservation_count -= 1;

        if self.reservation_count == 0 {
            self.reservation_blocks = 0;
            self.reservation_session = self.reservation_session.wrapping_add(1);
            self.reservation_state = ReservationState::Reserving;
        } else {
            self.reservation_state = ReservationState::Forfeiting;
        }
    }

    pub fn free_released(&mut self) {
        assert_eq!(self.reservation_count, 0);

        let mut cursor = 0;

        while let Some(block) = self.released.first_unset(cursor..self.acquired.len()) {
            self.released.unset(block);
            self.acquired.unset(block);

            let shard = block / SHARD_BITS;
            self.index.unset(shard);

            cursor = block + 1;
        }
    }
}
