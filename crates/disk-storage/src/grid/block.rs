use core::num::NonZeroUsize;

use async_stream::stream;
use futures_util::Stream;
use spacetimedb_runtime_core::io::{AlignedBytes, ErrorWith, SpacetimeIO};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{checksum::Checksum, datafile::Datafile, free_set::AcquiredAddress, grid::U64};

pub const BLOCK_SIZE: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ReadError<IO: SpacetimeIO> {
    #[error("block checksum mismatch: {} != {}", computed, expected)]
    ChecksumMismatch { computed: Checksum, expected: Checksum },
    #[error(transparent)]
    Io(IO::Error),
}

pub type WriteError<IO, T> = ErrorWith<<IO as SpacetimeIO>::Error, T>;

pub type WriteResult<IO, T> = Result<(T, BlockRef), WriteError<IO, (T, AcquiredAddress)>>;

/// Reference to an on-disk block.
///
/// The type of the target block ([ChainedBlock] or [FixedBlock]) depends on the
/// context.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromBytes, IntoBytes, KnownLayout, Immutable)]
pub struct BlockRef {
    /// Checksum of the block.
    checksum: Checksum,
    /// The block's offset within the data file .
    address: U64,
    _padding: U64,
}
const _: () = assert!(size_of::<BlockRef>() == 48);

impl BlockRef {
    /// `ZERO` denotes `None`: a hash-linked entry has no parent.
    pub const ZERO: Self = Self {
        checksum: Checksum::ZERO,
        address: U64::ZERO,
        _padding: U64::ZERO,
    };

    pub fn new(checksum: Checksum, address: u64) -> Self {
        Self {
            checksum,
            address: address.into(),
            _padding: U64::ZERO,
        }
    }

    pub fn is_zero(&self) -> bool {
        self == &Self::ZERO
    }

    pub fn checksum(&self) -> Checksum {
        self.checksum
    }

    pub fn address(&self) -> u64 {
        self.address.get()
    }
}

/// A block `T` that is currently being written.
///
/// Captures the [AcquiredAddress] and the [SpacetimeIO::Completion] of the write.
pub struct InFlightBlock<T, IO: SpacetimeIO> {
    pub(super) address: AcquiredAddress,
    pub(super) completion: IO::Completion<Result<T, ErrorWith<IO::Error, T>>>,
}

/// A block of data for dynamically-sized objects.
///
/// A block contains a [BlockRef] header, that hash-chains to a parent block if
/// the object is larger than a single block. [Checksum::ZERO] in the header
/// indicates that there are no more parents.
///
/// Note that [Self::load_chain] loads blocks in chain order: consumers will
/// need to reverse the chain to recover the write order.
#[repr(C, align(4096))]
#[derive(Debug, FromBytes, IntoBytes, KnownLayout, Immutable)]
pub struct ChainedBlock {
    pub(super) parent: BlockRef,
    pub(super) data: [u8; BLOCK_SIZE - size_of::<BlockRef>()],
}
const _: () = assert!(size_of::<ChainedBlock>() == BLOCK_SIZE);
const _: () = <ChainedBlock as AlignedBytes>::ASSERT_VALID_LAYOUT;

impl ChainedBlock {
    pub const LEN: usize = size_of::<Self>();
    pub const DATA_LEN: usize = Self::LEN - size_of::<BlockRef>();

    /// The number of blocks needed to store `data`, `None` if `data` is empty.
    pub fn num_blocks_for(data: &[u8]) -> Option<NonZeroUsize> {
        NonZeroUsize::new(data.len().div_ceil(Self::DATA_LEN))
    }

    pub async fn store<IO: SpacetimeIO>(
        mut self,
        df: &Datafile<'_, IO>,
        address: AcquiredAddress,
    ) -> WriteResult<IO, Self> {
        let raw_address = address.as_ref().get();
        self = df
            .write_all_at(self, raw_address)
            .await
            .map_err(|e| e.map_with(|buf| (buf, address)))?;
        let block_ref = BlockRef::new(self.checksum(), raw_address);
        Ok((self, block_ref))
    }

    /// Load a single block without following the chain, reusing the allocation
    /// `buf`.
    ///
    /// The block is loaded from the address described by [BlockRef], and its
    /// checksum is verified.
    ///
    /// Failure to load the block (including checksum verification failure)
    /// returns the `buf` allocation in an [ErrorWith]. The contents of `buf` is
    /// unspecified in this case.
    pub async fn load_into<IO: SpacetimeIO>(
        df: &Datafile<'_, IO>,
        ptr: &BlockRef,
        buf: Self,
    ) -> Result<Self, ErrorWith<ReadError<IO>, Self>> {
        let block = df
            .read_exact_at(buf, ptr.address.get())
            .await
            .map_err(|e| e.map_err(ReadError::Io))?;
        let checksum = block.checksum();
        if checksum != ptr.checksum {
            Err(ErrorWith {
                error: ReadError::ChecksumMismatch {
                    computed: checksum,
                    expected: ptr.checksum,
                },
                with: block,
            })
        } else {
            Ok(block)
        }
    }

    /// Load the chain of blocks starting at `head`.
    ///
    /// The `buf` function obtains [ChainedBlock] allocations as needed.
    ///
    /// Failure to load a block in the chain (including checksum verification
    /// failure) will terminate the stream after the `Err` has been yielded.
    /// The contents of the [ChainedBlock] allocation returned alongside the
    /// error is unspecified.
    pub fn load_chain<IO: SpacetimeIO>(
        df: &Datafile<'_, IO>,
        mut mem: impl FnMut() -> ChainedBlock,
        head: BlockRef,
    ) -> impl Stream<Item = Result<Self, ErrorWith<ReadError<IO>, Self>>> {
        stream! {
            let mut current_head = head;
            while !current_head.is_zero() {
                match ChainedBlock::load_into(df, &current_head, mem()).await {
                    Err(e) => {
                        yield Err(e);
                        break;
                    },
                    Ok(block) => {
                        current_head = block.parent;
                        yield Ok(block);
                    }
                }
            }
        }
    }

    /// Compute the [Checksum] of `self`.
    pub fn checksum(&self) -> Checksum {
        Checksum::from_bytes(AlignedBytes::as_bytes(self))
    }

    pub fn copy_from_slice(&mut self, parent: BlockRef, data: &[u8]) {
        self.data[..data.len()].copy_from_slice(data);
        if data.len() < Self::DATA_LEN {
            self.data[data.len()..].fill(0);
        }
        self.parent = parent;
    }
}

/// A fixed size block of data.
///
/// Unlike [ChainedBlock], this doesn't contain a parent pointer.
/// The data section is opaque, and may be interpreted by the application.
///
/// Used to store database pages.
#[repr(C, align(4096))]
#[derive(Debug, FromBytes, IntoBytes, KnownLayout, Immutable)]
pub struct FixedBlock {
    pub(super) data: [u8; 64 * 1024],
}
const _: () = assert!(size_of::<FixedBlock>() == BLOCK_SIZE);
const _: () = <FixedBlock as AlignedBytes>::ASSERT_VALID_LAYOUT;

impl FixedBlock {
    pub const LEN: usize = size_of::<Self>();

    pub async fn store<IO: SpacetimeIO>(
        mut self,
        df: &Datafile<'_, IO>,
        address: AcquiredAddress,
    ) -> WriteResult<IO, Self> {
        let raw_address: u64 = address.as_ref().get();
        self = df
            .write_all_at(self, raw_address)
            .await
            .map_err(|e| e.map_with(|buf| (buf, address)))?;
        let block_ref = BlockRef::new(self.checksum(), raw_address);
        Ok((self, block_ref))
    }

    /// Load a block into the allocation `buf` from the address described by
    /// [BlockRef], and verify its checksum.
    pub async fn load_into<IO: SpacetimeIO>(
        df: &Datafile<'_, IO>,
        ptr: &BlockRef,
        buf: Self,
    ) -> Result<Self, ErrorWith<ReadError<IO>, Self>> {
        let block = df
            .read_exact_at(buf, ptr.address.get())
            .await
            .map_err(|e| e.map_err(ReadError::Io))?;
        let checksum = block.checksum();
        if checksum != ptr.checksum {
            Err(ErrorWith {
                error: ReadError::ChecksumMismatch {
                    computed: checksum,
                    expected: ptr.checksum,
                },
                with: block,
            })
        } else {
            Ok(block)
        }
    }

    /// Compute the [Checksum] of `self`.
    fn checksum(&self) -> Checksum {
        Checksum::from_bytes(AlignedBytes::as_bytes(self))
    }
}
