use core::num::NonZeroU16;

use spacetimedb_runtime_core::io::{AlignedBytes, ErrorWith, SpacetimeIO};

use crate::{
    free_set::Reservation,
    grid::{Superblock, BLOCK_SIZE},
};

const K: u64 = 1024;
const M: u64 = 1024 * K;
// TODO: These could be runtime configuration.
pub const MIN_SIZE: u64 = 10 * M;
pub const ALLOC_SIZE: u64 = MIN_SIZE;

pub struct Datafile<'a, IO: SpacetimeIO> {
    io: &'a IO,
    fd: IO::Fd,
}

impl<'a, IO: SpacetimeIO> Datafile<'a, IO> {
    pub async fn open(io: &'a IO, path: &str) -> Result<Self, IO::Error> {
        let fd = io.open_file(path).await?;

        Ok(Self { io, fd })
    }

    pub async fn create(io: &'a IO, path: &str) -> Result<Self, IO::Error> {
        let fd = io.create_file(path).await?;
        io.reserve(fd.clone(), MIN_SIZE).await?;

        Ok(Self { io, fd })
    }

    /// Reserve enough space in the datafile to store all reserved blocks.
    ///
    /// Space is allocated in [ALLOC_SIZE] increments.
    /// `reservations` must not be empty.
    pub async fn ensure_capacity(
        &mut self,
        reservations: impl IntoIterator<Item = &Reservation>,
    ) -> Result<(), IO::Error> {
        let max_blocks = reservations
            .into_iter()
            .map(|r| r.block_range().end.saturating_sub(1))
            .max()
            .expect("empty reservations");
        let max_bytes = max_blocks as u64 * BLOCK_SIZE as u64;
        let total_bytes = max_bytes.next_multiple_of(ALLOC_SIZE);

        self.io.reserve(self.fd.clone(), total_bytes).await
    }

    #[allow(unused)]
    pub(crate) async fn fsync(&self) -> Result<(), IO::Error> {
        self.io.fsync(self.fd.clone()).await
    }

    pub(crate) async fn fdatasync(&self) -> Result<(), IO::Error> {
        self.io.fdatasync(self.fd.clone()).await
    }

    pub(crate) fn write_all_at<B: AlignedBytes + Send + 'static>(
        &self,
        buf: B,
        offset: u64,
    ) -> IO::Completion<Result<B, ErrorWith<IO::Error, B>>> {
        self.io.write_all_at(self.fd.clone(), buf, offset)
    }

    pub(crate) fn read_exact_at<B: AlignedBytes + Send + 'static>(
        &self,
        buf: B,
        offset: u64,
    ) -> IO::Completion<Result<B, ErrorWith<IO::Error, B>>> {
        self.io.read_exact_at(self.fd.clone(), buf, offset)
    }

    /// Return the byte offset of a [Superblock] copy within the datafile.
    ///
    /// We put this here so that any future layout changes to the datafile are
    /// confined to this type.
    pub(crate) fn superblock_offset(&self, copy: NonZeroU16) -> u64 {
        (Superblock::LEN * copy.get() as usize) as u64
    }
}
