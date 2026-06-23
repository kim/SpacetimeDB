use arrayvec::ArrayVec;
use core::{
    cell::RefCell,
    iter,
    num::NonZeroUsize,
    ops::Range,
    pin::{pin, Pin},
    task::{Context, Poll},
};
use futures_util::{FutureExt, TryStreamExt as _};
use spacetimedb_runtime_core::io::{ErrorWith, SpacetimeIO};

use crate::{
    checksum::Checksum,
    free_set::{AcquiredAddress, FreeSet, Reservation},
    grid::{BlockReadError, BlockRef, ChainedBlock, Datafile, InFlightBlock},
    manifest::BlobRef,
};

#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)]
pub enum ReadError<IO: SpacetimeIO> {
    #[error("blob checksum mismatch: {} != {}", computed, expected)]
    ChecksumMismatch { computed: Checksum, expected: Checksum },
    #[error(transparent)]
    Block(#[from] BlockReadError<IO>),
}

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("blob has no blocks")]
    EmptyBlock,
    #[error("not enough space to store {} blob blocks", required_blocks)]
    NoSpace { required_blocks: usize },
}

/// Load the blob described by `ptr` by copying block data into `dst`.
///
/// `dst` must be appropriately sized to hold the data of the blob, i.e.
/// `dst.len() == ptr.len.get()`. The slice is filled from the back as
/// traversing the block chain yields blocks in reverse order.
///
/// `buf` is a scratch [ChainedBlock] buffer that is filled from storage in each
/// step of following the chain. The buffer is returned on both sides of the
/// `Result` so that it can be reused.
///
/// When all blocks have been successully loaded, the checksum of the data bytes
/// is verified against `ptr.checksum`.
pub async fn load_into<IO: SpacetimeIO>(
    df: &Datafile<'_, IO>,
    ptr: &BlobRef,
    buf: ChainedBlock,
    dst: &mut [u8],
) -> Result<ChainedBlock, ErrorWith<ReadError<IO>, ChainedBlock>> {
    assert_eq!(dst.len(), ptr.len.get() as usize);

    let mut offset = dst.len();
    let alloc = RefCell::new(Some(buf));

    let mut chain = pin!(
        ChainedBlock::load_chain(df, || alloc.borrow_mut().take().unwrap(), ptr.block)
            .map_err(|e| e.map_err(ReadError::Block))
    );
    while let Some(block) = chain.try_next().await? {
        let len = offset.min(ChainedBlock::DATA_LEN);
        offset -= len;
        dst[offset..(offset + len)].copy_from_slice(&block.data[..len]);
        alloc.borrow_mut().replace(block);
    }
    // If we read the right number of blocks, offset should be zero.
    assert_eq!(offset, 0);

    let buf = alloc.borrow_mut().take().unwrap();
    let checksum = Checksum::from_bytes(dst);
    if checksum != ptr.checksum {
        Err(ErrorWith {
            error: ReadError::ChecksumMismatch {
                computed: checksum,
                expected: ptr.checksum,
            },
            with: buf,
        })
    } else {
        Ok(buf)
    }
}

/// A blob that is incrementally being written via [store_blobs].
pub struct PendingBlob<'a, IO: SpacetimeIO> {
    data: &'a [u8],
    block_count: NonZeroUsize,
    offset: usize,

    head: BlockRef,

    submitted: usize,
    completed: usize,
    error: Option<IO::Error>,
}

impl<'a, IO: SpacetimeIO> PendingBlob<'a, IO> {
    /// Create a new [PendingBlob] job that stores `data` by copying it into the
    /// required number of [ChainedBlock]s.
    ///
    /// `data` must not be empty.
    ///
    /// To write the blob to storage, pass it to [store_blobs].
    pub fn new(data: &'a [u8]) -> Self {
        let block_count = ChainedBlock::num_blocks_for(data).expect("empty blob");
        Self {
            data,
            block_count,
            offset: 0,
            head: BlockRef::ZERO,
            submitted: 0,
            completed: 0,
            error: None,
        }
    }

    /// The number of [ChainedBlock]s needed to store this blob.
    pub fn required_blocks(&self) -> NonZeroUsize {
        self.block_count
    }

    /// Acquire an address from the [FreeSet], copy the current slice of data
    /// into `memory`, submit the write operation and return an [InFlightBlock]
    /// holding the completion.
    ///
    /// Returns `memory` in an `Err` if either:
    ///
    /// - a previously completed write errored
    /// - there is no more data to be written
    ///
    fn submit_write(
        &mut self,
        df: &Datafile<'_, IO>,
        address: AcquiredAddress,
        mut memory: ChainedBlock,
    ) -> Result<InFlightBlock<ChainedBlock, IO>, ChainedBlock> {
        if self.offset >= self.data.len() || self.error.is_some() {
            return Err(memory);
        }

        let end = (self.offset + ChainedBlock::DATA_LEN).min(self.data.len());
        memory.copy_from_slice(self.head, &self.data[self.offset..end]);
        let block_ref = BlockRef::new(memory.checksum(), address.as_ref().get());
        let completion = df.write_all_at(memory, address.as_ref().get());

        self.offset = end;
        self.submitted += 1;
        self.head = block_ref;

        Ok(InFlightBlock { address, completion })
    }

    /// Update the state with the result if an [InFlightBlock] created by
    /// calling [Self::submit_write].
    fn complete_write(&mut self, result: Result<ChainedBlock, ErrorWith<IO::Error, ChainedBlock>>) -> ChainedBlock {
        self.completed += 1;
        match result {
            Ok(block) => block,
            Err(ErrorWith { error, with }) => {
                if self.error.is_none() {
                    self.error.replace(error);
                }
                with
            }
        }
    }

    /// Check whether this blob is complete.
    ///
    /// It is complete if all submitted IOPS have been completed, and there
    /// either was an error or all of the data has been written.
    fn is_complete(&self) -> bool {
        self.submitted == self.completed && (self.error.is_some() || self.offset == self.data.len())
    }
}

/// A [Future] to drive writing a set of [PendingBlob]s to storage.
///
/// The `MAX_CONCURRENCY` is determined by the number of [ChainedBlock]
/// allocations provided.
///
/// # Cancellation
///
/// Polling this future is **not** cancel safe: dropping it before it was polled
/// to completion will leak the provided [ChainedBlock] memory and
/// [Reservation]s.
///
/// To ensure the future gets polled to completion, use one of the following
/// patterns:
///
/// ```ignore
/// runtime.spawn(StoreBlobs::new(/* ... */)).await
/// ```
///
/// or
///
/// ```ignore
/// let mut writes = StoreBlobs::new(/* ... */);
/// loop {
///     select! {
///         results = &mut writes => {
///             return results;
///         }
///     }
/// }
/// ```
pub struct StoreBlobs<'a, 'io, 'free_set, 'jobs, const MAX_CONCURRENCY: usize, const JOB_COUNT: usize, IO: SpacetimeIO>
{
    state: Option<StoreBlobsInner<'a, 'io, 'free_set, 'jobs, MAX_CONCURRENCY, JOB_COUNT, IO>>,
}

impl<'a, 'io, 'free_set, 'jobs, const MAX_CONCURRENCY: usize, const JOB_COUNT: usize, IO>
    StoreBlobs<'a, 'io, 'free_set, 'jobs, MAX_CONCURRENCY, JOB_COUNT, IO>
where
    IO: SpacetimeIO,
{
    fn new(
        df: &'a Datafile<'io, IO>,
        free_set: &'a mut FreeSet<'free_set>,
        reserved_addresses: Reservation,
        block_memory: &'a mut ArrayVec<ChainedBlock, MAX_CONCURRENCY>,
        jobs: [PendingBlob<'jobs, IO>; JOB_COUNT],
    ) -> Self {
        assert!(!block_memory.is_empty());
        assert!(JOB_COUNT >= 1);
        let total_block_count = jobs
            .iter()
            .map(PendingBlob::required_blocks)
            .map(NonZeroUsize::get)
            .sum();
        assert!(reserved_addresses.block_count() >= total_block_count);

        Self {
            state: Some(StoreBlobsInner {
                df,
                free_set,
                reserved_addresses,
                block_memory,
                jobs,
                completions: core::array::from_fn(|_| None),
                next_job: (0..JOB_COUNT).cycle(),
            }),
        }
    }
}

impl<'a, 'io, 'free_set, 'jobs, const MAX_CONCURRENCY: usize, const JOB_COUNT: usize, IO> Future
    for StoreBlobs<'a, 'io, 'free_set, 'jobs, MAX_CONCURRENCY, JOB_COUNT, IO>
where
    IO: SpacetimeIO,
    IO::Error: Unpin,
{
    type Output = [Result<BlobRef, IO::Error>; JOB_COUNT];

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut inner = this.state.take().expect("store blobs future polled after completion");
        let poll = inner.poll(cx);
        match poll {
            Poll::Ready(()) => {
                inner.free_set.forfeit(inner.reserved_addresses);
                let results = inner.jobs.map(|mut job| {
                    job.error.take().map_or_else(
                        || {
                            Ok(BlobRef::new(
                                Checksum::from_bytes(job.data),
                                job.data.len() as u32,
                                0,
                                job.head,
                            ))
                        },
                        Err,
                    )
                });

                Poll::Ready(results)
            }
            Poll::Pending => {
                this.state = Some(inner);
                Poll::Pending
            }
        }
    }
}

struct StoreBlobsInner<'a, 'io, 'free_set, 'jobs, const MAX_CONCURRENCY: usize, const JOB_COUNT: usize, IO: SpacetimeIO>
{
    df: &'a Datafile<'io, IO>,
    free_set: &'a mut FreeSet<'free_set>,
    reserved_addresses: Reservation,
    block_memory: &'a mut ArrayVec<ChainedBlock, MAX_CONCURRENCY>,
    jobs: [PendingBlob<'jobs, IO>; JOB_COUNT],
    completions: [Option<(usize, InFlightBlock<ChainedBlock, IO>)>; MAX_CONCURRENCY],
    next_job: iter::Cycle<Range<usize>>,
}

impl<'a, 'io, 'free_set, 'jobs, const MAX_CONCURRENCY: usize, const JOB_COUNT: usize, IO: SpacetimeIO>
    StoreBlobsInner<'a, 'io, 'free_set, 'jobs, MAX_CONCURRENCY, JOB_COUNT, IO>
{
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            let mut made_progress = self.poll_completions(cx);
            made_progress |= self.schedule_jobs();

            // We are done when all jobs are done.
            if self.jobs.iter().all(PendingBlob::is_complete) {
                // Jobs should be waiting for all submitted ops to complete,
                // regardless of outcome. Therefore, there can't be any
                // in-flight operations.
                assert!(self.completions.iter().all(Option::is_none));
                return Poll::Ready(());
            }

            if !made_progress {
                return Poll::Pending;
            }
        }
    }

    /// Poll all outstanding operations.
    fn poll_completions(&mut self, cx: &mut Context<'_>) -> bool {
        let mut made_progress = false;

        for completion in self.completions.iter_mut().filter(|c| c.is_some()) {
            // TODO: Need to move `BlockWrite` so that `write.completion` can be
            // pinned. Perhaps we want to use pin-project instead.
            let (job_index, mut write) = completion.take().unwrap();
            match Pin::new(&mut write.completion).poll(cx) {
                Poll::Ready(result) => {
                    if result.is_err() {
                        // This block's address is definitely not used.
                        self.free_set.release(*write.address.as_ref());
                    }
                    let memory = self.jobs[job_index].complete_write(result);
                    self.block_memory.push(memory);

                    made_progress = true;
                }
                Poll::Pending => {
                    // Put back for later polling.
                    completion.replace((job_index, write));
                }
            }
        }

        made_progress
    }

    fn schedule_jobs(&mut self) -> bool {
        let mut made_progress = false;

        for completion in self.completions.iter_mut().filter(|x| x.is_none()) {
            for job_index in (0..JOB_COUNT)
                .cycle()
                .skip(self.next_job.next().unwrap())
                .take(JOB_COUNT)
            {
                let Some(memory) = self.block_memory.pop() else {
                    // Jobs need to complete to reclaim block memory.
                    return made_progress;
                };

                let address = self
                    .free_set
                    .acquire(&self.reserved_addresses)
                    .expect("insufficient reserved addresses");
                match self.jobs[job_index].submit_write(self.df, address, memory) {
                    Ok(write) => {
                        completion.replace((job_index, write));
                        made_progress = true;
                        // Break inner loop and try to fill next empty
                        // completion slot.
                        break;
                    }
                    Err(memory) => self.block_memory.push(memory),
                }
            }
        }

        made_progress
    }
}

/// Concurrently write a set of [PendingBlob]s to storage.
///
/// The `MAX_CONCURRENCY` is determined by the number of [ChainedBlock]
/// allocations provided.
///
/// Both the number of blocks provided and `JOB_COUNT` must be at least 1.
///
/// # Cancellation
///
/// Polling this future is **not** cancel safe: dropping it before it was polled
/// to completion will leak the provided [ChainedBlock] memory and
/// [Reservation]s.
///
/// To ensure the future gets polled to completion, use one of the following
/// patterns:
///
/// ```ignore
/// runtime.spawn(store_blobs(/* ... */)).await
/// ```
///
/// or
///
/// ```ignore
/// let mut writes = store_blobs(/* ... */);
/// loop {
///     select! {
///         results = &mut writes => {
///             return results;
///         }
///     }
/// }
/// ```
pub async fn store_blobs<const MAX_CONCURRENCY: usize, const JOB_COUNT: usize, IO>(
    df: &Datafile<'_, IO>,
    free_set: &mut FreeSet<'_>,
    reserved_addresses: Reservation,
    block_memory: &mut ArrayVec<ChainedBlock, MAX_CONCURRENCY>,
    jobs: [PendingBlob<'_, IO>; JOB_COUNT],
) -> [Result<BlobRef, IO::Error>; JOB_COUNT]
where
    IO: SpacetimeIO,
    IO::Error: Unpin,
{
    StoreBlobs::new(df, free_set, reserved_addresses, block_memory, jobs).await
}

/// Write a single [PendingBlob] to storage.
///
/// Individual blocks will be written concurrently, bounded by the number of
/// [ChainedBlock] allocations provided.
///
/// # Cancellation
///
/// Polling this future is **not** cancel safe: dropping it before it was polled
/// to completion will leak the provided [ChainedBlock] memory and
/// [Reservation]s.
///
/// To ensure the future gets polled to completion, use one of the following
/// patterns:
///
/// ```ignore
/// runtime.spawn(store_blob(/* ... */)).await
/// ```
///
/// or
///
/// ```ignore
/// let mut writes = store_blob(/* ... */);
/// loop {
///     select! {
///         result = &mut writes => {
///             return result;
///         }
///     }
/// }
/// ```
pub async fn store_blob<const MAX_CONCURRENCY: usize, IO>(
    df: &Datafile<'_, IO>,
    free_set: &mut FreeSet<'_>,
    reserved_addresses: Reservation,
    block_memory: &mut ArrayVec<ChainedBlock, MAX_CONCURRENCY>,
    blob: PendingBlob<'_, IO>,
) -> Result<BlobRef, IO::Error>
where
    IO: SpacetimeIO,
    IO::Error: Unpin,
{
    store_blobs(df, free_set, reserved_addresses, block_memory, [blob])
        .map(|[result]| result)
        .await
}
