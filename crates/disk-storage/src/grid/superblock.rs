use core::{
    cmp::{Ordering, Reverse},
    num::{NonZeroU16, NonZeroUsize},
};
use spacetimedb_runtime_core::io::{ErrorWith, SpacetimeIO};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{
    checksum::Checksum,
    datafile::Datafile,
    grid::{BlockRef, U16, U256, U32, U64},
};

#[derive(Debug, thiserror::Error)]
pub enum ReadError<IO: SpacetimeIO> {
    #[error("read quorum not reached")]
    NoReadQuorum { required: NonZeroUsize },
    #[error("expected copy {} but read {}", expected_copy, block.copy)]
    MisdirectedCopy { expected_copy: u16, block: Superblock },
    #[error("failed to read enough copies to reach a quorum")]
    InsufficientCopies {
        errors: [Option<LoadError<IO>>; Superblock::NUM_COPIES.get()],
    },
    #[error("checksum mismatch: {} != {}", computed, block.checksum)]
    ChecksumMismatch { computed: Checksum, block: Superblock },
    #[error("superblocks are not hash-chained: parent={} head={}", parent, head)]
    HashChainBroken { parent: Checksum, head: Checksum },
    #[error(transparent)]
    Io(#[from] ErrorWith<IO::Error, Superblock>),
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError<IO: SpacetimeIO> {
    #[error("expected copy {} but found {}", expected_copy, actual_copy)]
    MisdirectedCopy { expected_copy: u16, actual_copy: u16 },
    #[error("checksum mismatch: {} != {}", computed, expected)]
    ChecksumMismatch { computed: Checksum, expected: Checksum },
    #[error(transparent)]
    Io(IO::Error),
}

#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)]
pub enum WriteError<IO: SpacetimeIO> {
    #[error("write quorum not reached")]
    NoWriteQuorum {
        required: NonZeroUsize,
        #[source]
        source: ReadError<IO>,
    },
    #[error("failed to fdatasync the datafile")]
    Fdatasync(#[source] IO::Error),
    #[error(transparent)]
    Io(IO::Error),
}

#[repr(C, align(4096))]
#[derive(Debug, FromBytes, IntoBytes, KnownLayout, Immutable)]
pub struct Superblock {
    checksum: Checksum,
    parent: Checksum,

    // TODO: Should use NonZero, but zerocopy doesn't provide an endianness
    // wrapper.
    copy: U16,

    version: U16,
    features: U32,

    sequence: U64,

    database_identity: U256,

    free_set_blocks_acquired: BlockRef,
    free_set_blocks_released: BlockRef,

    sealed_snapshot: Checksum,

    // NOTE: We don't support legacy block sizes (512) in the DST harness, so
    // we'll need to inflate this type to 4096 bytes.
    reserved: [u8; 3856],
}
const _: () = assert!(size_of::<Superblock>() == 4096);

impl Superblock {
    pub const LEN: usize = size_of::<Self>();
    pub const VERSION: u16 = 1;

    const NUM_COPIES: NonZeroUsize = NonZeroUsize::new(4).unwrap();
    const READ_QUORUM: NonZeroUsize = NonZeroUsize::new(2).unwrap();
    const WRITE_QUORUM: NonZeroUsize = NonZeroUsize::new(3).unwrap();

    /// Store the superblock `copies` in the datafile, and verify that a write
    /// quorum was reached.
    ///
    /// `copies` must uphold:
    ///
    /// - all elements are `Some`
    /// - all superblock elements compare equal, except for their `copy` field
    ///
    /// The success return value is an index into `copies`.
    /// The copy at that index is guaranteed to be durable as per `fdatasync`
    /// semantics, and `Self::WRITE_QUORUM` writes have succeeded.
    ///
    /// Copies at other indexes should not be considered valid.
    ///
    /// If an error is returned, it is guaranteed that all elements of `copies`
    /// are still `Some`. The values are, however, unspecified.
    pub async fn store<IO: SpacetimeIO>(
        df: &Datafile<'_, IO>,
        copies: &mut [Option<Self>; Self::NUM_COPIES.get()],
    ) -> Result<usize, WriteError<IO>> {
        assert!(copies.iter().all(|c| c.is_some()));
        assert!(copies
            .iter()
            .all(|c| c.as_ref().unwrap().eq_without_copy(copies[0].as_ref().unwrap())));

        for (copy, block) in copies
            .iter_mut()
            .enumerate()
            .map(|(i, block)| (NonZeroU16::new(i as u16 + 1).unwrap(), block))
        {
            // Move the [Superblock] out of the `Option` for use as a write
            // buffer.
            let mut buf = block.take().unwrap();
            // Set the copy.
            buf.copy = copy.get().into();
            // Write at the offset designated for the copy and ensure to put the
            // [Superblock] back into `copies` in both the success or failure
            // case.
            let offset = df.superblock_offset(copy);
            match df.write_all_at(buf, offset).await {
                Ok(copy) => {
                    block.replace(copy);
                }
                Err(ErrorWith { error, with }) => {
                    block.replace(with);
                    return Err(WriteError::Io(error));
                }
            }
        }

        // Ensure we can read back the data.
        df.fdatasync().await.map_err(WriteError::Fdatasync)?;

        // Verify write quorum.
        let result = Self::load_into_with_quorum(df, Self::WRITE_QUORUM, copies)
            .await
            .map_err(|e| WriteError::NoWriteQuorum {
                required: Self::WRITE_QUORUM,
                source: e,
            });
        assert!(copies.iter().all(|c| c.is_some()));

        result
    }

    /// Read [Self::NUM_COPIES] of the superblock into `copies`, and ensure a
    /// [Self::READ_QUORUM] is reached.
    ///
    /// Quorum means that:
    ///
    /// - at least `quorum` copies could be successfully read from storage
    /// - their checksums verify
    /// - their checksums are equal
    /// - their `sequence` number is equal
    ///
    /// If two quorums exists (e.g. a 2/2 split when 4 copies are used), the
    /// higher sequence number is chosen if and only if it is chained with the
    /// lower sequence number, i.e. its `parent` checksum matches the previous
    /// `checksum`.
    ///
    /// `copies` must uphold that all elements are `Some`.
    ///
    /// The success return value is an index into `copies`.
    /// The copy at that index is guaranteed to satisfy the quorum requirements.
    ///
    /// If an error is returned, it is guaranteed that all elements of `copies`
    /// are still `Some`. The values are, however, unspecified.
    pub async fn load_into<IO: SpacetimeIO>(
        df: &Datafile<'_, IO>,
        copies: &mut [Option<Self>; Self::NUM_COPIES.get()],
    ) -> Result<usize, ReadError<IO>> {
        Self::load_into_with_quorum(df, Self::READ_QUORUM, copies).await
    }

    /// Read [Self::NUM_COPIES] of the superblock into `copies`, and ensure a
    /// `quorum` is reached.
    ///
    /// Quorum means that:
    ///
    /// - at least `quorum` copies could be successfully read from storage
    /// - their checksums verify
    /// - their checksums are equal
    /// - their `sequence` number is equal
    ///
    /// If two quorums exists (e.g. a 2/2 split when 4 copies are used), the
    /// higher sequence number is chosen if and only if it is chained with the
    /// lower sequence number, i.e. its `parent` checksum matches the previous
    /// `checksum`.
    ///
    /// `copies` must uphold that all elements are `Some`.
    ///
    /// The success return value is an index into `copies`.
    /// The copy at that index is guaranteed to satisfy the quorum requirements.
    ///
    /// If an error is returned, it is guaranteed that all elements of `copies`
    /// are still `Some`. The values are, however, unspecified.
    pub async fn load_into_with_quorum<IO: SpacetimeIO>(
        df: &Datafile<'_, IO>,
        quorum: NonZeroUsize,
        copies: &mut [Option<Self>; Self::NUM_COPIES.get()],
    ) -> Result<usize, ReadError<IO>> {
        const NUM_COPIES: usize = Superblock::NUM_COPIES.get();

        assert!(copies.iter().all(|c| c.is_some()));
        let mut errors: [Option<LoadError<IO>>; NUM_COPIES] = core::array::from_fn(|_| None);

        for (copy_idx, copy_num) in (0..Self::NUM_COPIES.get()).map(|i| (i, NonZeroU16::new(i as u16 + 1).unwrap())) {
            match Self::load_copy_into(df, copy_num, copies[copy_idx].take().unwrap()).await {
                Ok(copy) => copies[copy_idx] = Some(copy),
                Err(ErrorWith { error, with }) => {
                    copies[copy_idx] = Some(with);
                    errors[copy_idx] = Some(error);
                }
            }
        }

        // Give up early if we don't have enough valid copies to reach quorum.
        if errors.iter().filter(|e| e.is_some()).count() >= quorum.get() {
            return Err(ReadError::InsufficientCopies { errors });
        }

        // Order indices into `copies`:
        // Successful reads first, highest sequence first.
        // Failed copies are ignored and sorted to the end.
        let mut order: [usize; NUM_COPIES] = core::array::from_fn(|i| i);
        order.sort_unstable_by(|&a, &b| match (errors[a].is_none(), errors[b].is_none()) {
            (true, true) => {
                let a = copies[a].as_ref();
                let b = copies[b].as_ref();
                Reverse(a.unwrap().sequence).cmp(&Reverse(b.unwrap().sequence))
            }
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => Ordering::Equal,
        });

        // The index into `copies` of the copy with the highest sequence for
        // which a quorum af copies with the same checksum exists.
        let mut highest_quorum: Option<usize> = None;
        // The previous quorum.
        //
        // Two quorums can exist (assuming NUM_COPIES=2, READ_QUORUM=2) due to:
        // - a crash before WRITE_QUORUM was reached
        // - lost or misdirected writes (that were not checked / visible when
        //   verifying the write quorum)
        // - latent sector errors that prevented a write from becoming durable
        let mut previous_quorum: Option<usize> = None;

        let mut i = 0;
        while i < NUM_COPIES {
            let idx = order[i];

            // Everything after this is a failed read.
            if errors[idx].is_some() {
                break;
            }

            let sequence = copies[idx].as_ref().unwrap().sequence;

            // Find the end of this sequence group.
            let mut j = i + 1;
            while j < NUM_COPIES {
                let other_idx = order[j];

                if errors[other_idx].is_some() || copies[other_idx].as_ref().unwrap().sequence != sequence {
                    break;
                }

                j += 1;
            }

            // Within this sequence, find whether some checksum reaches quorum.
            //
            // N is small, so an O(N^2) scan avoids another temporary structure.
            let mut quorum_idx = None;
            for k in i..j {
                let candidate_idx = order[k];
                let candidate = copies[candidate_idx].as_ref().unwrap();

                let count = (i..j)
                    .filter(|&l| {
                        let other = copies[order[l]].as_ref().unwrap();
                        other.checksum == candidate.checksum
                    })
                    .count();

                if count >= quorum.get() {
                    quorum_idx = Some(candidate_idx);
                    break;
                }
            }

            // If we have a quorum, check that it is connected to the parent
            // quorum, if any.
            if let Some(current_idx) = quorum_idx {
                if highest_quorum.is_none() {
                    highest_quorum = Some(current_idx);
                }

                if let Some(child_idx) = previous_quorum {
                    let child = copies[child_idx].as_ref().unwrap();
                    let parent = copies[current_idx].as_ref().unwrap();

                    if child.parent != parent.checksum {
                        return Err(ReadError::HashChainBroken {
                            parent: parent.checksum,
                            head: child.checksum,
                        });
                    }
                }

                previous_quorum = Some(current_idx);
            }

            i = j;
        }

        assert!(copies.iter().all(|c| c.is_some()));

        highest_quorum.ok_or_else(|| ReadError::NoReadQuorum { required: quorum })
    }

    pub async fn load_copy_into<IO: SpacetimeIO>(
        df: &Datafile<'_, IO>,
        copy: NonZeroU16,
        buf: Self,
    ) -> Result<Self, ErrorWith<LoadError<IO>, Self>> {
        let block = df
            .read_exact_at(buf, df.superblock_offset(copy))
            .await
            .map_err(|e| e.map_err(LoadError::Io))?;

        // TODO: Specify how version changes should be handled.
        assert_eq!(block.version.get(), Self::VERSION);

        if block.copy != copy.get() {
            Err(ErrorWith {
                error: LoadError::MisdirectedCopy {
                    expected_copy: copy.get(),
                    actual_copy: block.copy.get(),
                },
                with: block,
            })
        } else {
            let checksum = Checksum::from_bytes(&block.as_bytes()[size_of::<Checksum>()..]);
            if checksum != block.checksum {
                Err(ErrorWith {
                    error: LoadError::ChecksumMismatch {
                        computed: checksum,
                        expected: block.checksum,
                    },
                    with: block,
                })
            } else {
                Ok(block)
            }
        }
    }

    /// `true` if all fields of `self`, except for the `copy` field, compare
    /// equal to the respective field in `other`.
    fn eq_without_copy(&self, other: &Self) -> bool {
        let Self {
            checksum,
            parent,
            copy: _,
            version,
            features,
            sequence,
            database_identity,
            free_set_blocks_acquired,
            free_set_blocks_released,
            sealed_snapshot,
            reserved,
        } = self;

        checksum == &other.checksum
            && parent == &other.parent
            && version == &other.version
            && features == &other.features
            && sequence == &other.sequence
            && database_identity == &other.database_identity
            && free_set_blocks_acquired == &other.free_set_blocks_acquired
            && free_set_blocks_released == &other.free_set_blocks_released
            && sealed_snapshot == &other.sealed_snapshot
            && reserved == &other.reserved
    }
}
