use spacetimedb_runtime_core::io::{AlignedBytes, SECTOR_SIZE};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{
    checksum::Checksum,
    grid::{BlockRef, U32, U64},
};

pub type TableId = u32;

#[repr(C)]
pub struct CommitHeader {
    min_tx_offset: U64,
    epoch: U32,
    n: U32,
    len: U32,
    crc: U32,
}
const _: () = assert!(size_of::<CommitHeader>() == 24);

/// Header of a snapshot manifest.
///
/// Snapshots form a hash-chained log.
/// The page / blob references are stored separate from the header in
/// fixed-length, hash-chained blocks.
#[repr(C, align(512))]
pub struct SnapshotHeader {
    /// Checksum of all of the below.
    checksum: Checksum,
    /// Checksum of the previous [Self].
    parent: Checksum,
    /// Commit at which this snapshot was taken.
    ///
    /// The commit describes the state the snapshot captures, it
    /// corresponds to the snapshot trigger (not the deadline).
    ///
    /// Note that `commit.sequence / snapshot_interval` allows to compute
    /// the byte offset of the [SnapshotHeader] in the log.
    commit: CommitHeader,

    /// Checksum of the [ArchivedSnapshot] corresponding to this snapshot.
    ///
    /// This checksum is computed while sealing the snapshot. It does not
    /// indicate that the snapshot was actually archived.
    archive_manifest: Checksum,

    /// The [TablesBlock] that stores the [TableEntry]s for this snapshot.
    ///
    /// [TablesBlock]: crate::grid::TablesBlock
    /// [TableEntry]: crate::grid::TableEntry
    tables_block: BlockRef,
    /// The [PagesBlock] that stores the [BlockRef]s for the physical pages
    /// referenced by this snapshot.
    ///
    /// [PagesBlock]: crate::grid::PagesBlock
    pages_block: BlockRef,
    /// The [BlobsBlock] that stores the [BlobRef]s for the physical blobs
    /// referenced by this snapshots.
    ///
    /// [BlobsBlock]: crate::grid::BlobsBlock
    blobs_block: BlockRef,

    /// Total number of tables in this snapshot.
    tables_count: U32,
    /// Total number of pages in this snapshot.
    pages_count: U32,
    /// Total number of blobs in this snapshot.
    blobs_count: U32,

    // padding
    reserved: [u8; 216],
}
const _: () = assert!(size_of::<SnapshotHeader>() == 512);

/// Describes a table and its allocated pages.
#[repr(C)]
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
pub struct TableEntry {
    /// The table.
    id: TableId,
    reserved: U32, // for alignment, also allows 64 bit table ids
    /// The offset into the [BlockRef]s across all [PagesBlock]s where the page
    /// entries for this table start.
    pages_offset: U64,
}
const _: () = assert!(size_of::<TableEntry>() == 16);

const MAX_ENTRIES_PER_TABLES_BLOCK: usize = 32_760;

/// A 512KiB block that stores [TableEntry]s.
#[repr(C, align(4096))]
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
pub struct TablesBlock {
    /// If there are more table entries than fit in a block, this references a
    /// parent block containing more entries. [BlockRef::ZERO] if there are no
    /// more [TablesBlock]s.
    parent: BlockRef,
    /// Number of [TableEntry]s in this block (**not** the total number of tables).
    entries_count: U32,
    // padding
    reserved: [u8; 76],
    /// The table entries.
    entries: [TableEntry; MAX_ENTRIES_PER_TABLES_BLOCK],
}
const _: () = assert!(size_of::<TablesBlock>() == 512 * 1024);
const _: () = assert!(size_of::<TablesBlock>().is_multiple_of(SECTOR_SIZE));
const _: () = <TablesBlock as AlignedBytes>::ASSERT_VALID_LAYOUT;

const MAX_ENTRIES_PER_PAGES_BLOCK: usize = 10_921;

/// A 512KiB block that stores references to [PageBlock]s.
#[repr(C, align(4096))]
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
pub struct PagesBlock {
    /// If there are more page entries than fit in a block, this references a
    /// parent block containing more entries. [BlockRef::ZERO] if there are no
    /// more [PagesBlock]s.
    parent: BlockRef,
    /// Number of [BlockRef]s in this block.
    entries_count: U32,
    // padding
    reserved: [u8; 28],
    /// The [BlockRef] entries.
    entries: [BlockRef; MAX_ENTRIES_PER_PAGES_BLOCK],
}
const _: () = assert!(size_of::<PagesBlock>() == 512 * 1024);
const _: () = assert!(size_of::<PagesBlock>().is_multiple_of(SECTOR_SIZE));
const _: () = <PagesBlock as AlignedBytes>::ASSERT_VALID_LAYOUT;

const MAX_ENTRIES_PER_BLOBS_BLOCK: usize = 5_460;

/// A 512KiB block that stores [BlobRef]s.
#[repr(C, align(4096))]
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
pub struct BlobsBlock {
    /// If there are more blob entries than fit in a block, this references a
    /// parent block containing more entries. [BlockRef::ZERO] if there are no
    /// more [BlobsBlock]s.
    parent: BlockRef,
    /// Number of [BlobRef]s in this block.
    entries_count: U32,
    // padding
    reserved: [u8; 76],
    /// The [BlobRef] entries.
    entries: [BlobRef; MAX_ENTRIES_PER_BLOBS_BLOCK],
}
const _: () = assert!(size_of::<BlobsBlock>() == 512 * 1024);
const _: () = <BlobsBlock as AlignedBytes>::ASSERT_VALID_LAYOUT;

/// A reference to a blob.
#[repr(C)]
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
pub struct BlobRef {
    /// Checksum of the complete blob.
    pub checksum: Checksum,
    /// The size in bytes of the complete blob.
    pub len: U32,
    /// Number of times this block is referenced from tables.
    pub refcount: U32,
    // padding
    // TODO: Maybe not needed if we adjust MAX_ENTRIES_PER_BLOBS_BLOCK
    reserved: [u8; 8],
    /// Reference to the "head" [BlobBlock].
    pub block: BlockRef,
}
const _: () = assert!(size_of::<BlobRef>() == 96);

impl BlobRef {
    pub fn new(checksum: Checksum, len: u32, refcount: u32, block: BlockRef) -> Self {
        Self {
            checksum,
            len: len.into(),
            refcount: refcount.into(),
            reserved: [0u8; 8],
            block,
        }
    }
}
