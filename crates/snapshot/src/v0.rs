use std::path::PathBuf;

use spacetimedb_durability::TxOffset;
use spacetimedb_fs_utils::dir_trie::{CountCreated, DirTrie};
use spacetimedb_lib::{bsatn, Identity};
use spacetimedb_sats::{Deserialize, Serialize};
use spacetimedb_table::{blob_store::BlobHash, page::Page, table::Table, tiered::BlobSet};

use crate::{
    table_num_present_pages, table_present_pages, BlobEntry, ObjectType, SnapshotError, TableEntry,
    ZERO_HASH_DENOTING_ABSENT_PAGE,
};

pub const SNAPSHOT_VERSION: u8 = 0;

#[derive(Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// A magic number: must be equal to [`MAGIC`].
    pub(crate) magic: [u8; 4],

    /// The snapshot version number. Must be equal to [`SNAPSHOT_VERSION`].
    pub(crate) version: u8,

    /// The identity of the snapshotted database.
    pub database_identity: Identity,
    /// The instance ID of the snapshotted database.
    pub replica_id: u64,

    /// ABI version of the module from which this snapshot was created, as [MAJOR, MINOR].
    ///
    /// As of this proposal, [7, 0].
    pub(crate) module_abi_version: [u16; 2],

    /// The transaction offset of the state this snapshot reflects.
    pub tx_offset: TxOffset,

    /// The hashes and reference counts of all objects in the blob store.
    pub(crate) blobs: Vec<BlobEntry>,

    /// For each table, its table ID followed by the hashes of all resident pages.
    ///
    /// It's necessary to store the table ID rather than relying on order
    /// because table IDs may be sparse (and will be, once we reserve a bunch of system table ids).
    pub(crate) tables: Vec<TableEntry>,
}

impl Snapshot {
    /// Insert a single large blob from a [`BlobSet`]
    /// into the in-memory snapshot `self`
    /// and the on-disk object repository `object_repo`.
    ///
    /// If the `prev_snapshot` is supplied, this method will attempt to hardlink the blob's on-disk object
    /// from that previous snapshot into `object_repo` rather than creating a fresh object.
    fn write_blob(
        &mut self,
        object_repo: &DirTrie,
        hash: &BlobHash,
        uses: usize,
        blob: &[u8],
        prev_snapshot: Option<&DirTrie>,
        counter: &mut CountCreated,
    ) -> Result<(), SnapshotError> {
        object_repo
            .hardlink_or_write(prev_snapshot, &hash.data, || blob, counter)
            .map_err(|cause| SnapshotError::WriteObject {
                ty: ObjectType::Blob(*hash),
                dest_repo: object_repo.root().to_path_buf(),
                source_repo: prev_snapshot.map(|dest_repo| dest_repo.root().to_path_buf()),
                cause,
            })?;
        self.blobs.push(BlobEntry {
            hash: *hash,
            uses: uses as u32,
        });
        Ok(())
    }

    /// Populate the in-memory snapshot `self`,
    /// and the on-disk object repository `object_repo`,
    /// with all large blobs from `blobs`.
    pub(crate) fn write_all_blobs(
        &mut self,
        object_repo: &DirTrie,
        blobs: &BlobSet,
        prev_snapshot: Option<&DirTrie>,
        counter: &mut CountCreated,
    ) -> Result<(), SnapshotError> {
        blobs.try_for_each(|hash, uses, blob| {
            self.write_blob(object_repo, &hash, uses.get(), blob, prev_snapshot, counter)
        })?;

        Ok(())
    }

    /// Write a single `page` into the on-disk object repository `object_repo`.
    ///
    /// `hash` must be the content hash of `page`, and must be stored in `page.unmodified_hash()`.
    ///
    /// Returns the `hash` for convenient use with [`Iter::map`] in [`Self::write_table`].
    ///
    /// If the `prev_snapshot` is supplied, this function will attempt to hardlink the page's on-disk object
    /// from that previous snapshot into `object_repo` rather than creating a fresh object.
    fn write_page(
        object_repo: &DirTrie,
        page: &Page,
        hash: blake3::Hash,
        prev_snapshot: Option<&DirTrie>,
        counter: &mut CountCreated,
    ) -> Result<blake3::Hash, SnapshotError> {
        debug_assert!(page.unmodified_hash().copied() == Some(hash));

        object_repo
            .hardlink_or_write(prev_snapshot, hash.as_bytes(), || bsatn::to_vec(page).unwrap(), counter)
            .map_err(|cause| SnapshotError::WriteObject {
                ty: ObjectType::Page(hash),
                dest_repo: object_repo.root().to_path_buf(),
                source_repo: prev_snapshot.map(|source_repo| source_repo.root().to_path_buf()),
                cause,
            })?;

        Ok(hash)
    }

    /// Populate the in-memory snapshot `self`,
    /// and the on-disk object repository `object_repo`,
    /// with all pages from `table`.
    fn write_table(
        &mut self,
        object_repo: &DirTrie,
        table: &mut Table,
        prev_snapshot: Option<&DirTrie>,
        counter: &mut CountCreated,
    ) -> Result<(), SnapshotError> {
        let mut pages = Vec::new();
        for page in table.iter_pages_with_hashes() {
            let hash = match page? {
                None => ZERO_HASH_DENOTING_ABSENT_PAGE,
                Some((hash, page)) => Self::write_page(object_repo, &page.read(), hash, prev_snapshot, counter)?,
            };

            pages.push(hash);
        }

        self.tables.push(TableEntry {
            table_id: table.schema.table_id,
            pages,
        });
        Ok(())
    }

    /// Populate the in-memory snapshot `self`,
    /// and the on-disk object repository `object_repo`,
    /// with all pages from all tables in `tables`.
    pub(crate) fn write_all_tables<'db>(
        &mut self,
        object_repo: &DirTrie,
        tables: impl Iterator<Item = &'db mut Table>,
        prev_snapshot: Option<&DirTrie>,
        counter: &mut CountCreated,
    ) -> Result<(), SnapshotError> {
        for table in tables {
            self.write_table(object_repo, table, prev_snapshot, counter)?;
        }
        Ok(())
    }

    /// The number of objects in this snapshot, both blobs and pages.
    pub fn total_objects(&self) -> usize {
        self.blobs.len()
            + self
                .tables
                .iter()
                .map(|table| table_num_present_pages(&table.pages) as usize)
                .sum::<usize>()
    }

    /// Obtain an iterator over the [`blake3::Hash`]es of all objects
    /// this snapshot is referring to.
    pub fn objects(&self) -> impl Iterator<Item = blake3::Hash> + '_ {
        self.blobs
            .iter()
            .map(|b| blake3::Hash::from_bytes(b.hash.data))
            .chain(self.tables.iter().flat_map(|t| table_present_pages(&t.pages)))
    }

    /// Obtain an iterator over the [`PathBuf`]s of all objects
    pub fn files<'a>(&'a self, src_repo: &'a DirTrie) -> impl Iterator<Item = (blake3::Hash, PathBuf)> + 'a {
        self.objects().map(move |hash| {
            let path = src_repo.file_path(hash.as_bytes());
            (hash, path)
        })
    }
}
