/*
use spacetimedb_runtime_core::io::SpacetimeIO;

use crate::{
    checksum::Checksum,
    grid::BlockRef,
    manifest::{SnapshotHeader, TableId},
};

pub struct BuildState {
    tables: BTreeMap<TableId, Vec<Page>>,
    blobs: BTreeMap<Checksum, Blob>,
}

impl BuildState {
    pub async fn from_manifest(io: impl SpacetimeIO, manifest: &SnapshotHeader) {}
}

pub struct Page {
    stored_at: BlockRef,
}

pub struct Blob {
    stored_at: BlockRef,
    refcount: usize,
}
*/
