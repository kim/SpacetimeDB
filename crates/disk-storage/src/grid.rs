pub mod blob;
pub use blob::ReadError as BlobReadError;

mod block;
pub use block::{
    BlockRef, ChainedBlock, FixedBlock, InFlightBlock, ReadError as BlockReadError, WriteError as BlockWriteError,
    BLOCK_SIZE,
};

mod superblock;
pub use superblock::{ReadError as SuperblockReadError, Superblock, WriteError as SuperblockWriteError};

mod uint;
pub use uint::{U16, U256, U32, U64};

use crate::datafile::Datafile;

/// Alias denoting a database page.
pub type Page = FixedBlock;
