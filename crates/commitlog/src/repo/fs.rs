use std::fmt;
use std::fs::{self, File};
use std::io;
use std::sync::Arc;

use log::{debug, warn};
use spacetimedb_fs_utils::compression::{compress_with_zstd, CompressReader};
use spacetimedb_paths::server::{CommitLogDir, SegmentFile as SegmentFilePath};
use tempfile::NamedTempFile;

use super::{Repo, SegmentLen, TxOffset, TxOffsetIndex, TxOffsetIndexMut};
use crate::repo::SegmentWriter;

const SEGMENT_FILE_EXT: &str = ".stdb.log";

// TODO
//
// - should use advisory locks?
//
// Experiment:
//
// - O_DIRECT | O_DSYNC
// - io_uring
//

pub type OnNewSegmentFn = dyn Fn() + Send + Sync + 'static;

/// Size on disk of a [Fs] repo.
///
/// Created by [Fs::size_on_disk].
#[derive(Clone, Copy, Default)]
pub struct SizeOnDisk {
    /// The total size in bytes of all segments and offset indexes in the repo.
    pub total_bytes: u64,
    /// The total number of 512-bytes blocks allocated by all segments and
    /// offset indexes in the repo.
    ///
    /// Only available on unix platforms.
    ///
    /// For other platforms, the number computed from the number of 4096-bytes
    /// pages that would be needed to store `total_bytes`. This may or may not
    /// reflect that actual storage allocation.
    ///
    /// The number of allocated blocks is typically larger than the number of
    /// actually written bytes.
    ///
    /// When the `fallocate` feature is enabled, the number can diverge
    /// substantially. Use `total_blocks` in this case to monitor disk space.
    pub total_blocks: u64,
}

impl SizeOnDisk {
    #[cfg(unix)]
    fn add(&mut self, stat: std::fs::Metadata) {
        self.total_bytes += stat.len();
        self.total_blocks += std::os::unix::fs::MetadataExt::blocks(&stat);
    }

    #[cfg(not(unix))]
    fn add(&mut self, _stat: std::fs::Metadata) {
        let imaginary_blocks = if self.total_bytes > 0 {
            8 * self.total_bytes.div_ceil(4096)
        } else {
            0
        };
        self.total_blocks = imaginary_blocks;
    }
}

/// A commitlog repository [`Repo`] which stores commits in ordinary files on
/// disk.
#[derive(Clone)]
pub struct Fs {
    /// The base directory within which segment files will be stored.
    root: CommitLogDir,

    /// Channel through which to send a message whenever we create a new segment.
    ///
    /// The other end of this channel will be a `SnapshotWorker`,
    /// which will capture a snapshot each time we rotate segments.
    on_new_segment: Option<Arc<OnNewSegmentFn>>,
}

impl std::fmt::Debug for Fs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fs").field("root", &self.root).finish_non_exhaustive()
    }
}

impl Fs {
    /// Create a commitlog repository which stores segments in the directory `root`.
    ///
    /// `root` must name an extant, accessible, writeable directory.
    pub fn new(root: CommitLogDir, on_new_segment: Option<Arc<OnNewSegmentFn>>) -> io::Result<Self> {
        root.create()?;
        Ok(Self { root, on_new_segment })
    }

    /// Get the filename for a segment starting with `offset` within this
    /// repository.
    pub fn segment_path(&self, offset: u64) -> SegmentFilePath {
        self.root.segment(offset)
    }

    /// Determine the size on disk as the sum of the sizes of all segments, as
    /// well as offset indexes.
    ///
    /// Note that the actively written-to segment (if any) is included.
    pub fn size_on_disk(&self) -> io::Result<SizeOnDisk> {
        let mut size = SizeOnDisk::default();

        for offset in self.existing_offsets()? {
            let segment = self.segment_path(offset);
            let stat = segment.metadata()?;
            size.add(stat);

            // Add the size of the offset index file if present.
            let index = self.root.index(offset);
            let Some(stat) = index.metadata().map(Some).or_else(|e| match e.kind() {
                io::ErrorKind::NotFound => Ok(None),
                _ => Err(e),
            })?
            else {
                continue;
            };
            size.add(stat);
        }

        Ok(size)
    }
}

impl fmt::Display for Fs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.root.display())
    }
}

impl SegmentLen for File {}

impl Repo for Fs {
    type SegmentWriter = PosixFile;
    type SegmentReader = CompressReader;

    fn create_segment(&self, offset: u64) -> io::Result<Self::SegmentWriter> {
        PosixFile::create(self.segment_path(offset))
            .or_else(|e| {
                if e.kind() == io::ErrorKind::AlreadyExists {
                    debug!("segment {offset} already exists");
                    // If the segment is completely empty, we can resume writing.
                    let mut file = self.open_segment_writer(offset)?;
                    if file.segment_len()? == 0 {
                        debug!("segment {offset} is empty");
                        return Ok(file);
                    }

                    // Otherwise, provide some context.
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!("repo {}: segment {} already exists and is non-empty", self, offset),
                    ));
                }

                Err(e)
            })
            .inspect(|_| {
                // We're rotating commitlog segments, so we should also take a snapshot at the earliest opportunity.
                if let Some(on_new_segment) = self.on_new_segment.as_ref() {
                    // No need to handle the error here: if the snapshot worker is closed we'll eventually close too,
                    // and we don't want to die prematurely if there are still TXes to write.
                    on_new_segment();
                }
            })
    }

    fn open_segment_writer(&self, offset: u64) -> io::Result<Self::SegmentWriter> {
        PosixFile::open(self.segment_path(offset))
    }

    fn open_segment_reader(&self, offset: u64) -> io::Result<Self::SegmentReader> {
        let file = File::open(self.segment_path(offset))?;
        CompressReader::new(file)
    }

    fn remove_segment(&self, offset: u64) -> io::Result<()> {
        let _ = self.remove_offset_index(offset).map_err(|e| {
            warn!(
                "repo {}: failed to remove offset index for segment {}: {}",
                self, offset, e
            );
        });
        fs::remove_file(self.segment_path(offset))
    }

    fn compress_segment(&self, offset: u64) -> io::Result<()> {
        let src = self.open_segment_reader(offset)?;
        // if it's already compressed, leave it be
        let CompressReader::None(mut src) = src else {
            return Ok(());
        };

        let mut dst = NamedTempFile::new_in(&self.root)?;
        // bytes per frame. in the future, it might be worth looking into putting
        // every commit into its own frame, to make seeking more efficient.
        let max_frame_size = 0x1000;
        compress_with_zstd(&mut src, &mut dst, Some(max_frame_size))?;
        dst.persist(self.segment_path(offset))?;

        Ok(())
    }

    fn existing_offsets(&self) -> io::Result<Vec<u64>> {
        let mut segments = Vec::new();

        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                let path = entry.path();
                let name = path.file_name().unwrap_or_default().to_string_lossy();
                let Some(file_name) = name.strip_suffix(SEGMENT_FILE_EXT) else {
                    continue;
                };
                let Ok(offset) = file_name.parse::<u64>() else {
                    continue;
                };

                segments.push(offset);
            }
        }

        segments.sort_unstable();

        Ok(segments)
    }

    fn create_offset_index(&self, offset: TxOffset, cap: u64) -> io::Result<TxOffsetIndexMut> {
        TxOffsetIndexMut::create_index_file(&self.root.index(offset), cap)
    }

    fn remove_offset_index(&self, offset: TxOffset) -> io::Result<()> {
        TxOffsetIndexMut::delete_index_file(&self.root.index(offset))
    }

    fn get_offset_index(&self, offset: TxOffset) -> io::Result<TxOffsetIndex> {
        TxOffsetIndex::open_index_file(&self.root.index(offset))
    }
}

impl SegmentLen for CompressReader {}

#[cfg(feature = "streaming")]
impl crate::stream::AsyncRepo for Fs {
    type AsyncSegmentWriter = tokio::io::BufWriter<tokio::fs::File>;
    type AsyncSegmentReader = spacetimedb_fs_utils::compression::AsyncCompressReader<tokio::fs::File>;

    async fn open_segment_reader_async(&self, offset: u64) -> io::Result<Self::AsyncSegmentReader> {
        let file = tokio::fs::File::open(self.segment_path(offset)).await?;
        spacetimedb_fs_utils::compression::AsyncCompressReader::new(file).await
    }
}

#[cfg(feature = "streaming")]
impl<T> crate::stream::AsyncLen for spacetimedb_fs_utils::compression::AsyncCompressReader<T> where
    T: tokio::io::AsyncSeek + tokio::io::AsyncRead + Unpin + Send
{
}

pub struct PosixFile {
    inner: File,
}

impl PosixFile {
    pub fn open(path: SegmentFilePath) -> io::Result<Self> {
        File::options()
            .read(true)
            .write(true)
            .open(path)
            .map(|inner| Self { inner })
    }

    pub fn create(path: SegmentFilePath) -> io::Result<Self> {
        File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map(|inner| Self { inner })
    }
}

impl SegmentWriter for PosixFile {
    fn fdatasync(&self) -> io::Result<()> {
        self.inner.sync_data()
    }

    fn ftruncate(&self, size: u64) -> io::Result<()> {
        self.inner.set_len(size)
    }

    #[cfg(all(feature = "fallocate", target_os = "linux"))]
    fn fallocate(&self, size: u64) -> io::Result<()> {
        use nix::fcntl::{fallocate, FallocateFlags};

        fallocate(&self.inner, FallocateFlags::FALLOC_FL_KEEP_SIZE, 0, size as _)?;
        Ok(())
    }

    // Fail compilation if `fallocate` is enabled but not supported.
    #[cfg(all(feature = "fallocate", not(target_os = "linux"), not(any(test, feature = "test"))))]
    compile_error!("`fallocate(2)` is not available on this platform");

    // No-op if `fallocate` is enabled, unsupported, but this is a test build.
    //
    // If it's a test build, we may want to run `fallocate` semantics against
    // an in-memory backend (on any platform). Hence, we need the method to be
    // present.
    #[cfg(all(feature = "fallocate", not(target_os = "linux"), any(test, feature = "test")))]
    fn fallocate(&mut self, _: u64) -> io::Result<()> {
        Ok(())
    }

    #[cfg(unix)]
    fn pwrite(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        std::os::unix::fs::FileExt::write_at(&self.inner, buf, offset)
    }

    #[cfg(windows)]
    fn pwrite(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        std::os::windows::fs::FileExt::seek_write(&self.inner, buf, offset)
    }

    #[cfg(unix)]
    fn pread(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        std::os::unix::fs::FileExt::read_at(&self.inner, buf, offset)
    }

    #[cfg(windows)]
    fn pread(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        std::os::windows::fs::FileExt::seek_read(&self.inner, buf, offset)
    }
}

impl io::Read for PosixFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl io::Seek for PosixFile {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl SegmentLen for PosixFile {}

#[cfg(feature = "streaming")]
impl crate::stream::IntoAsyncWriter for PosixFile {
    type AsyncWriter = <File as crate::stream::IntoAsyncWriter>::AsyncWriter;

    fn into_async_writer(self) -> Self::AsyncWriter {
        self.inner.into_async_writer()
    }
}
