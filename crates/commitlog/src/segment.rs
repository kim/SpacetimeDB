use std::{
    io::{self, ErrorKind, SeekFrom},
    num::NonZeroU64,
    ops::Range,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, RwLock,
    },
};

use bytes::BytesMut;
use log::{debug, warn};

use crate::{
    commit::{self, Commit, StoredCommit},
    error,
    index::IndexError,
    payload::Encode,
    repo::{SegmentWriter, TxOffset, TxOffsetIndex, TxOffsetIndexMut},
    Options,
};

pub const MAGIC: [u8; 6] = [b'(', b'd', b's', b')', b'^', b'2'];

pub const DEFAULT_LOG_FORMAT_VERSION: u8 = 1;
pub const DEFAULT_CHECKSUM_ALGORITHM: u8 = CHECKSUM_ALGORITHM_CRC32C;

pub const CHECKSUM_ALGORITHM_CRC32C: u8 = 0;
pub const CHECKSUM_CRC32C_LEN: usize = 4;

/// Lookup table for checksum length, index is [`Header::checksum_algorithm`].
// Supported algorithms must be numbered consecutively!
pub const CHECKSUM_LEN: [usize; 1] = [CHECKSUM_CRC32C_LEN];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Header {
    pub log_format_version: u8,
    pub checksum_algorithm: u8,
}

impl Header {
    pub const LEN: usize = MAGIC.len() + /* log_format_version + checksum_algorithm + reserved + reserved */ 4;

    pub fn write<W: io::Write>(&self, mut out: W) -> io::Result<()> {
        out.write_all(&MAGIC)?;
        out.write_all(&[self.log_format_version, self.checksum_algorithm, 0, 0])?;

        Ok(())
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&[self.log_format_version, self.checksum_algorithm, 0, 0]);
    }

    pub fn decode<R: io::Read>(mut read: R) -> io::Result<Self> {
        let mut buf = [0; Self::LEN];
        read.read_exact(&mut buf)?;

        if !buf.starts_with(&MAGIC) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "segment header does not start with magic",
            ));
        }

        Ok(Self {
            log_format_version: buf[MAGIC.len()],
            checksum_algorithm: buf[MAGIC.len() + 1],
        })
    }

    pub fn ensure_compatible(&self, max_log_format_version: u8, checksum_algorithm: u8) -> Result<(), String> {
        if self.log_format_version > max_log_format_version {
            return Err(format!("unsupported log format version: {}", self.log_format_version));
        }
        if self.checksum_algorithm != checksum_algorithm {
            return Err(format!("unsupported checksum algorithm: {}", self.checksum_algorithm));
        }

        Ok(())
    }
}

impl Default for Header {
    fn default() -> Self {
        Self {
            log_format_version: DEFAULT_LOG_FORMAT_VERSION,
            checksum_algorithm: DEFAULT_CHECKSUM_ALGORITHM,
        }
    }
}

/// Metadata about a [`Commit`] which was successfully written via [`Writer::commit`].
#[derive(Debug, PartialEq)]
pub struct Committed {
    /// The range of transaction offsets included in the commit.
    pub tx_range: Range<u64>,
    /// The crc32 checksum of the commit's serialized form,
    /// as written to the commitlog.
    pub checksum: u32,
}

pub struct WriterParams {
    pub epoch: u64,
    pub min_tx_offset: u64,
    pub next_tx_offset: u64,
    pub bytes_written: u64,
    pub flush_buffer: u64,
}

impl Default for WriterParams {
    fn default() -> Self {
        Self {
            epoch: 0,
            min_tx_offset: 0,
            next_tx_offset: 0,
            bytes_written: 0,
            flush_buffer: 4096,
        }
    }
}

#[derive(Debug)]
pub struct Writer<W: SegmentWriter> {
    commit: RwLock<Commit>,
    inner: W,
    buf: Mutex<BytesMut>,

    min_tx_offset: u64,
    bytes_written: AtomicU64,
    flush_buffer: u64,

    offset_index_head: Option<Mutex<OffsetIndexWriter>>,
}

impl<W: SegmentWriter> Writer<W> {
    pub fn new(
        inner: W,
        offset_index_head: Option<OffsetIndexWriter>,
        WriterParams {
            epoch,
            min_tx_offset,
            next_tx_offset,
            bytes_written,
            flush_buffer,
        }: WriterParams,
    ) -> Self {
        let commit = RwLock::new(Commit {
            min_tx_offset: next_tx_offset,
            n: 0,
            records: Vec::new(),
            epoch,
        });
        Self {
            commit,
            inner,
            buf: Mutex::new(BytesMut::new()),
            min_tx_offset,
            bytes_written: AtomicU64::new(bytes_written),
            flush_buffer,
            offset_index_head: offset_index_head.map(Mutex::new),
        }
    }

    pub fn commit<T: Into<Transaction<U>>, U: Encode>(
        &self,
        transactions: impl IntoIterator<Item = T>,
    ) -> io::Result<Option<Committed>> {
        let (committed_range, checksum, commit_len, should_flush) = {
            let mut commit = self.commit.write().unwrap();
            commit.records.clear();

            for tx in transactions {
                let tx = tx.into();
                let expected_offset = commit.min_tx_offset + commit.n as u64;
                if tx.offset != expected_offset {
                    commit.n = 0;
                    commit.records.clear();

                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("invalid transaction offset {}, expected {}", tx.offset, expected_offset),
                    ));
                }
                assert!(
                    commit.n < u16::MAX,
                    "maximum number of transactions in a single commit exceeded"
                );
                commit.n += 1;
                tx.txdata.encode_record(&mut commit.records);
            }

            if commit.n == 0 {
                return Ok(None);
            }

            let mut buf = self.buf.lock().unwrap();
            let checksum = commit.encode(&mut buf);

            let committed_range = commit.tx_range();
            let commit_len = commit.encoded_len() as u64;
            let should_flush = buf.len() >= self.flush_buffer as usize;

            commit.min_tx_offset += commit.n as u64;
            commit.n = 0;

            debug!(
                "committed={committed_range:?} buf-len={} flush={should_flush}",
                buf.len()
            );

            (committed_range, checksum, commit_len, should_flush)
        };

        if should_flush {
            let bytes_written_before_flush = self.bytes_written.load(Ordering::SeqCst);
            self.flush().expect("failed to flush segment");
            if let Some(index) = &self.offset_index_head {
                debug!(
                    "append_after_commit min_tx_offset={} bytes_written={} commit_len={}",
                    committed_range.start, bytes_written_before_flush, commit_len
                );
                let _ = index
                    .lock()
                    .unwrap()
                    .append_after_commit(committed_range.start, bytes_written_before_flush, commit_len)
                    .inspect_err(|e| debug!("failed to append to offset index: {e}"));
            }
        }

        Ok(Some(Committed {
            tx_range: committed_range,
            checksum,
        }))
    }

    pub fn flush(&self) -> io::Result<()> {
        let buf = {
            let mut buf = self.buf.lock().unwrap();
            buf.split()
        };

        if buf.is_empty() {
            return Ok(());
        }

        self.inner.pwrite_all(&buf, self.bytes_written.load(Ordering::SeqCst))?;
        self.bytes_written.fetch_add(buf.len() as u64, Ordering::SeqCst);

        Ok(())
    }

    pub fn sync_data(&self) -> io::Result<()> {
        self.inner.fdatasync()
    }

    /// Get the current epoch.
    pub fn epoch(&self) -> u64 {
        self.commit.read().unwrap().epoch
    }

    /// Update the epoch.
    ///
    /// The caller must ensure that:
    ///
    /// - The new epoch is greater than the current epoch.
    /// - [`Self::commit`] has been called as appropriate.
    ///
    pub fn set_epoch(&self, epoch: u64) {
        self.commit.write().unwrap().epoch = epoch;
    }

    /// The smallest transaction offset in this segment.
    pub fn min_tx_offset(&self) -> u64 {
        self.min_tx_offset
    }

    /// The next transaction offset to be written if [`Self::commit`] was called.
    pub fn next_tx_offset(&self) -> u64 {
        self.commit.read().unwrap().min_tx_offset
    }

    /// `true` if the segment contains no commits.
    ///
    /// The segment will, however, contain a header. This thus violates the
    /// convention that `is_empty == (len == 0)`.
    pub fn is_empty(&self) -> bool {
        self.bytes_written.load(Ordering::SeqCst) <= Header::LEN as u64
    }

    /// Number of bytes written to this segment, including the header.
    pub fn len(&self) -> u64 {
        self.bytes_written.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn copy_commit(&self) -> Commit {
        self.commit.read().unwrap().clone()
    }

    #[cfg(test)]
    pub(crate) fn modify_commit(&self, f: impl FnOnce(&mut Commit)) {
        let mut commit = self.commit.write().unwrap();
        f(&mut commit)
    }
}

#[derive(Debug)]
pub struct OffsetIndexWriter {
    pub(crate) head: TxOffsetIndexMut,

    require_segment_fsync: bool,
    min_write_interval: NonZeroU64,

    pub(crate) candidate_min_tx_offset: TxOffset,
    pub(crate) candidate_byte_offset: u64,
    pub(crate) bytes_since_last_index: u64,
}

impl OffsetIndexWriter {
    pub fn new(head: TxOffsetIndexMut, opts: Options) -> Self {
        OffsetIndexWriter {
            head,
            require_segment_fsync: opts.offset_index_require_segment_fsync,
            min_write_interval: opts.offset_index_interval_bytes,
            candidate_min_tx_offset: TxOffset::default(),
            candidate_byte_offset: 0,
            bytes_since_last_index: 0,
        }
    }

    fn reset(&mut self) {
        self.candidate_byte_offset = 0;
        self.candidate_min_tx_offset = TxOffset::default();
        self.bytes_since_last_index = 0;
    }

    /// Either append to index or save offsets to append at future fsync
    pub fn append_after_commit(
        &mut self,
        min_tx_offset: TxOffset,
        byte_offset: u64,
        commit_len: u64,
    ) -> Result<(), IndexError> {
        self.bytes_since_last_index += commit_len;

        if self.candidate_min_tx_offset == 0 {
            self.candidate_byte_offset = byte_offset;
            self.candidate_min_tx_offset = min_tx_offset;
        }

        if !self.require_segment_fsync {
            self.append_internal()?;
        }

        Ok(())
    }

    fn append_internal(&mut self) -> Result<(), IndexError> {
        // If the candidate offset is zero, there has not been a commit since the last offset entry
        if self.candidate_min_tx_offset == 0 {
            return Ok(());
        }

        if self.bytes_since_last_index < self.min_write_interval.get() {
            return Ok(());
        }

        log::info!(
            "append {}->{} to index",
            self.candidate_min_tx_offset,
            self.candidate_byte_offset
        );
        self.head
            .append(self.candidate_min_tx_offset, self.candidate_byte_offset)?;
        self.head.async_flush()?;
        self.reset();

        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        let _ = self
            .append_internal()
            .inspect_err(|e| warn!("failed to append to offset index: {e:#}"));
        self.head
            .async_flush()
            .inspect_err(|e| warn!("failed to flush offset index: {e:#}"))
    }

    pub fn truncate(&mut self, offset: TxOffset) {
        self.reset();
        let _ = self
            .head
            .truncate(offset)
            .inspect_err(|e| warn!("failed to truncate offset index at {offset}: {e:#}"));
    }
}

#[derive(Debug)]
pub struct Reader<R> {
    pub header: Header,
    pub min_tx_offset: u64,
    inner: R,
}

impl<R: io::Read + io::Seek> Reader<R> {
    pub fn new(max_log_format_version: u8, min_tx_offset: u64, mut inner: R) -> io::Result<Self> {
        let header = Header::decode(&mut inner)?;
        header
            .ensure_compatible(max_log_format_version, Commit::CHECKSUM_ALGORITHM)
            .map_err(|msg| io::Error::new(io::ErrorKind::InvalidData, msg))?;

        Ok(Self {
            header,
            min_tx_offset,
            inner,
        })
    }
}

impl<R: io::BufRead + io::Seek> Reader<R> {
    pub fn commits(self) -> Commits<R> {
        Commits {
            header: self.header,
            reader: self.inner,
        }
    }

    pub fn seek_to_offset(&mut self, index_file: &TxOffsetIndex, start_tx_offset: u64) -> Result<u64, IndexError> {
        seek_to_offset(&mut self.inner, index_file, start_tx_offset)
    }

    #[cfg(test)]
    pub fn transactions<'a, D>(self, de: &'a D) -> impl Iterator<Item = Result<Transaction<D::Record>, D::Error>> + 'a
    where
        D: crate::Decoder,
        D::Error: From<io::Error>,
        R: 'a,
    {
        use itertools::Itertools as _;

        self.commits()
            .with_log_format_version()
            .map(|x| x.map_err(Into::into))
            .map_ok(move |(version, commit)| {
                let start = commit.min_tx_offset;
                commit.into_transactions(version, start, de)
            })
            .flatten_ok()
            .map(|x| x.and_then(|y| y))
    }

    #[cfg(test)]
    pub(crate) fn metadata(self) -> Result<Metadata, error::SegmentMetadata> {
        Metadata::with_header(self.min_tx_offset, self.header, self.inner, None)
    }
}

/// Advances the `segment` reader to the position corresponding to the `start_tx_offset`
/// using the `index_file` for efficient seeking.
///
/// Input:
/// - `segment` - segment reader
/// - `min_tx_offset` - minimum transaction offset in the segment
/// - `start_tx_offset` - transaction offset to advance to
///
/// Returns the byte position `segment` is at after seeking.
pub fn seek_to_offset<R: io::Read + io::Seek>(
    mut segment: &mut R,
    index_file: &TxOffsetIndex,
    start_tx_offset: u64,
) -> Result<u64, IndexError> {
    let (index_key, byte_offset) = index_file.key_lookup(start_tx_offset)?;

    // If the index_key is 0, it means the index file is empty, return error without seeking
    if index_key == 0 {
        return Err(IndexError::KeyNotFound);
    }
    debug!("index lookup for key={start_tx_offset}: found key={index_key} at byte-offset={byte_offset}");
    // returned `index_key` should never be greater than `start_tx_offset`
    debug_assert!(index_key <= start_tx_offset);

    // Check if the offset index is pointing to the right commit.
    let hdr = validate_commit_header(&mut segment, byte_offset)?;
    if hdr.min_tx_offset == index_key {
        // Advance the segment Seek if expected commit is found.
        segment.seek(SeekFrom::Start(byte_offset))
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "mismatched key in offset index file",
        ))
    }
    .map_err(Into::into)
}

/// Try to extract the commit header from the asked position without advancing seek.
/// `IndexFileMut` fsync asynchoronously, which makes it important for reader to verify its entry
pub fn validate_commit_header<Reader: io::Read + io::Seek>(
    mut reader: &mut Reader,
    byte_offset: u64,
) -> io::Result<commit::Header> {
    let pos = reader.stream_position()?;
    reader.seek(SeekFrom::Start(byte_offset))?;

    let hdr = commit::Header::decode(&mut reader)
        .and_then(|hdr| hdr.ok_or_else(|| io::Error::new(ErrorKind::UnexpectedEof, "unexpected EOF")));

    // Restore the original position
    reader.seek(SeekFrom::Start(pos))?;

    hdr
}

/// Pair of transaction offset and payload.
///
/// Created by iterators which "flatten" commits into individual transaction
/// records.
#[derive(Debug, PartialEq)]
pub struct Transaction<T> {
    /// The offset of this transaction relative to the start of the log.
    pub offset: u64,
    /// The transaction payload.
    pub txdata: T,
}

impl<T> From<(u64, T)> for Transaction<T> {
    fn from((offset, txdata): (u64, T)) -> Self {
        Self { offset, txdata }
    }
}

pub struct Commits<R> {
    pub header: Header,
    reader: R,
}

impl<R: io::BufRead> Iterator for Commits<R> {
    type Item = io::Result<StoredCommit>;

    fn next(&mut self) -> Option<Self::Item> {
        StoredCommit::decode_internal(&mut self.reader, self.header.log_format_version).transpose()
    }
}

#[cfg(test)]
impl<R: io::BufRead> Commits<R> {
    pub fn with_log_format_version(self) -> impl Iterator<Item = io::Result<(u8, StoredCommit)>> {
        CommitsWithVersion { inner: self }
    }
}

#[cfg(test)]
struct CommitsWithVersion<R> {
    inner: Commits<R>,
}

#[cfg(test)]
impl<R: io::BufRead> Iterator for CommitsWithVersion<R> {
    type Item = io::Result<(u8, StoredCommit)>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.inner.next()?;
        match next {
            Ok(commit) => {
                let version = self.inner.header.log_format_version;
                Some(Ok((version, commit)))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Metadata {
    /// The segment header.
    pub header: Header,
    /// The range of transactions contained in the segment.
    pub tx_range: Range<u64>,
    /// The size of the segment.
    pub size_in_bytes: u64,
    /// The largest epoch found in the segment.
    pub max_epoch: u64,
    /// The latest commit found in the segment.
    ///
    /// The value is the `min_tx_offset` of the commit, i.e.
    /// `max_commit_offset..tx_range.end` is the range of
    /// transactions contained in it.
    pub max_commit_offset: u64,
    pub max_commit: Option<commit::Metadata>,
}

impl Metadata {
    /// Reads and validates metadata from a segment.
    /// It will look for last commit index offset and then traverse the segment
    ///
    /// Determines `max_tx_offset`, `size_in_bytes`, and `max_epoch` from the segment.
    pub(crate) fn extract<R: io::Read + io::Seek>(
        min_tx_offset: TxOffset,
        mut reader: R,
        offset_index: Option<&TxOffsetIndex>,
    ) -> Result<Self, error::SegmentMetadata> {
        let header = Header::decode(&mut reader)?;
        Self::with_header(min_tx_offset, header, reader, offset_index)
    }

    fn with_header<R: io::Read + io::Seek>(
        min_tx_offset: u64,
        header: Header,
        mut reader: R,
        offset_index: Option<&TxOffsetIndex>,
    ) -> Result<Self, error::SegmentMetadata> {
        let mut sofar = offset_index
            .and_then(|index| Self::find_valid_indexed_commit(min_tx_offset, header, &mut reader, index).ok())
            .unwrap_or_else(|| Self {
                header,
                tx_range: Range {
                    start: min_tx_offset,
                    end: min_tx_offset,
                },
                size_in_bytes: Header::LEN as u64,
                max_epoch: u64::default(),
                max_commit_offset: min_tx_offset,
                max_commit: None,
            });

        reader.seek(SeekFrom::Start(sofar.size_in_bytes))?;

        fn commit_meta<R: io::Read>(
            reader: &mut R,
            sofar: &Metadata,
        ) -> Result<Option<commit::Metadata>, error::SegmentMetadata> {
            commit::Metadata::extract(reader).map_err(|e| {
                if matches!(e.kind(), io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof) {
                    error::SegmentMetadata::InvalidCommit {
                        sofar: sofar.clone(),
                        source: e,
                    }
                } else {
                    e.into()
                }
            })
        }
        while let Some(commit) = commit_meta(&mut reader, &sofar)? {
            debug!("commit::{commit:?}");
            if commit.tx_range.start != sofar.tx_range.end {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "out-of-order offset: expected={} actual={}",
                        sofar.tx_range.end, commit.tx_range.start,
                    ),
                )
                .into());
            }
            sofar.tx_range.end = commit.tx_range.end;
            sofar.size_in_bytes += commit.size_in_bytes;
            // TODO: Should it be an error to encounter an epoch going backwards?
            sofar.max_epoch = commit.epoch.max(sofar.max_epoch);
            sofar.max_commit_offset = commit.tx_range.start;
            sofar.max_commit = Some(commit);
        }

        Ok(sofar)
    }

    /// Finds the last valid commit in the segment using the offset index.
    /// It traverses the index in reverse order, starting from the last key.
    ///
    /// Returns
    /// * `Ok((Metadata)` - If a valid commit is found containing the commit, It adds a default
    ///   header, which should be replaced with the actual header.
    /// * `Err` - If no valid commit is found or if the index is empty
    fn find_valid_indexed_commit<R: io::Read + io::Seek>(
        min_tx_offset: u64,
        header: Header,
        reader: &mut R,
        offset_index: &TxOffsetIndex,
    ) -> io::Result<Metadata> {
        let mut candidate_last_key = TxOffset::MAX;

        while let Ok((key, byte_offset)) = offset_index.key_lookup(candidate_last_key) {
            match Self::validate_commit_at_offset(reader, key, byte_offset) {
                Ok(commit) => {
                    return Ok(Metadata {
                        header,
                        tx_range: Range {
                            start: min_tx_offset,
                            end: commit.tx_range.end,
                        },
                        size_in_bytes: byte_offset + commit.size_in_bytes,
                        max_epoch: commit.epoch,
                        max_commit_offset: commit.tx_range.start,
                        max_commit: Some(commit),
                    });
                }

                // `TxOffset` at `byte_offset` is not valid, so try with previous entry
                Err(_) => {
                    candidate_last_key = key.saturating_sub(1);
                    if candidate_last_key == 0 {
                        break;
                    }
                }
            }
        }

        Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("No valid commit found in index up to key: {candidate_last_key}"),
        ))
    }

    /// Validates and decodes a commit at `byte_offset` in the segment.
    ///
    /// # Returns
    /// * `Ok(commit::Metadata)` - If a valid commit is found with matching transaction offset
    /// * `Err` - If commit can't be decoded or has mismatched transaction offset
    fn validate_commit_at_offset<R: io::Read + io::Seek>(
        reader: &mut R,
        tx_offset: TxOffset,
        byte_offset: u64,
    ) -> io::Result<commit::Metadata> {
        reader.seek(SeekFrom::Start(byte_offset))?;
        let commit = commit::Metadata::extract(reader)?
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "failed to decode commit"))?;

        if commit.tx_range.start != tx_offset {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "mismatch key in index offset file: expected={} actual={}",
                    tx_offset, commit.tx_range.start
                ),
            ));
        }

        Ok(commit)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use pretty_assertions::assert_matches;
    use spacetimedb_paths::server::CommitLogDir;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        payload::ArrayDecoder,
        repo::{self, mem::PAGE_SIZE},
        Options,
    };

    #[test]
    fn header_roundtrip() {
        let hdr = Header {
            log_format_version: 42,
            checksum_algorithm: 7,
        };

        let mut buf = [0u8; Header::LEN];
        hdr.write(&mut &mut buf[..]).unwrap();
        let h2 = Header::decode(&buf[..]).unwrap();

        assert_eq!(hdr, h2);
    }

    #[test]
    fn write_read_roundtrip() {
        let repo = repo::Memory::unlimited();

        let writer = repo::create_segment_writer(&repo, <_>::default(), Commit::DEFAULT_EPOCH, 0).unwrap();
        writer.commit([(0, [0; 32]), (1, [1; 32]), (2, [2; 32])]).unwrap();
        writer.flush().unwrap();

        let reader = repo::open_segment_reader(&repo, DEFAULT_LOG_FORMAT_VERSION, 0).unwrap();
        let header = reader.header;
        let commit = reader
            .commits()
            .next()
            .expect("expected one commit")
            .expect("unexpected IO");

        assert_eq!(
            header,
            Header {
                log_format_version: DEFAULT_LOG_FORMAT_VERSION,
                checksum_algorithm: DEFAULT_CHECKSUM_ALGORITHM
            }
        );
        assert_eq!(commit.min_tx_offset, 0);
        assert_eq!(commit.records, [[0; 32], [1; 32], [2; 32]].concat());
    }

    #[test]
    fn metadata() {
        let repo = repo::Memory::unlimited();

        let writer = repo::create_segment_writer(&repo, <_>::default(), Commit::DEFAULT_EPOCH, 0).unwrap();
        // Commit 0..2
        writer.commit([(0, [0; 32]), (1, [0; 32])]).unwrap();
        // Commit 2..3
        writer.commit([(2, [1; 32])]).unwrap();
        // Commit 3..5
        writer.commit([(3, [2; 32]), (4, [2; 32])]).unwrap();

        writer.flush().unwrap();

        let reader = repo::open_segment_reader(&repo, DEFAULT_LOG_FORMAT_VERSION, 0).unwrap();
        let Metadata {
            header,
            tx_range,
            size_in_bytes,
            max_epoch,
            max_commit_offset,
            max_commit,
        } = reader.metadata().unwrap();

        assert_eq!(
            (
                header,
                tx_range,
                size_in_bytes,
                max_epoch,
                max_commit_offset,
                max_commit.is_some_and(|meta| meta.tx_range == (3..5))
            ),
            (
                Header::default(),
                0..5,
                // header + 5 txs + 3 commits
                (Header::LEN + (5 * 32) + (3 * Commit::FRAMING_LEN)) as u64,
                Commit::DEFAULT_EPOCH,
                3,
                true
            )
        );
    }

    #[test]
    fn commits() {
        let repo = repo::Memory::unlimited();
        let commits = vec![
            vec![(0, [1; 32]), (1, [2; 32])],
            vec![(2, [3; 32])],
            vec![(3, [4; 32]), (4, [5; 32])],
        ];

        let writer = repo::create_segment_writer(&repo, <_>::default(), Commit::DEFAULT_EPOCH, 0).unwrap();

        for commit in &commits {
            writer.commit(commit.clone()).unwrap();
        }

        writer.flush().unwrap();

        let reader = repo::open_segment_reader(&repo, DEFAULT_LOG_FORMAT_VERSION, 0).unwrap();
        let mut commits1 = Vec::with_capacity(commits.len());
        let mut min_tx_offset = 0;
        for txs in commits {
            let n = txs.len();
            commits1.push(Commit {
                min_tx_offset,
                n: n as u16,
                records: itertools::concat(txs.into_iter().map(|(_, payload)| payload.to_vec())),
                epoch: 0,
            });
            min_tx_offset += n as u64;
        }
        let commits2 = reader
            .commits()
            .map_ok(Into::into)
            .collect::<Result<Vec<Commit>, _>>()
            .unwrap();
        assert_eq!(commits1, commits2);
    }

    #[test]
    fn transactions() {
        let repo = repo::Memory::unlimited();
        let commits = vec![
            vec![(0, [1; 32]), (1, [2; 32])],
            vec![(2, [3; 32])],
            vec![(3, [4; 32]), (4, [5; 32])],
        ];

        let writer = repo::create_segment_writer(&repo, <_>::default(), Commit::DEFAULT_EPOCH, 0).unwrap();
        for commit in &commits {
            writer.commit(commit.clone()).unwrap();
        }

        writer.flush().unwrap();

        let reader = repo::open_segment_reader(&repo, DEFAULT_LOG_FORMAT_VERSION, 0).unwrap();
        let txs = reader
            .transactions(&ArrayDecoder)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(txs, commits.into_iter().flatten().map(Into::into).collect::<Vec<_>>());
    }

    #[test]
    fn next_tx_offset() {
        let writer = Writer::new(repo::mem::Segment::new(PAGE_SIZE as u64), None, WriterParams::default());

        assert_eq!(0, writer.next_tx_offset());
        writer.commit([(0, [0; 16])]).unwrap();
        assert_eq!(1, writer.next_tx_offset());
        writer.commit([(1, [1; 16])]).unwrap();
        writer.commit([(2, [1; 16])]).unwrap();
        assert_eq!(3, writer.next_tx_offset());
    }

    #[test]
    fn offset_index_writer_truncates_to_offset() {
        use spacetimedb_paths::FromPathUnchecked as _;

        let tmp = tempdir().unwrap();
        let commitlog_dir = CommitLogDir::from_path_unchecked(tmp.path());
        let index_path = commitlog_dir.index(0);
        let mut writer = OffsetIndexWriter::new(
            TxOffsetIndexMut::create_index_file(&index_path, 100).unwrap(),
            Options {
                // Ensure we're writing every index entry.
                offset_index_interval_bytes: 127.try_into().unwrap(),
                offset_index_require_segment_fsync: false,
                ..Default::default()
            },
        );

        for i in 1..=10 {
            writer.append_after_commit(i, i * 128, 128).unwrap();
        }
        // Ensure all entries have been written.
        for i in 1..=10 {
            assert_eq!(writer.head.key_lookup(i).unwrap(), (i, i * 128));
        }

        // Truncating to any offset in the written range or larger
        // retains that offset - 1, or the max offset written.
        for truncate_to in (2..=10u64).rev() {
            let retained_key = truncate_to.saturating_sub(1).min(10);
            let retained_val = retained_key * 128;
            let retained = (retained_key, retained_val);

            writer.truncate(truncate_to);
            assert_matches!(
                writer.head.key_lookup(truncate_to),
                Ok(x) if x == retained,
                "truncate to {truncate_to} should retain {retained:?}"
            );
            // Make sure this also holds after reopen.
            let index = TxOffsetIndex::open_index_file(&index_path).unwrap();
            assert_matches!(
                index.key_lookup(truncate_to),
                Ok(x) if x == retained,
                "truncate to {truncate_to} should retain {retained:?} after reopen"
            );
        }

        // Truncating to 1 leaves no entries in the index
        writer.truncate(1);
        assert_matches!(writer.head.key_lookup(1), Err(IndexError::KeyNotFound));
    }
}
