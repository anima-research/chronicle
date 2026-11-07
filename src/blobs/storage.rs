//! Blob storage implementation.

use super::chunks;
use crate::error::{Result, StoreError};
use crate::types::{Blob, Hash};
use lru::LruCache;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Magic bytes for blob files.
const BLOB_MAGIC: &[u8; 4] = b"BLB\0";

const BLOB_VERSION: u8 = 1;
const CHUNKED_VERSION: u8 = 2;
const UNSUPPORTED_VERSION: &str = "Unsupported blob version";
const CHUNK_CONTENT_TYPE: &str = "application/octet-stream";

const STAGING_PREFIX: &str = ".chronicle-blob-";
const REPLACED_BY_STORING: &str = "; storing the same bytes again replaces it";

#[cfg(test)]
thread_local! {
    /// The directories this thread's stores have synced, in order, so unit
    /// tests can see which syncs a store makes. A directory this account may
    /// not open is listed too, as the sync it went without.
    static SYNCED_DIRECTORIES: std::cell::RefCell<Vec<PathBuf>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// A step of a store's write after the staging file is synced, where a unit
/// test can make the next store fail, to see what the failure leaves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteStep {
    /// The rename of the staging file to the blob's path.
    Rename,
    /// The sync of the shard directory after that rename.
    ShardSync,
}

/// Cached blob data (content + content_type).
#[derive(Clone)]
struct CachedBlob {
    content: Vec<u8>,
    content_type: String,
}

struct ChunkList {
    content_type: String,
    len: u64,
    chunks: Vec<Hash>,
}

enum StoredFile {
    Content(CachedBlob),
    Chunks(ChunkList),
}

struct ShardDurability {
    creations_begun: AtomicU64,
    creations_finished: AtomicU64,
    creations_synced: AtomicU64,
    renames_begun: AtomicU64,
    renames_finished: AtomicU64,
    renames_synced: AtomicU64,
}

/// Content-addressed blob storage.
pub struct BlobStorage {
    /// Base directory for blobs.
    path: PathBuf,

    /// LRU cache for recently accessed blobs. It holds only blobs this
    /// storage wrote, or read from a whole file.
    cache: Mutex<LruCache<Hash, CachedBlob>>,

    known: Mutex<HashMap<Hash, u64>>,

    entry_synced: AtomicBool,
    shards: Box<[ShardDurability]>,

    /// The step of a write that a unit test set to fail, once, standing in
    /// for a device error there.
    #[cfg(test)]
    fails_at: Mutex<Option<WriteStep>>,
}

impl BlobStorage {
    /// Create a new blob storage at the given path.
    pub fn new(path: impl AsRef<Path>, cache_size: usize) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        fs::create_dir_all(&path)?;

        let cache_size = NonZeroUsize::new(cache_size.max(1)).unwrap();

        Ok(Self {
            path,
            cache: Mutex::new(LruCache::new(cache_size)),
            known: Mutex::new(HashMap::new()),
            entry_synced: AtomicBool::new(false),
            shards: (0..=u8::MAX)
                .map(|_| ShardDurability {
                    creations_begun: AtomicU64::new(0),
                    creations_finished: AtomicU64::new(0),
                    creations_synced: AtomicU64::new(0),
                    renames_begun: AtomicU64::new(0),
                    renames_finished: AtomicU64::new(0),
                    renames_synced: AtomicU64::new(0),
                })
                .collect(),
            #[cfg(test)]
            fails_at: Mutex::new(None),
        })
    }

    /// Store a blob, returning its hash.
    ///
    /// The blob is written whole under a staging name in the blob directory,
    /// synced, and renamed to its own name, so a store never leaves part of a
    /// blob under that name: one that stops before its rename leaves the blob
    /// as it was. A store killed before its rename leaves its staging file
    /// (`.chronicle-blob-*`), which the next [`Store::open`](crate::Store::open)
    /// removes where it can list the blob directory.
    ///
    /// On Unix, before this returns, the blob directory's entry in its parent
    /// has been synced once by this `BlobStorage`, and the blob's shard
    /// directory and that directory's entry in the blob directory have been
    /// synced since this `BlobStorage` last created or renamed anything into
    /// them, where the system syncs directories and this account may open
    /// them. What an earlier process made there is taken as it is, and so is
    /// what another `BlobStorage` on the same directory makes: a `Store`
    /// holds one, and its lock refuses a second `Store`, but
    /// `BlobStorage::new` takes no lock. A sync that fails before the rename
    /// returns an error with nothing stored; one that fails after it returns
    /// an error with the blob in place. Either way, storing the blob again
    /// through this `BlobStorage` tries the sync again.
    ///
    /// When the blob's file is already there and reads back as `content`, it is
    /// left as it is, content type included. That holds for a file naming the
    /// blob's chunks, as the Node binding's `storeBlobAsync` writes with
    /// `chunked: true`, whose chunks read back as `content` in order. A file
    /// that's cut short, damaged or holds other bytes is replaced, and so is a
    /// file naming chunks that are missing, damaged or other bytes. A symbolic
    /// link at the blob's name is read through: one whose target reads back as
    /// `content` is left, and one whose target is cut short, damaged, holds
    /// other bytes or doesn't exist is itself replaced by the blob, leaving any
    /// target as it was; one that loops is an error. Anything else at the
    /// blob's name or a link's target, a file that can't be opened or read at
    /// all, or a blob file in a format version newer than this chronicle
    /// reads, such as one a newer chronicle wrote, is an error and is left as
    /// it is, and so is a file naming chunks one of which is any of these. A
    /// file holding a blob's or a chunk's bytes whole, which a store of this
    /// `BlobStorage` has written, or read and checked, is taken as stored
    /// without reading it again while a regular file of the same length, or a
    /// link to one, is at its name, until a read of it fails, in a
    /// [`get`](Self::get) or in a store's check of the chunks a file names.
    /// A `get` takes nothing as stored. A file naming chunks isn't: storing
    /// the same content again reads it, and this store reads its chunks too.
    /// A content type longer than 65,535 bytes is refused.
    pub fn store(&self, content: &[u8], content_type: &str) -> Result<Hash> {
        let content_type_len = content_type_len(content_type)?;
        let hash = Hash::from_bytes(content);
        self.store_as(hash, content, content_type, content_type_len, false)?;
        Ok(hash)
    }

    fn store_as(
        &self,
        hash: Hash,
        content: &[u8],
        content_type: &str,
        content_type_len: u16,
        chunk: bool,
    ) -> Result<()> {
        let blob_path = self.blob_path(&hash);
        if self.known_at(&hash, &blob_path) {
            return self.sync_existing(&hash, &blob_path);
        }
        let readable = match Self::read_file(&blob_path) {
            Ok(Some(StoredFile::Content(stored))) if stored.content == content => {
                self.remember(hash, &stored);
                true
            }
            Ok(Some(StoredFile::Chunks(list))) if !chunk => {
                self.chunks_read_back_as(&list, &blob_path, content)?
            }
            Ok(_) => false,
            Err(error) if unreadable(&error) => false,
            Err(error) => return Err(error),
        };
        if readable {
            return self.sync_existing(&hash, &blob_path);
        }
        let content_len = (content.len() as u64).to_le_bytes();
        let checksum = crc32fast::hash(content).to_le_bytes();
        self.cache.lock().pop(&hash);
        self.write(
            &hash,
            &blob_path,
            &[
                BLOB_MAGIC,
                &[BLOB_VERSION],
                &content_type_len.to_le_bytes(),
                content_type.as_bytes(),
                &content_len,
                content,
                &checksum,
            ],
        )?;
        self.known
            .lock()
            .insert(hash, whole_file_len(content_type.len(), content.len()));
        Ok(())
    }

    #[cfg_attr(not(any(test, feature = "napi-bindings")), allow(dead_code))]
    pub(crate) fn store_chunked(&self, content: &[u8], content_type: &str) -> Result<Hash> {
        let content_type_len = content_type_len(content_type)?;
        let ends = chunks::chunk_ends(content);
        let hash = Hash::from_bytes(content);
        if ends.len() < 2 {
            self.store_as(hash, content, content_type, content_type_len, false)?;
            return Ok(hash);
        }
        let blob_path = self.blob_path(&hash);
        if self.known_at(&hash, &blob_path) {
            self.sync_existing(&hash, &blob_path)?;
            return Ok(hash);
        }
        let pieces: Vec<(Hash, &[u8])> = ends
            .iter()
            .scan(0, |start, &end| {
                let piece = &content[*start..end];
                *start = end;
                Some((Hash::from_bytes(piece), piece))
            })
            .collect();
        let chunk_count = u32::try_from(pieces.len()).map_err(|_| {
            StoreError::InvalidOperation(format!(
                "a blob of {} bytes cuts into {} chunks, more than a chunked file can name, so nothing was stored",
                content.len(),
                pieces.len()
            ))
        })?;
        let same_list = match Self::read_file(&blob_path) {
            Ok(Some(StoredFile::Content(stored))) if stored.content == content => {
                self.remember(hash, &stored);
                self.sync_existing(&hash, &blob_path)?;
                return Ok(hash);
            }
            Ok(Some(StoredFile::Chunks(list))) => {
                list.len == content.len() as u64
                    && list.chunks.iter().eq(pieces.iter().map(|(chunk, _)| chunk))
            }
            Ok(_) => false,
            Err(error) if unreadable(&error) => false,
            Err(error) => return Err(error),
        };

        let chunk_type_len = CHUNK_CONTENT_TYPE.len() as u16;
        for (chunk, piece) in &pieces {
            self.store_as(*chunk, piece, CHUNK_CONTENT_TYPE, chunk_type_len, true)?;
        }
        if same_list {
            self.sync_existing(&hash, &blob_path)?;
            return Ok(hash);
        }

        let mut list = Vec::with_capacity(12 + pieces.len() * 32);
        list.extend_from_slice(&(content.len() as u64).to_le_bytes());
        list.extend_from_slice(&chunk_count.to_le_bytes());
        for (chunk, _) in &pieces {
            list.extend_from_slice(&chunk.0);
        }
        let content_type_len = content_type_len.to_le_bytes();
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&content_type_len);
        hasher.update(content_type.as_bytes());
        hasher.update(&list);
        let checksum = hasher.finalize().to_le_bytes();
        self.cache.lock().pop(&hash);
        self.write(
            &hash,
            &blob_path,
            &[
                BLOB_MAGIC,
                &[CHUNKED_VERSION],
                &content_type_len,
                content_type.as_bytes(),
                &list,
                &checksum,
            ],
        )?;
        Ok(hash)
    }

    fn known_at(&self, hash: &Hash, blob_path: &Path) -> bool {
        let Some(len) = self.known.lock().get(hash).copied() else {
            return false;
        };
        if fs::metadata(blob_path).is_ok_and(|metadata| metadata.is_file() && metadata.len() == len)
        {
            return true;
        }
        self.known.lock().remove(hash);
        false
    }

    fn remember(&self, hash: Hash, stored: &CachedBlob) {
        let len = whole_file_len(stored.content_type.len(), stored.content.len());
        self.known.lock().insert(hash, len);
    }

    fn sync_existing(&self, hash: &Hash, blob_path: &Path) -> Result<()> {
        self.sync_name(hash).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "the blob {} is stored, but syncing its directory failed: {error}; storing it again tries the sync again",
                    blob_path.display()
                ),
            )
        })?;
        Ok(())
    }

    fn chunks_read_back_as(
        &self,
        list: &ChunkList,
        list_path: &Path,
        content: &[u8],
    ) -> Result<bool> {
        if list.len != content.len() as u64 {
            return Ok(false);
        }
        let mut offset = 0;
        for chunk in &list.chunks {
            let piece = match self.chunk_content(chunk, false) {
                Ok(piece) => piece,
                Err(error) if damage(&error) => return Ok(false),
                Err(error) => return Err(chunk_error(list_path, chunk, error)),
            };
            match content.get(offset..offset + piece.len()) {
                Some(expected) if expected == piece.as_slice() => offset += piece.len(),
                _ => return Ok(false),
            }
        }
        if offset != content.len() {
            return Ok(false);
        }
        for chunk in &list.chunks {
            self.sync_existing(chunk, &self.blob_path(chunk))?;
        }
        Ok(true)
    }

    fn write(&self, hash: &Hash, blob_path: &Path, parts: &[&[u8]]) -> Result<()> {
        let shard_dir = self.shard_path(hash);
        if !shard_dir.is_dir() {
            let shard = self.shard(hash);
            shard.creations_begun.fetch_add(1, Ordering::SeqCst);
            let created = fs::create_dir_all(&shard_dir);
            shard.creations_finished.fetch_add(1, Ordering::SeqCst);
            created?;
        }
        self.sync_shard_entry(hash).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "cannot sync the directories holding {}: {error}; {} wasn't written, and storing the blob again tries the sync again",
                    self.path.display(),
                    blob_path.display()
                ),
            )
        })?;

        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o666);
        let mut staged = tempfile::Builder::new()
            .prefix(STAGING_PREFIX)
            .make_in(&self.path, |name| options.open(name))?;

        let file = staged.as_file_mut();
        for part in parts {
            file.write_all(part)?;
        }
        file.sync_all()?;

        // std's rename converts long Windows paths, which tempfile's persist
        // passes to MoveFileExW unconverted.
        let (file, mut staging) = staged.into_parts();
        drop(file);
        let shard = self.shard(hash);
        shard.renames_begun.fetch_add(1, Ordering::SeqCst);
        let renamed = self
            .fail_at(WriteStep::Rename)
            .and_then(|()| fs::rename(&staging, blob_path));
        shard.renames_finished.fetch_add(1, Ordering::SeqCst);
        renamed.map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "cannot rename the new blob {} into place: {error}",
                    blob_path.display()
                ),
            )
        })?;
        staging.disable_cleanup(true);

        self.sync_name(hash).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "the new blob {} is in place, but syncing its directory failed: {error}; storing it again tries the sync again",
                    blob_path.display()
                ),
            )
        })?;

        Ok(())
    }

    /// Fails in place of `step` when a unit test set the next write to fail
    /// there, once. Otherwise, and always outside unit tests, does nothing.
    fn fail_at(&self, step: WriteStep) -> io::Result<()> {
        #[cfg(test)]
        {
            let mut fails_at = self.fails_at.lock();
            if *fails_at == Some(step) {
                *fails_at = None;
                return Err(io::Error::other(format!("injected failure at {step:?}")));
            }
        }
        let _ = step;
        Ok(())
    }

    fn shard(&self, hash: &Hash) -> &ShardDurability {
        &self.shards[usize::from(hash.0[0])]
    }

    fn sync_shard_entry(&self, hash: &Hash) -> io::Result<()> {
        if !self.entry_synced.load(Ordering::SeqCst) {
            let parent = self
                .path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            sync_directory(parent)?;
            self.entry_synced.store(true, Ordering::SeqCst);
        }
        let shard = self.shard(hash);
        if shard.creations_synced.load(Ordering::SeqCst) < shard.creations_begun.load(Ordering::SeqCst) {
            let finished = shard.creations_finished.load(Ordering::SeqCst);
            sync_directory(&self.path)?;
            shard.creations_synced.fetch_max(finished, Ordering::SeqCst);
        }
        Ok(())
    }

    fn sync_name(&self, hash: &Hash) -> io::Result<()> {
        self.sync_shard_entry(hash)?;
        let shard = self.shard(hash);
        if shard.renames_synced.load(Ordering::SeqCst) < shard.renames_begun.load(Ordering::SeqCst) {
            let finished = shard.renames_finished.load(Ordering::SeqCst);
            self.fail_at(WriteStep::ShardSync)?;
            sync_directory(&self.shard_path(hash))?;
            shard.renames_synced.fetch_max(finished, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Get a blob by its hash, or `None` when none is stored under it.
    ///
    /// A blob whose content is in the cache, which holds blobs read before,
    /// is answered from it, without reading its file. Otherwise a blob file
    /// that's cut short or damaged is an error, and storing the same content
    /// again replaces it. A file naming the blob's chunks is read through
    /// them, each checked against its hash and the whole against `hash`: a
    /// chunk that's missing, cut short, damaged, other bytes or itself a file
    /// naming chunks is an error, and storing the same content again repairs
    /// the blob. The cache holds the chunks read, not the whole. A get that
    /// fails on the blob's file or a chunk's makes this `BlobStorage` read
    /// that file again before taking it as stored (see [`store`](Self::store)).
    pub fn get(&self, hash: &Hash) -> Result<Option<Blob>> {
        let read = self.read_blob(hash);
        if read.is_err() {
            self.known.lock().remove(hash);
        }
        read
    }

    fn read_blob(&self, hash: &Hash) -> Result<Option<Blob>> {
        if let Some(cached) = self.cache.lock().get(hash).cloned() {
            return Ok(Some(Blob {
                hash: *hash,
                content: cached.content,
                content_type: cached.content_type,
            }));
        }

        let blob_path = self.blob_path(hash);
        let stored = match Self::read_file(&blob_path)? {
            None => return Ok(None),
            Some(StoredFile::Content(stored)) => {
                check_hash(hash, &stored.content)?;
                self.cache.lock().put(*hash, stored.clone());
                stored
            }
            Some(StoredFile::Chunks(list)) => {
                let content = self.read_chunks(&list, &blob_path, true)?;
                check_hash(hash, &content)?;
                CachedBlob {
                    content,
                    content_type: list.content_type,
                }
            }
        };

        Ok(Some(Blob {
            hash: *hash,
            content: stored.content,
            content_type: stored.content_type,
        }))
    }

    fn read_chunks(&self, list: &ChunkList, list_path: &Path, cached: bool) -> Result<Vec<u8>> {
        let mut content = Vec::new();
        for chunk in &list.chunks {
            let piece = self
                .chunk_content(chunk, cached)
                .map_err(|error| chunk_error(list_path, chunk, error))?;
            if (content.len() + piece.len()) as u64 > list.len {
                return Err(damaged(
                    list_path,
                    &format!("it names {} bytes of content, and its chunks hold more", list.len),
                ));
            }
            content.extend_from_slice(&piece);
        }
        if content.len() as u64 != list.len {
            return Err(damaged(
                list_path,
                &format!(
                    "it names {} bytes of content, and its chunks hold {}",
                    list.len,
                    content.len()
                ),
            ));
        }
        Ok(content)
    }

    fn chunk_content(&self, chunk: &Hash, cached: bool) -> Result<Vec<u8>> {
        if cached {
            if let Some(hit) = self.cache.lock().get(chunk) {
                return Ok(hit.content.clone());
            }
        }
        let read = match Self::read_file(&self.blob_path(chunk)) {
            Ok(Some(StoredFile::Content(stored))) => {
                check_hash(chunk, &stored.content).map(|()| stored)
            }
            Ok(Some(StoredFile::Chunks(_))) => Err(StoreError::InvalidFormat(
                "is itself a file naming chunks".into(),
            )),
            Ok(None) => Err(StoreError::InvalidFormat("isn't stored".into())),
            Err(StoreError::InvalidFormat(what)) if !what.starts_with(UNSUPPORTED_VERSION) => {
                Err(StoreError::InvalidFormat(format!(
                    "can't be read: {}",
                    what.trim_end_matches(REPLACED_BY_STORING)
                )))
            }
            Err(error) => Err(error),
        };
        let stored = match read {
            Ok(stored) => stored,
            Err(error) => {
                self.known.lock().remove(chunk);
                return Err(error);
            }
        };
        if cached {
            self.cache.lock().put(*chunk, stored.clone());
        } else {
            self.remember(*chunk, &stored);
        }
        Ok(stored.content)
    }

    fn read_file(path: &Path) -> Result<Option<StoredFile>> {
        match fs::metadata(path) {
            Ok(metadata) if !metadata.is_file() => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "{} isn't a regular file, so it isn't read as a blob or replaced, and storing this blob fails until it's removed from the blob directory",
                        path.display()
                    ),
                )
                .into());
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!("cannot look at the blob file {}: {error}", path.display()),
                )
                .into())
            }
        }
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let size = file.metadata()?.len();

        let mut magic = [0u8; 4];
        file.read_exact(&mut magic).map_err(read_error(path))?;
        if &magic != BLOB_MAGIC {
            return Err(StoreError::InvalidFormat("Invalid blob magic".into()));
        }

        let mut version = [0u8; 1];
        file.read_exact(&mut version).map_err(read_error(path))?;
        if version[0] > CHUNKED_VERSION {
            return Err(StoreError::InvalidFormat(format!(
                "{UNSUPPORTED_VERSION} {} in {}: a newer chronicle may have written it, and this one doesn't read it, so storing the same bytes leaves the file as it is",
                version[0],
                path.display()
            )));
        }
        if version[0] != BLOB_VERSION && version[0] != CHUNKED_VERSION {
            return Err(damaged(
                path,
                &format!(
                    "its header names blob format version {}, which no chronicle writes",
                    version[0]
                ),
            ));
        }

        let mut content_type_len_bytes = [0u8; 2];
        file.read_exact(&mut content_type_len_bytes).map_err(read_error(path))?;
        let content_type_len = u16::from_le_bytes(content_type_len_bytes) as usize;

        let mut content_type_bytes = vec![0u8; content_type_len];
        file.read_exact(&mut content_type_bytes).map_err(read_error(path))?;
        let content_type = String::from_utf8_lossy(&content_type_bytes).into_owned();

        let mut content_len_bytes = [0u8; 8];
        file.read_exact(&mut content_len_bytes).map_err(read_error(path))?;
        let content_len = u64::from_le_bytes(content_len_bytes);

        if version[0] == CHUNKED_VERSION {
            let mut count_bytes = [0u8; 4];
            file.read_exact(&mut count_bytes)
                .map_err(read_error(path))?;
            let count = u32::from_le_bytes(count_bytes);
            let names = 4 + 1 + 2 + content_type_len as u64 + 8 + 4 + u64::from(count) * 32 + 4;
            if size != names {
                return Err(damaged(
                    path,
                    &format!("its header names {count} chunks, which takes a file of {names} bytes, and it holds {size}"),
                ));
            }
            let mut hashes = vec![0u8; count as usize * 32];
            file.read_exact(&mut hashes).map_err(read_error(path))?;
            let mut checksum_bytes = [0u8; 4];
            file.read_exact(&mut checksum_bytes)
                .map_err(read_error(path))?;
            let stored_checksum = u32::from_le_bytes(checksum_bytes);
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&content_type_len_bytes);
            hasher.update(&content_type_bytes);
            hasher.update(&content_len_bytes);
            hasher.update(&count_bytes);
            hasher.update(&hashes);
            let computed_checksum = hasher.finalize();
            if stored_checksum != computed_checksum {
                return Err(damaged(path, "its checksum doesn't match"));
            }
            if content_len > u64::from(count) * chunks::MAX_CHUNK as u64 {
                return Err(damaged(
                    path,
                    &format!(
                        "it names {content_len} bytes of content, more than its {count} chunk(s) of at most {} bytes can hold",
                        chunks::MAX_CHUNK
                    ),
                ));
            }
            let chunks = hashes
                .chunks_exact(32)
                .map(|bytes| Hash(bytes.try_into().expect("32-byte chunks")))
                .collect();
            return Ok(Some(StoredFile::Chunks(ChunkList {
                content_type,
                len: content_len,
                chunks,
            })));
        }

        let room = size.saturating_sub(4 + 1 + 2 + content_type_len as u64 + 8 + 4);
        if content_len > room {
            return Err(damaged(
                path,
                &format!("its header names {content_len} bytes of content, more than the file holds"),
            ));
        }

        let mut content = vec![0u8; content_len as usize];
        file.read_exact(&mut content).map_err(read_error(path))?;

        let mut checksum_bytes = [0u8; 4];
        file.read_exact(&mut checksum_bytes).map_err(read_error(path))?;
        let stored_checksum = u32::from_le_bytes(checksum_bytes);
        let computed_checksum = crc32fast::hash(&content);

        if stored_checksum != computed_checksum {
            return Err(StoreError::ChecksumMismatch {
                expected: stored_checksum,
                got: computed_checksum,
            });
        }

        Ok(Some(StoredFile::Content(CachedBlob {
            content,
            content_type,
        })))
    }

    /// Remove the staging files that stores killed before their rename left
    /// in the blob directory. The caller holds the store lock and calls this
    /// before any store, so none of them is a store in progress. One that
    /// can't be removed is reported and left, and so are all of them when
    /// the blob directory can't be listed.
    pub(crate) fn remove_staging_files(&self) {
        crate::atomic_file::remove_files_staged_as(&self.path, STAGING_PREFIX, "blob", "store");
    }

    /// Check if a blob exists: whether this storage has it cached, or
    /// something is at its name, through a link to its target. Nothing is
    /// read, so this doesn't say the blob is whole.
    pub fn exists(&self, hash: &Hash) -> bool {
        if self.cache.lock().contains(hash) {
            return true;
        }
        self.blob_path(hash).exists()
    }

    /// Delete a blob (for garbage collection). A chunk that a chunked blob
    /// names is a blob of its own: deleting it leaves that blob unreadable
    /// until the same content is stored again.
    pub fn delete(&self, hash: &Hash) -> Result<bool> {
        self.cache.lock().pop(hash);
        self.known.lock().remove(hash);

        let blob_path = self.blob_path(hash);
        if blob_path.exists() {
            fs::remove_file(&blob_path)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// List all blob hashes.
    pub fn list(&self) -> Result<Vec<Hash>> {
        let mut hashes = Vec::new();

        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                for blob_entry in fs::read_dir(entry.path())? {
                    let blob_entry = blob_entry?;
                    let filename = blob_entry.file_name();
                    let filename_str = filename.to_string_lossy();
                    if let Ok(hash) = Hash::from_hex(&filename_str) {
                        hashes.push(hash);
                    }
                }
            }
        }

        Ok(hashes)
    }

    /// Get total size of all blobs. Staging files, which lie beside the
    /// shards rather than in them, aren't counted.
    pub fn total_size(&self) -> Result<u64> {
        let mut total = 0u64;

        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                for blob_entry in fs::read_dir(entry.path())? {
                    let blob_entry = blob_entry?;
                    total += blob_entry.metadata()?.len();
                }
            }
        }

        Ok(total)
    }

    /// Get the shard directory for a hash.
    fn shard_path(&self, hash: &Hash) -> PathBuf {
        self.path.join(hash.shard_prefix())
    }

    /// Get the full path for a blob.
    fn blob_path(&self, hash: &Hash) -> PathBuf {
        self.shard_path(hash).join(hash.to_hex())
    }
}

fn content_type_len(content_type: &str) -> Result<u16> {
    u16::try_from(content_type.len()).map_err(|_| {
        StoreError::InvalidOperation(format!(
            "a blob's content type can be at most 65535 bytes, and this one is {}, so nothing was stored",
            content_type.len()
        ))
    })
}

fn whole_file_len(content_type_len: usize, content_len: usize) -> u64 {
    (BLOB_MAGIC.len() + 1 + 2 + content_type_len + 8 + content_len + 4) as u64
}

fn check_hash(expected: &Hash, content: &[u8]) -> Result<()> {
    let got = Hash::from_bytes(content);
    if &got != expected {
        return Err(StoreError::HashMismatch {
            expected: *expected,
            got,
        });
    }
    Ok(())
}

fn chunk_error(path: &Path, chunk: &Hash, error: StoreError) -> StoreError {
    let what = match error {
        StoreError::InvalidFormat(what) if !what.starts_with(UNSUPPORTED_VERSION) => what,
        StoreError::ChecksumMismatch { .. } => "is damaged: its checksum doesn't match".into(),
        StoreError::HashMismatch { got, .. } => format!("holds other bytes, whose hash is {got}"),
        StoreError::Io(error) => {
            return io::Error::new(
                error.kind(),
                format!(
                    "blob file {} names the chunk {chunk}, which can't be read: {error}",
                    path.display()
                ),
            )
            .into()
        }
        other => return other,
    };
    StoreError::InvalidFormat(format!(
        "blob file {} names the chunk {chunk}, which {what}; storing the same bytes again repairs the blob",
        path.display()
    ))
}

fn damage(error: &StoreError) -> bool {
    unreadable(error) || matches!(error, StoreError::HashMismatch { .. })
}

fn unreadable(error: &StoreError) -> bool {
    match error {
        StoreError::InvalidFormat(message) => !message.starts_with(UNSUPPORTED_VERSION),
        StoreError::ChecksumMismatch { .. } => true,
        _ => false,
    }
}

fn damaged(path: &Path, what: &str) -> StoreError {
    StoreError::InvalidFormat(format!(
        "blob file {} is damaged: {what}{REPLACED_BY_STORING}",
        path.display()
    ))
}

fn read_error(path: &Path) -> impl Fn(io::Error) -> StoreError + '_ {
    move |error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            damaged(path, "it is cut short")
        } else {
            error.into()
        }
    }
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> io::Result<()> {
    crate::atomic_file::sync_directory_at(path)?;
    #[cfg(test)]
    SYNCED_DIRECTORIES.with(|synced| synced.borrow_mut().push(path.to_owned()));
    Ok(())
}

// Windows has no portable directory sync, and checkpoint saves go without it
// there too.
#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// A new storage over the blobs in `dir`, with nothing cached, so its
    /// reads and stores go to the files.
    fn reopened(dir: &TempDir) -> BlobStorage {
        BlobStorage::new(dir.path().join("blobs"), 100).unwrap()
    }

    /// The directories this thread's stores have synced since the last call.
    fn synced_directories() -> Vec<PathBuf> {
        SYNCED_DIRECTORIES.with(|synced| synced.take())
    }

    /// Content other than `content` whose hash falls in the same shard.
    #[cfg(unix)]
    fn neighbour_of(content: &[u8]) -> Vec<u8> {
        let shard = Hash::from_bytes(content).shard_prefix();
        (0u32..)
            .map(|i| format!("neighbour {i}").into_bytes())
            .find(|other| other != content && Hash::from_bytes(other).shard_prefix() == shard)
            .unwrap()
    }

    /// How many staging files are in the blob directory.
    fn staged(blobs: &Path) -> usize {
        fs::read_dir(blobs)
            .unwrap()
            .filter(|entry| {
                let name = entry.as_ref().unwrap().file_name();
                name.as_encoded_bytes()
                    .starts_with(STAGING_PREFIX.as_bytes())
            })
            .count()
    }

    // What a crash in an in-place write leaves at a blob's final path, as
    // releases that wrote there could: the file cut short within each field,
    // empty, or at its full length with the pages that never reached the
    // disk read back as zeros. Each fails get, and the next store of the
    // content replaces it. Those releases found a file there and returned,
    // so it stayed torn and every get failed. tests/blob_storage.rs covers
    // other kinds of damage through the store.
    #[test]
    fn a_torn_blob_file_is_replaced_by_the_next_store_of_its_content() {
        let dir = TempDir::new().unwrap();
        let content: Vec<u8> = (0..10_000u32).map(|i| (i * 7 % 251) as u8).collect();
        let hash = reopened(&dir).store(&content, "image/png").unwrap();
        let path = reopened(&dir).blob_path(&hash);
        let whole = fs::read(&path).unwrap();
        // Magic 4, version 1, type length 2, "image/png" 9, content length 8,
        // the content, its checksum 4.
        let header = 4 + 1 + 2 + 9 + 8;
        assert_eq!(whole.len(), header + content.len() + 4);

        let mut zeroed = whole.clone();
        zeroed[header + 4096..].fill(0);
        let cuts = [
            0,
            3,
            5,
            7,
            12,
            header - 1,
            header,
            header + 4096,
            whole.len() - 1,
        ];
        let torn = cuts
            .into_iter()
            .map(|len| (format!("cut to {len} bytes"), whole[..len].to_vec()))
            .chain([("zeros past the first page".to_string(), zeroed)]);
        for (how, bytes) in torn {
            fs::write(&path, &bytes).unwrap();
            assert!(reopened(&dir).get(&hash).is_err(), "{how}: get must fail");

            assert_eq!(
                reopened(&dir).store(&content, "image/png").unwrap(),
                hash,
                "{how}"
            );
            assert_eq!(fs::read(&path).unwrap(), whole, "{how}");
            let blob = reopened(&dir).get(&hash).unwrap().unwrap();
            assert_eq!(blob.content, content, "{how}");
            assert_eq!(blob.content_type, "image/png", "{how}");
        }
        assert_eq!(staged(&dir.path().join("blobs")), 0);
    }

    // A whole file is left as it is: storing its content again from a
    // storage that hasn't read it writes nothing, and the file keeps the
    // content type it was first stored with, which the cache then answers.
    #[test]
    fn storing_content_already_held_whole_writes_nothing() {
        let dir = TempDir::new().unwrap();
        let hash = reopened(&dir).store(b"held", "text/plain").unwrap();
        let path = reopened(&dir).blob_path(&hash);
        let before = same_file::Handle::from_path(&path).unwrap();
        let modified = fs::metadata(&path).unwrap().modified().unwrap();

        let storage = reopened(&dir);
        assert_eq!(
            storage.store(b"held", "application/octet-stream").unwrap(),
            hash
        );
        assert_eq!(same_file::Handle::from_path(&path).unwrap(), before);
        assert_eq!(fs::metadata(&path).unwrap().modified().unwrap(), modified);
        assert_eq!(
            storage.get(&hash).unwrap().unwrap().content_type,
            "text/plain"
        );
        assert_eq!(
            reopened(&dir).get(&hash).unwrap().unwrap().content_type,
            "text/plain"
        );
        assert_eq!(staged(&dir.path().join("blobs")), 0);
    }

    // The limit on a content type doesn't depend on what is stored: content
    // this storage has cached, and content whose whole file it finds, are
    // refused as new content is, and the file is left as it was.
    #[test]
    fn a_content_type_too_long_is_refused_for_content_already_stored() {
        let dir = TempDir::new().unwrap();
        let too_long = "x".repeat(u16::MAX as usize + 1);
        let storage = reopened(&dir);
        let hash = storage.store(b"stored", "text/plain").unwrap();
        let path = storage.blob_path(&hash);
        let whole = fs::read(&path).unwrap();

        for (how, storage) in [("cached", storage), ("read from its file", reopened(&dir))] {
            let error = storage.store(b"stored", &too_long).unwrap_err();
            assert!(
                matches!(error, StoreError::InvalidOperation(_)),
                "{how}: {error}"
            );
            assert_eq!(fs::read(&path).unwrap(), whole, "{how}");
            assert_eq!(
                storage.get(&hash).unwrap().unwrap().content_type,
                "text/plain",
                "{how}"
            );
        }
        assert_eq!(staged(&dir.path().join("blobs")), 0);
    }

    // A storage syncs the blob directory's entry in its parent once, the
    // blob directory when it made a shard there, and the shard when it
    // renamed a blob into it, each before the store returns. A store that
    // writes nothing syncs nothing more: one of content this storage has
    // cached, or of content whose whole file it finds, which in a new
    // storage syncs only the blob directory's entry.
    #[cfg(unix)]
    #[test]
    fn a_new_blob_syncs_its_shard_and_a_new_shard_the_blob_directory() {
        let dir = TempDir::new().unwrap();
        let blobs = dir.path().join("blobs");
        let storage = reopened(&dir);
        synced_directories();

        let first = storage.store(b"first", "text/plain").unwrap();
        let shard = storage.shard_path(&first);
        assert_eq!(
            synced_directories(),
            [dir.path().to_owned(), blobs.clone(), shard.clone()]
        );

        let neighbour = neighbour_of(b"first");
        let second = storage.store(&neighbour, "text/plain").unwrap();
        assert_ne!(second, first);
        assert_eq!(synced_directories(), [shard.clone()]);

        storage.store(b"first", "text/plain").unwrap();
        assert_eq!(synced_directories(), Vec::<PathBuf>::new());
        reopened(&dir).store(b"first", "text/plain").unwrap();
        reopened(&dir).store(&neighbour, "image/png").unwrap();
        assert_eq!(
            synced_directories(),
            [dir.path().to_owned(), dir.path().to_owned()]
        );

        // Replacing a torn file goes into a shard that is there already.
        fs::write(storage.blob_path(&first), b"BLB\0").unwrap();
        reopened(&dir).store(b"first", "text/plain").unwrap();
        assert_eq!(synced_directories(), [dir.path().to_owned(), shard]);
        assert_eq!(staged(&blobs), 0);
    }

    // A rename that fails removes the staging file and returns its error.
    // Nothing reaches the blob's path, a damaged file that was there stays
    // as it was, nothing is cached, and the next store writes the blob.
    #[test]
    fn a_failed_rename_removes_the_staging_file_and_leaves_the_path_as_it_was() {
        let dir = TempDir::new().unwrap();
        let blobs = dir.path().join("blobs");
        let storage = reopened(&dir);
        let hash = Hash::from_bytes(b"renamed");
        let path = storage.blob_path(&hash);

        *storage.fails_at.lock() = Some(WriteStep::Rename);
        let error = storage.store(b"renamed", "text/plain").unwrap_err();
        assert!(
            error.to_string().contains("cannot rename the new blob"),
            "{error}"
        );
        assert_eq!(staged(&blobs), 0);
        assert!(!path.exists());
        assert!(!storage.exists(&hash));
        assert!(storage.get(&hash).unwrap().is_none());

        fs::write(&path, b"BLB\0").unwrap();
        *storage.fails_at.lock() = Some(WriteStep::Rename);
        storage.store(b"renamed", "text/plain").unwrap_err();
        assert_eq!(staged(&blobs), 0);
        assert_eq!(fs::read(&path).unwrap(), b"BLB\0");
        assert!(!storage.cache.lock().contains(&hash));

        assert_eq!(storage.store(b"renamed", "text/plain").unwrap(), hash);
        assert_eq!(staged(&blobs), 0);
        assert_eq!(
            reopened(&dir).get(&hash).unwrap().unwrap().content,
            b"renamed"
        );
    }

    // A sync of the shard that fails after the rename returns its error with
    // the blob in place and whole, and nothing cached. The next store of the
    // content through this storage finds the file whole and tries that sync
    // again, and once it has gone through, a later store makes none.
    #[test]
    fn a_failed_sync_after_the_rename_is_tried_again_by_the_next_store() {
        let dir = TempDir::new().unwrap();
        let storage = reopened(&dir);
        let hash = Hash::from_bytes(b"in place");
        let shard = storage.shard_path(&hash);

        *storage.fails_at.lock() = Some(WriteStep::ShardSync);
        let error = storage.store(b"in place", "text/plain").unwrap_err();
        assert!(matches!(error, StoreError::Io(_)), "{error}");
        assert!(
            error
                .to_string()
                .contains("is in place, but syncing its directory failed"),
            "{error}"
        );
        assert_eq!(staged(&dir.path().join("blobs")), 0);
        assert!(!storage.cache.lock().contains(&hash));
        assert_eq!(
            reopened(&dir).get(&hash).unwrap().unwrap().content,
            b"in place"
        );

        synced_directories();
        assert_eq!(storage.store(b"in place", "text/plain").unwrap(), hash);
        if cfg!(unix) {
            assert_eq!(synced_directories(), [shard]);
        }
        assert!(storage.cache.lock().contains(&hash));
        assert_eq!(storage.store(b"in place", "text/plain").unwrap(), hash);
        assert_eq!(synced_directories(), Vec::<PathBuf>::new());
    }

    // Stores of one content racing from several storages each stage their
    // own file and rename it into place, and reads meanwhile find the blob
    // whole or absent, never partly written. The writers and readers start
    // together on a barrier, and the readers read until every writer is
    // done, so reads overlap the writes on most runs; the test is
    // probabilistic, as a scheduler may still keep them apart.
    #[test]
    fn racing_stores_and_reads_of_one_blob_find_it_whole_or_absent() {
        const WRITERS: usize = 4;
        const READERS: usize = 4;
        let dir = TempDir::new().unwrap();
        let content: Vec<u8> = (0..200_000u32).map(|i| (i % 241) as u8).collect();
        let hash = Hash::from_bytes(&content);
        let start = std::sync::Barrier::new(WRITERS + READERS);
        let writers_done = AtomicBool::new(false);
        std::thread::scope(|scope| {
            let writers: Vec<_> = (0..WRITERS)
                .map(|_| {
                    scope.spawn(|| {
                        start.wait();
                        assert_eq!(reopened(&dir).store(&content, "image/png").unwrap(), hash);
                    })
                })
                .collect();
            for _ in 0..READERS {
                scope.spawn(|| {
                    start.wait();
                    loop {
                        // Read once more after the writers are done, so each
                        // reader also finds the finished blob.
                        let done = writers_done.load(Ordering::Acquire);
                        match reopened(&dir).get(&hash).unwrap() {
                            Some(blob) => assert_eq!(blob.content, content),
                            None => assert!(!done, "absent after every store returned"),
                        }
                        if done {
                            break;
                        }
                    }
                });
            }
            // The readers stop on this flag, so it is set even when a writer
            // panicked, and that panic is raised after it.
            let joined: Vec<_> = writers.into_iter().map(|writer| writer.join()).collect();
            writers_done.store(true, Ordering::Release);
            for result in joined {
                if let Err(panic) = result {
                    std::panic::resume_unwind(panic);
                }
            }
        });
        assert_eq!(staged(&dir.path().join("blobs")), 0);
        let storage = reopened(&dir);
        assert_eq!(fs::read_dir(storage.shard_path(&hash)).unwrap().count(), 1);
        assert_eq!(storage.get(&hash).unwrap().unwrap().content, content);
    }

    // The sweep removes only regular files with the staging prefix.
    #[test]
    fn the_sweep_removes_only_staging_files() {
        let dir = TempDir::new().unwrap();
        let storage = reopened(&dir);
        let hash = storage.store(b"kept", "text/plain").unwrap();
        let blobs = dir.path().join("blobs");
        fs::write(blobs.join(format!("{STAGING_PREFIX}a1B2c3")), b"unfinished").unwrap();
        fs::create_dir(blobs.join(format!("{STAGING_PREFIX}directory"))).unwrap();
        fs::write(blobs.join("unrelated"), b"not a stage").unwrap();

        storage.remove_staging_files();
        assert!(!blobs.join(format!("{STAGING_PREFIX}a1B2c3")).exists());
        assert!(blobs.join(format!("{STAGING_PREFIX}directory")).is_dir());
        assert!(blobs.join("unrelated").exists());
        assert_eq!(reopened(&dir).get(&hash).unwrap().unwrap().content, b"kept");
    }

    #[test]
    fn test_store_and_retrieve() {
        let dir = TempDir::new().unwrap();
        let storage = BlobStorage::new(dir.path().join("blobs"), 100).unwrap();

        let content = b"Hello, world!";
        let hash = storage.store(content, "text/plain").unwrap();

        let blob = storage.get(&hash).unwrap().unwrap();
        assert_eq!(blob.content, content);
        assert_eq!(blob.content_type, "text/plain");
    }

    #[test]
    fn test_deduplication() {
        let dir = TempDir::new().unwrap();
        let storage = BlobStorage::new(dir.path().join("blobs"), 100).unwrap();

        let content = b"Same content";
        let hash1 = storage.store(content, "text/plain").unwrap();
        let hash2 = storage.store(content, "text/plain").unwrap();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_exists() {
        let dir = TempDir::new().unwrap();
        let storage = BlobStorage::new(dir.path().join("blobs"), 100).unwrap();

        let content = b"Test content";
        let hash = storage.store(content, "text/plain").unwrap();

        assert!(storage.exists(&hash));

        let other_hash = Hash::from_bytes(b"nonexistent");
        assert!(!storage.exists(&other_hash));
    }

    #[test]
    fn test_delete() {
        let dir = TempDir::new().unwrap();
        let storage = BlobStorage::new(dir.path().join("blobs"), 100).unwrap();

        let content = b"To be deleted";
        let hash = storage.store(content, "text/plain").unwrap();

        assert!(storage.exists(&hash));
        assert!(storage.delete(&hash).unwrap());
        assert!(!storage.exists(&hash));
    }

    #[test]
    fn test_list() {
        let dir = TempDir::new().unwrap();
        let storage = BlobStorage::new(dir.path().join("blobs"), 100).unwrap();

        let hash1 = storage.store(b"content1", "text/plain").unwrap();
        let hash2 = storage.store(b"content2", "text/plain").unwrap();
        let hash3 = storage.store(b"content3", "text/plain").unwrap();

        let hashes = storage.list().unwrap();
        assert_eq!(hashes.len(), 3);
        assert!(hashes.contains(&hash1));
        assert!(hashes.contains(&hash2));
        assert!(hashes.contains(&hash3));
    }

    fn varied(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed | 1;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect()
    }

    fn version_at(storage: &BlobStorage, hash: &Hash) -> u8 {
        fs::read(storage.blob_path(hash)).unwrap()[4]
    }

    fn chunks_named(storage: &BlobStorage, hash: &Hash) -> Vec<Hash> {
        match BlobStorage::read_file(&storage.blob_path(hash)).unwrap() {
            Some(StoredFile::Chunks(list)) => list.chunks,
            _ => panic!("no chunked file at {hash}"),
        }
    }

    fn write_list(storage: &BlobStorage, at: &Hash, len: u64, chunks: &[Hash]) {
        let mut list = Vec::new();
        list.extend_from_slice(&len.to_le_bytes());
        list.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
        for chunk in chunks {
            list.extend_from_slice(&chunk.0);
        }
        let mut file = Vec::new();
        let mut checked = Vec::new();
        checked.extend_from_slice(&(b"application/json".len() as u16).to_le_bytes());
        checked.extend_from_slice(b"application/json");
        checked.extend_from_slice(&list);
        file.extend_from_slice(BLOB_MAGIC);
        file.push(CHUNKED_VERSION);
        file.extend_from_slice(&checked);
        file.extend_from_slice(&crc32fast::hash(&checked).to_le_bytes());
        let path = storage.blob_path(at);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, file).unwrap();
    }

    #[test]
    fn a_chunked_store_reads_back_as_the_bytes_stored_whatever_their_size() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs");
        let storage = BlobStorage::new(&path, 100).unwrap();
        let sizes = [0, 1, 2047, 2048, 2049, 8192, 65536, 65537, 300_000];
        let mut stored = Vec::new();
        for (i, &len) in sizes.iter().enumerate() {
            for content in [vec![0u8; len], varied(len, i as u64)] {
                let hash = storage.store_chunked(&content, "application/json").unwrap();
                assert_eq!(hash, Hash::from_bytes(&content), "{len} bytes");
                stored.push((hash, content));
            }
        }
        let fresh = BlobStorage::new(&path, 100).unwrap();
        for (hash, content) in stored {
            let blob = fresh.get(&hash).unwrap().unwrap();
            assert_eq!(blob.content, content, "{} bytes", content.len());
            assert_eq!(blob.content_type, "application/json");
        }
    }

    #[test]
    fn a_chunked_blob_is_a_file_naming_chunks_each_stored_under_its_own_hash() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs");
        let storage = BlobStorage::new(&path, 100).unwrap();
        let content = varied(100_000, 3);
        let hash = storage.store_chunked(&content, "application/json").unwrap();

        assert_eq!(version_at(&storage, &hash), CHUNKED_VERSION);
        let chunks = chunks_named(&storage, &hash);
        assert_eq!(storage.list().unwrap().len(), chunks.len() + 1);
        let fresh = BlobStorage::new(&path, 100).unwrap();
        let mut joined = Vec::new();
        for chunk in chunks {
            assert_eq!(version_at(&storage, &chunk), BLOB_VERSION);
            joined.extend(fresh.get(&chunk).unwrap().unwrap().content);
        }
        assert_eq!(joined, content);
    }

    #[test]
    fn a_chunked_store_that_fails_partway_leaves_no_file_naming_its_chunks() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs");
        let content = varied(100_000, 4);
        let hash = Hash::from_bytes(&content);
        let probe_dir = TempDir::new().unwrap();
        let probe = BlobStorage::new(probe_dir.path().join("blobs"), 100).unwrap();
        probe.store_chunked(&content, "application/json").unwrap();
        let chunks = chunks_named(&probe, &hash);

        let storage = BlobStorage::new(&path, 100).unwrap();
        let blocked = storage.blob_path(&chunks[chunks.len() / 2]);
        fs::create_dir_all(&blocked).unwrap();
        assert!(storage.store_chunked(&content, "application/json").is_err());
        assert!(!storage.blob_path(&hash).exists());

        fs::remove_dir(&blocked).unwrap();
        storage.store_chunked(&content, "application/json").unwrap();
        let fresh = BlobStorage::new(&path, 100).unwrap();
        assert_eq!(fresh.get(&hash).unwrap().unwrap().content, content);
    }

    #[test]
    fn either_form_of_a_blob_already_stored_is_left_as_it_is() {
        let content = varied(100_000, 5);
        let hash = Hash::from_bytes(&content);
        for chunked_first in [false, true] {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("blobs");
            let first = BlobStorage::new(&path, 100).unwrap();
            if chunked_first {
                first.store_chunked(&content, "application/json").unwrap();
            } else {
                first.store(&content, "application/json").unwrap();
            }
            let file = fs::read(first.blob_path(&hash)).unwrap();

            let second = BlobStorage::new(&path, 100).unwrap();
            if chunked_first {
                second.store(&content, "text/plain").unwrap();
            } else {
                second.store_chunked(&content, "text/plain").unwrap();
            }
            assert_eq!(fs::read(second.blob_path(&hash)).unwrap(), file);
            let fresh = BlobStorage::new(&path, 100).unwrap();
            let blob = fresh.get(&hash).unwrap().unwrap();
            assert_eq!(blob.content, content);
            assert_eq!(blob.content_type, "application/json");
        }
    }

    #[test]
    fn a_chunked_get_refuses_a_chunk_that_is_missing_or_other_bytes_and_storing_again_repairs_it() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs");
        let storage = BlobStorage::new(&path, 100).unwrap();
        let content = varied(100_000, 6);
        let hash = storage.store_chunked(&content, "application/json").unwrap();
        let other = storage.store(b"some other bytes", "text/plain").unwrap();
        let chunks = chunks_named(&storage, &hash);

        let chunk_file = fs::read(storage.blob_path(&chunks[1])).unwrap();
        fs::copy(storage.blob_path(&other), storage.blob_path(&chunks[1])).unwrap();
        let list_file = storage.blob_path(&hash).display().to_string();
        let refusal = |expected: &str| match BlobStorage::new(&path, 100).unwrap().get(&hash) {
            Err(StoreError::InvalidFormat(message)) => {
                assert!(message.contains(&list_file), "{message}");
                assert!(message.contains(expected), "{message}");
                assert!(
                    message.contains("storing the same bytes again repairs the blob"),
                    "{message}"
                );
            }
            other => panic!(
                "expected the chunk refused, got {:?}",
                other.map(|b| b.map(|b| b.hash))
            ),
        };
        refusal(&format!(
            "names the chunk {}, which holds other bytes",
            chunks[1]
        ));
        let mut damaged_chunk = chunk_file.clone();
        let middle = damaged_chunk.len() / 2;
        damaged_chunk[middle] ^= 0xff;
        fs::write(storage.blob_path(&chunks[1]), damaged_chunk).unwrap();
        refusal(&format!("names the chunk {}, which is damaged", chunks[1]));
        fs::write(storage.blob_path(&chunks[1]), chunk_file).unwrap();
        assert_eq!(
            BlobStorage::new(&path, 100)
                .unwrap()
                .get(&hash)
                .unwrap()
                .unwrap()
                .content,
            content
        );

        fs::remove_file(storage.blob_path(&chunks[2])).unwrap();
        refusal(&format!(
            "names the chunk {}, which isn't stored",
            chunks[2]
        ));

        BlobStorage::new(&path, 100)
            .unwrap()
            .store_chunked(&content, "application/json")
            .unwrap();
        let fresh = BlobStorage::new(&path, 100).unwrap();
        assert_eq!(fresh.get(&hash).unwrap().unwrap().content, content);
    }

    #[cfg(unix)]
    #[test]
    fn a_blob_this_storage_stored_is_taken_as_stored_without_reading_its_file() {
        use std::os::unix::fs::PermissionsExt;
        // SAFETY: geteuid has no preconditions.
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let content = varied(100_000, 7);
        for chunked in [false, true] {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("blobs");
            let storage = BlobStorage::new(&path, 100).unwrap();
            let store = |storage: &BlobStorage| {
                if chunked {
                    storage.store_chunked(&content, "application/json")
                } else {
                    storage.store(&content, "application/json")
                }
            };
            let hash = store(&storage).unwrap();
            let file = if chunked {
                storage.blob_path(&chunks_named(&storage, &hash)[1])
            } else {
                storage.blob_path(&hash)
            };
            fs::set_permissions(&file, fs::Permissions::from_mode(0o000)).unwrap();

            let again = store(&storage);
            let elsewhere = store(&BlobStorage::new(&path, 100).unwrap());
            fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
            assert_eq!(again.unwrap(), hash, "chunked: {chunked}");
            assert!(elsewhere.is_err(), "chunked: {chunked}");

            let reader = BlobStorage::new(&path, 100).unwrap();
            assert_eq!(reader.get(&hash).unwrap().unwrap().content, content);
            fs::set_permissions(&file, fs::Permissions::from_mode(0o000)).unwrap();
            let after_reading = store(&reader);
            fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
            assert!(
                after_reading.is_err(),
                "a get takes nothing as stored, chunked: {chunked}"
            );
        }
    }

    #[test]
    fn a_get_after_a_store_reads_the_file() {
        let dir = TempDir::new().unwrap();
        let storage = BlobStorage::new(dir.path().join("blobs"), 100).unwrap();
        let hash = storage.store(b"Hello, world!", "text/plain").unwrap();
        let file = storage.blob_path(&hash);
        let bytes = fs::read(&file).unwrap();
        fs::write(&file, &bytes[..bytes.len() - 3]).unwrap();

        assert!(storage.get(&hash).is_err());
    }

    #[test]
    fn a_store_after_a_get_finds_the_blob_damaged_repairs_it_in_the_same_storage() {
        let content = varied(100_000, 8);
        for chunked in [false, true] {
            let dir = TempDir::new().unwrap();
            let storage = BlobStorage::new(dir.path().join("blobs"), 100).unwrap();
            let store = || {
                if chunked {
                    storage.store_chunked(&content, "application/json")
                } else {
                    storage.store(&content, "application/json")
                }
            };
            let hash = store().unwrap();
            if chunked {
                fs::remove_file(storage.blob_path(&chunks_named(&storage, &hash)[1])).unwrap();
            } else {
                let file = storage.blob_path(&hash);
                let bytes = fs::read(&file).unwrap();
                fs::write(&file, &bytes[..bytes.len() / 2]).unwrap();
            }
            assert!(storage.get(&hash).is_err(), "chunked: {chunked}");

            store().unwrap();
            assert_eq!(
                storage.get(&hash).unwrap().unwrap().content,
                content,
                "chunked: {chunked}"
            );
        }
    }

    #[test]
    fn a_chunk_list_naming_good_chunks_out_of_order_is_a_hash_mismatch_on_the_whole() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs");
        let storage = BlobStorage::new(&path, 100).unwrap();
        let content = varied(100_000, 9);
        let hash = storage.store_chunked(&content, "application/json").unwrap();
        let mut chunks = chunks_named(&storage, &hash);
        chunks.swap(0, 1);
        write_list(&storage, &hash, content.len() as u64, &chunks);

        match BlobStorage::new(&path, 100).unwrap().get(&hash) {
            Err(StoreError::HashMismatch { expected, .. }) => assert_eq!(expected, hash),
            other => panic!(
                "expected a hash mismatch, got {:?}",
                other.map(|b| b.map(|b| b.hash))
            ),
        }
    }

    #[test]
    fn a_chunk_list_with_its_content_type_damaged_or_bytes_past_its_checksum_is_refused_and_a_chunked_store_replaces_it() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs");
        let storage = BlobStorage::new(&path, 100).unwrap();
        let content = varied(100_000, 21);
        let hash = storage.store_chunked(&content, "application/json").unwrap();
        let file = storage.blob_path(&hash);
        let good = fs::read(&file).unwrap();
        let mut other_type = good.clone();
        other_type[7] ^= 1;
        let mut longer = good.clone();
        longer.push(0);

        for damaged in [other_type, longer] {
            fs::write(&file, &damaged).unwrap();
            let fresh = BlobStorage::new(&path, 100).unwrap();
            match fresh.get(&hash) {
                Err(StoreError::ChecksumMismatch { .. }) | Err(StoreError::InvalidFormat(_)) => {}
                other => panic!(
                    "expected the list refused as damaged, got {:?}",
                    other.map(|b| b.map(|b| b.content_type))
                ),
            }
            fresh.store_chunked(&content, "application/json").unwrap();
            let reader = BlobStorage::new(&path, 100).unwrap();
            assert_eq!(reader.get(&hash).unwrap().unwrap().content, content);
        }
    }

    #[test]
    fn a_chunk_list_whose_checksum_doesnt_match_names_its_file_and_the_repair() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs");
        let storage = BlobStorage::new(&path, 100).unwrap();
        let content = varied(100_000, 25);
        let hash = storage.store_chunked(&content, "application/json").unwrap();
        let file = storage.blob_path(&hash);
        let mut damaged = fs::read(&file).unwrap();
        let named = damaged.len() - 4 - 1;
        damaged[named] ^= 1;
        fs::write(&file, &damaged).unwrap();

        match BlobStorage::new(&path, 100).unwrap().get(&hash) {
            Err(StoreError::InvalidFormat(what)) => {
                assert!(what.contains(&file.display().to_string()), "{what}");
                assert!(what.contains("its checksum doesn't match"), "{what}");
                assert!(what.ends_with(REPLACED_BY_STORING), "{what}");
            }
            other => panic!("expected the list refused as damaged, got {:?}", other.map(|b| b.map(|b| b.hash))),
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_chunk_that_cant_be_opened_is_named_with_the_list_that_names_it() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs");
        let storage = BlobStorage::new(&path, 100).unwrap();
        let chunk = storage.store(&varied(4 * 1024, 26), "application/octet-stream").unwrap();
        let at = Hash::from_bytes(b"a list");
        write_list(&storage, &at, 4 * 1024, &[chunk]);
        let chunk_file = storage.blob_path(&chunk);
        fs::set_permissions(&chunk_file, fs::Permissions::from_mode(0o000)).unwrap();
        if File::open(&chunk_file).is_ok() {
            return;
        }

        let got = BlobStorage::new(&path, 100).unwrap().get(&at);
        fs::set_permissions(&chunk_file, fs::Permissions::from_mode(0o644)).unwrap();
        match got {
            Err(StoreError::Io(error)) => {
                let what = error.to_string();
                assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
                assert!(what.contains(&storage.blob_path(&at).display().to_string()), "{what}");
                assert!(what.contains(&chunk.to_string()), "{what}");
            }
            other => panic!("expected the chunk's I/O error, got {:?}", other.map(|b| b.map(|b| b.hash))),
        }
    }

    #[test]
    fn a_chunk_list_naming_more_than_its_chunks_can_hold_is_refused_before_any_chunk_is_read() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs");
        let storage = BlobStorage::new(&path, 100).unwrap();
        let at = Hash::from_bytes(b"a list");
        write_list(&storage, &at, chunks::MAX_CHUNK as u64 + 1, &[Hash::from_bytes(b"never stored")]);

        match storage.get(&at) {
            Err(StoreError::InvalidFormat(what)) => assert!(what.contains("is damaged"), "{what}"),
            other => panic!("expected a damaged list, got {:?}", other.map(|b| b.map(|b| b.hash))),
        }
    }

    #[test]
    fn a_chunk_list_whose_chunks_hold_more_than_it_names_is_refused_at_the_first_chunk_past_it() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs");
        let storage = BlobStorage::new(&path, 100).unwrap();
        let chunk = storage.store(&varied(64 * 1024, 23), "application/octet-stream").unwrap();
        let at = Hash::from_bytes(b"a list");
        write_list(&storage, &at, 1, &[chunk, chunk]);

        match storage.get(&at) {
            Err(StoreError::InvalidFormat(what)) => {
                assert!(what.contains("its chunks hold more"), "{what}")
            }
            other => panic!("expected a damaged list, got {:?}", other.map(|b| b.map(|b| b.hash))),
        }
    }

    #[test]
    fn a_plain_store_replaces_a_chunk_list_with_a_chunk_missing() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs");
        let storage = BlobStorage::new(&path, 100).unwrap();
        let content = varied(100_000, 10);
        let hash = storage.store_chunked(&content, "application/json").unwrap();
        fs::remove_file(storage.blob_path(&chunks_named(&storage, &hash)[1])).unwrap();

        let fresh = BlobStorage::new(&path, 100).unwrap();
        fresh.store(&content, "application/json").unwrap();
        assert_eq!(version_at(&fresh, &hash), BLOB_VERSION);
        assert_eq!(
            BlobStorage::new(&path, 100)
                .unwrap()
                .get(&hash)
                .unwrap()
                .unwrap()
                .content,
            content
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_plain_store_fails_on_a_chunk_it_may_not_read_and_leaves_the_list() {
        use std::os::unix::fs::PermissionsExt;
        // SAFETY: geteuid has no preconditions.
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs");
        let storage = BlobStorage::new(&path, 100).unwrap();
        let content = varied(100_000, 11);
        let hash = storage.store_chunked(&content, "application/json").unwrap();
        let list = fs::read(storage.blob_path(&hash)).unwrap();
        let chunk = storage.blob_path(&chunks_named(&storage, &hash)[1]);
        fs::set_permissions(&chunk, fs::Permissions::from_mode(0o000)).unwrap();

        let stored = BlobStorage::new(&path, 100)
            .unwrap()
            .store(&content, "application/json");
        fs::set_permissions(&chunk, fs::Permissions::from_mode(0o644)).unwrap();
        match stored {
            Err(StoreError::Io(error)) => {
                assert_eq!(error.kind(), io::ErrorKind::PermissionDenied, "{error}")
            }
            other => panic!("expected the store to refuse, got {other:?}"),
        }
        assert_eq!(fs::read(storage.blob_path(&hash)).unwrap(), list);
    }

    #[test]
    fn a_plain_store_checking_a_chunk_list_keeps_none_of_it_in_memory() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs");
        let content = varied(100_000, 12);
        let hash = BlobStorage::new(&path, 100)
            .unwrap()
            .store_chunked(&content, "application/json")
            .unwrap();
        let storage = BlobStorage::new(&path, 100).unwrap();
        storage.store(&content, "application/json").unwrap();
        let chunk = storage.blob_path(&chunks_named(&storage, &hash)[1]);
        let chunk_file = fs::read(&chunk).unwrap();
        fs::remove_file(&chunk).unwrap();
        assert!(storage.get(&hash).is_err());

        fs::write(&chunk, chunk_file).unwrap();
        let reader = BlobStorage::new(&path, 100).unwrap();
        assert_eq!(reader.get(&hash).unwrap().unwrap().content, content);
        fs::remove_file(&chunk).unwrap();
        reader.store(&content, "application/json").unwrap();
        assert_eq!(version_at(&reader, &hash), BLOB_VERSION);
    }

    #[test]
    fn a_get_after_a_removed_blob_is_stored_again_answers_the_new_content_type() {
        let dir = TempDir::new().unwrap();
        let storage = BlobStorage::new(dir.path().join("blobs"), 100).unwrap();
        let hash = storage.store(b"Hello, world!", "text/plain").unwrap();
        assert_eq!(
            storage.get(&hash).unwrap().unwrap().content_type,
            "text/plain"
        );
        fs::remove_file(storage.blob_path(&hash)).unwrap();

        storage.store(b"Hello, world!", "text/markdown").unwrap();
        assert_eq!(
            storage.get(&hash).unwrap().unwrap().content_type,
            "text/markdown"
        );
    }

    #[test]
    fn a_chunked_store_of_content_grown_from_earlier_content_writes_few_files() {
        let dir = TempDir::new().unwrap();
        let storage = BlobStorage::new(dir.path().join("blobs"), 100).unwrap();
        let earlier = varied(300_000, 16);
        let mut grown = earlier[..100_000].to_vec();
        grown.extend_from_slice(&varied(100, 17));
        grown.extend_from_slice(&earlier[100_000..]);
        grown.extend_from_slice(&varied(20_000, 18));
        storage.store_chunked(&earlier, "application/json").unwrap();
        let before = storage.list().unwrap().len();
        storage.store_chunked(&grown, "application/json").unwrap();
        let added = storage.list().unwrap().len() - before;
        assert!(
            added <= 10,
            "{added} new files of {} for the grown content",
            storage.list().unwrap().len()
        );
    }

    #[test]
    fn a_chunked_store_replaces_a_cut_short_whole_blob_and_a_list_at_a_chunks_name() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs");
        let storage = BlobStorage::new(&path, 100).unwrap();
        let content = varied(100_000, 19);
        let hash = storage.store(&content, "application/json").unwrap();
        let file = storage.blob_path(&hash);
        let bytes = fs::read(&file).unwrap();
        fs::write(&file, &bytes[..bytes.len() / 2]).unwrap();
        BlobStorage::new(&path, 100)
            .unwrap()
            .store_chunked(&content, "application/json")
            .unwrap();
        assert_eq!(version_at(&storage, &hash), CHUNKED_VERSION);

        let chunk = chunks_named(&storage, &hash)[1];
        let piece = BlobStorage::new(&path, 100)
            .unwrap()
            .get(&chunk)
            .unwrap()
            .unwrap()
            .content;
        let halves = [
            storage
                .store(&piece[..piece.len() / 2], "application/octet-stream")
                .unwrap(),
            storage
                .store(&piece[piece.len() / 2..], "application/octet-stream")
                .unwrap(),
        ];
        write_list(&storage, &chunk, piece.len() as u64, &halves);
        match BlobStorage::new(&path, 100).unwrap().get(&hash) {
            Err(StoreError::InvalidFormat(message)) => {
                assert!(
                    message.contains("is itself a file naming chunks"),
                    "{message}"
                )
            }
            other => panic!(
                "expected the list at a chunk refused, got {:?}",
                other.map(|b| b.map(|b| b.hash))
            ),
        }
        BlobStorage::new(&path, 100)
            .unwrap()
            .store_chunked(&content, "application/json")
            .unwrap();
        assert_eq!(version_at(&storage, &chunk), BLOB_VERSION);
        assert_eq!(
            BlobStorage::new(&path, 100)
                .unwrap()
                .get(&hash)
                .unwrap()
                .unwrap()
                .content,
            content
        );
    }

    #[test]
    fn a_plain_store_fails_on_a_chunk_in_a_newer_format_and_leaves_the_list() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs");
        let storage = BlobStorage::new(&path, 100).unwrap();
        let content = varied(100_000, 20);
        let hash = storage.store_chunked(&content, "application/json").unwrap();
        let list = fs::read(storage.blob_path(&hash)).unwrap();
        let chunk = storage.blob_path(&chunks_named(&storage, &hash)[1]);
        let mut newer = fs::read(&chunk).unwrap();
        newer[4] = CHUNKED_VERSION + 1;
        fs::write(&chunk, &newer).unwrap();

        match BlobStorage::new(&path, 100)
            .unwrap()
            .store(&content, "application/json")
        {
            Err(StoreError::InvalidFormat(message)) => {
                assert!(message.contains(UNSUPPORTED_VERSION), "{message}")
            }
            other => panic!("expected the store to refuse, got {other:?}"),
        }
        assert_eq!(fs::read(storage.blob_path(&hash)).unwrap(), list);
    }

    #[test]
    fn a_chunked_store_replaces_a_list_at_a_chunks_name_that_a_get_has_read() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs");
        let storage = BlobStorage::new(&path, 100).unwrap();
        let content = varied(100_000, 21);
        let hash = storage.store_chunked(&content, "application/json").unwrap();
        let chunk = chunks_named(&storage, &hash)[1];
        let piece = BlobStorage::new(&path, 100)
            .unwrap()
            .get(&chunk)
            .unwrap()
            .unwrap()
            .content;
        let halves = [
            storage
                .store(&piece[..piece.len() / 2], "application/octet-stream")
                .unwrap(),
            storage
                .store(&piece[piece.len() / 2..], "application/octet-stream")
                .unwrap(),
        ];
        write_list(&storage, &chunk, piece.len() as u64, &halves);

        let reader = BlobStorage::new(&path, 100).unwrap();
        assert_eq!(reader.get(&chunk).unwrap().unwrap().content, piece);
        reader.store_chunked(&content, "application/json").unwrap();
        assert_eq!(version_at(&storage, &chunk), BLOB_VERSION);
        assert_eq!(
            BlobStorage::new(&path, 100)
                .unwrap()
                .get(&hash)
                .unwrap()
                .unwrap()
                .content,
            content
        );
    }

    #[test]
    fn a_store_reads_a_list_written_where_a_blob_it_knew_was_deleted() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs");
        let storage = BlobStorage::new(&path, 100).unwrap();
        let content = varied(20_000, 22);
        let chunk_count = chunks::chunk_ends(&content).len();
        assert!(chunk_count >= 2);
        let hash = storage.store(&content, "a").unwrap();
        storage.delete(&hash).unwrap();
        let same_length_type = "a".repeat(1 + content.len() - 4 - 32 * chunk_count);
        BlobStorage::new(&path, 100)
            .unwrap()
            .store_chunked(&content, &same_length_type)
            .unwrap();
        assert_eq!(
            fs::metadata(storage.blob_path(&hash)).unwrap().len(),
            whole_file_len(1, content.len())
        );
        fs::remove_file(storage.blob_path(&chunks_named(&storage, &hash)[1])).unwrap();

        storage.store(&content, "a").unwrap();
        assert_eq!(version_at(&storage, &hash), BLOB_VERSION);
        assert_eq!(
            BlobStorage::new(&path, 100)
                .unwrap()
                .get(&hash)
                .unwrap()
                .unwrap()
                .content,
            content
        );
    }

    #[test]
    fn a_get_that_finds_a_chunk_damaged_makes_a_chunked_store_naming_it_rewrite_it() {
        let dir = TempDir::new().unwrap();
        let storage = BlobStorage::new(dir.path().join("blobs"), 100).unwrap();
        let first = varied(100_000, 23);
        let mut second = first.clone();
        second.extend_from_slice(&varied(20_000, 24));
        let first_hash = storage.store_chunked(&first, "application/json").unwrap();
        let second_hash = storage.store_chunked(&second, "application/json").unwrap();
        let shared = chunks_named(&storage, &first_hash)[0];
        assert_eq!(chunks_named(&storage, &second_hash)[0], shared);
        let file = storage.blob_path(&shared);
        let mut bytes = fs::read(&file).unwrap();
        let middle = bytes.len() / 2;
        bytes[middle] ^= 0xff;
        fs::write(&file, bytes).unwrap();
        assert!(storage.get(&first_hash).is_err());

        storage.store_chunked(&second, "application/json").unwrap();
        assert_eq!(storage.get(&second_hash).unwrap().unwrap().content, second);
    }

    #[test]
    fn a_store_rewrites_a_file_it_knew_once_it_is_cut_short() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs");
        let storage = BlobStorage::new(&path, 100).unwrap();
        let content = varied(100_000, 25);
        let hash = storage.store(&content, "application/json").unwrap();
        let file = storage.blob_path(&hash);
        let bytes = fs::read(&file).unwrap();
        fs::write(&file, &bytes[..bytes.len() - 100]).unwrap();

        storage.store(&content, "application/json").unwrap();
        assert_eq!(
            BlobStorage::new(&path, 100)
                .unwrap()
                .get(&hash)
                .unwrap()
                .unwrap()
                .content,
            content
        );
    }
}
