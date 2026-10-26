//! Blob storage implementation.

use crate::error::{Result, StoreError};
use crate::types::{Blob, Hash};
use lru::LruCache;
use parking_lot::Mutex;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Magic bytes for blob files.
const BLOB_MAGIC: &[u8; 4] = b"BLB\0";

/// Current blob format version.
const BLOB_VERSION: u8 = 1;

const STAGING_PREFIX: &str = ".chronicle-blob-";

/// Cached blob data (content + content_type).
#[derive(Clone)]
struct CachedBlob {
    content: Vec<u8>,
    content_type: String,
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

    /// LRU cache for recently accessed blobs.
    cache: Mutex<LruCache<Hash, CachedBlob>>,

    entry_synced: AtomicBool,
    shards: Box<[ShardDurability]>,
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
    /// left as it is, content type included. A file that's cut short, damaged
    /// or holds other bytes is replaced. A symbolic link at the blob's name is
    /// read through: one whose target reads back as `content` is left, and one
    /// whose target is cut short, damaged, holds other bytes or doesn't exist
    /// is itself replaced by the blob, leaving any target as it was; one that
    /// loops is an error. Anything else at the blob's name or a link's target,
    /// or a file that can't be opened or read at all, is an error and is left
    /// as it is. A blob whose content is in the cache is taken as stored
    /// without reading its file, while a regular file, or a link to one, is at
    /// its name. A content type longer than 65,535 bytes is refused.
    pub fn store(&self, content: &[u8], content_type: &str) -> Result<Hash> {
        let content_type_len = u16::try_from(content_type.len()).map_err(|_| {
            StoreError::InvalidOperation(format!(
                "a blob's content type can be at most 65535 bytes, and this one is {}, so nothing was stored",
                content_type.len()
            ))
        })?;
        let hash = Hash::from_bytes(content);
        let blob_path = self.blob_path(&hash);

        let readable = (self.cache.lock().contains(&hash)
            && fs::metadata(&blob_path).is_ok_and(|metadata| metadata.is_file()))
            || match Self::read_file(&blob_path) {
                Ok(Some(stored)) if stored.content == content => {
                    self.cache.lock().put(hash, stored);
                    true
                }
                Ok(_) => false,
                Err(error) if unreadable(&error) => false,
                Err(error) => return Err(error),
            };
        if !readable {
            self.write(&hash, &blob_path, content, content_type, content_type_len)?;
            self.cache.lock().put(hash, CachedBlob {
                content: content.to_vec(),
                content_type: content_type.to_string(),
            });
            return Ok(hash);
        }

        self.sync_name(&hash).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "the blob {} is stored, but syncing its directory failed: {error}; storing it again tries the sync again",
                    blob_path.display()
                ),
            )
        })?;
        Ok(hash)
    }

    fn write(
        &self,
        hash: &Hash,
        blob_path: &Path,
        content: &[u8],
        content_type: &str,
        content_type_len: u16,
    ) -> Result<()> {
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
                    "cannot sync the directories holding {}: {error}; nothing was stored, and storing the blob again tries the sync again",
                    self.path.display()
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
        file.write_all(BLOB_MAGIC)?;
        file.write_all(&[BLOB_VERSION])?;
        file.write_all(&content_type_len.to_le_bytes())?;
        file.write_all(content_type.as_bytes())?;
        file.write_all(&(content.len() as u64).to_le_bytes())?;
        file.write_all(content)?;
        file.write_all(&crc32fast::hash(content).to_le_bytes())?;
        file.sync_all()?;

        // std's rename converts long Windows paths, which tempfile's persist
        // passes to MoveFileExW unconverted.
        let (file, mut staging) = staged.into_parts();
        drop(file);
        let shard = self.shard(hash);
        shard.renames_begun.fetch_add(1, Ordering::SeqCst);
        let renamed = fs::rename(&staging, blob_path);
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
            sync_directory(&self.shard_path(hash))?;
            shard.renames_synced.fetch_max(finished, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Get a blob by its hash, or `None` when none is stored under it.
    ///
    /// A blob whose content is in the cache is answered from it, without
    /// reading its file. Otherwise a blob file that's cut short or damaged is
    /// an error, and storing the same content again replaces it.
    pub fn get(&self, hash: &Hash) -> Result<Option<Blob>> {
        // Check cache first
        if let Some(cached) = self.cache.lock().get(hash).cloned() {
            return Ok(Some(Blob {
                hash: *hash,
                content: cached.content,
                content_type: cached.content_type,
            }));
        }

        let Some(stored) = Self::read_file(&self.blob_path(hash))? else {
            return Ok(None);
        };

        let computed_hash = Hash::from_bytes(&stored.content);
        if &computed_hash != hash {
            return Err(StoreError::HashMismatch {
                expected: *hash,
                got: computed_hash,
            });
        }

        self.cache.lock().put(*hash, stored.clone());

        Ok(Some(Blob {
            hash: *hash,
            content: stored.content,
            content_type: stored.content_type,
        }))
    }

    fn read_file(path: &Path) -> Result<Option<CachedBlob>> {
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

        // Read and verify magic
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic).map_err(read_error(path))?;
        if &magic != BLOB_MAGIC {
            return Err(StoreError::InvalidFormat("Invalid blob magic".into()));
        }

        // Read version
        let mut version = [0u8; 1];
        file.read_exact(&mut version).map_err(read_error(path))?;
        if version[0] != BLOB_VERSION {
            return Err(StoreError::InvalidFormat(format!(
                "Unsupported blob version: {}",
                version[0]
            )));
        }

        // Read content type
        let mut content_type_len_bytes = [0u8; 2];
        file.read_exact(&mut content_type_len_bytes).map_err(read_error(path))?;
        let content_type_len = u16::from_le_bytes(content_type_len_bytes) as usize;

        let mut content_type_bytes = vec![0u8; content_type_len];
        file.read_exact(&mut content_type_bytes).map_err(read_error(path))?;
        let content_type = String::from_utf8_lossy(&content_type_bytes).into_owned();

        // Read content
        let mut content_len_bytes = [0u8; 8];
        file.read_exact(&mut content_len_bytes).map_err(read_error(path))?;
        let content_len = u64::from_le_bytes(content_len_bytes);
        let room = size.saturating_sub(4 + 1 + 2 + content_type_len as u64 + 8 + 4);
        if content_len > room {
            return Err(damaged(
                path,
                &format!("its header names {content_len} bytes of content, more than the file holds"),
            ));
        }

        let mut content = vec![0u8; content_len as usize];
        file.read_exact(&mut content).map_err(read_error(path))?;

        // Read and verify checksum
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

        Ok(Some(CachedBlob {
            content,
            content_type,
        }))
    }

    pub(crate) fn remove_staging_files(&self) {
        crate::atomic_file::remove_files_staged_as(&self.path, STAGING_PREFIX, "blob", "store");
    }

    /// Check if a blob exists.
    pub fn exists(&self, hash: &Hash) -> bool {
        if self.cache.lock().contains(hash) {
            return true;
        }
        self.blob_path(hash).exists()
    }

    /// Delete a blob (for garbage collection).
    pub fn delete(&self, hash: &Hash) -> Result<bool> {
        self.cache.lock().pop(hash);

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

    /// Get total size of all blobs.
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

fn unreadable(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::InvalidFormat(_) | StoreError::ChecksumMismatch { .. }
    )
}

fn damaged(path: &Path, what: &str) -> StoreError {
    StoreError::InvalidFormat(format!(
        "blob file {} is damaged: {what}; storing the same bytes again replaces it",
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
    crate::atomic_file::sync_directory_at(path)
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
}
