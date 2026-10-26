//! Blob storage implementation.

use crate::error::{Result, StoreError};
use crate::types::{Blob, Hash};
use lru::LruCache;
use parking_lot::Mutex;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

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

/// Content-addressed blob storage.
pub struct BlobStorage {
    /// Base directory for blobs.
    path: PathBuf,

    /// LRU cache for recently accessed blobs.
    cache: Mutex<LruCache<Hash, CachedBlob>>,
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
        })
    }

    /// Store a blob, returning its hash.
    ///
    /// The blob is written whole under a staging name in the blob directory,
    /// synced, and renamed to its own name, so a store never leaves part of a
    /// blob under that name. On Unix the directories naming it are synced too
    /// before this returns, where the system syncs directories and this
    /// account may open them. A store killed before its rename leaves its
    /// staging file (`.chronicle-blob-*`), which the next
    /// [`Store::open`](crate::Store::open) removes.
    ///
    /// When the blob's file is already there and reads back as `content`, it
    /// is left as it is, content type included, and so is one whose content
    /// is in the cache. A file that can't be read back as `content`, because
    /// it's cut short, damaged or holds other bytes, is replaced. An error that
    /// keeps the file from being read at all, such as a permission error, is
    /// returned. A content type longer than 65,535 bytes is refused.
    pub fn store(&self, content: &[u8], content_type: &str) -> Result<Hash> {
        let content_type_len = u16::try_from(content_type.len()).map_err(|_| {
            StoreError::InvalidOperation(format!(
                "store_blob: the content type is {} bytes, and a blob records at most 65535",
                content_type.len()
            ))
        })?;
        let hash = Hash::from_bytes(content);
        let blob_path = self.blob_path(&hash);

        if self.cache.lock().contains(&hash) && blob_path.exists() {
            return Ok(hash);
        }
        match Self::read_file(&blob_path) {
            Ok(Some(stored)) if stored.content == content => {
                self.cache.lock().put(hash, stored);
                return Ok(hash);
            }
            Ok(_) => {}
            Err(error) if unreadable(&error) => {}
            Err(error) => return Err(error),
        }

        self.write(&hash, &blob_path, content, content_type, content_type_len)?;

        self.cache.lock().put(hash, CachedBlob {
            content: content.to_vec(),
            content_type: content_type.to_string(),
        });

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
        #[cfg(unix)]
        let new_shard = !shard_dir.is_dir();
        fs::create_dir_all(&shard_dir)?;

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
        fs::rename(&staging, blob_path).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "cannot rename the new blob {} into place: {error}",
                    blob_path.display()
                ),
            )
        })?;
        staging.disable_cleanup(true);

        #[cfg(unix)]
        {
            let synced = crate::atomic_file::sync_directory_at(&shard_dir).and_then(|()| {
                if new_shard {
                    crate::atomic_file::sync_directory_at(&self.path)
                } else {
                    Ok(())
                }
            });
            synced.map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!(
                        "the new blob {} is in place, but syncing its directory failed: {error}",
                        blob_path.display()
                    ),
                )
            })?;
        }

        Ok(())
    }

    /// Get a blob by its hash, or `None` when none is stored under it.
    ///
    /// A blob file that can't be read back, such as one cut short or damaged,
    /// is an error. Storing the same content again replaces it.
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
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let size = file.metadata()?.len();

        // Read and verify magic
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != BLOB_MAGIC {
            return Err(StoreError::InvalidFormat("Invalid blob magic".into()));
        }

        // Read version
        let mut version = [0u8; 1];
        file.read_exact(&mut version)?;
        if version[0] != BLOB_VERSION {
            return Err(StoreError::InvalidFormat(format!(
                "Unsupported blob version: {}",
                version[0]
            )));
        }

        // Read content type
        let mut content_type_len_bytes = [0u8; 2];
        file.read_exact(&mut content_type_len_bytes)?;
        let content_type_len = u16::from_le_bytes(content_type_len_bytes) as usize;

        let mut content_type_bytes = vec![0u8; content_type_len];
        file.read_exact(&mut content_type_bytes)?;
        let content_type = String::from_utf8_lossy(&content_type_bytes).into_owned();

        // Read content
        let mut content_len_bytes = [0u8; 8];
        file.read_exact(&mut content_len_bytes)?;
        let content_len = u64::from_le_bytes(content_len_bytes);
        let room = size.saturating_sub(4 + 1 + 2 + content_type_len as u64 + 8 + 4);
        if content_len > room {
            return Err(StoreError::InvalidFormat(format!(
                "blob file {} names {content_len} bytes of content but has room for {room}",
                path.display()
            )));
        }

        let mut content = vec![0u8; content_len as usize];
        file.read_exact(&mut content)?;

        // Read and verify checksum
        let mut checksum_bytes = [0u8; 4];
        file.read_exact(&mut checksum_bytes)?;
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
        crate::atomic_file::remove_files_staged_as(&self.path, STAGING_PREFIX, "blob");
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
    match error {
        StoreError::InvalidFormat(_) | StoreError::ChecksumMismatch { .. } => true,
        StoreError::Io(error) => error.kind() == io::ErrorKind::UnexpectedEof,
        _ => false,
    }
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
