//! Blob storage implementation.

use crate::error::{Result, StoreError};
use crate::types::{Blob, Hash};
use lru::LruCache;
use parking_lot::Mutex;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
#[cfg(any(unix, test))]
use std::sync::atomic::Ordering;

/// Magic bytes for blob files.
const BLOB_MAGIC: &[u8; 4] = b"BLB\0";

/// Current blob format version.
const BLOB_VERSION: u8 = 1;

/// Starts the name of every staging file: a store writes a new blob file
/// under one in the blob directory, beside the shards, and renames it into
/// its shard.
const STAGING_PREFIX: &str = ".chronicle-blob-";

/// Makes the next store in this process exit while it writes its staging
/// file, so a test can see what a crash there leaves.
#[cfg(test)]
static EXIT_WHILE_WRITING: AtomicBool = AtomicBool::new(false);

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

    /// LRU cache for recently accessed blobs. It holds only blobs this
    /// process wrote, or read from a whole file.
    cache: Mutex<LruCache<Hash, CachedBlob>>,

    /// Whether a store has said that it went without syncing a directory
    /// this process may not open.
    unsynced_directory_reported: AtomicBool,
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
            unsynced_directory_reported: AtomicBool::new(false),
        })
    }

    /// Store a blob, returning its hash.
    ///
    /// The same content is stored once. When a whole file for it is already
    /// stored, this writes nothing and returns the hash, and the file keeps
    /// the content type it was first stored with. A file under the hash that
    /// isn't whole, such as one a crash left while an earlier release wrote
    /// it in place, is replaced. Whole means what [`BlobStorage::get`]
    /// checks, so once this returns, `get` returns the content.
    ///
    /// A new file is written under a staging name in the blob directory,
    /// synced, and then renamed to its final path, replacing any file there.
    /// A crash therefore leaves the blob's file whole or absent, never part
    /// of it. On Unix the directory it is renamed into is synced too, and so
    /// is the blob directory when the shard is new, so that a blob whose
    /// store returned survives a power loss. As for metadata checkpoints,
    /// that sync is skipped where this process may not open the directory,
    /// which is logged once, and where the system doesn't sync directories.
    /// If a sync fails after the rename, this returns the error with the
    /// blob in place, and a later store of the same content finds it whole
    /// and returns without that sync. A process killed before the rename
    /// leaves its staging file, and the next `Store::open` removes it.
    ///
    /// The content type is at most 65,535 bytes, the most its length field
    /// holds; a longer one is refused before anything is written.
    pub fn store(&self, content: &[u8], content_type: &str) -> Result<Hash> {
        let hash = Hash::from_bytes(content);

        if self.cache.lock().contains(&hash) {
            return Ok(hash);
        }

        let blob_path = self.blob_path(&hash);
        if let Some(stored_type) = self.whole_file_holding(&blob_path, content) {
            self.cache.lock().put(
                hash,
                CachedBlob {
                    content: content.to_vec(),
                    content_type: stored_type,
                },
            );
            return Ok(hash);
        }

        self.write(&hash, &blob_path, content, content_type)?;

        // Add to cache
        self.cache.lock().put(
            hash,
            CachedBlob {
                content: content.to_vec(),
                content_type: content_type.to_string(),
            },
        );

        Ok(hash)
    }

    /// Get a blob by its hash.
    ///
    /// The file is read and checked whole: its framing, the checksum stored
    /// after the content, and that the content hashes to `hash`. A file that
    /// fails a check is an error every time it is read, until a store of the
    /// same content replaces it.
    pub fn get(&self, hash: &Hash) -> Result<Option<Blob>> {
        // Check cache first
        if let Some(cached) = self.cache.lock().get(hash).cloned() {
            return Ok(Some(Blob {
                hash: *hash,
                content: cached.content,
                content_type: cached.content_type,
            }));
        }

        let mut bytes = match fs::read(self.blob_path(hash)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let (content_type, content) = decode(&bytes)?;

        // Verify hash
        let computed_hash = Hash::from_bytes(&bytes[content.clone()]);
        if &computed_hash != hash {
            return Err(StoreError::HashMismatch {
                expected: *hash,
                got: computed_hash,
            });
        }
        bytes.truncate(content.end);
        bytes.drain(..content.start);
        let content = bytes;

        // Add to cache
        self.cache.lock().put(
            *hash,
            CachedBlob {
                content: content.clone(),
                content_type: content_type.clone(),
            },
        );

        Ok(Some(Blob {
            hash: *hash,
            content,
            content_type,
        }))
    }

    /// Check if a blob exists: whether this process holds it, or a file is
    /// stored under its hash. The file isn't read, so this doesn't say it is
    /// whole; [`BlobStorage::get`] checks that.
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

    /// Remove the staging files that stores killed before their rename left
    /// in the blob directory. The caller holds the store lock and calls this
    /// before any store, so none of them is a store in progress. One that
    /// can't be removed is reported and left, and so are all of them when
    /// the blob directory can't be listed.
    pub(crate) fn remove_staging_files(&self) {
        let entries = match fs::read_dir(&self.path) {
            Ok(entries) => entries,
            Err(error) => {
                tracing::warn!(
                    directory = %self.path.display(),
                    %error,
                    "could not look for blob staging files left by interrupted stores"
                );
                return;
            }
        };
        for entry in entries.flatten() {
            let staged = entry
                .file_name()
                .as_encoded_bytes()
                .starts_with(STAGING_PREFIX.as_bytes());
            if !staged || !entry.file_type().is_ok_and(|kind| kind.is_file()) {
                continue;
            }
            let path = entry.path();
            match fs::remove_file(&path) {
                Ok(()) => tracing::warn!(
                    path = %path.display(),
                    "removed a blob staging file left by an interrupted store"
                ),
                Err(error) => tracing::warn!(
                    path = %path.display(),
                    %error,
                    "could not remove a blob staging file left by an interrupted store"
                ),
            }
        }
    }

    /// The content type of the file at `path`, when it is a whole blob file
    /// holding `content`. `None` when there is no file, and when there is
    /// one that can't be read or isn't whole, which the caller replaces.
    fn whole_file_holding(&self, path: &Path, content: &[u8]) -> Option<String> {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return None,
            Err(error) => {
                tracing::warn!(
                    path = %path.display(),
                    %error,
                    "could not read a stored blob file; storing its content again replaces it"
                );
                return None;
            }
        };
        let problem = match decode(&bytes) {
            Ok((content_type, stored)) if bytes[stored.clone()] == *content => {
                return Some(content_type)
            }
            Ok(_) => "it holds other content than its name says".to_string(),
            Err(error) => error.to_string(),
        };
        tracing::warn!(
            path = %path.display(),
            problem = %problem,
            "a stored blob file isn't whole; storing its content again replaces it"
        );
        None
    }

    /// Write the blob file for `content` under a staging name, sync it, and
    /// rename it to `blob_path`, then sync the shard directory.
    fn write(
        &self,
        hash: &Hash,
        blob_path: &Path,
        content: &[u8],
        content_type: &str,
    ) -> Result<()> {
        let content_type_len = u16::try_from(content_type.len()).map_err(|_| {
            StoreError::InvalidOperation(format!(
                "a blob's content type is at most {} bytes, and this one is {}",
                u16::MAX,
                content_type.len()
            ))
        })?;

        let shard_dir = self.shard_path(hash);
        if !shard_dir.is_dir() {
            fs::create_dir_all(&shard_dir)?;
            self.sync_directory(&self.path).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!(
                        "created the blob shard {}, but syncing the blob directory failed: {error}",
                        shard_dir.display()
                    ),
                )
            })?;
        }

        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        // File::create's mode, 0666 less the umask, which blob files have
        // always had.
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o666);
        let mut staged = tempfile::Builder::new()
            .prefix(STAGING_PREFIX)
            .make_in(&self.path, |name| options.open(name))?;

        let mut header = Vec::with_capacity(4 + 1 + 2 + content_type.len() + 8);
        header.extend_from_slice(BLOB_MAGIC);
        header.push(BLOB_VERSION);
        header.extend_from_slice(&content_type_len.to_le_bytes());
        header.extend_from_slice(content_type.as_bytes());
        header.extend_from_slice(&(content.len() as u64).to_le_bytes());
        let file = staged.as_file_mut();
        file.write_all(&header)?;
        #[cfg(test)]
        if EXIT_WHILE_WRITING.load(Ordering::Relaxed) {
            file.write_all(&content[..content.len() / 2])?;
            file.sync_all()?;
            std::process::exit(73);
        }
        file.write_all(content)?;
        file.write_all(&crc32fast::hash(content).to_le_bytes())?;
        file.sync_all()?;

        // If the rename fails, dropping `staging` removes the staging file.
        // A rename that took effect despite reporting an error has moved the
        // file off that name, so the removal can't reach the blob.
        let (file, mut staging) = staged.into_parts();
        drop(file);
        fs::rename(&staging, blob_path).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "cannot rename the new blob file over {}: {error}",
                    blob_path.display()
                ),
            )
        })?;
        staging.disable_cleanup(true);

        self.sync_directory(&shard_dir).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "the new blob file {} is in place, but syncing its directory failed: {error}",
                    blob_path.display()
                ),
            )
            .into()
        })
    }

    /// Sync `directory`, so that an entry a store just made in it survives a
    /// power loss, as metadata checkpoints sync theirs. Where this process
    /// may not open the directory, the sync is skipped and that is logged
    /// once; where the system doesn't sync directories, it is skipped
    /// silently. Windows has no portable directory sync here.
    fn sync_directory(&self, directory: &Path) -> io::Result<()> {
        #[cfg(unix)]
        match fs::File::open(directory) {
            Ok(handle) => crate::atomic_file::sync_directory(&handle)?,
            Err(error) if crate::atomic_file::directory_open_refused(&error) => {
                if !self
                    .unsynced_directory_reported
                    .swap(true, Ordering::Relaxed)
                {
                    tracing::warn!(
                        directory = %directory.display(),
                        %error,
                        "storing blobs without syncing their directory, which this process \
                         may not open: a power loss may lose the newest blobs"
                    );
                }
            }
            Err(error) => return Err(error),
        }
        #[cfg(not(unix))]
        let _ = (directory, &self.unsynced_directory_reported);
        Ok(())
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

/// The content type of a blob file's bytes and where its content lies in
/// them, once the magic, version and lengths fit and the content matches the
/// checksum stored after it. A length field is checked against the bytes
/// there are before anything is taken from them, so a damaged one can't ask
/// for more memory than the file holds. A file that ends early is an
/// `UnexpectedEof` I/O error, as reading it field by field gave. Bytes past
/// the checksum are ignored, as they always have been.
fn decode(bytes: &[u8]) -> Result<(String, Range<usize>)> {
    let mut at = 0usize;
    let mut take = |len: usize| -> Result<Range<usize>> {
        match at.checked_add(len).filter(|&end| end <= bytes.len()) {
            Some(end) => {
                let field = at..end;
                at = end;
                Ok(field)
            }
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "the blob file ends after {} bytes, before a field of {len} bytes at {at}",
                    bytes.len()
                ),
            )
            .into()),
        }
    };

    if bytes[take(4)?] != BLOB_MAGIC[..] {
        return Err(StoreError::InvalidFormat("Invalid blob magic".into()));
    }
    let version = bytes[take(1)?.start];
    if version != BLOB_VERSION {
        return Err(StoreError::InvalidFormat(format!(
            "Unsupported blob version: {}",
            version
        )));
    }

    let content_type_len = u16::from_le_bytes(bytes[take(2)?].try_into().unwrap()) as usize;
    let content_type = String::from_utf8_lossy(&bytes[take(content_type_len)?]).into_owned();

    let content_len = u64::from_le_bytes(bytes[take(8)?].try_into().unwrap());
    let content = take(usize::try_from(content_len).unwrap_or(usize::MAX))?;

    // Read and verify checksum
    let stored_checksum = u32::from_le_bytes(bytes[take(4)?].try_into().unwrap());
    let computed_checksum = crc32fast::hash(&bytes[content.clone()]);
    if stored_checksum != computed_checksum {
        return Err(StoreError::ChecksumMismatch {
            expected: stored_checksum,
            got: computed_checksum,
        });
    }

    Ok((content_type, content))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use tempfile::TempDir;

    /// A new storage over the blobs in `dir`, with nothing cached, so its
    /// reads and stores go to the files.
    fn reopened(dir: &TempDir) -> BlobStorage {
        BlobStorage::new(dir.path().join("blobs"), 100).unwrap()
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
    // or at its full length with the pages that never reached the disk read
    // back as zeros. Each fails get, and the next store of the content
    // replaces it. Those releases found a file there and returned, so it
    // stayed torn and every get failed.
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

    // A whole file under another content's hash fails get, and a store of
    // the content its name says replaces it.
    #[test]
    fn a_file_holding_other_content_is_replaced() {
        let dir = TempDir::new().unwrap();
        let storage = reopened(&dir);
        let other = storage.store(b"other content", "text/plain").unwrap();
        let hash = Hash::from_bytes(b"named content");
        fs::create_dir_all(storage.shard_path(&hash)).unwrap();
        fs::copy(storage.blob_path(&other), storage.blob_path(&hash)).unwrap();

        assert!(matches!(
            reopened(&dir).get(&hash),
            Err(StoreError::HashMismatch { .. })
        ));
        assert_eq!(
            reopened(&dir)
                .store(b"named content", "text/plain")
                .unwrap(),
            hash
        );
        assert_eq!(
            reopened(&dir).get(&hash).unwrap().unwrap().content,
            b"named content"
        );
    }

    // A whole file is left as it is: storing its content again from a
    // storage that hasn't read it writes nothing, and the file keeps the
    // content type it was first stored with.
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
        // The cache answers with what the file holds.
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

    // get's errors for a file that isn't whole are the ones it gave when it
    // read the file field by field.
    #[test]
    fn a_file_that_is_not_whole_fails_get_as_before() {
        let dir = TempDir::new().unwrap();
        let hash = reopened(&dir).store(b"some content", "text/plain").unwrap();
        let path = reopened(&dir).blob_path(&hash);
        let whole = fs::read(&path).unwrap();
        let get = |bytes: &[u8]| {
            fs::write(&path, bytes).unwrap();
            reopened(&dir).get(&hash).unwrap_err()
        };

        let mut magic = whole.clone();
        magic[0] = b'X';
        assert!(matches!(get(&magic), StoreError::InvalidFormat(m) if m == "Invalid blob magic"));
        let mut version = whole.clone();
        version[4] = 9;
        assert!(
            matches!(get(&version), StoreError::InvalidFormat(m) if m == "Unsupported blob version: 9")
        );
        let mut checksum = whole.clone();
        *checksum.last_mut().unwrap() ^= 0xff;
        assert!(matches!(
            get(&checksum),
            StoreError::ChecksumMismatch { .. }
        ));
        match get(&whole[..whole.len() - 2]) {
            StoreError::Io(error) => assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof),
            other => panic!("expected an early end, got {other:?}"),
        }
        // Bytes past the checksum are ignored, as they were.
        let mut longer = whole.clone();
        longer.extend_from_slice(b"trailing");
        fs::write(&path, &longer).unwrap();
        assert_eq!(
            reopened(&dir).get(&hash).unwrap().unwrap().content,
            b"some content"
        );
    }

    // A length field damaged to claim more bytes than the file holds fails
    // the read before anything that size is allocated. Reading field by
    // field allocated the claimed length, which aborts on u64::MAX.
    #[test]
    fn a_damaged_length_field_fails_the_read_without_allocating_it() {
        let dir = TempDir::new().unwrap();
        let hash = reopened(&dir).store(b"some content", "text/plain").unwrap();
        let path = reopened(&dir).blob_path(&hash);
        let whole = fs::read(&path).unwrap();
        let content_len_at = 4 + 1 + 2 + "text/plain".len();
        for (at, claimed) in [
            (content_len_at, &u64::MAX.to_le_bytes()[..]),
            (5, &[0xff, 0xff][..]),
        ] {
            let mut bytes = whole.clone();
            bytes[at..at + claimed.len()].copy_from_slice(claimed);
            fs::write(&path, &bytes).unwrap();
            match reopened(&dir).get(&hash) {
                Err(StoreError::Io(error)) => {
                    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof, "{error}")
                }
                other => panic!("expected the read to fail, got {other:?}"),
            }
            reopened(&dir).store(b"some content", "text/plain").unwrap();
            assert_eq!(fs::read(&path).unwrap(), whole);
        }
    }

    // A content type longer than its 16-bit length field is refused before
    // anything is written. Its length used to wrap, writing a file no read
    // could parse.
    #[test]
    fn a_content_type_too_long_for_its_length_field_is_refused() {
        let dir = TempDir::new().unwrap();
        let storage = reopened(&dir);
        let hash = Hash::from_bytes(b"content");
        let error = storage
            .store(b"content", &"x".repeat(u16::MAX as usize + 1))
            .unwrap_err();
        assert!(matches!(error, StoreError::InvalidOperation(_)), "{error}");
        assert!(!storage.exists(&hash));
        assert_eq!(staged(&dir.path().join("blobs")), 0);

        let longest = "y".repeat(u16::MAX as usize);
        storage.store(b"content", &longest).unwrap();
        assert_eq!(
            reopened(&dir).get(&hash).unwrap().unwrap().content_type,
            longest
        );
    }

    // Stores of one content racing from several storages each stage their
    // own file and rename it into place, and reads meanwhile find the blob
    // whole or absent, never partly written.
    #[test]
    fn racing_stores_and_reads_of_one_blob_find_it_whole_or_absent() {
        let dir = TempDir::new().unwrap();
        let content: Vec<u8> = (0..200_000u32).map(|i| (i % 241) as u8).collect();
        let hash = Hash::from_bytes(&content);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    assert_eq!(reopened(&dir).store(&content, "image/png").unwrap(), hash);
                });
            }
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..50 {
                        if let Some(blob) = reopened(&dir).get(&hash).unwrap() {
                            assert_eq!(blob.content, content);
                        }
                    }
                });
            }
        });
        assert_eq!(staged(&dir.path().join("blobs")), 0);
        let storage = reopened(&dir);
        assert_eq!(fs::read_dir(storage.shard_path(&hash)).unwrap().count(), 1);
        assert_eq!(storage.get(&hash).unwrap().unwrap().content, content);
    }

    fn interrupted_content() -> Vec<u8> {
        (0..100_000u32).map(|i| (i % 253) as u8).collect()
    }

    // This child exits inside a store, half way through writing the
    // content to its staging file, bypassing every destructor.
    #[test]
    fn interrupted_store_child() {
        let Some(path) = std::env::var_os("CHRONICLE_BLOB_STORE_CHILD_PATH") else {
            return;
        };
        let storage = BlobStorage::new(Path::new(&path), 100).unwrap();
        EXIT_WHILE_WRITING.store(true, Ordering::Relaxed);
        storage.store(&interrupted_content(), "image/png").unwrap();
        panic!("child should exit before the rename");
    }

    // A store killed while it writes leaves its staging file and nothing at
    // the blob's final path, so the next store writes the blob whole, and the
    // sweep at the store's next open removes what was left.
    #[test]
    fn a_store_killed_while_writing_leaves_nothing_at_the_final_path() {
        let dir = TempDir::new().unwrap();
        let blobs = dir.path().join("blobs");
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "blobs::storage::tests::interrupted_store_child"])
            .env("CHRONICLE_BLOB_STORE_CHILD_PATH", &blobs)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(73));

        let content = interrupted_content();
        let hash = Hash::from_bytes(&content);
        let storage = reopened(&dir);
        assert!(!storage.exists(&hash));
        assert!(storage.get(&hash).unwrap().is_none());
        assert_eq!(staged(&blobs), 1);
        // The staging file lies beside the shards, so neither the listing
        // nor the size counts it.
        assert!(storage.list().unwrap().is_empty());
        assert_eq!(storage.total_size().unwrap(), 0);

        assert_eq!(storage.store(&content, "image/png").unwrap(), hash);
        assert_eq!(reopened(&dir).get(&hash).unwrap().unwrap().content, content);
        storage.remove_staging_files();
        assert_eq!(staged(&blobs), 0);
        assert_eq!(storage.list().unwrap(), vec![hash]);
        assert_eq!(reopened(&dir).get(&hash).unwrap().unwrap().content, content);
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
}
