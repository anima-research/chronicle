//! Publish one metadata checkpoint without truncating its previous version.

use crate::error::Result;
use parking_lot::Mutex;
use same_file::Handle;
use sha2::{Digest, Sha256};
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

/// Starts the name of every staging file: a save writes one beside its
/// checkpoint and renames it over the checkpoint.
const STAGING_PREFIX: &str = ".chronicle-checkpoint-";

/// How many symbolic links a save follows to the file it replaces. Linux
/// bounds a path lookup the same way, failing with ELOOP.
const MAX_LINKS: usize = 40;

/// Remembers the last checkpoint this writer successfully made durable.
/// The serialized digest avoids a second in-memory copy of large field indexes.
/// File identity prevents a deleted/replaced checkpoint from being skipped.
#[derive(Debug, Default)]
pub(crate) struct Checkpoint {
    saved: Mutex<Option<SavedCheckpoint>>,
    /// Whether this writer has said that it saves without syncing its
    /// checkpoint's directory, which it may not open.
    unsynced_directory_reported: AtomicBool,
}

#[derive(Debug)]
struct SavedCheckpoint {
    path: PathBuf,
    digest: [u8; 32],
    identity: Handle,
    stamp: FileStamp,
}

#[derive(Debug, PartialEq)]
struct FileStamp {
    len: u64,
    modified: SystemTime,
    #[cfg(unix)]
    changed: (i64, i64),
}

impl FileStamp {
    fn read(metadata: Metadata) -> io::Result<Self> {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Ok(Self {
            len: metadata.len(),
            modified: metadata.modified()?,
            #[cfg(unix)]
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }
}

// A cloned manager owns a new checkpoint writer. It must establish durability
// itself rather than inherit another manager's publication acknowledgement.
impl Clone for Checkpoint {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl Checkpoint {
    pub(crate) fn save(&self, path: &Path, chunks: &[&[u8]]) -> Result<()> {
        self.save_with(path, chunks, || {
            let published = atomic_write(path, |file| {
                for chunk in chunks {
                    file.write_all(chunk)?;
                }
                Ok(())
            })?;
            if let Some(error) = published.unsynced_directory {
                if !self
                    .unsynced_directory_reported
                    .swap(true, Ordering::Relaxed)
                {
                    tracing::warn!(
                        %error,
                        "saving this checkpoint without syncing its directory, which this \
                         process may not open: a power loss may roll the newest save back"
                    );
                }
            }
            Ok(published.file)
        })
    }

    fn save_with(
        &self,
        path: &Path,
        chunks: &[&[u8]],
        publish: impl FnOnce() -> Result<File>,
    ) -> Result<()> {
        let mut hash = Sha256::new();
        for chunk in chunks {
            hash.update(chunk);
        }
        let digest: [u8; 32] = hash.finalize().into();
        let mut saved = self.saved.lock();
        if let Some(previous) = saved.as_ref() {
            if previous.path == path && previous.digest == digest {
                if let Ok(current) = Handle::from_path(path) {
                    if current == previous.identity
                        && current
                            .as_file()
                            .metadata()
                            .and_then(FileStamp::read)
                            .ok()
                            .as_ref()
                            == Some(&previous.stamp)
                    {
                        return Ok(());
                    }
                }
            }
        }

        // Clear BEFORE attempting publication: rename may succeed and then
        // directory sync fail. Retrying must run all barriers in that case.
        *saved = None;
        let file = publish()?;

        // Failure to obtain a cache identity is not a failed checkpoint: the
        // publication above is already durable. It just prevents skipping the
        // next save. Concurrent out-of-band file edits are not supported.
        if let Ok(identity) = Handle::from_file(file) {
            if let Ok(stamp) = identity.as_file().metadata().and_then(FileStamp::read) {
                *saved = Some(SavedCheckpoint {
                    path: path.to_owned(),
                    digest,
                    identity,
                    stamp,
                });
            }
        }
        Ok(())
    }
}

/// A checkpoint `atomic_write` has published: its new file, still open, and
/// why its directory wasn't synced, if it wasn't.
#[derive(Debug)]
struct Published {
    file: File,
    unsynced_directory: Option<io::Error>,
}

/// Write a complete checkpoint beside its destination, sync it, then replace
/// the destination atomically. Readers see either the old file or the new one.
/// This is a per-file guarantee, not a transaction across metadata files.
///
/// Unix also syncs the directory, so the replacement survives a power loss.
/// Where the writer may not open the directory, the save goes without that
/// sync and says why in what it returns. Where the system doesn't sync
/// directories (EBADF or EINVAL from the sync), it goes without it silently.
/// Windows supports the file sync and atomic replacement, but has no portable
/// directory-sync equivalent here. Errors before replacement leave the old
/// checkpoint intact; a directory-sync error is returned after replacement.
///
/// A symbolic link at `path` is followed, as git follows one at a lockfile's
/// target, and the file it leads to is replaced in its own directory. The link
/// stays, and so does the volume it points to.
///
/// The replacement is a new file, so any ACL it has comes from its directory's
/// defaults, as for any new file there. On Unix it takes the old file's owner
/// and group as far as the writer may set them, and its mode, less what was
/// granted to an owner or group it couldn't take.
///
/// A save that fails before the rename removes its staging file. A process
/// killed before then leaves it behind. Loaders only open the final names, and
/// the next `Store::open` removes the ones in the store directory.
fn atomic_write(path: &Path, write: impl FnOnce(&mut File) -> Result<()>) -> Result<Published> {
    let path = resolve_links(path)?;
    // A bare relative filename has an empty parent, which means the current
    // directory rather than a directory that can be opened by its empty name.
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));

    #[cfg(unix)]
    let (directory, unsynced_directory) = match File::open(parent) {
        Ok(directory) => (Some(directory), None),
        Err(error) if directory_open_refused(&error) => (
            None,
            Some(io::Error::new(
                error.kind(),
                format!(
                    "cannot open checkpoint directory {} to sync it: {error}",
                    parent.display()
                ),
            )),
        ),
        Err(error) => return Err(error.into()),
    };
    #[cfg(not(unix))]
    let unsynced_directory = None;

    #[cfg(unix)]
    let previous = match fs::metadata(&path) {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };

    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    // A first checkpoint gets File::create's mode, 0666 less the umask. A
    // replacement stays private until it takes the old file's mode.
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(
        &mut options,
        if previous.is_some() { 0o600 } else { 0o666 },
    );
    let mut temporary = tempfile::Builder::new()
        .prefix(STAGING_PREFIX)
        .make_in(parent, |name| options.open(name))?;

    write(temporary.as_file_mut())?;
    #[cfg(unix)]
    if let Some(previous) = &previous {
        use std::os::unix::fs::MetadataExt;
        carry_over(
            temporary.as_file(),
            previous.uid(),
            previous.gid(),
            previous.mode(),
        )?;
    }
    temporary.as_file().sync_all()?;

    // std's rename converts long Windows paths, which tempfile's persist
    // passes to MoveFileExW unconverted. If the rename fails, dropping
    // `staging` deletes whatever is at the staging name. A rename that took
    // effect despite reporting an error has moved the file off that name, so
    // the cleanup can't reach a published checkpoint. `file` stays open
    // across the rename, which Windows allows because std opens every file
    // sharing delete access.
    let (file, mut staging) = temporary.into_parts();
    fs::rename(&staging, &path).map_err(|error| {
        // The OS error alone names neither the file nor the step, and this is
        // where a single-file bind mount or a sticky directory refuses a save.
        io::Error::new(
            error.kind(),
            format!(
                "cannot rename the new checkpoint over {}: {error}",
                path.display()
            ),
        )
    })?;
    staging.disable_cleanup(true);

    #[cfg(unix)]
    if let Some(directory) = &directory {
        sync_directory(directory)?;
    }

    Ok(Published {
        file,
        unsynced_directory,
    })
}

/// The file a save replaces: `path` itself, or where the symbolic links at
/// `path` lead, whether or not that file exists yet.
fn resolve_links(path: &Path) -> io::Result<PathBuf> {
    let mut target = path.to_owned();
    let mut followed = 0;
    loop {
        match fs::symlink_metadata(&target) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                if followed == MAX_LINKS {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "checkpoint {} leads through more than {MAX_LINKS} symbolic links",
                            path.display()
                        ),
                    ));
                }
                followed += 1;
                // A relative link is resolved from the directory holding it.
                let link = fs::read_link(&target)?;
                target = target.parent().unwrap_or(Path::new("")).join(link);
            }
            Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
            _ => return Ok(target),
        }
    }
}

/// Give a replacement the old file's owner and group, as far as the writer
/// may set them, and then its mode, less what was granted to an id it
/// couldn't take. Each is changed only where it differs, so a filesystem that
/// reports one owner and mode for every file is never asked to change them.
#[cfg(unix)]
fn carry_over(file: &File, uid: u32, gid: u32, mode: u32) -> io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let mut staged = file.metadata()?;
    if (staged.uid(), staged.gid()) != (uid, gid) {
        restore_owner(file, uid, gid)?;
        staged = file.metadata()?;
    }
    // Setuid runs a file as its owner, so it's kept only where the owner
    // carried over. The group class bits and setgid were granted to the old
    // group, so where the group didn't carry over, the group the file has
    // instead gets none of them. cp keeps the group bits, and drops every
    // special bit whenever it can't set both ids (coreutils copy.c, after
    // set_owner); cp makes a copy, where this replaces a file in place.
    let mut mode = mode & 0o7777;
    if staged.uid() != uid {
        mode &= !(libc::S_ISUID as u32);
    }
    if staged.gid() != gid {
        mode &= !(libc::S_ISGID as u32 | 0o070);
    }
    if staged.mode() & 0o7777 != mode {
        file.set_permissions(fs::Permissions::from_mode(mode))?;
    }
    Ok(())
}

/// Set a replacement's owner and group, or else its group alone, as cp does.
/// The errors cp's `chown_failure_ok` accepts mean the writer may not keep
/// ownership, and the file stays the writer's: EPERM without the privilege,
/// EINVAL for an id this user namespace doesn't map, and EACCES, which Linux
/// CIFS returns instead of EPERM (https://bugs.gnu.org/65599). Any other
/// error fails the save.
#[cfg(unix)]
fn restore_owner(file: &File, uid: u32, gid: u32) -> io::Result<()> {
    use std::os::unix::fs::fchown;
    match fchown(file, Some(uid), Some(gid)) {
        Err(error) if ownership_refused(&error) => match fchown(file, None, Some(gid)) {
            Err(error) if ownership_refused(&error) => Ok(()),
            other => other,
        },
        other => other,
    }
}

/// Whether a failed fchown means the writer may not keep ownership, as
/// coreutils' `chown_failure_ok` reads it, rather than that the save failed.
#[cfg(unix)]
fn ownership_refused(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::EPERM | libc::EINVAL | libc::EACCES)
    )
}

/// Sync the directory a checkpoint was renamed in, so that the rename
/// survives a power loss.
#[cfg(unix)]
fn sync_directory(directory: &File) -> io::Result<()> {
    match directory.sync_all() {
        Err(error) if directory_sync_refused(&error) => Ok(()),
        other => other,
    }
}

/// Whether a failed directory sync means the system doesn't sync
/// directories. PostgreSQL reads EBADF and EINVAL that way ("Some OSes don't
/// allow us to fsync directories at all", src/backend/storage/file/fd.c,
/// fsync_fname_ext). A save there goes without the directory sync, as saves
/// went when they rewrote checkpoints in place.
#[cfg(unix)]
fn directory_sync_refused(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(libc::EBADF | libc::EINVAL))
}

/// Whether failing to open a checkpoint's directory means the writer may not
/// open it, as in a directory it can write but not read, rather than that the
/// save failed. A save there goes without the directory sync: PostgreSQL's
/// fsync_fname_ext returns success when opening a directory fails with
/// EACCES (src/backend/storage/file/fd.c), and SQLite's unixSync goes on
/// when it can't open the directory (src/os_unix.c).
#[cfg(unix)]
fn directory_open_refused(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(libc::EACCES | libc::EPERM))
}

/// Remove the staging files that interrupted saves left in the store
/// directory. The caller holds the store lock, so no save of this store is in
/// progress, and nothing reads them. One that can't be removed is reported
/// and left. A linked checkpoint's stage is written beside the file the link
/// leads to, which may lie outside this directory.
pub(crate) fn remove_staging_files(directory: &Path) {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) => {
            tracing::warn!(
                directory = %directory.display(),
                %error,
                "could not look for checkpoint staging files left by interrupted saves"
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
                "removed a checkpoint staging file left by an interrupted save"
            ),
            Err(error) => tracing::warn!(
                path = %path.display(),
                %error,
                "could not remove a checkpoint staging file left by an interrupted save"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::process::Command;
    use tempfile::TempDir;

    #[test]
    fn unchanged_checkpoint_skips_publication_but_changed_bytes_replace_it() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.bin");
        let checkpoint = Checkpoint::default();
        checkpoint.save(&path, &[b"old", b" bytes"]).unwrap();
        let before = Handle::from_path(&path).unwrap();
        let stamp = FileStamp::read(before.as_file().metadata().unwrap()).unwrap();

        checkpoint
            .save_with(&path, &[b"old bytes"], || {
                panic!("unchanged durable checkpoint must not publish");
            })
            .unwrap();
        assert_eq!(Handle::from_path(&path).unwrap(), before);
        assert_eq!(
            FileStamp::read(fs::metadata(&path).unwrap()).unwrap(),
            stamp
        );

        checkpoint.save(&path, &[b"new bytes"]).unwrap();
        assert_ne!(Handle::from_path(&path).unwrap(), before);
        assert_eq!(fs::read(&path).unwrap(), b"new bytes");
    }

    #[test]
    fn failed_publication_invalidates_prior_acknowledgement() {
        for replace_first in [false, true] {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("state.bin");
            let checkpoint = Checkpoint::default();
            checkpoint.save(&path, &[b"old"]).unwrap();
            let result = checkpoint.save_with(&path, &[b"new"], || {
                if replace_first {
                    // Model an error after rename, such as failed directory sync.
                    // The caller must not infer success from the new file's bytes.
                    atomic_write(&path, |file| Ok(file.write_all(b"new")?))?;
                }
                Err(io::Error::new(io::ErrorKind::Other, "publication failed").into())
            });
            assert!(result.is_err());
            assert!(checkpoint.saved.lock().is_none());
            let current = Handle::from_path(&path).unwrap();
            let bytes: &[u8] = if replace_first { b"new" } else { b"old" };

            // Even equal on-disk content must be republished after the error.
            checkpoint.save(&path, &[bytes]).unwrap();
            assert_ne!(Handle::from_path(&path).unwrap(), current);
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
    }

    #[test]
    fn missing_replaced_and_modified_destinations_are_repaired() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.bin");
        let checkpoint = Checkpoint::default();
        checkpoint.save(&path, &[b"expected"]).unwrap();

        atomic_write(&path, |file| Ok(file.write_all(b"replaced")?)).unwrap();
        checkpoint.save(&path, &[b"expected"]).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"expected");

        fs::write(&path, b"short").unwrap();
        checkpoint.save(&path, &[b"expected"]).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"expected");

        fs::remove_file(&path).unwrap();
        checkpoint.save(&path, &[b"expected"]).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"expected");
    }

    #[test]
    fn another_path_or_writer_must_establish_its_own_durability() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.bin");
        let alias = dir.path().join("alias.bin");
        let checkpoint = Checkpoint::default();
        checkpoint.save(&path, &[b"same"]).unwrap();
        let original = Handle::from_path(&path).unwrap();

        // A second hard-link name has not had its directory entry synced by us.
        fs::hard_link(&path, &alias).unwrap();
        checkpoint.save(&alias, &[b"same"]).unwrap();
        assert_ne!(Handle::from_path(&alias).unwrap(), original);

        let before = Handle::from_path(&alias).unwrap();
        checkpoint.clone().save(&alias, &[b"same"]).unwrap();
        assert_ne!(Handle::from_path(&alias).unwrap(), before);
        let before = Handle::from_path(&alias).unwrap();
        Checkpoint::default().save(&alias, &[b"same"]).unwrap();
        assert_ne!(Handle::from_path(&alias).unwrap(), before);
    }

    // A directory the writer can write and search but not read, as at mode
    // 0300, is saved in without its sync, and each writer says so once.
    #[cfg(unix)]
    #[test]
    fn unreadable_directory_is_saved_without_its_sync() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.bin");
        let checkpoint = Checkpoint::default();
        checkpoint.save(&path, &[b"previous checkpoint"]).unwrap();
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o300)).unwrap();
        // Restore even after an assertion failure, so TempDir can clean up.
        struct Restore<'a>(&'a Path);
        impl Drop for Restore<'_> {
            fn drop(&mut self) {
                fs::set_permissions(self.0, fs::Permissions::from_mode(0o700)).unwrap();
            }
        }
        let _restore = Restore(dir.path());
        if File::open(dir.path()).is_ok() {
            eprintln!("directory access checks bypassed by this user; permission probe skipped");
            return;
        }
        let log = logged(|| {
            checkpoint.save(&path, &[b"second"]).unwrap();
            checkpoint.save(&path, &[b"third"]).unwrap();
        });
        assert_eq!(fs::read(&path).unwrap(), b"third");
        assert_eq!(
            log.matches("without syncing its directory").count(),
            1,
            "{log}"
        );
        assert!(
            log.contains(&format!(
                "cannot open checkpoint directory {}",
                dir.path().display()
            )),
            "{log}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn only_a_refused_directory_open_is_passed_over() {
        for errno in [libc::EACCES, libc::EPERM] {
            assert!(
                directory_open_refused(&io::Error::from_raw_os_error(errno)),
                "{errno}"
            );
        }
        for errno in [libc::ENOENT, libc::ENOTDIR, libc::EIO] {
            assert!(
                !directory_open_refused(&io::Error::from_raw_os_error(errno)),
                "{errno}"
            );
        }
    }

    /// What `f` logs through `tracing` on this thread.
    #[cfg(unix)]
    fn logged(f: impl FnOnce()) -> String {
        #[derive(Clone, Default)]
        struct Buffer(std::sync::Arc<Mutex<Vec<u8>>>);
        impl Write for Buffer {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0.lock().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let buffer = Buffer::default();
        let writer = buffer.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        let bytes = buffer.0.lock().clone();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn partial_write_error_preserves_previous_checkpoint() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.bin");
        fs::write(&path, b"complete previous checkpoint").unwrap();

        let result = atomic_write(&path, |file| {
            file.write_all(b"partial")?;
            assert_eq!(fs::read(&path)?, b"complete previous checkpoint");
            Err(io::Error::new(io::ErrorKind::Other, "injected write failure").into())
        });

        assert!(result.is_err());
        assert_eq!(fs::read(&path).unwrap(), b"complete previous checkpoint");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn failed_first_write_leaves_no_checkpoint() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.bin");
        let result = atomic_write(&path, |file| {
            file.write_all(b"partial")?;
            Err(io::Error::new(io::ErrorKind::Other, "injected write failure").into())
        });
        assert!(result.is_err());
        assert!(!path.exists());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn replacement_keeps_old_open_file_intact() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("branches.bin");
        atomic_write(&path, |file| Ok(file.write_all(b"old checkpoint")?)).unwrap();
        let mut old_file = File::open(&path).unwrap();

        atomic_write(&path, |file| Ok(file.write_all(b"new")?)).unwrap();

        let mut old_bytes = Vec::new();
        old_file.read_to_end(&mut old_bytes).unwrap();
        assert_eq!(old_bytes, b"old checkpoint");
        assert_eq!(fs::read(&path).unwrap(), b"new");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn replacement_failure_is_reported_and_cleans_temporary_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.bin");
        fs::create_dir(&path).unwrap();
        let result = atomic_write(&path, |file| Ok(file.write_all(b"checkpoint")?));
        match result {
            Err(crate::StoreError::Io(error)) => assert!(
                error.to_string().contains(&format!(
                    "cannot rename the new checkpoint over {}",
                    path.display()
                )),
                "{error}"
            ),
            other => panic!("expected the rename to fail, got {other:?}"),
        }
        assert!(path.is_dir());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn first_checkpoint_uses_normal_creation_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let ordinary = dir.path().join("ordinary");
        File::create(&ordinary).unwrap();
        let path = dir.path().join("state.bin");
        atomic_write(&path, |file| Ok(file.write_all(b"checkpoint")?)).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            fs::metadata(&ordinary).unwrap().permissions().mode() & 0o777,
        );
    }

    #[cfg(unix)]
    #[test]
    fn replacement_preserves_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.bin");
        fs::write(&path, b"old").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        atomic_write(&path, |file| Ok(file.write_all(b"new")?)).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o640
        );
    }

    // A rename needs the directory's write permission, not the file's, so a
    // read-only checkpoint no longer stops a save.
    #[cfg(unix)]
    #[test]
    fn read_only_checkpoint_is_replaced() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.bin");
        fs::write(&path, b"old").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o444)).unwrap();
        atomic_write(&path, |file| Ok(file.write_all(b"new")?)).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"new");
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o444
        );
    }

    // A private store's checkpoint must not be readable by others while its
    // replacement is written: anyone who opened the stage then could read it.
    #[cfg(unix)]
    #[test]
    fn replacement_stays_private_while_written() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.bin");
        fs::write(&path, b"old").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        atomic_write(&path, |file| {
            let mode = file.metadata()?.permissions().mode();
            assert_eq!(mode & 0o077, 0, "stage mode {mode:o} grants others access");
            Ok(file.write_all(b"new")?)
        })
        .unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[cfg(unix)]
    fn ids() -> (u32, Vec<u32>) {
        // SAFETY: getgroups writes at most `len` ids into a buffer of that size.
        let euid = unsafe { libc::geteuid() };
        let len = unsafe { libc::getgroups(0, std::ptr::null_mut()) };
        let mut groups = vec![0; len.max(0) as usize];
        let len = unsafe { libc::getgroups(len, groups.as_mut_ptr()) };
        groups.truncate(len.max(0) as usize);
        groups.push(unsafe { libc::getegid() });
        (euid, groups.into_iter().map(|gid| gid as u32).collect())
    }

    // Whether this account may give its file the group `gid`. Membership alone
    // doesn't decide that everywhere (CAP_CHOWN, macOS's implicit groups).
    #[cfg(unix)]
    fn may_set_group(dir: &Path, gid: u32) -> bool {
        let probe = File::create(dir.join("probe")).unwrap();
        std::os::unix::fs::fchown(&probe, None, Some(gid)).is_ok()
    }

    // A writer that may not take on the old owner or group keeps the file as
    // its own. Neither setuid nor setgid carries over to an id it wasn't set
    // for, and the group the file keeps gets none of the old group's access.
    #[cfg(unix)]
    #[test]
    fn ownership_the_writer_may_not_set_is_left_and_what_it_granted_dropped() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let (euid, groups) = ids();
        if euid == 0 {
            eprintln!("running as root, which may set any owner; probe skipped");
            return;
        }
        let dir = TempDir::new().unwrap();
        let Some(foreign) = (0..4096)
            .filter(|gid| !groups.contains(gid))
            .find(|&gid| !may_set_group(dir.path(), gid))
        else {
            eprintln!("this account may set every group it was tried with; probe skipped");
            return;
        };
        let file = File::create(dir.path().join("stage")).unwrap();
        let before = file.metadata().unwrap();
        carry_over(&file, 0, foreign, 0o6664).unwrap();
        let after = file.metadata().unwrap();
        assert_eq!((after.uid(), after.gid()), (before.uid(), before.gid()));
        assert_eq!(after.permissions().mode() & 0o7777, 0o604);
    }

    // Where both ids carry over, so does everything the mode grants them.
    #[cfg(unix)]
    #[test]
    fn special_bits_survive_where_both_ids_carry_over() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let dir = TempDir::new().unwrap();
        let file = File::create(dir.path().join("stage")).unwrap();
        let ids = file.metadata().unwrap();
        carry_over(&file, ids.uid(), ids.gid(), 0o6640).unwrap();
        assert_eq!(
            file.metadata().unwrap().permissions().mode() & 0o7777,
            0o6640
        );
    }

    // A file whose owner, group and mode already match isn't asked to change
    // them, which a filesystem reporting one owner and mode for every file
    // may refuse. Changing either, even to the same values, sets the ctime.
    #[cfg(unix)]
    #[test]
    fn matching_owner_and_mode_are_left_alone() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let dir = TempDir::new().unwrap();
        let file = File::create(dir.path().join("stage")).unwrap();
        file.set_permissions(fs::Permissions::from_mode(0o640))
            .unwrap();
        let before = file.metadata().unwrap();
        // Past the coarse clock tick that ctime is stamped from.
        std::thread::sleep(std::time::Duration::from_millis(50));
        carry_over(&file, before.uid(), before.gid(), before.mode()).unwrap();
        let after = file.metadata().unwrap();
        assert_eq!(
            (after.ctime(), after.ctime_nsec()),
            (before.ctime(), before.ctime_nsec())
        );
    }

    #[cfg(unix)]
    #[test]
    fn only_the_refusals_cp_accepts_leave_ownership_to_the_writer() {
        for errno in [libc::EPERM, libc::EINVAL, libc::EACCES] {
            assert!(
                ownership_refused(&io::Error::from_raw_os_error(errno)),
                "{errno}"
            );
        }
        for errno in [libc::EIO, libc::ENOSPC] {
            assert!(
                !ownership_refused(&io::Error::from_raw_os_error(errno)),
                "{errno}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn only_a_refused_directory_sync_is_passed_over() {
        for errno in [libc::EBADF, libc::EINVAL] {
            assert!(
                directory_sync_refused(&io::Error::from_raw_os_error(errno)),
                "{errno}"
            );
        }
        for errno in [libc::EIO, libc::ENOSPC] {
            assert!(
                !directory_sync_refused(&io::Error::from_raw_os_error(errno)),
                "{errno}"
            );
        }
    }

    // Where the writer may set the old group, the group carries over even
    // though the owner can't, and so does setgid, which runs as that group.
    #[cfg(unix)]
    #[test]
    fn group_carries_over_when_the_owner_cannot() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let (euid, groups) = ids();
        if euid == 0 {
            eprintln!("running as root, which may set any owner; probe skipped");
            return;
        }
        let dir = TempDir::new().unwrap();
        let file = File::create(dir.path().join("stage")).unwrap();
        let current = file.metadata().unwrap().gid();
        let Some(other) = groups
            .into_iter()
            .find(|&gid| gid != current && may_set_group(dir.path(), gid))
        else {
            eprintln!("this account may set no group but its file's own; probe skipped");
            return;
        };
        carry_over(&file, 0, other, 0o6640).unwrap();
        let after = file.metadata().unwrap();
        assert_eq!((after.uid(), after.gid()), (euid, other));
        assert_eq!(after.permissions().mode() & 0o7777, 0o2640);
    }

    #[cfg(unix)]
    #[test]
    fn linked_checkpoint_is_replaced_where_its_links_lead() {
        use std::os::unix::fs::symlink;
        let store = TempDir::new().unwrap();
        let volume = TempDir::new().unwrap();
        let path = store.path().join("state.bin");
        let target = volume.path().join("state.bin");
        // A relative link to an absolute one, before the target exists.
        symlink("hop", &path).unwrap();
        symlink(&target, store.path().join("hop")).unwrap();

        for bytes in [b"first".as_slice(), b"second".as_slice()] {
            atomic_write(&path, |file| {
                // The stage is written beside the target, not the link.
                assert_eq!(staged(volume.path()), 1);
                assert_eq!(staged(store.path()), 0);
                Ok(file.write_all(bytes)?)
            })
            .unwrap();
            assert_eq!(fs::read(&target).unwrap(), bytes);
            assert_eq!(fs::read_link(&path).unwrap(), Path::new("hop"));
            assert_eq!(fs::read_link(store.path().join("hop")).unwrap(), target);
            assert_eq!(fs::read_dir(store.path()).unwrap().count(), 2);
            assert_eq!(fs::read_dir(volume.path()).unwrap().count(), 1);
        }
    }

    #[cfg(unix)]
    #[test]
    fn link_cycle_fails_the_save() {
        use std::os::unix::fs::symlink;
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.bin");
        symlink("loop", &path).unwrap();
        symlink("state.bin", dir.path().join("loop")).unwrap();
        let result = atomic_write(&path, |_| panic!("a looping path must fail before writing"));
        assert_link_bound(result);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
    }

    // Linux follows forty links in a path and refuses the forty-first.
    #[cfg(unix)]
    #[test]
    fn forty_links_are_followed_and_a_forty_first_is_refused() {
        use std::os::unix::fs::symlink;
        let dir = TempDir::new().unwrap();
        // link0 -> link1 -> ... -> link39 -> state.bin
        let mut next = PathBuf::from("state.bin");
        for n in (0..MAX_LINKS).rev() {
            symlink(&next, dir.path().join(format!("link{n}"))).unwrap();
            next = PathBuf::from(format!("link{n}"));
        }
        atomic_write(&dir.path().join("link0"), |file| {
            Ok(file.write_all(b"forty")?)
        })
        .unwrap();
        assert_eq!(fs::read(dir.path().join("state.bin")).unwrap(), b"forty");

        let path = dir.path().join("one-more");
        symlink("link0", &path).unwrap();
        let result = atomic_write(&path, |_| {
            panic!("forty-one links must fail before writing")
        });
        assert_link_bound(result);
        assert_eq!(fs::read(dir.path().join("state.bin")).unwrap(), b"forty");
    }

    #[cfg(unix)]
    fn assert_link_bound(result: Result<Published>) {
        match result {
            Err(crate::StoreError::Io(error)) => assert!(
                error
                    .to_string()
                    .contains(&format!("more than {MAX_LINKS} symbolic links")),
                "{error}"
            ),
            other => panic!("expected the link bound to refuse the save, got {other:?}"),
        }
    }

    #[cfg(unix)]
    fn staged(dir: &Path) -> usize {
        fs::read_dir(dir)
            .unwrap()
            .filter(|entry| {
                let name = entry.as_ref().unwrap().file_name();
                name.as_encoded_bytes()
                    .starts_with(STAGING_PREFIX.as_bytes())
            })
            .count()
    }

    // The cleanup at the rename relies on tempfile removing a stage by its
    // name: once the file has been renamed away, dropping its TempPath leaves
    // it where it went, even while the stage's own handle is open.
    #[test]
    fn dropping_a_staging_path_after_its_rename_leaves_the_renamed_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.bin");
        let mut temporary = tempfile::Builder::new()
            .prefix(STAGING_PREFIX)
            .make_in(dir.path(), |name| {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(name)
            })
            .unwrap();
        temporary.as_file_mut().write_all(b"published").unwrap();
        let (file, staging) = temporary.into_parts();
        fs::rename(&staging, &path).unwrap();
        drop(staging);
        drop(file);
        assert_eq!(fs::read(&path).unwrap(), b"published");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    // main saved through std, which converts long paths; tempfile's persist
    // hands MoveFileExW the plain path, which fails past MAX_PATH.
    #[cfg(windows)]
    #[test]
    fn ordinary_long_paths_support_creation_and_replacement() {
        use std::os::windows::ffi::OsStrExt;
        let root = TempDir::new().unwrap();
        let mut parent = root.path().to_owned();
        for _ in 0..6 {
            parent.push("checkpoint-long-path-segment-without-verbatim-prefix");
        }
        fs::create_dir_all(&parent).unwrap();
        let path = parent.join("state.bin");
        assert!(path.as_os_str().encode_wide().count() > 300);
        assert!(!path.to_string_lossy().starts_with(r"\\?\"));
        for bytes in [b"first".as_slice(), b"replacement".as_slice()] {
            atomic_write(&path, |file| Ok(file.write_all(bytes)?)).unwrap();
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
        let verbatim = fs::canonicalize(&path).unwrap();
        atomic_write(&verbatim, |file| Ok(file.write_all(b"verbatim")?)).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"verbatim");
    }

    // This child exits inside the actual writer, bypassing all Rust destructors.
    // The parent verifies that even a synced but unfinished temp is never read
    // as the checkpoint, and that an abandoned temp does not block the next save.
    #[test]
    fn interrupted_writer_child() {
        let Some(path) = std::env::var_os("CHRONICLE_ATOMIC_WRITE_CHILD_PATH") else {
            return;
        };
        atomic_write(Path::new(&path), |file| {
            file.write_all(b"unfinished checkpoint")?;
            file.sync_all()?;
            std::process::exit(73);
        })
        .unwrap();
        panic!("child should exit before publication");
    }

    #[test]
    fn process_exit_during_write_preserves_checkpoint() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.bin");
        fs::write(&path, b"previous checkpoint").unwrap();

        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "atomic_file::tests::interrupted_writer_child"])
            .env("CHRONICLE_ATOMIC_WRITE_CHILD_PATH", &path)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(73));
        assert_eq!(fs::read(&path).unwrap(), b"previous checkpoint");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);

        atomic_write(&path, |file| Ok(file.write_all(b"next checkpoint")?)).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"next checkpoint");

        // The abandoned stage is what the store's next open sweeps.
        remove_staging_files(dir.path());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
        assert_eq!(fs::read(&path).unwrap(), b"next checkpoint");
    }
}
