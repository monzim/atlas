//! One participant's copy of a Shared Thread's canonical state.
//!
//! A replica is a **detached git worktree at the Base**, created out of the way
//! of the person's own checkout, plus one [`FileDoc`] per file the thread has
//! touched. It is created lazily: joining only builds the documents in memory,
//! and nothing is checked out until the person first opens a file or prompts
//! ([`Replica::materialize`]) — watching a thread is free.
//!
//! Two rules keep disk and documents honest with each other:
//!
//! * **Remote writes are atomic** — a temporary file in the same directory,
//!   then a rename — so an editor or a build never reads half a file.
//! * **A remote write is never echoed back.** The hash of what this replica
//!   last wrote (or last read) is kept per file; a watcher event for a file
//!   whose bytes still hash the same is our own write, and produces nothing.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::doc::{random_client_id, FileDoc};
use crate::git;
use crate::path;
use crate::secrets::secret_reason;
use crate::wire::{FileKind, TreeEntry};

/// Prefix of the temporary files atomic writes go through. The watcher and
/// [`path::relative`] ignore anything named like this.
pub const TEMP_PREFIX: &str = ".atlas-sync-tmp-";

/// Files at or above this size are not co-edited as text: they sync whole, as
/// blobs (ATL-403).
pub const MAX_TEXT_BYTES: usize = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ReplicaError {
    #[error(transparent)]
    Git(#[from] git::GitError),
    #[error(transparent)]
    Path(#[from] path::PathError),
    #[error(transparent)]
    Doc(#[from] crate::doc::DocError),
    #[error("i/o on {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("this repository does not have the thread's Base commit {0}")]
    BaseMissing(String),
    #[error("{0:?} is not a commit id")]
    BadBase(String),
    #[error("no file {0} in this thread")]
    UnknownFile(u64),
    #[error("{0} did not hash to the blob it was fetched as")]
    CorruptBlob(String),
}

fn io(path: &Path) -> impl FnOnce(std::io::Error) -> ReplicaError + '_ {
    move |source| ReplicaError::Io {
        path: path.to_path_buf(),
        source,
    }
}

type Hash = [u8; 32];

fn hash(bytes: &[u8]) -> Hash {
    Sha256::digest(bytes).into()
}

fn hex(hash: &Hash) -> String {
    hash.iter().map(|b| format!("{b:02x}")).collect()
}

/// How `bytes` sync: as co-edited text, or whole (ATL-403).
pub fn kind_of(bytes: &[u8]) -> FileKind {
    if looks_textual(bytes) {
        FileKind::Text
    } else {
        FileKind::Binary
    }
}

/// Git's own heuristic: a NUL in the first 8000 bytes means binary.
pub fn looks_textual(bytes: &[u8]) -> bool {
    bytes.len() < MAX_TEXT_BYTES
        && !bytes.iter().take(8000).any(|b| *b == 0)
        && std::str::from_utf8(bytes).is_ok()
}

struct TrackedFile {
    path: String,
    kind: FileKind,
    /// The text, for a text file; unused for a binary one.
    doc: FileDoc,
    /// What this replica last wrote to, or read from, disk for this file.
    /// `None` until the worktree exists.
    disk: Option<Hash>,
    /// Set while the file on disk holds something that looks like a secret:
    /// the document as it was at that moment. Nothing from disk is sent and
    /// nothing is written to disk until the secret is gone; then the person's
    /// edit is made relative to this snapshot and merged, so changes the
    /// thread took meanwhile survive.
    held: Option<Vec<u8>>,
    /// A binary file's canonical content: the hex SHA-256 of its blob.
    blob: Option<String>,
    /// Deleted in the thread. The entry and document stay, so a revival
    /// brings the same file back.
    deleted: bool,
    /// The path it entered the thread under; a checkout at the Base still
    /// holds the file there after a rename.
    origin: String,
}

/// What a save on disk amounts to.
#[derive(Debug, PartialEq, Eq)]
pub enum LocalChange {
    /// The bytes are what this replica wrote: our own echo. Nothing to send.
    Echo,
    /// An edit to a file the thread already holds: send this update.
    Update { file_id: u64, update: Vec<u8> },
    /// A file the thread does not hold yet: it needs a tree entry first.
    NewFile { path: String },
    /// A binary file's bytes changed: upload them, then set the blob.
    Blob {
        file_id: u64,
        sha256: String,
        bytes: Vec<u8>,
    },
    /// A file the thread holds is gone from disk. A deletion — unless the
    /// same bytes turn up at a new path, which makes it a rename.
    Missing { file_id: u64 },
    /// The same bytes as a file that went missing, at a new path: a rename.
    Renamed { file_id: u64, from: String, to: String },
    /// Not something that syncs (a file that turned binary or grew past the
    /// text limit, or the worktree does not exist yet).
    Ignored,
}

/// One tracked file as a Run forked it.
#[derive(Debug, Clone)]
pub struct ForkFile {
    pub path: String,
    pub snapshot: Vec<u8>,
    pub content: String,
}

pub struct Replica {
    /// The repository the worktree comes from: the person's own, or the
    /// thread repository a bundle was fetched into (ATL-402). `None` while
    /// this machine lacks the Base — the replica can then only watch.
    repo: Option<PathBuf>,
    base: String,
    root: PathBuf,
    client_id: u64,
    materialized: bool,
    files: BTreeMap<u64, TrackedFile>,
    by_path: HashMap<String, u64>,
}

impl Replica {
    /// A replica of a thread whose Base is `base`, backed by the person's own
    /// repository at `repo`, to be checked out at `root` when first needed.
    ///
    /// Refuses when the repository lacks the Base: see
    /// [`Replica::without_base`] and [`crate::bootstrap`].
    pub fn new(repo: &Path, base: &str, root: &Path) -> Result<Self, ReplicaError> {
        let mut replica = Self::without_base(base, root)?;
        replica.attach_repo(repo)?;
        Ok(replica)
    }

    /// A replica on a machine that does not have the Base yet. It follows the
    /// thread — every file rebuilt from the journal alone, since the replica
    /// that introduces a file also publishes its Base seed — but cannot be
    /// checked out, and so cannot edit, until [`Replica::attach_repo`].
    pub fn without_base(base: &str, root: &Path) -> Result<Self, ReplicaError> {
        if !git::is_commit_sha(base) {
            return Err(ReplicaError::BadBase(base.to_string()));
        }
        Ok(Self {
            repo: None,
            base: base.to_string(),
            root: root.to_path_buf(),
            client_id: random_client_id(),
            materialized: false,
            files: BTreeMap::new(),
            by_path: HashMap::new(),
        })
    }

    /// The Base arrived (or was always in `repo`): worktrees come from `repo`
    /// from now on. Every file already followed is seeded from its Base
    /// content too — harmless, since seeds are identical everywhere.
    pub fn attach_repo(&mut self, repo: &Path) -> Result<(), ReplicaError> {
        if !git::has_commit(repo, &self.base) {
            return Err(ReplicaError::BaseMissing(self.base.clone()));
        }
        self.repo = Some(repo.to_path_buf());
        self.materialized = self.root.join(".git").exists();
        let files: Vec<(u64, String)> = self
            .files
            .iter()
            .map(|(id, f)| (*id, f.path.clone()))
            .collect();
        for (id, path) in files {
            for update in self.seed_for(&path)? {
                self.files[&id].doc.apply(&update)?;
            }
        }
        Ok(())
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    /// The repository the replica's worktrees come from, once it has the Base.
    pub fn repo(&self) -> Option<&Path> {
        self.repo.as_deref()
    }

    /// Can this machine check the thread out and edit it? Not until it holds
    /// the Base.
    pub fn has_base(&self) -> bool {
        self.repo.is_some()
    }

    /// Every tracked file as it is now, to fork a Run from (ATL-405).
    pub fn fork_files(&self) -> BTreeMap<u64, ForkFile> {
        self.files
            .iter()
            .filter(|(_, f)| !f.deleted && f.kind == FileKind::Text)
            .map(|(id, f)| {
                (
                    *id,
                    ForkFile {
                        path: f.path.clone(),
                        snapshot: f.doc.snapshot(),
                        content: f.doc.content(),
                    },
                )
            })
            .collect()
    }

    /// One file's document as a snapshot.
    pub fn snapshot(&self, file_id: u64) -> Option<Vec<u8>> {
        Some(self.files.get(&file_id)?.doc.snapshot())
    }

    /// The document a file starts from in this thread — its Base content, or
    /// nothing — as a snapshot. A Run that creates a file forks from this.
    pub fn seed_snapshot(&self, path: &str) -> Result<Vec<u8>, ReplicaError> {
        let doc = FileDoc::new(random_client_id());
        for update in self.seed_for(path)? {
            doc.apply(&update)?;
        }
        Ok(doc.snapshot())
    }

    /// Where the worktree is (or will be).
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn is_materialized(&self) -> bool {
        self.materialized
    }

    pub fn file_id(&self, path: &str) -> Option<u64> {
        self.by_path.get(path).copied()
    }

    /// Every live file the thread holds, by id and path.
    pub fn files(&self) -> impl Iterator<Item = (u64, &str)> {
        self.files
            .iter()
            .filter(|(_, f)| !f.deleted)
            .map(|(id, f)| (*id, f.path.as_str()))
    }

    pub fn kind(&self, file_id: u64) -> Option<FileKind> {
        self.files.get(&file_id).map(|f| f.kind)
    }

    pub fn is_deleted(&self, file_id: u64) -> bool {
        self.files.get(&file_id).is_some_and(|f| f.deleted)
    }

    /// A binary file's canonical blob.
    pub fn blob(&self, file_id: u64) -> Option<&str> {
        self.files.get(&file_id)?.blob.as_deref()
    }

    /// Paths a checkout at the Base holds that canonical state does not:
    /// deleted files, and where renamed ones used to be.
    pub fn removed_paths(&self) -> Vec<String> {
        let mut out = Vec::new();
        for f in self.files.values() {
            if f.deleted && !self.by_path.contains_key(&f.path) {
                out.push(f.path.clone());
            }
            if f.origin != f.path && !self.by_path.contains_key(&f.origin) {
                out.push(f.origin.clone());
            }
        }
        out.sort();
        out.dedup();
        out
    }

    /// A file's canonical text as this replica holds it.
    pub fn text(&self, path: &str) -> Option<String> {
        let id = self.by_path.get(path)?;
        Some(self.files.get(id)?.doc.content())
    }

    /// The deterministic seed updates for `path` at the Base (see `doc.rs`):
    /// empty for a file the Base does not have.
    pub fn seed_for(&self, path: &str) -> Result<Vec<Vec<u8>>, ReplicaError> {
        let base = self.base_bytes(path)?.unwrap_or_default();
        if !looks_textual(&base) {
            return Ok(Vec::new());
        }
        Ok(FileDoc::seed_updates(&String::from_utf8_lossy(&base)))
    }

    /// A file's bytes at the Base, or `None` when the Base has no such file —
    /// or this machine does not have the Base yet.
    pub fn base_bytes(&self, path: &str) -> Result<Option<Vec<u8>>, ReplicaError> {
        match &self.repo {
            Some(repo) => Ok(git::blob_at(repo, &self.base, path)?),
            None => Ok(None),
        }
    }

    /// Learn a tree entry: create its document and seed it from the Base. A
    /// file already known is left alone. Answers whether it was new.
    pub fn add_entry(
        &mut self,
        file_id: u64,
        rel: &str,
        kind: FileKind,
    ) -> Result<bool, ReplicaError> {
        if self.files.contains_key(&file_id) {
            return Ok(false);
        }
        if !path::is_valid(rel) {
            return Err(path::PathError::Invalid(rel.to_string()).into());
        }
        let doc = FileDoc::new(self.client_id);
        if kind == FileKind::Text {
            for update in self.seed_for(rel)? {
                doc.apply(&update)?;
            }
        }
        // Nothing is written here, even with the worktree checked out: what is
        // on disk at this path is either the Base (equal to the seed) or the
        // person's own new file, which the next save or remote update folds in
        // before anything is written back. `disk: None` makes sure it is read.
        self.files.insert(
            file_id,
            TrackedFile {
                path: rel.to_string(),
                kind,
                doc,
                disk: None,
                held: None,
                blob: None,
                deleted: false,
                origin: rel.to_string(),
            },
        );
        self.by_path.insert(rel.to_string(), file_id);
        Ok(true)
    }

    /// Bring one tree entry into this replica — a new file, a rename, a
    /// deletion, a revival or a binary file's new blob — moving or removing
    /// the file on disk to match. Answers a blob to fetch and write with
    /// [`Replica::write_blob`] when disk does not hold the canonical one.
    pub fn learn(&mut self, entry: &TreeEntry) -> Result<Option<String>, ReplicaError> {
        let id = entry.file_id;
        if !path::is_valid(&entry.path) {
            return Err(path::PathError::Invalid(entry.path.clone()).into());
        }
        if self.add_entry(id, &entry.path, entry.kind)? {
            let file = self.files.get_mut(&id).ok_or(ReplicaError::UnknownFile(id))?;
            if let Some(origin) = entry.origin.as_ref().filter(|o| path::is_valid(o)) {
                file.origin.clone_from(origin);
            }
        }
        // A rename: the bytes move with the file.
        let old_path = self.files[&id].path.clone();
        if old_path != entry.path {
            if self.by_path.get(&old_path) == Some(&id) {
                self.by_path.remove(&old_path);
            }
            if self.materialized && !self.files[&id].deleted {
                self.move_on_disk(&old_path, &entry.path)?;
            }
            self.files.get_mut(&id).expect("known").path.clone_from(&entry.path);
        }
        // Deleted, or back.
        let was_deleted = self.files[&id].deleted;
        if entry.deleted && !was_deleted {
            self.by_path.remove(&entry.path);
            if self.materialized {
                self.remove_on_disk(&entry.path)?;
            }
        } else if !entry.deleted {
            self.by_path.insert(entry.path.clone(), id);
        }
        {
            let file = self.files.get_mut(&id).expect("known");
            file.deleted = entry.deleted;
            if file.kind == FileKind::Binary {
                file.blob.clone_from(&entry.blob);
            }
        }
        if entry.deleted || !self.materialized {
            return Ok(None);
        }
        match self.files[&id].kind {
            FileKind::Text => {
                if was_deleted {
                    self.sync_disk(id)?;
                }
                Ok(None)
            }
            FileKind::Binary => Ok(self.blob_to_fetch(id)),
        }
    }

    /// The canonical blob of a binary file, if disk does not hold it.
    fn blob_to_fetch(&self, file_id: u64) -> Option<String> {
        let file = self.files.get(&file_id)?;
        let sha = file.blob.clone()?;
        if file.deleted || file.disk.as_ref().map(hex).as_deref() == Some(sha.as_str()) {
            return None;
        }
        Some(sha)
    }

    /// Every binary file's blob the worktree does not hold yet.
    pub fn blobs_to_fetch(&self) -> Vec<(u64, String)> {
        if !self.materialized {
            return Vec::new();
        }
        self.files
            .keys()
            .filter_map(|id| self.blob_to_fetch(*id).map(|sha| (*id, sha)))
            .collect()
    }

    /// Write a binary file's canonical bytes, fetched from the thread. Refused
    /// unless they hash to the blob canonical state names.
    pub fn write_blob(&mut self, file_id: u64, bytes: &[u8]) -> Result<(), ReplicaError> {
        let file = self
            .files
            .get_mut(&file_id)
            .ok_or(ReplicaError::UnknownFile(file_id))?;
        let seen = hash(bytes);
        if file.blob.as_deref() != Some(hex(&seen).as_str()) {
            return Err(ReplicaError::CorruptBlob(file.path.clone()));
        }
        if file.deleted || !self.materialized {
            return Ok(());
        }
        let target = path::resolve(&self.root, &file.path)?;
        file.disk = Some(seen);
        write_atomic(&target, bytes)
    }

    /// This replica renamed a file itself (the person moved it on disk).
    pub fn rename(&mut self, file_id: u64, to: &str) -> Result<(), ReplicaError> {
        if !path::is_valid(to) {
            return Err(path::PathError::Invalid(to.to_string()).into());
        }
        let file = self
            .files
            .get_mut(&file_id)
            .ok_or(ReplicaError::UnknownFile(file_id))?;
        self.by_path.remove(&file.path);
        file.path = to.to_string();
        self.by_path.insert(to.to_string(), file_id);
        Ok(())
    }

    /// The server answered this replica's `tree.ensure` with a file it knew
    /// was deleted: the same file is back, at `rel`.
    pub fn revive(&mut self, file_id: u64, rel: &str) -> Result<(), ReplicaError> {
        if !path::is_valid(rel) {
            return Err(path::PathError::Invalid(rel.to_string()).into());
        }
        let file = self
            .files
            .get_mut(&file_id)
            .ok_or(ReplicaError::UnknownFile(file_id))?;
        file.deleted = false;
        file.path = rel.to_string();
        self.by_path.insert(rel.to_string(), file_id);
        Ok(())
    }

    /// This replica deleted a file itself (the person removed it on disk).
    pub fn delete(&mut self, file_id: u64) -> Result<(), ReplicaError> {
        let file = self
            .files
            .get_mut(&file_id)
            .ok_or(ReplicaError::UnknownFile(file_id))?;
        file.deleted = true;
        file.disk = None;
        self.by_path.remove(&file.path);
        Ok(())
    }

    /// This replica set a binary file's blob itself.
    pub fn set_blob(&mut self, file_id: u64, sha256: &str) -> Result<(), ReplicaError> {
        let file = self
            .files
            .get_mut(&file_id)
            .ok_or(ReplicaError::UnknownFile(file_id))?;
        file.blob = Some(sha256.to_string());
        Ok(())
    }

    fn move_on_disk(&mut self, from: &str, to: &str) -> Result<(), ReplicaError> {
        let source = path::resolve(&self.root, from)?;
        let target = path::resolve(&self.root, to)?;
        if !source.exists() || target.exists() {
            return Ok(());
        }
        if let Some(dir) = target.parent() {
            fs::create_dir_all(dir).map_err(io(dir))?;
        }
        fs::rename(&source, &target).map_err(io(&target))
    }

    fn remove_on_disk(&mut self, rel: &str) -> Result<(), ReplicaError> {
        let target = path::resolve(&self.root, rel)?;
        match fs::remove_file(&target) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io(&target)(e)),
        }
    }

    /// A worktree file's text as it is on disk now (lossy UTF-8), for checks
    /// made before it is synced.
    pub fn read_disk(&self, rel: &str) -> Result<String, ReplicaError> {
        let target = path::resolve(&self.root, rel)?;
        let bytes = fs::read(&target).map_err(io(&target))?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    /// Make a file's document hold `content` (the sharer's working copy, read
    /// from their own checkout) and answer the update. The replica's disk is
    /// brought along when it exists.
    pub fn set_text(
        &mut self,
        file_id: u64,
        content: &str,
    ) -> Result<Option<Vec<u8>>, ReplicaError> {
        let file = self
            .files
            .get(&file_id)
            .ok_or(ReplicaError::UnknownFile(file_id))?;
        let update = file.doc.set_content(content);
        if self.materialized && update.is_some() {
            self.sync_disk(file_id)?;
        }
        Ok(update)
    }

    /// Apply an update from the thread. When the worktree exists the file is
    /// rewritten; if the person had saved over it meanwhile, that save is folded
    /// in first and returned as an update to send.
    pub fn apply_remote(
        &mut self,
        file_id: u64,
        update: &[u8],
    ) -> Result<Option<Vec<u8>>, ReplicaError> {
        let file = self
            .files
            .get(&file_id)
            .ok_or(ReplicaError::UnknownFile(file_id))?;
        // A deleted file's document still follows (a revival brings it back),
        // but nothing is written where it used to be.
        if !self.materialized || file.deleted {
            file.doc.apply(update)?;
            return Ok(None);
        }
        // A save we have not seen yet goes into the document before the
        // remote change, or writing the merge back would erase it.
        let pending = self.ingest_disk(file_id)?;
        let file = &self.files[&file_id];
        file.doc.apply(update)?;
        // A held file keeps the person's bytes on disk; the change waits in
        // the document and lands with the merge when the secret is removed.
        if file.held.is_none() {
            self.sync_disk(file_id)?;
        }
        Ok(pending)
    }

    /// The person saved, created, moved or removed `rel` in their replica
    /// (from any editor, or a shell).
    pub fn local_change(&mut self, rel: &str) -> Result<LocalChange, ReplicaError> {
        if !self.materialized || !path::is_valid(rel) {
            return Ok(LocalChange::Ignored);
        }
        let target = path::resolve(&self.root, rel)?;
        let bytes = match fs::read(&target) {
            Ok(bytes) => Some(bytes),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            // A directory, or unreadable: nothing to sync.
            Err(_) => return Ok(LocalChange::Ignored),
        };
        match (self.by_path.get(rel).copied(), bytes) {
            (Some(file_id), None) => Ok(LocalChange::Missing { file_id }),
            (Some(file_id), Some(bytes)) => match self.files[&file_id].kind {
                FileKind::Text => Ok(match self.ingest_disk(file_id)? {
                    Some(update) => LocalChange::Update { file_id, update },
                    None => LocalChange::Echo,
                }),
                FileKind::Binary => {
                    let seen = hash(&bytes);
                    let file = self.files.get_mut(&file_id).expect("known");
                    if file.disk == Some(seen) {
                        return Ok(LocalChange::Echo);
                    }
                    file.disk = Some(seen);
                    Ok(LocalChange::Blob {
                        file_id,
                        sha256: hex(&seen),
                        bytes,
                    })
                }
            },
            (None, None) => Ok(LocalChange::Ignored),
            (None, Some(bytes)) => {
                // The same bytes as a file that is gone from where it was: the
                // person moved it.
                let seen = hash(&bytes);
                if let Some(file_id) = self.vanished_with(&seen) {
                    let from = self.files[&file_id].path.clone();
                    return Ok(LocalChange::Renamed {
                        file_id,
                        from,
                        to: rel.to_string(),
                    });
                }
                Ok(LocalChange::NewFile {
                    path: rel.to_string(),
                })
            }
        }
    }

    /// A live file whose bytes, as this replica last saw them, were `seen`,
    /// and which is no longer on disk where it was.
    fn vanished_with(&self, seen: &Hash) -> Option<u64> {
        self.files.iter().find_map(|(id, f)| {
            let gone = !f.deleted
                && f.disk.as_ref() == Some(seen)
                && path::resolve(&self.root, &f.path).is_ok_and(|p| !p.exists());
            gone.then_some(*id)
        })
    }

    /// Is a tracked file still missing from disk?
    pub fn is_missing(&self, file_id: u64) -> bool {
        self.files.get(&file_id).is_some_and(|f| {
            !f.deleted && path::resolve(&self.root, &f.path).is_ok_and(|p| !p.exists())
        })
    }

    /// Read a new file's bytes to introduce it.
    pub fn read_bytes(&self, rel: &str) -> Result<Vec<u8>, ReplicaError> {
        let target = path::resolve(&self.root, rel)?;
        fs::read(&target).map_err(io(&target))
    }

    /// Remember the bytes this replica just introduced a binary file with.
    pub fn saw_bytes(&mut self, file_id: u64, bytes: &[u8]) {
        if let Some(file) = self.files.get_mut(&file_id) {
            file.disk = Some(hash(bytes));
        }
    }

    /// Read the file's bytes off disk and, unless they are what this replica
    /// already knows, make the document match them. Answers the update.
    fn ingest_disk(&mut self, file_id: u64) -> Result<Option<Vec<u8>>, ReplicaError> {
        let file = self
            .files
            .get_mut(&file_id)
            .ok_or(ReplicaError::UnknownFile(file_id))?;
        let target = path::resolve(&self.root, &file.path)?;
        let bytes = match fs::read(&target) {
            Ok(bytes) => bytes,
            // Deletion is a tree change (ATL-403); until then a missing file
            // is left to the next remote write to restore.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(ReplicaError::Io {
                    path: target,
                    source,
                })
            }
        };
        let seen = hash(&bytes);
        // A text file that turned binary, or grew past the text limit, stays
        // as it was in the thread: a file's kind is fixed when it enters.
        if file.disk == Some(seen) || !looks_textual(&bytes) {
            return Ok(None);
        }
        let content = String::from_utf8_lossy(&bytes).into_owned();

        // The same gate as sharing and new files, for every later save: a
        // credential pasted into a tracked file is held on this machine.
        if secret_reason(&file.path, &content).is_some() {
            if file.held.is_none() {
                tracing::info!(target: "atlas_thread_sync", path = %file.path, "holding a file that now looks secret");
                file.held = Some(file.doc.snapshot());
            }
            return Ok(None);
        }

        file.disk = Some(seen);
        match file.held.take() {
            None => Ok(file.doc.set_content(&content)),
            Some(snapshot) => {
                // Resume: the person's edit, relative to the moment the file
                // was held, merged into whatever the thread did since. Then
                // disk gets the merge.
                let fork = FileDoc::from_snapshot(self.client_id, &snapshot)?;
                let update = fork.set_content(&content);
                if let Some(update) = &update {
                    file.doc.apply(update)?;
                }
                self.sync_disk(file_id)?;
                Ok(update)
            }
        }
    }

    /// Files held back because they now look like they contain a secret.
    pub fn held_files(&self) -> Vec<String> {
        self.files
            .values()
            .filter(|f| f.held.is_some() && !f.deleted)
            .map(|f| f.path.clone())
            .collect()
    }

    /// Write the document to disk if disk differs, atomically, and remember
    /// what was written so the watcher's report of it is recognised as ours.
    fn sync_disk(&mut self, file_id: u64) -> Result<(), ReplicaError> {
        let file = self
            .files
            .get_mut(&file_id)
            .ok_or(ReplicaError::UnknownFile(file_id))?;
        let target = path::resolve(&self.root, &file.path)?;
        let content = file.doc.content();
        let wanted = hash(content.as_bytes());
        let on_disk = fs::read(&target).ok().map(|b| hash(&b));
        file.disk = Some(wanted);
        if on_disk == Some(wanted) {
            return Ok(());
        }
        write_atomic(&target, content.as_bytes())
    }

    /// Check out the worktree, if it is not already, and bring every tracked
    /// file on it up to the thread's canonical state. Answers its root.
    ///
    /// A worktree that already existed (the app restarted) is left as it is:
    /// it may hold saves the thread has not heard yet, and every later remote
    /// update reads disk before writing, so nothing there is lost.
    pub fn materialize(&mut self) -> Result<&Path, ReplicaError> {
        if self.materialized {
            return Ok(&self.root);
        }
        let repo = self
            .repo
            .clone()
            .ok_or_else(|| ReplicaError::BaseMissing(self.base.clone()))?;
        if let Some(parent) = self.root.parent() {
            fs::create_dir_all(parent).map_err(io(parent))?;
        }
        git::add_worktree(&repo, &self.root, &self.base)?;
        self.materialized = true;
        // Renamed and deleted files leave the Base's copy behind.
        for rel in self.removed_paths() {
            self.remove_on_disk(&rel)?;
        }
        let ids: Vec<u64> = self
            .files
            .iter()
            .filter(|(_, f)| !f.deleted && f.kind == FileKind::Text)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.sync_disk(id)?;
        }
        // Binary files are fetched by the session: see `blobs_to_fetch`.
        Ok(&self.root)
    }
}

/// Write `bytes` to `target` through a temporary file in the same directory
/// and a rename, so a reader sees the old file or the new one and never half.
pub fn write_atomic(target: &Path, bytes: &[u8]) -> Result<(), ReplicaError> {
    let dir = target.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(dir).map_err(io(dir))?;
    let temp = dir.join(format!("{TEMP_PREFIX}{}", uuid::Uuid::new_v4().simple()));
    let result = (|| {
        // Created owner-only, so the bytes are never readable by anybody the
        // finished file would not be; the target's own mode is applied before
        // the rename (an executable script stays one, a 0600 file stays 0600).
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let mut file = options.open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        match fs::metadata(target) {
            Ok(meta) => fs::set_permissions(&temp, meta.permissions())?,
            #[cfg(unix)]
            Err(_) => {
                fs::set_permissions(&temp, std::os::unix::fs::PermissionsExt::from_mode(0o644))?
            }
            #[cfg(not(unix))]
            Err(_) => {}
        }
        fs::rename(&temp, target)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result.map_err(io(target))
}
