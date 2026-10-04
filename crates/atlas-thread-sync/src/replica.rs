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

/// Prefix of the temporary files atomic writes go through. The watcher and
/// [`path::relative`] ignore anything named like this.
pub const TEMP_PREFIX: &str = ".atlas-sync-tmp-";

/// Files at or above this size are not co-edited as text (ATL-403 syncs them
/// whole).
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

/// Git's own heuristic: a NUL in the first 8000 bytes means binary.
pub fn looks_textual(bytes: &[u8]) -> bool {
    bytes.len() < MAX_TEXT_BYTES
        && !bytes.iter().take(8000).any(|b| *b == 0)
        && std::str::from_utf8(bytes).is_ok()
}

struct TrackedFile {
    path: String,
    doc: FileDoc,
    /// What this replica last wrote to, or read from, disk for this file.
    /// `None` until the worktree exists.
    disk: Option<Hash>,
}

/// What a save on disk amounts to.
#[derive(Debug, PartialEq, Eq)]
pub enum LocalChange {
    /// The bytes are what this replica wrote: our own echo. Nothing to send.
    Echo,
    /// An edit to a file the thread already holds: send this update.
    Update { file_id: u64, update: Vec<u8> },
    /// A text file the thread does not hold yet: it needs a tree entry first.
    NewFile { path: String },
    /// Not something this slice syncs (binary, too large, deleted, or the
    /// worktree does not exist yet).
    Ignored,
}

pub struct Replica {
    repo: PathBuf,
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
    /// Refuses when the repository lacks the Base: bringing it over is a
    /// separate negotiation (ATL-402).
    pub fn new(repo: &Path, base: &str, root: &Path) -> Result<Self, ReplicaError> {
        if !git::is_commit_sha(base) {
            return Err(ReplicaError::BadBase(base.to_string()));
        }
        if !git::has_commit(repo, base) {
            return Err(ReplicaError::BaseMissing(base.to_string()));
        }
        Ok(Self {
            repo: repo.to_path_buf(),
            base: base.to_string(),
            root: root.to_path_buf(),
            client_id: random_client_id(),
            materialized: root.join(".git").exists(),
            files: BTreeMap::new(),
            by_path: HashMap::new(),
        })
    }

    pub fn base(&self) -> &str {
        &self.base
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

    pub fn files(&self) -> impl Iterator<Item = (u64, &str)> {
        self.files.iter().map(|(id, f)| (*id, f.path.as_str()))
    }

    /// A file's canonical text as this replica holds it.
    pub fn text(&self, path: &str) -> Option<String> {
        let id = self.by_path.get(path)?;
        Some(self.files.get(id)?.doc.content())
    }

    /// The deterministic seed updates for `path` at the Base (see `doc.rs`):
    /// empty for a file the Base does not have.
    pub fn seed_for(&self, path: &str) -> Result<Vec<Vec<u8>>, ReplicaError> {
        let base = git::blob_at(&self.repo, &self.base, path)?.unwrap_or_default();
        if !looks_textual(&base) {
            return Ok(Vec::new());
        }
        Ok(FileDoc::seed_updates(&String::from_utf8_lossy(&base)))
    }

    /// Learn a tree entry: create its document and seed it from the Base. A
    /// file already known is left alone. Answers whether it was new.
    pub fn add_entry(&mut self, file_id: u64, rel: &str) -> Result<bool, ReplicaError> {
        if self.files.contains_key(&file_id) {
            return Ok(false);
        }
        if !path::is_valid(rel) {
            return Err(path::PathError::Invalid(rel.to_string()).into());
        }
        let doc = FileDoc::new(self.client_id);
        for update in self.seed_for(rel)? {
            doc.apply(&update)?;
        }
        // Nothing is written here, even with the worktree checked out: what is
        // on disk at this path is either the Base (equal to the seed) or the
        // person's own new file, which the next save or remote update folds in
        // before anything is written back. `disk: None` makes sure it is read.
        self.files.insert(
            file_id,
            TrackedFile {
                path: rel.to_string(),
                doc,
                disk: None,
            },
        );
        self.by_path.insert(rel.to_string(), file_id);
        Ok(true)
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
        if !self.materialized {
            file.doc.apply(update)?;
            return Ok(None);
        }
        // A save we have not seen yet goes into the document before the
        // remote change, or writing the merge back would erase it.
        let pending = self.ingest_disk(file_id)?;
        self.files[&file_id].doc.apply(update)?;
        self.sync_disk(file_id)?;
        Ok(pending)
    }

    /// The person saved `rel` in their replica (from any editor).
    pub fn local_change(&mut self, rel: &str) -> Result<LocalChange, ReplicaError> {
        if !self.materialized || !path::is_valid(rel) {
            return Ok(LocalChange::Ignored);
        }
        match self.by_path.get(rel).copied() {
            Some(file_id) => Ok(match self.ingest_disk(file_id)? {
                Some(update) => LocalChange::Update { file_id, update },
                None => LocalChange::Echo,
            }),
            None => {
                let target = path::resolve(&self.root, rel)?;
                match fs::read(&target) {
                    Ok(bytes) if looks_textual(&bytes) => Ok(LocalChange::NewFile {
                        path: rel.to_string(),
                    }),
                    _ => Ok(LocalChange::Ignored),
                }
            }
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
        if file.disk == Some(seen) || !looks_textual(&bytes) {
            return Ok(None);
        }
        file.disk = Some(seen);
        Ok(file.doc.set_content(&String::from_utf8_lossy(&bytes)))
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
        if let Some(parent) = self.root.parent() {
            fs::create_dir_all(parent).map_err(io(parent))?;
        }
        git::add_worktree(&self.repo, &self.root, &self.base)?;
        self.materialized = true;
        let ids: Vec<u64> = self.files.keys().copied().collect();
        for id in ids {
            self.sync_disk(id)?;
        }
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
