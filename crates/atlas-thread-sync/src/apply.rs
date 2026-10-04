//! Apply (ATL-408): how work leaves a Shared Thread. The thread's changes
//! since the Base are written into the person's own checkout as uncommitted
//! changes, three-way onto whatever commit it is at; they commit and push
//! their usual way. Atlas never touches a git remote, and never the person's
//! branches, index or commits.
//!
//! Per file, against the Base:
//! - the checkout left it as the Base had it → the thread's version is written;
//! - the checkout's commit changed it too → `git merge-file` merges the two,
//!   leaving standard conflict markers where they overlap;
//! - the person has **uncommitted** edits to it → the whole Apply is refused
//!   with the files listed, unless they asked to stash those first. Their
//!   work is never overwritten.

use std::fs;
use std::path::{Path, PathBuf};

use crate::git::{self, GitError};
use crate::path::{self, PathError};
use crate::replica::{write_atomic, ReplicaError};

/// One file the thread changed, as canonical state holds it now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadChange {
    /// Where it is in the thread now.
    pub path: String,
    /// Where it was in the Base, when it moved there from elsewhere.
    pub origin: Option<String>,
    /// Its content now; `None` when the thread deleted it.
    pub content: Option<Vec<u8>>,
}

/// What Apply did.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Applied {
    /// Files written, created or deleted cleanly.
    pub files: Vec<String>,
    /// Files left with conflict markers, or kept as the checkout had them
    /// where a merge is impossible (a binary file, a delete against an edit).
    pub conflicted: Vec<String>,
    /// For a binary conflict, where the thread's version was put beside it.
    pub beside: Vec<String>,
    /// The message the person's own edits were stashed under, if they were.
    pub stashed: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    /// Uncommitted edits in the checkout touch files the thread changed.
    /// Nothing was written; [`apply`] with `stash` set moves them aside first.
    #[error("you have uncommitted changes to files this thread changed: {}", .0.join(", "))]
    Dirty(Vec<String>),
    /// The checkout's repository does not hold the thread's Base commit.
    #[error("this repository does not have the thread's starting commit {0}; fetch or pull first")]
    BaseMissing(String),
    #[error(transparent)]
    Git(#[from] GitError),
    #[error(transparent)]
    Path(#[from] PathError),
    #[error(transparent)]
    Replica(#[from] ReplicaError),
    #[error("could not write {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// What asking to Apply came to: done, or refused with what to do about it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "outcome", rename_all = "camelCase")]
pub enum ApplyOutcome {
    Applied(Applied),
    /// Uncommitted edits touch these files; stash-and-apply moves them aside.
    Dirty {
        files: Vec<String>,
    },
    /// Conflicts are open in the thread: resolve them first.
    ConflictsOpen {
        count: usize,
    },
}

/// Suffix of the file a binary conflict leaves the thread's version in.
pub const THREAD_COPY: &str = ".atlas-thread";

/// Write `changes` — the thread's state of every file it touched — into
/// `checkout`, three-way against `base`. With `stash`, uncommitted edits to
/// those files are stashed first (and only those); without it they refuse
/// the Apply.
pub fn apply(
    checkout: &Path,
    base: &str,
    changes: &[ThreadChange],
    stash: Option<&str>,
) -> Result<Applied, ApplyError> {
    if !git::has_commit(checkout, base) {
        return Err(ApplyError::BaseMissing(base.to_string()));
    }
    let head = git::head_commit(checkout)?;

    // Every path Apply may write: where files are now, and where moved ones were.
    let mut touched: Vec<String> = changes
        .iter()
        .flat_map(|c| std::iter::once(c.path.clone()).chain(c.origin.clone()))
        .filter(|p| path::is_valid(p))
        .collect();
    touched.sort();
    touched.dedup();
    // An uncommitted file already as Apply would leave it — an earlier Apply
    // of the same state — is not the person's work in the way.
    let mut dirty = git::dirty_among(checkout, &touched)?;
    dirty.retain(|rel| !already_applied(checkout, rel, changes));
    let mut applied = Applied::default();
    if !dirty.is_empty() {
        match stash {
            None => return Err(ApplyError::Dirty(dirty)),
            Some(message) => {
                git::stash_paths(checkout, &dirty, message)?;
                applied.stashed = Some(message.to_string());
            }
        }
    }

    for change in changes {
        if !path::is_valid(&change.path) {
            continue;
        }
        // A file moved in the thread leaves its old place, if the checkout
        // still has it as the Base did.
        if let Some(origin) = change.origin.as_deref().filter(|o| *o != change.path) {
            if path::is_valid(origin) {
                let base_old = git::blob_at(checkout, base, origin)?;
                let head_old = git::blob_at(checkout, &head, origin)?;
                if base_old.is_some() && head_old == base_old {
                    remove(checkout, origin)?;
                    applied.files.push(origin.to_string());
                } else if head_old.is_some() {
                    // Changed in the checkout since, and moved in the thread.
                    applied.conflicted.push(origin.to_string());
                }
            }
        }
        let from = change.origin.as_deref().unwrap_or(&change.path);
        let base_bytes = git::blob_at(checkout, base, from)?;
        let head_bytes = git::blob_at(checkout, &head, &change.path)?;
        let theirs = change.content.as_ref();

        if head_bytes.as_ref() == theirs {
            continue; // Already so.
        }
        if head_bytes == base_bytes {
            write_or_remove(checkout, &change.path, theirs)?;
            applied.files.push(change.path.clone());
            continue;
        }
        if theirs == base_bytes.as_ref() {
            continue; // The thread left it as the Base had it.
        }
        // Both changed it since the Base.
        match (&head_bytes, theirs) {
            (Some(ours), Some(theirs)) if is_text(ours) && is_text(theirs) => {
                let base_text = base_bytes.clone().unwrap_or_default();
                let (merged, conflicts) = git::merge_file(
                    checkout,
                    ours,
                    &base_text,
                    theirs,
                    ["yours", "base", "shared thread"],
                )?;
                write(checkout, &change.path, &merged)?;
                if conflicts {
                    applied.conflicted.push(change.path.clone());
                } else {
                    applied.files.push(change.path.clone());
                }
            }
            (Some(_), Some(theirs)) => {
                // Binary: no markers to leave. Yours stays; the thread's goes beside it.
                let beside = format!("{}{THREAD_COPY}", change.path);
                if path::is_valid(&beside) {
                    write(checkout, &beside, theirs)?;
                    applied.beside.push(beside);
                }
                applied.conflicted.push(change.path.clone());
            }
            (None, Some(theirs)) => {
                // Gone from the checkout, changed in the thread: the thread's
                // version comes back for the person to decide.
                write(checkout, &change.path, theirs)?;
                applied.conflicted.push(change.path.clone());
            }
            (Some(_), None) => {
                // Changed in the checkout, deleted in the thread: theirs stays.
                applied.conflicted.push(change.path.clone());
            }
            (None, None) => {}
        }
    }
    applied.files.sort();
    applied.files.dedup();
    applied.conflicted.sort();
    applied.conflicted.dedup();
    Ok(applied)
}

/// Does `rel` in the checkout already hold what the thread has there — the
/// thread's content, or nothing where the thread removed or moved it away?
fn already_applied(checkout: &Path, rel: &str, changes: &[ThreadChange]) -> bool {
    let Ok(target) = path::resolve(checkout, rel) else {
        return false;
    };
    let on_disk = fs::read(&target).ok();
    if let Some(change) = changes.iter().find(|c| c.path == rel) {
        return on_disk == change.content;
    }
    // Only a move's old place: applied once it is gone.
    on_disk.is_none()
}

/// Text, for merging: no NUL in the first 8 KiB, as git decides.
fn is_text(bytes: &[u8]) -> bool {
    !bytes.iter().take(8000).any(|b| *b == 0)
}

fn write_or_remove(root: &Path, rel: &str, bytes: Option<&Vec<u8>>) -> Result<(), ApplyError> {
    match bytes {
        Some(bytes) => write(root, rel, bytes),
        None => remove(root, rel),
    }
}

fn write(root: &Path, rel: &str, bytes: &[u8]) -> Result<(), ApplyError> {
    let target = path::resolve(root, rel)?;
    if let Some(dir) = target.parent() {
        fs::create_dir_all(dir).map_err(|source| ApplyError::Io {
            path: dir.to_path_buf(),
            source,
        })?;
    }
    Ok(write_atomic(&target, bytes)?)
}

fn remove(root: &Path, rel: &str) -> Result<(), ApplyError> {
    let target = path::resolve(root, rel)?;
    match fs::remove_file(&target) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(ApplyError::Io {
            path: target,
            source,
        }),
    }
}
