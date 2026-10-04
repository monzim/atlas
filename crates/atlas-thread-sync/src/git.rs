//! The handful of git operations a replica needs, through the git CLI.
//!
//! The CLI rather than a library for the same reason `atlas-checkpoint` uses
//! it: it reads the person's own configuration (safe directories, object
//! alternates, worktrees) exactly as their terminal would. Atlas never runs a
//! command here that talks to a remote.

use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum GitError {
    #[error("could not run git: {0}")]
    Spawn(#[from] std::io::Error),
    #[error("git {args} failed: {stderr}")]
    Failed { args: String, stderr: String },
}

fn run(repo: &Path, args: &[&str]) -> Result<Vec<u8>, GitError> {
    let output = atlas_process::command("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()?;
    if !output.status.success() {
        return Err(GitError::Failed {
            args: args.join(" "),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(output.stdout)
}

/// The commit `HEAD` names — the Base a share starts from.
pub fn head_commit(repo: &Path) -> Result<String, GitError> {
    let out = run(repo, &["rev-parse", "--verify", "HEAD^{commit}"])?;
    Ok(String::from_utf8_lossy(&out).trim().to_string())
}

/// Does this repository already hold `sha` as a commit?
pub fn has_commit(repo: &Path, sha: &str) -> bool {
    run(repo, &["cat-file", "-e", &format!("{sha}^{{commit}}")]).is_ok()
}

/// A file's bytes at `sha`, or `None` when the commit has no such file.
pub fn blob_at(repo: &Path, sha: &str, path: &str) -> Result<Option<Vec<u8>>, GitError> {
    let spec = format!("{sha}:{path}");
    if run(repo, &["cat-file", "-e", &spec]).is_err() {
        return Ok(None);
    }
    run(repo, &["cat-file", "blob", &spec]).map(Some)
}

/// Check out `sha` into a new, detached worktree at `dest`, leaving the
/// person's own checkout and branches untouched.
pub fn add_worktree(repo: &Path, dest: &Path, sha: &str) -> Result<(), GitError> {
    let dest = dest.to_string_lossy();
    run(
        repo,
        &["worktree", "add", "--detach", "--quiet", &dest, sha],
    )
    .map(|_| ())
}

/// One path the working tree differs from `HEAD` on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dirty {
    pub path: String,
    pub deleted: bool,
}

/// Every path whose working-tree state differs from `HEAD`: modified, added,
/// renamed and untracked files (ignored ones excluded), and deletions marked.
///
/// `-z` porcelain v1, parsed by hand: a rename or copy record carries its
/// source path as the next NUL-separated field, which belongs to it and is not
/// an entry of its own.
pub fn dirty_paths(repo: &Path) -> Result<Vec<Dirty>, GitError> {
    let out = run(
        repo,
        &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
    )?;
    let mut fields = out.split(|b| *b == 0).filter(|f| !f.is_empty());
    let mut dirty = Vec::new();
    while let Some(field) = fields.next() {
        if field.len() < 4 {
            continue;
        }
        let (x, y) = (field[0], field[1]);
        let path = String::from_utf8_lossy(&field[3..]).into_owned();
        if x == b'R' || x == b'C' {
            fields.next();
        }
        dirty.push(Dirty {
            deleted: x == b'D' || y == b'D',
            path,
        });
    }
    Ok(dirty)
}
