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

/// Is `sha` a full commit name — 40 (SHA-1) or 64 (SHA-256) lower-case hex?
///
/// Every commit id that reaches a git argument passes this first. A Base comes
/// from the server or a link, and a value starting with `-` would otherwise be
/// read by git as an option.
pub fn is_commit_sha(sha: &str) -> bool {
    matches!(sha.len(), 40 | 64)
        && sha
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn checked(sha: &str) -> Result<&str, GitError> {
    if is_commit_sha(sha) {
        Ok(sha)
    } else {
        Err(GitError::Failed {
            args: "(refused)".into(),
            stderr: format!("not a commit id: {sha:?}"),
        })
    }
}

/// The commit `HEAD` names — the Base a share starts from.
pub fn head_commit(repo: &Path) -> Result<String, GitError> {
    let out = run(repo, &["rev-parse", "--verify", "HEAD^{commit}"])?;
    Ok(String::from_utf8_lossy(&out).trim().to_string())
}

/// Does this repository already hold `sha` as a commit?
pub fn has_commit(repo: &Path, sha: &str) -> bool {
    is_commit_sha(sha) && run(repo, &["cat-file", "-e", &format!("{sha}^{{commit}}")]).is_ok()
}

/// A file's bytes at `sha`, or `None` when the commit has no such file.
pub fn blob_at(repo: &Path, sha: &str, path: &str) -> Result<Option<Vec<u8>>, GitError> {
    let spec = format!("{}:{path}", checked(sha)?);
    if run(repo, &["cat-file", "-e", &spec]).is_err() {
        return Ok(None);
    }
    run(repo, &["cat-file", "blob", &spec]).map(Some)
}

/// Check out `sha` into a new, detached worktree at `dest`, leaving the
/// person's own checkout and branches untouched.
pub fn add_worktree(repo: &Path, dest: &Path, sha: &str) -> Result<(), GitError> {
    let sha = checked(sha)?;
    let dest = dest.to_string_lossy();
    run(
        repo,
        &["worktree", "add", "--detach", "--quiet", "--", &dest, sha],
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

/// Put an existing worktree back at `sha`: tracked files reset, untracked ones
/// removed. **Ignored files survive** (`clean` without `-x`), so a Run
/// worktree keeps its `node_modules` and build output warm across Runs.
pub fn reset_worktree(root: &Path, sha: &str) -> Result<(), GitError> {
    let sha = checked(sha)?;
    run(root, &["reset", "--hard", "--quiet", sha])?;
    run(root, &["clean", "-d", "--force", "--quiet"]).map(|_| ())
}
