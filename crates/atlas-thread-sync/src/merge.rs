//! A Run's three-way merge (ADR-0022, ATL-405): the fork state the Run started
//! from, the Run's result, and canonical state now.
//!
//! The merge is computed as line hunks — the fork against the Run's result —
//! applied to a document rebuilt from the fork snapshot. The resulting Yjs
//! update is what `merge.submit` carries: applied to canonical state it lands
//! the Run's hunks and leaves everybody else's edits since the fork alone.
//!
//! Hunks that overlap (or touch) a change canonical state made since the fork
//! are **not** merged: this slice fails the merge visibly with the overlapping
//! lines, and ATL-410 turns them into Conflicts.

use std::ops::Range;

use similar::{capture_diff_slices, Algorithm, DiffTag};

use crate::doc::{random_client_id, DocError, FileDoc};

/// One changed region: fork lines `old` became `new` lines of the other side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hunk {
    pub old: Range<usize>,
    pub new: Range<usize>,
}

/// A text as lines, each with its line ending.
pub fn lines(text: &str) -> Vec<&str> {
    text.split_inclusive('\n').collect()
}

/// The hunks that turn `old` into `new`, in order.
pub fn hunks(old: &str, new: &str) -> Vec<Hunk> {
    let (a, b) = (lines(old), lines(new));
    capture_diff_slices(Algorithm::Myers, &a, &b)
        .into_iter()
        .filter_map(|op| {
            let (tag, old, new) = op.as_tag_tuple();
            (tag != DiffTag::Equal).then_some(Hunk { old, new })
        })
        .collect()
}

/// Do two hunks over the same fork collide? Touching counts, as in git: two
/// edits on adjacent lines, or two insertions at one point, have no order
/// anybody chose.
fn collide(a: &Hunk, b: &Hunk) -> bool {
    a.old.start <= b.old.end && b.old.start <= a.old.end
}

#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    /// The Run changed fork lines canonical state also changed since.
    #[error("the Run's changes overlap edits made since it started (lines {lines:?})")]
    Overlap { lines: Vec<Range<usize>> },
    #[error(transparent)]
    Doc(#[from] DocError),
}

/// A merged file: the update to submit, and the content it produces on
/// canonical state as this replica holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Merged {
    pub update: Vec<u8>,
    pub content: String,
}

/// Merge the Run's result `run` for one file.
///
/// `fork` is the file's document at the Run's fork, `canonical` the document
/// now (both as [`FileDoc::snapshot`]s). Answers `None` when the Run left the
/// file as it forked it, or when canonical state already holds every hunk.
pub fn three_way(fork: &[u8], run: &str, canonical: &[u8]) -> Result<Option<Merged>, MergeError> {
    // Each merge edits under a client id of its own: the fork document's
    // clocks were the replica's at the fork, and the live document has moved
    // on under that id since — reusing it would collide.
    let fork_doc = FileDoc::from_snapshot(random_client_id(), fork)?;
    let fork_text = fork_doc.content();
    if fork_text == run {
        return Ok(None);
    }
    let canonical_doc = FileDoc::from_snapshot(random_client_id(), canonical)?;
    let canonical_text = canonical_doc.content();

    let ours = hunks(&fork_text, run);
    let theirs = hunks(&fork_text, &canonical_text);
    let (run_lines, canon_lines) = (lines(run), lines(&canonical_text));
    // The same edit made on both sides is already there; anything else that
    // collides with their changes is an overlap.
    let mut kept = Vec::new();
    let mut overlaps = Vec::new();
    for hunk in ours {
        let same = theirs.iter().any(|t| {
            t.old == hunk.old && run_lines[hunk.new.clone()] == canon_lines[t.new.clone()]
        });
        if same {
            continue;
        }
        if let Some(t) = theirs.iter().find(|t| collide(&hunk, t)) {
            overlaps.push(hunk.old.start.min(t.old.start)..hunk.old.end.max(t.old.end));
            continue;
        }
        kept.push(hunk);
    }
    if !overlaps.is_empty() {
        return Err(MergeError::Overlap { lines: overlaps });
    }
    let Some(update) = fork_doc.replace_lines(&fork_text, &kept, run) else {
        return Ok(None);
    };
    canonical_doc.apply(&update)?;
    Ok(Some(Merged {
        update,
        content: canonical_doc.content(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(text: &str) -> FileDoc {
        let d = FileDoc::new(random_client_id());
        for u in FileDoc::seed_updates(text) {
            d.apply(&u).unwrap();
        }
        d
    }

    const BASE: &str = "one\ntwo\nthree\nfour\nfive\nsix\n";

    #[test]
    fn far_apart_edits_both_survive_and_the_middle_is_untouched() {
        let fork = doc(BASE);
        let canonical = FileDoc::from_snapshot(random_client_id(), &fork.snapshot()).unwrap();
        canonical.set_content("one\ntwo\nthree\nFOUR\nfive\nsix\n");
        let run = "ONE\ntwo\nthree\nfour\nfive\nSIX\n";
        let merged = three_way(&fork.snapshot(), run, &canonical.snapshot())
            .unwrap()
            .unwrap();
        assert_eq!(merged.content, "ONE\ntwo\nthree\nFOUR\nfive\nSIX\n");
        canonical.apply(&merged.update).unwrap();
        assert_eq!(canonical.content(), merged.content);
    }

    #[test]
    fn overlapping_and_touching_edits_fail_with_their_lines() {
        let fork = doc(BASE);
        let canonical = FileDoc::from_snapshot(random_client_id(), &fork.snapshot()).unwrap();
        canonical.set_content("one\ntwo\nTHREE\nfour\nfive\nsix\n");
        for run in [
            "one\ntwo\nthree!\nfour\nfive\nsix\n",
            "one\ntwo\nthree\nfour?\nfive\nsix\n",
        ] {
            let err = three_way(&fork.snapshot(), run, &canonical.snapshot()).unwrap_err();
            assert!(matches!(err, MergeError::Overlap { .. }), "{run:?}");
        }
    }

    #[test]
    fn the_same_edit_on_both_sides_is_not_duplicated() {
        let fork = doc(BASE);
        let canonical = FileDoc::from_snapshot(random_client_id(), &fork.snapshot()).unwrap();
        canonical.set_content("one\ntwo\nTHREE\nfour\nfive\nsix\n");
        let merged = three_way(
            &fork.snapshot(),
            "one\ntwo\nTHREE\nfour\nfive\nsix!\n",
            &canonical.snapshot(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(merged.content, "one\ntwo\nTHREE\nfour\nfive\nsix!\n");
        assert_eq!(
            three_way(&fork.snapshot(), BASE, &canonical.snapshot()).unwrap(),
            None
        );
    }

    #[test]
    fn a_file_without_a_trailing_newline_and_astral_text_merge_whole() {
        let fork = doc("a🚀\nb\nc");
        let canonical = FileDoc::from_snapshot(random_client_id(), &fork.snapshot()).unwrap();
        let merged = three_way(&fork.snapshot(), "a🚀\nb\nc🚁", &canonical.snapshot())
            .unwrap()
            .unwrap();
        assert_eq!(merged.content, "a🚀\nb\nc🚁");
    }
}
