//! One touched text file as a Yjs document.
//!
//! Two details are part of the contract with every other replica — the web
//! viewer runs `yjs` itself — and are not free choices:
//!
//! * the text lives under the root name [`TEXT_NAME`];
//! * offsets count **UTF-16** units, as Yjs in a browser does.
//!
//! And one trick makes a file's starting point safe without coordination.
//! Every replica that holds the Base builds the file's Base content as the same
//! updates — same client id ([`SEED_CLIENT`]), same clocks, same chunks — so the
//! seed is byte-identical everywhere and applying it twice, or from two
//! replicas, changes nothing. Edits then happen under each replica's own
//! random client id.

use yrs::updates::decoder::Decode;
use yrs::{
    Doc, GetString, OffsetKind, Options, ReadTxn, StateVector, Text, TextRef, Transact, Update,
};

pub const TEXT_NAME: &str = "content";

/// The client id every replica seeds Base content under. Never used for edits.
pub const SEED_CLIENT: u64 = 1;

/// Seed chunk size in UTF-8 bytes, comfortably under the wire's payload cap
/// once Yjs' own encoding is added.
const SEED_CHUNK_BYTES: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum DocError {
    #[error("not a Yjs update: {0}")]
    Decode(String),
    #[error("could not apply update: {0}")]
    Apply(String),
}

fn options(client_id: u64) -> Options {
    let mut options = Options::with_client_id(client_id);
    options.offset_kind = OffsetKind::Utf16;
    options
}

/// A replica's own Yjs client id: random, never the seed's.
pub fn random_client_id() -> u64 {
    let n = uuid::Uuid::new_v4().as_u128() as u64 & 0x7fff_ffff;
    n.max(SEED_CLIENT + 1)
}

pub struct FileDoc {
    doc: Doc,
    text: TextRef,
}

impl FileDoc {
    pub fn new(client_id: u64) -> Self {
        let doc = Doc::with_options(options(client_id));
        let text = doc.get_or_insert_text(TEXT_NAME);
        Self { doc, text }
    }

    /// The deterministic updates that build `base` from nothing.
    pub fn seed_updates(base: &str) -> Vec<Vec<u8>> {
        let doc = Doc::with_options(options(SEED_CLIENT));
        let text = doc.get_or_insert_text(TEXT_NAME);
        let mut updates = Vec::new();
        let mut rest = base;
        while !rest.is_empty() {
            let mut cut = rest.len().min(SEED_CHUNK_BYTES);
            while !rest.is_char_boundary(cut) {
                cut -= 1;
            }
            let (chunk, tail) = rest.split_at(cut);
            let mut txn = doc.transact_mut();
            text.push(&mut txn, chunk);
            updates.push(txn.encode_update_v1());
            drop(txn);
            rest = tail;
        }
        updates
    }

    pub fn apply(&self, update: &[u8]) -> Result<(), DocError> {
        let update = Update::decode_v1(update).map_err(|e| DocError::Decode(e.to_string()))?;
        let mut txn = self.doc.transact_mut();
        txn.apply_update(update)
            .map_err(|e| DocError::Apply(e.to_string()))
    }

    /// The whole document as one update, to rebuild it later with
    /// [`FileDoc::from_snapshot`].
    pub fn snapshot(&self) -> Vec<u8> {
        self.doc
            .transact()
            .encode_state_as_update_v1(&StateVector::default())
    }

    /// A document in exactly the state `snapshot` recorded, editing under
    /// `client_id`. Used to make an edit *relative to that moment* and merge
    /// it into the live document — a three-way merge done by the CRDT.
    pub fn from_snapshot(client_id: u64, snapshot: &[u8]) -> Result<Self, DocError> {
        let doc = Self::new(client_id);
        doc.apply(snapshot)?;
        Ok(doc)
    }

    pub fn content(&self) -> String {
        self.text.get_string(&self.doc.transact())
    }

    /// Make the text equal `next` with the smallest single edit, and answer the
    /// update to send — or `None` when it already was.
    ///
    /// One replace between the common prefix and suffix rather than a full
    /// diff: a save from an editor is usually one region, and a replace of the
    /// changed span keeps concurrent edits elsewhere in the file intact. The
    /// span is widened so it never splits a surrogate pair.
    pub fn set_content(&self, next: &str) -> Option<Vec<u8>> {
        let current = self.content();
        if current == next {
            return None;
        }
        let old: Vec<u16> = current.encode_utf16().collect();
        let new: Vec<u16> = next.encode_utf16().collect();
        let mut prefix = old.iter().zip(&new).take_while(|(a, b)| a == b).count();
        while prefix > 0 && is_high_surrogate(old[prefix - 1]) {
            prefix -= 1;
        }
        let max_suffix = old.len().min(new.len()) - prefix;
        let mut suffix = old
            .iter()
            .rev()
            .zip(new.iter().rev())
            .take(max_suffix)
            .take_while(|(a, b)| a == b)
            .count();
        while suffix > 0 && is_low_surrogate(old[old.len() - suffix]) {
            suffix -= 1;
        }
        let removed = old.len() - prefix - suffix;
        let inserted = String::from_utf16_lossy(&new[prefix..new.len() - suffix]);

        let mut txn = self.doc.transact_mut();
        if removed > 0 {
            self.text
                .remove_range(&mut txn, prefix as u32, removed as u32);
        }
        if !inserted.is_empty() {
            self.text.insert(&mut txn, prefix as u32, &inserted);
        }
        Some(txn.encode_update_v1())
    }
}

fn is_high_surrogate(unit: u16) -> bool {
    (0xd800..0xdc00).contains(&unit)
}

fn is_low_surrogate(unit: u16) -> bool {
    (0xdc00..0xe000).contains(&unit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeding_from_two_replicas_does_not_duplicate() {
        let base = "line one\nline two\n";
        let a = FileDoc::new(random_client_id());
        let b = FileDoc::new(random_client_id());
        for u in FileDoc::seed_updates(base) {
            a.apply(&u).unwrap();
            b.apply(&u).unwrap();
        }
        // b's seed arrives at a again, as it would from the journal.
        for u in FileDoc::seed_updates(base) {
            a.apply(&u).unwrap();
        }
        assert_eq!(a.content(), base);
        assert_eq!(FileDoc::seed_updates(base), FileDoc::seed_updates(base));
    }

    #[test]
    fn large_bases_seed_in_chunks_that_fit_the_wire() {
        let base = "é".repeat(100_000);
        let updates = FileDoc::seed_updates(&base);
        assert!(updates.len() > 1);
        assert!(updates
            .iter()
            .all(|u| u.len() < crate::wire::MAX_PAYLOAD_BYTES));
        let doc = FileDoc::new(random_client_id());
        for u in &updates {
            doc.apply(u).unwrap();
        }
        assert_eq!(doc.content(), base);
    }

    #[test]
    fn concurrent_edits_in_different_places_both_survive() {
        let base = "alpha\nbeta\ngamma\n";
        let a = FileDoc::new(random_client_id());
        let b = FileDoc::new(random_client_id());
        for u in FileDoc::seed_updates(base) {
            a.apply(&u).unwrap();
            b.apply(&u).unwrap();
        }
        let ua = a.set_content("ALPHA\nbeta\ngamma\n").unwrap();
        let ub = b.set_content("alpha\nbeta\nGAMMA 🚀\n").unwrap();
        a.apply(&ub).unwrap();
        b.apply(&ua).unwrap();
        assert_eq!(a.content(), "ALPHA\nbeta\nGAMMA 🚀\n");
        assert_eq!(a.content(), b.content());
        assert!(a.set_content(&a.content()).is_none());
    }

    #[test]
    fn edits_next_to_astral_characters_keep_them_whole() {
        let doc = FileDoc::new(random_client_id());
        doc.set_content("a🚀b");
        doc.set_content("a🚁b");
        assert_eq!(doc.content(), "a🚁b");
        doc.set_content("🚀🚁");
        assert_eq!(doc.content(), "🚀🚁");
    }
}
