//! Thread socket wire protocol v1, transcribed from the server's contract
//! (`@atlas/contracts` `threads.ts`, ATL-394).
//!
//! Two kinds of frame share the socket:
//!
//! * **binary** for hot traffic: `version` (1 byte), `kind` (1 byte), then
//!   unsigned LEB128 varints `seq`, `file_id`, `client_seq`, then an opaque
//!   payload to the end. The server reads only the header;
//! * **JSON text** for rare control messages, discriminated by `t`.
//!
//! The golden frames in the tests below are the server's `THREAD_WIRE_FIXTURES`
//! byte for byte. Changing either side without the other fails a test there or
//! here.

use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u8 = 1;

/// Largest payload one frame may carry. The server refuses anything bigger
/// with `nack payload_too_large`, so the client splits first.
pub const MAX_PAYLOAD_BYTES: usize = 256 * 1024;

/// JavaScript's `Number.MAX_SAFE_INTEGER`: the server decodes varints into a
/// JS number, so anything above it would not survive the round trip.
const MAX_SAFE: u64 = (1 << 53) - 1;

/// Binary frame kinds. Canonical updates are journaled; the rest are relayed
/// live and never stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    /// A Yjs update to one text file's canonical document. Journaled.
    CanonicalUpdate = 1,
    /// Awareness / presence. Never journaled.
    Awareness = 2,
    /// A live Run's SessionDelta stream. Never journaled.
    RunStream = 3,
    /// A Run worktree's file stream. Never journaled.
    RunFile = 4,
}

/// A decoded binary frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub version: u8,
    pub kind: u8,
    /// Assigned by the server; `0` on a frame a client sends.
    pub seq: u64,
    pub file_id: u64,
    /// The sender's own counter; `0` on a frame the server relays.
    pub client_seq: u64,
    pub payload: Vec<u8>,
}

impl Frame {
    /// A canonical update as a client sends it.
    pub fn update(file_id: u64, client_seq: u64, payload: Vec<u8>) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            kind: FrameKind::CanonicalUpdate as u8,
            seq: 0,
            file_id,
            client_seq,
            payload,
        }
    }

    /// A live Run frame (ATL-405): the header's `file_id` slot carries the
    /// Run's `runNo`, and the server relays it with `seq` 0, never storing it.
    pub fn run(kind: FrameKind, run_no: u64, client_seq: u64, payload: Vec<u8>) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            kind: kind as u8,
            seq: 0,
            file_id: run_no,
            client_seq,
            payload,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WireError {
    #[error("varint {0} does not fit the wire (max 2^53 - 1)")]
    OutOfRange(u64),
}

fn write_varint(out: &mut Vec<u8>, value: u64) -> Result<(), WireError> {
    if value > MAX_SAFE {
        return Err(WireError::OutOfRange(value));
    }
    let mut v = value;
    while v >= 0x80 {
        out.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
    Ok(())
}

fn read_varint(bytes: &[u8], at: &mut usize) -> Option<u64> {
    let mut result: u64 = 0;
    for n in 0..8 {
        let byte = *bytes.get(*at)?;
        *at += 1;
        result |= u64::from(byte & 0x7f) << (7 * n);
        if byte & 0x80 == 0 {
            return (result <= MAX_SAFE).then_some(result);
        }
    }
    None
}

/// Encode a frame. Fails only for a number the server could not read back.
pub fn encode(frame: &Frame) -> Result<Vec<u8>, WireError> {
    let mut out = Vec::with_capacity(8 + frame.payload.len());
    out.push(frame.version);
    out.push(frame.kind);
    write_varint(&mut out, frame.seq)?;
    write_varint(&mut out, frame.file_id)?;
    write_varint(&mut out, frame.client_seq)?;
    out.extend_from_slice(&frame.payload);
    Ok(out)
}

/// Decode a frame, or `None` when the header is truncated or malformed.
pub fn decode(bytes: &[u8]) -> Option<Frame> {
    if bytes.len() < 2 {
        return None;
    }
    let mut at = 2;
    let seq = read_varint(bytes, &mut at)?;
    let file_id = read_varint(bytes, &mut at)?;
    let client_seq = read_varint(bytes, &mut at)?;
    Some(Frame {
        version: bytes[0],
        kind: bytes[1],
        seq,
        file_id,
        client_seq,
        payload: bytes[at..].to_vec(),
    })
}

/// One file in a thread's canonical state. `file_id` is stable across renames.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TreeEntry {
    pub file_id: u64,
    pub path: String,
    pub kind: FileKind,
    /// Advanced by every accepted merge, never by keystrokes (ADR-0022): what
    /// `merge.submit` compares. Absent from servers before ATL-398.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_version: Option<u64>,
    /// A binary file's content now: a blob under the thread (ATL-403).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
    /// Deleted. The entry stays, with its id and history.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deleted: bool,
    /// The path the file entered the thread under, when a rename moved it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
}

/// How a file syncs (ATL-403): co-edited text, or whole blobs — what git's NUL
/// heuristic calls binary, or anything over 1 MiB — last writer wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileKind {
    Text,
    Binary,
}

/// A thread role, as the server names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Owner,
    Participant,
    Viewer,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Owner => "owner",
            Role::Participant => "participant",
            Role::Viewer => "viewer",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "owner" => Some(Role::Owner),
            "participant" => Some(Role::Participant),
            "viewer" => Some(Role::Viewer),
            _ => None,
        }
    }
}

/// Control frames a client sends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "t")]
pub enum ClientControl {
    /// First frame on every connection.
    #[serde(rename = "hello", rename_all = "camelCase")]
    Hello {
        protocol: u8,
        client_id: String,
        since: u64,
    },
    /// Make sure a file has a tree entry; idempotent across replicas.
    #[serde(rename = "tree.ensure", rename_all = "camelCase")]
    TreeEnsure {
        client_seq: u64,
        path: String,
        kind: FileKind,
        /// The file's Base content, uploaded as a blob first (ATL-402): its
        /// hash, `Some(None)` when the Base has no such file, or absent when
        /// this replica could not say.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        base_blob: Option<Option<String>>,
    },
    /// Move a file: the same id at a new path (ATL-403).
    #[serde(rename = "tree.rename", rename_all = "camelCase")]
    TreeRename {
        client_seq: u64,
        file_id: u64,
        path: String,
    },
    /// Delete a file; its entry stays.
    #[serde(rename = "tree.delete", rename_all = "camelCase")]
    TreeDelete { client_seq: u64, file_id: u64 },
    /// A binary file's content is now this blob, uploaded first.
    #[serde(rename = "blob.set", rename_all = "camelCase")]
    BlobSet {
        client_seq: u64,
        file_id: u64,
        blob: String,
    },
    /// This replica lacks the Base: `have` lists the commits it holds, so a
    /// thin bundle can be built. Empty asks for a full one (ATL-402).
    #[serde(rename = "bundle.request", rename_all = "camelCase")]
    BundleRequest { client_seq: u64, have: Vec<String> },
    /// The bundle somebody asked for is uploaded under `sha`.
    #[serde(rename = "bundle.ready", rename_all = "camelCase")]
    BundleReady {
        client_seq: u64,
        request_id: String,
        sha: String,
    },
    /// The bundle built for somebody was over the Organisation's limit.
    #[serde(rename = "bundle.failed", rename_all = "camelCase")]
    BundleFailed {
        client_seq: u64,
        request_id: String,
        reason: BundleFailure,
        bytes: u64,
    },
    /// A Run begins (ADR-0022). This replica is its Runner.
    #[serde(rename = "run.start", rename_all = "camelCase")]
    RunStart {
        client_seq: u64,
        run_id: String,
        agent: String,
        model: String,
        fork_seq: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context_anchor: Option<String>,
    },
    /// The Run's turn is over.
    #[serde(rename = "run.end", rename_all = "camelCase")]
    RunEnd {
        client_seq: u64,
        run_id: String,
        outcome: RunOutcome,
    },
    /// The Run's hunks, one Yjs update per file computed against the merge
    /// version the Runner merged with.
    #[serde(rename = "merge.submit", rename_all = "camelCase")]
    MergeSubmit {
        client_seq: u64,
        run_id: String,
        files: Vec<MergeFile>,
    },
}

/// Why no bundle is coming.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BundleFailure {
    /// Over the Organisation's Base bundle limit.
    TooLarge,
    /// Nothing cached fits and nobody who could build one is online.
    NoReplicaOnline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunOutcome {
    Completed,
    Interrupted,
}

/// One file of a `merge.submit`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeFile {
    pub file_id: u64,
    pub base_version: u64,
    /// The Yjs update, base64.
    pub update: String,
    /// SHA-256 hex of the file's content after the merge, uploaded under the
    /// thread for its Thread Version.
    pub blob: String,
}

/// A file at a merge version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileVersion {
    pub file_id: u64,
    pub version: u64,
}

/// A file another Runner's merge moved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MergedFile {
    pub file_id: u64,
    pub version: u64,
    pub blob: String,
}

/// A Run as the server describes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadRun {
    pub run_id: String,
    pub run_no: u64,
    pub prompted_by: String,
    pub runner_id: String,
    pub agent: String,
    pub model: String,
    pub fork_seq: u64,
    #[serde(default)]
    pub context_anchor: Option<String>,
    /// `running`, `merged`, `ended`, `interrupted` or `declined`.
    pub status: String,
    pub started_at: u64,
    #[serde(default)]
    pub ended_at: Option<u64>,
    #[serde(default)]
    pub merged_version: Option<u64>,
}

/// Control frames the server sends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "t")]
pub enum ServerControl {
    #[serde(rename = "welcome", rename_all = "camelCase")]
    Welcome {
        protocol: u8,
        thread_id: String,
        workspace_id: String,
        org_id: String,
        role: Role,
        head: u64,
        last_client_seq: u64,
    },
    #[serde(rename = "tree")]
    Tree { seq: u64, entry: TreeEntry },
    #[serde(rename = "ack", rename_all = "camelCase")]
    Ack {
        client_seq: u64,
        seq: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        file_id: Option<u64>,
    },
    #[serde(rename = "nack", rename_all = "camelCase")]
    Nack {
        client_seq: u64,
        code: String,
        message: String,
    },
    #[serde(rename = "synced")]
    Synced { head: u64 },
    #[serde(rename = "error")]
    Error { code: String, message: String },
    /// A Run started, merged, ended or was interrupted. Sent to every socket.
    #[serde(rename = "run")]
    Run { run: ThreadRun },
    #[serde(rename = "merge.accepted", rename_all = "camelCase")]
    MergeAccepted {
        client_seq: u64,
        run_id: String,
        version: u64,
        files: Vec<FileVersion>,
    },
    /// Some file moved past its `baseVersion`: recompute and resubmit.
    #[serde(rename = "merge.rejected", rename_all = "camelCase")]
    MergeRejected {
        client_seq: u64,
        run_id: String,
        versions: Vec<FileVersion>,
    },
    /// Another Runner's merge landed.
    #[serde(rename = "merged", rename_all = "camelCase")]
    Merged {
        run_id: String,
        version: u64,
        files: Vec<MergedFile>,
    },
    /// Somebody lacks the Base: build a bundle against `have`, upload it,
    /// and say `bundle.ready` (ATL-402).
    #[serde(rename = "bundle.wanted", rename_all = "camelCase")]
    BundleWanted { request_id: String, have: Vec<String> },
    /// The first answer to `bundle.request`; what follows names `request_id`.
    #[serde(rename = "bundle.pending", rename_all = "camelCase")]
    BundlePending { client_seq: u64, request_id: String },
    #[serde(rename = "bundle.available", rename_all = "camelCase")]
    BundleAvailable {
        request_id: String,
        sha: String,
        bytes: u64,
    },
    #[serde(rename = "bundle.unavailable", rename_all = "camelCase")]
    BundleUnavailable {
        request_id: String,
        reason: BundleFailure,
        #[serde(default)]
        bytes: Option<u64>,
    },
    /// Any frame this client does not act on yet (presence, bundles, roles…):
    /// read and ignored rather than reported as unreadable.
    #[serde(other)]
    Other,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// The server's wire fixtures (`@atlas/contracts`
    /// `fixtures/thread-wire-v1.json`), vendored here as a copy.
    const FIXTURES: &str = include_str!("../tests/fixtures/thread-wire-v1.json");

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Fixture {
        name: String,
        frame: FixtureFrame,
        hex: String,
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct FixtureFrame {
        version: u8,
        kind: u8,
        seq: u64,
        file_id: u64,
        client_seq: u64,
        payload: Vec<u8>,
    }

    #[test]
    fn matches_the_server_fixtures() {
        let fixtures: Vec<Fixture> = serde_json::from_str(FIXTURES).unwrap();
        assert!(!fixtures.is_empty());
        for f in fixtures {
            let frame = Frame {
                version: f.frame.version,
                kind: f.frame.kind,
                seq: f.frame.seq,
                file_id: f.frame.file_id,
                client_seq: f.frame.client_seq,
                payload: f.frame.payload,
            };
            let bytes = encode(&frame).unwrap();
            assert_eq!(hex(&bytes), f.hex, "{}", f.name);
            assert_eq!(decode(&bytes).unwrap(), frame, "{}", f.name);
        }
    }

    /// When the server checkout sits beside this one (the usual layout), the
    /// vendored copy must still equal the contract's file.
    #[test]
    fn the_vendored_fixtures_match_the_server_checkout_when_present() {
        let server = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../server/packages/contracts/fixtures/thread-wire-v1.json");
        let Ok(theirs) = std::fs::read_to_string(&server) else {
            return;
        };
        let ours: serde_json::Value = serde_json::from_str(FIXTURES).unwrap();
        let theirs: serde_json::Value = serde_json::from_str(&theirs).unwrap();
        assert_eq!(ours, theirs, "re-copy {}", server.display());
    }

    #[test]
    fn refuses_truncated_headers_and_unsafe_numbers() {
        assert_eq!(decode(&[1]), None);
        assert_eq!(decode(&[1, 1, 0x80]), None);
        assert_eq!(decode(&[1, 1, 0, 0]), None);
        assert_eq!(
            encode(&Frame::update(1, 1 << 53, vec![])),
            Err(WireError::OutOfRange(1 << 53))
        );
    }

    #[test]
    fn control_frames_speak_the_servers_json() {
        let hello = ClientControl::Hello {
            protocol: 1,
            client_id: "replica-01".into(),
            since: 3,
        };
        assert_eq!(
            serde_json::to_value(&hello).unwrap(),
            serde_json::json!({ "t": "hello", "protocol": 1, "clientId": "replica-01", "since": 3 })
        );
        let ensure = ClientControl::TreeEnsure {
            client_seq: 2,
            path: "src/a.ts".into(),
            kind: FileKind::Text,
            base_blob: None,
        };
        assert_eq!(
            serde_json::to_value(&ensure).unwrap(),
            serde_json::json!({ "t": "tree.ensure", "clientSeq": 2, "path": "src/a.ts", "kind": "text" })
        );
        let welcome: ServerControl = serde_json::from_value(serde_json::json!({
            "t": "welcome", "protocol": 1, "threadId": "T", "workspaceId": "W", "orgId": "O",
            "role": "participant", "head": 5, "lastClientSeq": 2
        }))
        .unwrap();
        assert!(matches!(
            welcome,
            ServerControl::Welcome {
                head: 5,
                last_client_seq: 2,
                role: Role::Participant,
                ..
            }
        ));
        let start = ClientControl::RunStart {
            client_seq: 4,
            run_id: "run-0001".into(),
            agent: "claude-code".into(),
            model: "opus".into(),
            fork_seq: 7,
            context_anchor: None,
        };
        assert_eq!(
            serde_json::to_value(&start).unwrap(),
            serde_json::json!({ "t": "run.start", "clientSeq": 4, "runId": "run-0001",
                "agent": "claude-code", "model": "opus", "forkSeq": 7 })
        );
        let submit = ClientControl::MergeSubmit {
            client_seq: 5,
            run_id: "run-0001".into(),
            files: vec![MergeFile {
                file_id: 2,
                base_version: 1,
                update: "AQ==".into(),
                blob: "a".repeat(64),
            }],
        };
        assert_eq!(
            serde_json::to_value(&submit).unwrap()["files"][0],
            serde_json::json!({ "fileId": 2, "baseVersion": 1, "update": "AQ==", "blob": "a".repeat(64) })
        );
        let end = ClientControl::RunEnd {
            client_seq: 6,
            run_id: "run-0001".into(),
            outcome: RunOutcome::Interrupted,
        };
        assert_eq!(
            serde_json::to_value(&end).unwrap(),
            serde_json::json!({ "t": "run.end", "clientSeq": 6, "runId": "run-0001", "outcome": "interrupted" })
        );
        let rejected: ServerControl = serde_json::from_value(serde_json::json!({
            "t": "merge.rejected", "clientSeq": 5, "runId": "run-0001",
            "versions": [{ "fileId": 2, "version": 3 }]
        }))
        .unwrap();
        assert!(
            matches!(rejected, ServerControl::MergeRejected { ref versions, .. }
            if versions == &[FileVersion { file_id: 2, version: 3 }])
        );
        let run: ServerControl = serde_json::from_value(serde_json::json!({
            "t": "run", "run": { "runId": "run-0001", "runNo": 1, "promptedBy": "u1",
            "runnerId": "u1", "agent": "a", "model": "m", "forkSeq": 0, "contextAnchor": null,
            "status": "running", "startedAt": 1, "endedAt": null, "mergedVersion": null }
        }))
        .unwrap();
        assert!(matches!(run, ServerControl::Run { ref run } if run.run_no == 1));
        // Frames this client does not act on are read, not reported unreadable.
        let presence: ServerControl =
            serde_json::from_str(r#"{"t":"presence.left","peerId":"p","userId":"u"}"#).unwrap();
        assert_eq!(presence, ServerControl::Other);
        let unavailable: ServerControl = serde_json::from_str(
            r#"{"t":"bundle.unavailable","requestId":"r","reason":"too_large","bytes":9}"#,
        )
        .unwrap();
        assert_eq!(
            unavailable,
            ServerControl::BundleUnavailable {
                request_id: "r".into(),
                reason: BundleFailure::TooLarge,
                bytes: Some(9)
            }
        );
        let ensure = ClientControl::TreeEnsure {
            client_seq: 3,
            path: "a.ts".into(),
            kind: FileKind::Text,
            base_blob: Some(None),
        };
        assert_eq!(serde_json::to_value(&ensure).unwrap()["baseBlob"], serde_json::Value::Null);
        let entry: TreeEntry = serde_json::from_str(
            r#"{"fileId":1,"path":"a.ts","kind":"text","baseBlob":null,"mergeVersion":2}"#,
        )
        .unwrap();
        assert_eq!(entry.merge_version, Some(2));

        let ack: ServerControl =
            serde_json::from_str(r#"{"t":"ack","clientSeq":1,"seq":4,"fileId":9}"#).unwrap();
        assert_eq!(
            ack,
            ServerControl::Ack {
                client_seq: 1,
                seq: 4,
                file_id: Some(9)
            }
        );
    }
}
