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

/// Binary frame kinds. Only [`FrameKind::CanonicalUpdate`] is accepted by the
/// server today; the others arrive with presence and Runs.
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileKind {
    Text,
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
    },
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// The server's `THREAD_WIRE_FIXTURES`, verbatim.
    #[test]
    fn matches_the_server_fixtures() {
        let fixtures = [
            (
                Frame {
                    version: 1,
                    kind: 1,
                    seq: 0,
                    file_id: 1,
                    client_seq: 1,
                    payload: vec![0xaa, 0xbb],
                },
                "0101000101aabb",
            ),
            (
                Frame {
                    version: 1,
                    kind: 1,
                    seq: 300,
                    file_id: 128,
                    client_seq: 0,
                    payload: vec![],
                },
                "0101ac02800100",
            ),
            (
                Frame {
                    version: 1,
                    kind: 2,
                    seq: 0,
                    file_id: 0,
                    client_seq: 1 << 32,
                    payload: vec![0x01],
                },
                "01020000808080801001",
            ),
        ];
        for (frame, expected) in fixtures {
            let bytes = encode(&frame).unwrap();
            assert_eq!(hex(&bytes), expected);
            assert_eq!(decode(&bytes).unwrap(), frame);
        }
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
