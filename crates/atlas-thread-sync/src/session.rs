//! One connection to one Shared Thread, driving a [`Replica`].
//!
//! The session speaks wire v1: `hello`, the catch-up replay until `synced`,
//! `tree.ensure` for files this replica introduces, and canonical updates both
//! ways. Every frame it sends carries the next `client_seq`, so a resend after
//! a lost ack is recognised by the server and stored once.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::git;
use crate::replica::{looks_textual, LocalChange, Replica, ReplicaError};
use crate::secrets::{secret_reason, SecretReason};
use crate::transport::{Message, Transport, TransportError};
use crate::wire::{self, ClientControl, FileKind, Frame, FrameKind, Role, ServerControl};

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error(transparent)]
    Replica(#[from] ReplicaError),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("the server refused: {code}: {message}")]
    Refused { code: String, message: String },
    #[error("the socket closed before the thread was synced")]
    ClosedEarly,
    #[error("timed out waiting for the server")]
    Timeout,
}

/// How long to wait for the server's answer to something we asked.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(15);

/// What sharing the sharer's working changes did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ShareReport {
    /// Paths that became canonical changes.
    pub shared: Vec<String>,
    /// Paths held back because they look like secrets, and why.
    pub blocked: Vec<(String, SecretReason)>,
}

pub struct ThreadSession<T: Transport> {
    transport: T,
    replica: Replica,
    role: Option<Role>,
    head: u64,
    next_client_seq: u64,
    /// `tree.ensure` frames awaiting their ack, by `client_seq`.
    pending_tree: HashMap<u64, String>,
    gaps: u64,
    updates_sent: u64,
    last_nack: Option<(String, String)>,
}

impl<T: Transport> ThreadSession<T> {
    /// Say hello as `client_id` and apply the thread's journal until `synced`.
    ///
    /// `client_id` should be stable for this replica across reconnects: the
    /// server's welcome then says which of our frames it already stored.
    pub async fn open(
        mut transport: T,
        replica: Replica,
        client_id: &str,
    ) -> Result<Self, SessionError> {
        let hello = ClientControl::Hello {
            protocol: wire::PROTOCOL_VERSION,
            client_id: client_id.to_string(),
            since: 0,
        };
        transport
            .send(Message::Text(
                serde_json::to_string(&hello).expect("hello is JSON"),
            ))
            .await?;
        let mut session = Self {
            transport,
            replica,
            role: None,
            head: 0,
            next_client_seq: 1,
            pending_tree: HashMap::new(),
            gaps: 0,
            updates_sent: 0,
            last_nack: None,
        };
        loop {
            let message = tokio::time::timeout(ANSWER_TIMEOUT, session.transport.recv())
                .await
                .map_err(|_| SessionError::Timeout)?
                .ok_or(SessionError::ClosedEarly)?;
            if session.handle(message).await? == Handled::Synced {
                return Ok(session);
            }
        }
    }

    pub fn replica(&self) -> &Replica {
        &self.replica
    }

    pub fn role(&self) -> Option<Role> {
        self.role
    }

    /// The newest `seq` this replica has seen.
    pub fn head(&self) -> u64 {
        self.head
    }

    /// How many times `seq` jumped — a frame this replica never received.
    pub fn gaps(&self) -> u64 {
        self.gaps
    }

    /// Canonical updates this session has sent. A remote write that echoed back
    /// would show up here.
    pub fn updates_sent(&self) -> u64 {
        self.updates_sent
    }

    pub fn last_nack(&self) -> Option<&(String, String)> {
        self.last_nack.as_ref()
    }

    /// Check the worktree out (lazily, once) and write the canonical state on
    /// it. Called when the person first opens a file or prompts.
    pub fn materialize(&mut self) -> Result<PathBuf, SessionError> {
        Ok(self.replica.materialize()?.to_path_buf())
    }

    /// Receive and apply whatever arrives, until nothing has for `idle`.
    pub async fn pump(&mut self, idle: Duration) -> Result<usize, SessionError> {
        let mut handled = 0;
        while let Ok(next) = tokio::time::timeout(idle, self.transport.recv()).await {
            let Some(message) = next else {
                return Err(SessionError::ClosedEarly);
            };
            self.handle(message).await?;
            handled += 1;
        }
        Ok(handled)
    }

    /// Apply one message from the server.
    pub async fn receive(&mut self, message: Message) -> Result<(), SessionError> {
        self.handle(message).await.map(|_| ())
    }

    /// The next message from the server, for a caller running its own loop.
    pub async fn next_message(&mut self) -> Option<Message> {
        self.transport.recv().await
    }

    /// The person saved `rel` in the replica worktree, from any editor.
    /// Answers what it amounted to; an [`LocalChange::Echo`] sent nothing.
    pub async fn file_saved(&mut self, rel: &str) -> Result<LocalChange, SessionError> {
        match self.replica.local_change(rel)? {
            LocalChange::Update { file_id, update } => {
                self.send_update(file_id, update).await?;
                Ok(LocalChange::Update {
                    file_id,
                    update: Vec::new(),
                })
            }
            LocalChange::NewFile { path } => {
                // A credential created in the replica stays on this machine,
                // for the same reason it is held back at share time.
                let content = self.replica.read_disk(&path)?;
                if let Some(reason) = secret_reason(&path, &content) {
                    tracing::info!(target: "atlas_thread_sync", ?reason, "holding back a new file that looks secret");
                    return Ok(LocalChange::Ignored);
                }
                let file_id = self.ensure_file(&path, true).await?;
                if let LocalChange::Update { update, .. } = self.replica.local_change(&path)? {
                    self.send_update(file_id, update).await?;
                }
                Ok(LocalChange::NewFile { path })
            }
            other => Ok(other),
        }
    }

    /// Make the sharer's uncommitted work the thread's first canonical changes:
    /// every modified, added or untracked text file in `checkout` (ignored
    /// files never appear; binary and deleted ones wait for ATL-403), except
    /// files that look like secrets, which are held back and reported. The
    /// person's checkout is only read.
    pub async fn share_working_changes(
        &mut self,
        checkout: &Path,
    ) -> Result<ShareReport, SessionError> {
        let mut report = ShareReport::default();
        for dirty in git::dirty_paths(checkout).map_err(ReplicaError::from)? {
            if dirty.deleted || !crate::path::is_valid(&dirty.path) {
                continue;
            }
            let Ok(target) = crate::path::resolve(checkout, &dirty.path) else {
                continue;
            };
            let Ok(bytes) = std::fs::read(&target) else {
                continue;
            };
            if !looks_textual(&bytes) {
                continue;
            }
            let content = String::from_utf8_lossy(&bytes);
            if let Some(reason) = secret_reason(&dirty.path, &content) {
                report.blocked.push((dirty.path, reason));
                continue;
            }
            let file_id = self.ensure_file(&dirty.path, true).await?;
            if let Some(update) = self.replica.set_text(file_id, &content)? {
                self.send_update(file_id, update).await?;
            }
            report.shared.push(dirty.path);
        }
        Ok(report)
    }

    /// The file's id in this thread, asking the server for an entry if it has
    /// none. When `introduced`, this replica also publishes the file's Base
    /// seed: harmless if another replica already did, since seeds are
    /// byte-identical everywhere, and it lets a replica without the Base
    /// rebuild the file from the journal alone.
    async fn ensure_file(&mut self, path: &str, introduced: bool) -> Result<u64, SessionError> {
        if let Some(id) = self.replica.file_id(path) {
            return Ok(id);
        }
        let client_seq = self.take_client_seq();
        self.pending_tree.insert(client_seq, path.to_string());
        let ensure = ClientControl::TreeEnsure {
            client_seq,
            path: path.to_string(),
            kind: FileKind::Text,
        };
        self.transport
            .send(Message::Text(
                serde_json::to_string(&ensure).expect("tree.ensure is JSON"),
            ))
            .await?;
        let file_id = loop {
            if let Some(id) = self.replica.file_id(path) {
                break id;
            }
            if !self.pending_tree.contains_key(&client_seq) {
                let (code, message) = self.last_nack.clone().unwrap_or_default();
                return Err(SessionError::Refused { code, message });
            }
            let message = tokio::time::timeout(ANSWER_TIMEOUT, self.transport.recv())
                .await
                .map_err(|_| SessionError::Timeout)?
                .ok_or(SessionError::ClosedEarly)?;
            self.handle(message).await?;
        };
        if introduced {
            for seed in self.replica.seed_for(path)? {
                self.send_update(file_id, seed).await?;
            }
        }
        Ok(file_id)
    }

    async fn send_update(&mut self, file_id: u64, update: Vec<u8>) -> Result<(), SessionError> {
        if update.len() > wire::MAX_PAYLOAD_BYTES {
            // The server would refuse it. Splitting one edit across frames is
            // part of offline buffering (ATL-404); until then it is reported
            // rather than sent to be refused.
            return Err(SessionError::Refused {
                code: "payload_too_large".into(),
                message: format!("one edit of {} bytes is over the frame limit", update.len()),
            });
        }
        let client_seq = self.take_client_seq();
        let bytes =
            wire::encode(&Frame::update(file_id, client_seq, update)).expect("small numbers");
        self.transport.send(Message::Binary(bytes)).await?;
        self.updates_sent += 1;
        Ok(())
    }

    fn take_client_seq(&mut self) -> u64 {
        let seq = self.next_client_seq;
        self.next_client_seq += 1;
        seq
    }

    fn saw_seq(&mut self, seq: u64) {
        if seq > self.head + 1 {
            self.gaps += 1;
        }
        self.head = self.head.max(seq);
    }

    async fn handle(&mut self, message: Message) -> Result<Handled, SessionError> {
        match message {
            Message::Text(text) => {
                let Ok(frame) = serde_json::from_str::<ServerControl>(&text) else {
                    tracing::warn!(target: "atlas_thread_sync", "unreadable control frame");
                    return Ok(Handled::Other);
                };
                match frame {
                    ServerControl::Welcome {
                        role,
                        last_client_seq,
                        ..
                    } => {
                        self.role = Some(role);
                        // Never reuse a client_seq the server already stored.
                        self.next_client_seq = self.next_client_seq.max(last_client_seq + 1);
                    }
                    ServerControl::Tree { seq, entry } => {
                        self.saw_seq(seq);
                        self.replica.add_entry(entry.file_id, &entry.path)?;
                    }
                    ServerControl::Ack {
                        client_seq,
                        seq,
                        file_id,
                    } => {
                        self.saw_seq(seq);
                        if let (Some(path), Some(file_id)) =
                            (self.pending_tree.remove(&client_seq), file_id)
                        {
                            self.replica.add_entry(file_id, &path)?;
                        }
                    }
                    ServerControl::Nack {
                        client_seq,
                        code,
                        message,
                    } => {
                        tracing::warn!(target: "atlas_thread_sync", %code, "frame refused");
                        self.pending_tree.remove(&client_seq);
                        self.last_nack = Some((code, message));
                    }
                    ServerControl::Synced { head } => {
                        self.head = self.head.max(head);
                        return Ok(Handled::Synced);
                    }
                    ServerControl::Error { code, message } => {
                        return Err(SessionError::Refused { code, message });
                    }
                }
            }
            Message::Binary(bytes) => {
                let Some(frame) = wire::decode(&bytes) else {
                    tracing::warn!(target: "atlas_thread_sync", "unreadable binary frame");
                    return Ok(Handled::Other);
                };
                if frame.kind != FrameKind::CanonicalUpdate as u8 {
                    return Ok(Handled::Other);
                }
                self.saw_seq(frame.seq);
                // A save the person made while this arrived is folded in and
                // goes out as its own change.
                if let Some(local) = self.replica.apply_remote(frame.file_id, &frame.payload)? {
                    self.send_update(frame.file_id, local).await?;
                }
            }
        }
        Ok(Handled::Other)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Handled {
    Synced,
    Other,
}
