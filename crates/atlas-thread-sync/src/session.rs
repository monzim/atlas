//! One connection to one Shared Thread, driving a [`Replica`].
//!
//! The session speaks wire v1: `hello`, the catch-up replay until `synced`,
//! `tree.ensure` for files this replica introduces, and canonical updates both
//! ways. Every frame it sends carries the next `client_seq`, so a resend after
//! a lost ack is recognised by the server and stored once.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine as _;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use crate::git;
use crate::merge::{self, MergeError};
use crate::replica::{looks_textual, LocalChange, Replica, ReplicaError};
use crate::runs::{ActiveRun, Fork, RunReport, RunSpec, RunWorktree};
use crate::secrets::{secret_reason, SecretReason};
use crate::transport::{Message, Transport, TransportError};
use crate::wire::{
    self, ClientControl, FileKind, FileVersion, Frame, FrameKind, MergeFile, Role, RunOutcome,
    ServerControl, ThreadRun,
};

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
    /// The Run's changes overlap edits made since it forked. Nothing was
    /// merged; the Run's result is still in its worktree (ATL-410 turns these
    /// into Conflicts).
    #[error("the Run's changes overlap edits made since it started: {}", .0.iter().map(|(p, _)| p.as_str()).collect::<Vec<_>>().join(", "))]
    Overlap(Vec<(String, Vec<std::ops::Range<usize>>)>),
    #[error("the merge was rejected {0} times in a row; try again")]
    MergeStarved(u32),
}

/// Where a Run's resulting blobs go for its Thread Version: the thread's blob
/// door (`PUT /threads/{id}/blobs/{sha256}`) in the app, a map in tests.
pub trait BlobSink: Send + Sync {
    fn put(&self, sha256: &str, bytes: Vec<u8>) -> impl Future<Output = Result<(), String>> + Send;
}

/// What the app hears about besides status: other people's live Run frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadEvent {
    /// A live Run frame — a `SessionDelta` (kind 3) or a Run file (kind 4).
    /// Never stored; the durable copy is the Runner's Session.
    RunFrame {
        run_no: u64,
        kind: u8,
        payload: Vec<u8>,
    },
}

/// A Run as the app shows it: the server's view plus the files its merge
/// changed, by path.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunView {
    #[serde(flatten)]
    pub run: ThreadRun,
    pub files: Vec<String>,
}

/// How many times a rejected merge is recomputed before giving up.
const MERGE_ATTEMPTS: u32 = 8;

/// Runs kept for display; the server has the full list.
const RUNS_KEPT: usize = 50;

/// What the server answered to one frame this session is waiting on.
#[derive(Debug)]
enum Answer {
    Ack,
    Nack {
        code: String,
        message: String,
    },
    Accepted {
        version: u64,
        files: Vec<FileVersion>,
    },
    Rejected {
        versions: Vec<FileVersion>,
    },
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
    /// Each file's merge version, as last heard (ADR-0022).
    versions: HashMap<u64, u64>,
    runs: BTreeMap<String, RunView>,
    /// `client_seq`s whose answer a caller is waiting for, and the answers.
    awaiting: HashSet<u64>,
    answers: HashMap<u64, Answer>,
    events: Option<mpsc::UnboundedSender<ThreadEvent>>,
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
            versions: HashMap::new(),
            runs: BTreeMap::new(),
            awaiting: HashSet::new(),
            answers: HashMap::new(),
            events: None,
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

    /// Where other people's live Run frames go.
    pub fn set_events(&mut self, events: mpsc::UnboundedSender<ThreadEvent>) {
        self.events = Some(events);
    }

    /// The Runs this session has heard of, newest first.
    pub fn runs(&self) -> Vec<RunView> {
        let mut runs: Vec<RunView> = self.runs.values().cloned().collect();
        runs.sort_by_key(|r| std::cmp::Reverse(r.run.run_no));
        runs
    }

    /// A file's merge version as this session last heard it.
    pub fn merge_version(&self, file_id: u64) -> u64 {
        self.versions.get(&file_id).copied().unwrap_or(0)
    }

    // -----------------------------------------------------------------------
    // Runs (ADR-0022, ATL-405)
    // -----------------------------------------------------------------------

    /// The Run worktree at `root`, of this replica's repository and Base.
    pub fn run_worktree(&self, root: &Path) -> RunWorktree {
        RunWorktree::new(self.replica.repo(), self.replica.base(), root)
    }

    /// Reset the Run worktree to canonical state now, creating it if needed.
    /// A Run started later resets it again at its own fork.
    pub fn prepare_run(&self, worktree: &RunWorktree) -> Result<(), SessionError> {
        let fork = Fork {
            seq: self.head,
            files: self.replica.fork_files(),
        };
        Ok(worktree.reset(&fork)?)
    }

    /// Start a Run: fork canonical state as this replica holds it now, reset
    /// the Run worktree to it, and tell the thread. Refused by the server over
    /// the concurrent-Runs limit, for a viewer, or on a closed thread.
    pub async fn start_run(
        &mut self,
        worktree: &RunWorktree,
        spec: RunSpec,
    ) -> Result<ActiveRun, SessionError> {
        let fork = Fork {
            seq: self.head,
            files: self.replica.fork_files(),
        };
        worktree.reset(&fork)?;
        let client_seq = self.take_client_seq();
        let start = ClientControl::RunStart {
            client_seq,
            run_id: spec.run_id.clone(),
            agent: spec.agent,
            model: spec.model,
            fork_seq: fork.seq,
            context_anchor: spec.context_anchor,
        };
        match self.ask(client_seq, &start).await? {
            Answer::Ack => {}
            Answer::Nack { code, message } => return Err(SessionError::Refused { code, message }),
            other => return Err(unexpected(&other)),
        }
        // The `run` frame that names its number follows the ack.
        let run_no = loop {
            if let Some(view) = self.runs.get(&spec.run_id) {
                break view.run.run_no;
            }
            self.receive_one().await?;
        };
        Ok(ActiveRun {
            run_id: spec.run_id,
            run_no,
            fork,
            worktree: worktree.root().to_path_buf(),
        })
    }

    /// Stream one live Run frame (a serialized `SessionDelta`, say) to
    /// everyone else. Live frames are a view, not a record: one too large for
    /// a frame is dropped, since the Session drain carries the durable copy.
    pub async fn stream_run(
        &mut self,
        run_no: u64,
        kind: FrameKind,
        payload: Vec<u8>,
    ) -> Result<(), SessionError> {
        if payload.len() > wire::MAX_PAYLOAD_BYTES {
            tracing::debug!(target: "atlas_thread_sync", bytes = payload.len(), "live Run frame too large; skipped");
            return Ok(());
        }
        let client_seq = self.take_client_seq();
        let bytes =
            wire::encode(&Frame::run(kind, run_no, client_seq, payload)).expect("small numbers");
        self.transport.send(Message::Binary(bytes)).await?;
        Ok(())
    }

    /// The turn is over: merge what the Run left in its worktree into
    /// canonical state — three-way against the fork and canonical state now,
    /// submitted with each file's merge version and recomputed whenever
    /// another merge got there first — then upload the resulting blobs for the
    /// Thread Version and end the Run.
    ///
    /// Overlapping hunks fail the merge with [`SessionError::Overlap`]; the
    /// Run is ended unmerged and its result stays in the worktree.
    pub async fn finish_run<B: BlobSink>(
        &mut self,
        run: &ActiveRun,
        worktree: &RunWorktree,
        blobs: &B,
    ) -> Result<RunReport, SessionError> {
        let changes = worktree.changes(&run.fork)?;
        let mut report = RunReport::default();
        for _ in 0..MERGE_ATTEMPTS {
            // Plan on the newest state the socket has delivered: what already
            // arrived is handled first, so an overlap with it is found before
            // anything — a new file's tree entry included — reaches the thread.
            self.drain_ready().await?;
            let mut planned = Vec::new();
            let mut overlaps = Vec::new();
            for change in &changes {
                // A file the Run created forks from its Base content (or
                // nothing), even if somebody else created it meanwhile; it
                // gets a tree entry only once the whole merge is known to go
                // ahead, so a refused merge leaves the thread as it was.
                let file_id = change
                    .file_id
                    .or_else(|| self.replica.file_id(&change.path));
                let fork = match change.file_id {
                    Some(id) => run.fork.files[&id].snapshot.clone(),
                    None => self.replica.seed_snapshot(&change.path)?,
                };
                let canonical = match file_id {
                    Some(id) => self
                        .replica
                        .snapshot(id)
                        .ok_or(ReplicaError::UnknownFile(id))?,
                    None => fork.clone(),
                };
                // The version this merge is computed against, read now: an
                // answer handled later (an `ensure_file` below) may move it,
                // and submitting the newer one would pass the compare-and-set
                // with a merge computed against the older state.
                let base_version = file_id.map_or(0, |id| self.merge_version(id));
                match merge::three_way(&fork, &change.content, &canonical) {
                    Ok(Some(merged)) => {
                        planned.push((file_id, base_version, change.path.clone(), merged))
                    }
                    Ok(None) => {}
                    Err(MergeError::Overlap { lines }) => {
                        overlaps.push((change.path.clone(), lines))
                    }
                    Err(MergeError::Doc(e)) => return Err(ReplicaError::Doc(e).into()),
                }
            }
            if !overlaps.is_empty() {
                self.end_run(&run.run_id, RunOutcome::Completed).await?;
                return Err(SessionError::Overlap(overlaps));
            }
            if planned.is_empty() {
                self.end_run(&run.run_id, RunOutcome::Completed).await?;
                return Ok(report);
            }
            let total: usize = planned.iter().map(|(_, _, _, m)| m.update.len()).sum();
            if total > MAX_MERGE_BYTES
                || planned
                    .iter()
                    .any(|(_, _, _, m)| m.update.len() > wire::MAX_PAYLOAD_BYTES)
            {
                // Splitting a merge across submits is not this slice's.
                self.end_run(&run.run_id, RunOutcome::Completed).await?;
                return Err(SessionError::Refused {
                    code: "payload_too_large".into(),
                    message: format!(
                        "the Run's merge is {total} bytes of updates, over what one merge carries"
                    ),
                });
            }
            let mut ready = Vec::with_capacity(planned.len());
            for (file_id, base_version, path, merged) in planned {
                let file_id = match file_id {
                    Some(id) => id,
                    None => self.ensure_file(&path, true).await?,
                };
                ready.push((file_id, base_version, path, merged));
            }
            let planned = ready;
            let files: Vec<MergeFile> = planned
                .iter()
                .map(|(file_id, base_version, _, m)| MergeFile {
                    file_id: *file_id,
                    base_version: *base_version,
                    update: base64::engine::general_purpose::STANDARD.encode(&m.update),
                    blob: sha256_hex(m.content.as_bytes()),
                })
                .collect();
            let client_seq = self.take_client_seq();
            let submit = ClientControl::MergeSubmit {
                client_seq,
                run_id: run.run_id.clone(),
                files: files.clone(),
            };
            match self.ask(client_seq, &submit).await? {
                Answer::Accepted {
                    version,
                    files: landed,
                } => {
                    for (file_id, _, _, merged) in &planned {
                        // The server relays the merge to everybody else; this
                        // replica applies it itself. A save made meanwhile is
                        // folded in and goes out as its own change.
                        if let Some(local) = self.replica.apply_remote(*file_id, &merged.update)? {
                            self.send_update(*file_id, local).await?;
                        }
                    }
                    for v in landed {
                        self.versions.insert(v.file_id, v.version);
                    }
                    self.head = self.head.max(version);
                    report.version = Some(version);
                    report.files = planned.iter().map(|(_, _, path, _)| path.clone()).collect();
                    if let Some(view) = self.runs.get_mut(&run.run_id) {
                        view.files.clone_from(&report.files);
                    }
                    for ((_, _, path, merged), file) in planned.into_iter().zip(&files) {
                        if let Err(e) = blobs.put(&file.blob, merged.content.into_bytes()).await {
                            tracing::warn!(target: "atlas_thread_sync", %path, "Thread Version blob upload failed: {e}");
                            report.unuploaded.push(path);
                        }
                    }
                    self.end_run(&run.run_id, RunOutcome::Completed).await?;
                    return Ok(report);
                }
                Answer::Rejected { versions } => {
                    // Another merge landed on one of these files; its changes
                    // arrived ahead of this answer, so recomputing against
                    // the replica now merges onto them.
                    for v in versions {
                        self.versions.insert(v.file_id, v.version);
                    }
                    report.retries += 1;
                }
                Answer::Nack { code, message } => {
                    return Err(SessionError::Refused { code, message })
                }
                Answer::Ack => return Err(unexpected(&Answer::Ack)),
            }
        }
        self.end_run(&run.run_id, RunOutcome::Completed).await?;
        Err(SessionError::MergeStarved(MERGE_ATTEMPTS))
    }

    /// The Run will not finish (the agent failed or was cancelled): mark it
    /// interrupted, merging nothing.
    pub async fn interrupt_run(&mut self, run_id: &str) -> Result<(), SessionError> {
        self.end_run(run_id, RunOutcome::Interrupted).await
    }

    async fn end_run(&mut self, run_id: &str, outcome: RunOutcome) -> Result<(), SessionError> {
        let client_seq = self.take_client_seq();
        let end = ClientControl::RunEnd {
            client_seq,
            run_id: run_id.to_string(),
            outcome,
        };
        match self.ask(client_seq, &end).await? {
            Answer::Ack => Ok(()),
            Answer::Nack { code, message } => Err(SessionError::Refused { code, message }),
            other => Err(unexpected(&other)),
        }
    }

    /// Send a control frame and handle whatever arrives until it is answered.
    async fn ask(
        &mut self,
        client_seq: u64,
        frame: &ClientControl,
    ) -> Result<Answer, SessionError> {
        self.awaiting.insert(client_seq);
        let sent = self
            .transport
            .send(Message::Text(
                serde_json::to_string(frame).expect("control frames are JSON"),
            ))
            .await;
        if let Err(e) = sent {
            self.awaiting.remove(&client_seq);
            return Err(e.into());
        }
        let answer = loop {
            if let Some(answer) = self.answers.remove(&client_seq) {
                break Ok(answer);
            }
            if let Err(e) = self.receive_one().await {
                break Err(e);
            }
        };
        self.awaiting.remove(&client_seq);
        answer
    }

    /// Handle every message that has already arrived, without waiting for more.
    async fn drain_ready(&mut self) -> Result<(), SessionError> {
        while let Ok(next) = tokio::time::timeout(Duration::ZERO, self.transport.recv()).await {
            let message = next.ok_or(SessionError::ClosedEarly)?;
            self.handle(message).await?;
        }
        Ok(())
    }

    async fn receive_one(&mut self) -> Result<(), SessionError> {
        let message = tokio::time::timeout(ANSWER_TIMEOUT, self.transport.recv())
            .await
            .map_err(|_| SessionError::Timeout)?
            .ok_or(SessionError::ClosedEarly)?;
        self.handle(message).await.map(|_| ())
    }

    fn answer(&mut self, client_seq: u64, answer: Answer) {
        if self.awaiting.contains(&client_seq) {
            self.answers.insert(client_seq, answer);
        }
    }

    fn note_run(&mut self, run: ThreadRun) {
        match self.runs.get_mut(&run.run_id) {
            Some(view) => view.run = run,
            None => {
                self.runs.insert(
                    run.run_id.clone(),
                    RunView {
                        run,
                        files: Vec::new(),
                    },
                );
            }
        }
        while self.runs.len() > RUNS_KEPT {
            let oldest = self
                .runs
                .iter()
                .min_by_key(|(_, v)| v.run.run_no)
                .map(|(k, _)| k.clone());
            match oldest {
                Some(k) => self.runs.remove(&k),
                None => break,
            };
        }
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
                        if let Some(version) = entry.merge_version {
                            self.versions.insert(entry.file_id, version);
                        }
                        self.replica.add_entry(entry.file_id, &entry.path)?;
                    }
                    ServerControl::Ack {
                        client_seq,
                        seq,
                        file_id,
                    } => {
                        self.saw_seq(seq);
                        self.answer(client_seq, Answer::Ack);
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
                        self.answer(
                            client_seq,
                            Answer::Nack {
                                code: code.clone(),
                                message: message.clone(),
                            },
                        );
                        self.last_nack = Some((code, message));
                    }
                    ServerControl::Synced { head } => {
                        self.head = self.head.max(head);
                        return Ok(Handled::Synced);
                    }
                    ServerControl::Error { code, message } => {
                        return Err(SessionError::Refused { code, message });
                    }
                    ServerControl::Run { run } => self.note_run(run),
                    ServerControl::MergeAccepted {
                        client_seq,
                        version,
                        files,
                        ..
                    } => self.answer(client_seq, Answer::Accepted { version, files }),
                    ServerControl::MergeRejected {
                        client_seq,
                        versions,
                        ..
                    } => self.answer(client_seq, Answer::Rejected { versions }),
                    ServerControl::Merged {
                        run_id,
                        version,
                        files,
                    } => {
                        // The merge's updates arrived ahead of this, in `seq`
                        // order; here is only where each file now stands.
                        self.head = self.head.max(version);
                        let mut paths = Vec::new();
                        for f in files {
                            self.versions.insert(f.file_id, f.version);
                            if let Some((_, path)) =
                                self.replica.files().find(|(id, _)| *id == f.file_id)
                            {
                                paths.push(path.to_string());
                            }
                        }
                        if let Some(view) = self.runs.get_mut(&run_id) {
                            view.files = paths;
                        }
                    }
                    ServerControl::Other => {}
                }
            }
            Message::Binary(bytes) => {
                let Some(frame) = wire::decode(&bytes) else {
                    tracing::warn!(target: "atlas_thread_sync", "unreadable binary frame");
                    return Ok(Handled::Other);
                };
                if frame.kind == FrameKind::RunStream as u8
                    || frame.kind == FrameKind::RunFile as u8
                {
                    if let Some(events) = &self.events {
                        let _ = events.send(ThreadEvent::RunFrame {
                            run_no: frame.file_id,
                            kind: frame.kind,
                            payload: frame.payload,
                        });
                    }
                    return Ok(Handled::Other);
                }
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

/// Largest total of updates one `merge.submit` may carry (the server's
/// `THREAD_MAX_MERGE_BYTES`).
const MAX_MERGE_BYTES: usize = 768 * 1024;

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn unexpected(answer: &Answer) -> SessionError {
    SessionError::Refused {
        code: "unexpected_answer".into(),
        message: format!("the server answered {answer:?}"),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Handled {
    Synced,
    Other,
}
