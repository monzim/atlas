//! One connection to one Shared Thread, driving a [`Replica`].
//!
//! The session speaks wire v1: `hello`, the catch-up replay until `synced`,
//! `tree.ensure` for files this replica introduces, and canonical updates both
//! ways. Every frame it sends carries the next `client_seq`, so a resend after
//! a lost ack is recognised by the server and stored once.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use crate::bootstrap::{self, BootstrapError, ThreadRepo};
use crate::git;
use crate::merge::{self, MergeError};
use crate::replica::{kind_of, LocalChange, Replica, ReplicaError};
use crate::runs::{ActiveRun, Fork, RunReport, RunSpec, RunWorktree};
use crate::secrets::{secret_reason, SecretReason};
use crate::share::{self, ShareKind};
use crate::store::{NoStore, ObjectStore, StoreError};
use crate::transport::{Message, Transport, TransportError};
use crate::wire::{
    self, BundleFailure, ClientControl, FileKind, FileVersion, Frame, FrameKind, MergeFile, Role,
    RunOutcome, ServerControl, ThreadRun,
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
    #[error(transparent)]
    Bootstrap(#[from] BootstrapError),
    #[error(transparent)]
    Store(#[from] StoreError),
    /// This replica may only watch, and why — said to the person as is.
    #[error("{0}")]
    ReadOnly(String),
}

/// How joining went for a machine without the Base (ATL-402).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bootstrapped {
    /// The Base is here: the replica can be checked out and edited.
    Ready,
    /// It is not, and will not be for now; the replica follows the thread
    /// read-only. The reason is for the person.
    WatchOnly(String),
}

/// Somebody else needs the Base; this replica may be able to build it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleWant {
    pub request_id: String,
    pub have: Vec<String>,
}

/// The answer a `bundle.request` is waiting for.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BundleAnswer {
    Available { sha: String },
    Unavailable { reason: BundleFailure, bytes: Option<u64> },
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

/// Bundle requests kept while the person has not agreed to send history.
const MAX_WANTED: usize = 16;

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

/// How long a joiner waits for somebody to build and upload a bundle. Large
/// histories take a while to pack.
const BUNDLE_TIMEOUT: Duration = Duration::from_secs(600);

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
    pending_tree: HashMap<u64, (String, FileKind)>,
    /// Files gone from disk, not yet known to be deleted rather than moved
    /// (ATL-403). Settled by [`ThreadSession::settle_removals`].
    missing: Vec<u64>,
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
    /// The thread's object doors.
    store: Arc<dyn ObjectStore>,
    /// This machine's bare repository for the thread, where bundles are built
    /// and fetched (ATL-402).
    thread_repo: Option<ThreadRepo>,
    /// Our own `bundle.request`: its `client_seq`, the id the server gave it,
    /// and the answer once it came.
    bundle_request: Option<(u64, Option<String>)>,
    bundle_answer: Option<BundleAnswer>,
    /// Requests from others this replica has not served yet.
    wanted: Vec<BundleWant>,
    /// Has the person agreed to send this repository's history to teammates
    /// who lack the Base? A bundle is the whole history behind the Base, so
    /// nothing is built until they say yes (ATL-402).
    serve_bundles: bool,
    /// Blocked paths the person included anyway in this share: their Base
    /// content may be published too.
    included: HashSet<String>,
    /// Why this replica can only watch, when it can.
    watch_only: Option<String>,
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
            missing: Vec::new(),
            gaps: 0,
            updates_sent: 0,
            last_nack: None,
            versions: HashMap::new(),
            runs: BTreeMap::new(),
            awaiting: HashSet::new(),
            answers: HashMap::new(),
            events: None,
            store: Arc::new(NoStore),
            thread_repo: None,
            bundle_request: None,
            bundle_answer: None,
            wanted: Vec::new(),
            serve_bundles: false,
            included: HashSet::new(),
            watch_only: None,
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

    /// The thread's object doors: blobs, bundles and snapshots.
    pub fn set_store(&mut self, store: Arc<dyn ObjectStore>) {
        self.store = store;
    }

    /// This machine's bare repository for the thread (ATL-402).
    pub fn set_thread_repo(&mut self, repo: ThreadRepo) {
        self.thread_repo = Some(repo);
    }

    /// Why this replica may not change the thread, or `None` when it may.
    pub fn read_only(&self) -> Option<String> {
        if let Some(why) = &self.watch_only {
            return Some(why.clone());
        }
        if !self.replica.has_base() {
            return Some("This machine does not have the thread's starting commit yet.".into());
        }
        None
    }

    // -----------------------------------------------------------------------
    // Bootstrap without a shared Base (ATL-402)
    // -----------------------------------------------------------------------

    /// Bring the Base onto this machine if it lacks it: report the commits
    /// `own_repo` (the person's repository, if they have one) holds, wait for
    /// a replica that has the Base to build and upload a bundle of the rest,
    /// fetch it into the thread repository, and attach that to the replica.
    ///
    /// Answers [`Bootstrapped::WatchOnly`] — never an error — when no bundle
    /// can come: nobody holding the Base is online, or the history is over
    /// the Organisation's bundle limit. The replica then follows the thread
    /// read-only and says why.
    pub async fn bootstrap(&mut self, own_repo: Option<&Path>) -> Result<Bootstrapped, SessionError> {
        if self.replica.has_base() {
            return Ok(Bootstrapped::Ready);
        }
        let repo = self
            .thread_repo
            .clone()
            .ok_or_else(|| SessionError::ReadOnly("no thread repository to fetch into".into()))?;
        repo.ensure(own_repo)?;
        if repo.has(self.replica.base()) {
            self.replica.attach_repo(repo.path())?;
            self.watch_only = None;
            return Ok(Bootstrapped::Ready);
        }
        if self.role == Some(Role::Viewer) {
            return Ok(self.watch_only_because(
                "Viewers follow the thread without its repository history.".into(),
            ));
        }
        let have = own_repo.map_or_else(Vec::new, |r| git::have_commits(r, bootstrap::MAX_HAVE));
        let client_seq = self.take_client_seq();
        self.bundle_request = Some((client_seq, None));
        self.bundle_answer = None;
        self.awaiting.insert(client_seq);
        let sent = self.send_control(&ClientControl::BundleRequest { client_seq, have }).await;
        let answer = match sent {
            Ok(()) => self.await_bundle(client_seq).await,
            Err(e) => Err(e),
        };
        self.awaiting.remove(&client_seq);
        self.bundle_request = None;
        let answer = match answer? {
            Some(answer) => answer,
            None => {
                return Ok(self.watch_only_because(
                    "Nobody who has this thread's starting commit sent it in time. You can watch; join again to edit."
                        .into(),
                ))
            }
        };
        let sha = match answer {
            BundleAnswer::Available { sha } => sha,
            BundleAnswer::Unavailable {
                reason: BundleFailure::TooLarge,
                bytes,
            } => {
                let size = bytes.map_or_else(String::new, |b| format!(" ({} MB)", b.div_ceil(1024 * 1024)));
                return Ok(self.watch_only_because(format!(
                    "The repository history this thread needs{size} is over your organisation's Base bundle limit, so you can watch but not edit. Fetch the commit yourself and join again to edit."
                )));
            }
            BundleAnswer::Unavailable { .. } => {
                return Ok(self.watch_only_because(
                    "Nobody who has this thread's starting commit is online to send it. You can watch; join again later to edit."
                        .into(),
                ))
            }
        };
        let bytes = self.store.get_bundle(sha.clone()).await?;
        let base = self.replica.base().to_string();
        repo.install(&base, &sha, &bytes)?;
        self.replica.attach_repo(repo.path())?;
        self.watch_only = None;
        Ok(Bootstrapped::Ready)
    }

    fn watch_only_because(&mut self, why: String) -> Bootstrapped {
        self.watch_only = Some(why.clone());
        Bootstrapped::WatchOnly(why)
    }

    /// Handle messages until our bundle request is answered, or give up after
    /// [`BUNDLE_TIMEOUT`] (`None`).
    async fn await_bundle(&mut self, client_seq: u64) -> Result<Option<BundleAnswer>, SessionError> {
        let deadline = tokio::time::Instant::now() + BUNDLE_TIMEOUT;
        loop {
            if let Some(answer) = self.bundle_answer.take() {
                return Ok(Some(answer));
            }
            if let Some(Answer::Nack { code, message }) = self.answers.remove(&client_seq) {
                return Err(SessionError::Refused { code, message });
            }
            match tokio::time::timeout_at(deadline, self.transport.recv()).await {
                Err(_) => return Ok(None),
                Ok(None) => return Err(SessionError::ClosedEarly),
                Ok(Some(message)) => {
                    self.handle(message).await?;
                }
            }
        }
    }

    /// Let this replica send the repository's history — everything behind the
    /// Base — to teammates who lack it. Off until the person turns it on;
    /// requests heard meanwhile wait, and are served once it is.
    pub fn set_serve_bundles(&mut self, on: bool) {
        self.serve_bundles = on;
    }

    pub fn serves_bundles(&self) -> bool {
        self.serve_bundles
    }

    /// Bundle requests waiting for the person to agree to send history.
    pub fn bundles_wanted(&self) -> usize {
        if self.serve_bundles {
            0
        } else {
            self.wanted.len()
        }
    }

    /// Bundle requests from others to serve now: none until the person has
    /// agreed to send history ([`ThreadSession::set_serve_bundles`]).
    pub fn take_bundle_wants(&mut self) -> Vec<BundleWant> {
        if !self.serve_bundles {
            return Vec::new();
        }
        std::mem::take(&mut self.wanted)
    }

    /// Build the bundle somebody asked for, upload it and say so — or, when it
    /// is over the Organisation's limit, say that instead so they stop
    /// waiting. A replica without the Base, or without a thread repository,
    /// leaves the request to somebody else.
    pub async fn serve_bundle(&mut self, want: BundleWant) -> Result<(), SessionError> {
        let (Some(own), Some(repo)) = (self.replica.repo().map(Path::to_path_buf), self.thread_repo.clone())
        else {
            return Ok(());
        };
        repo.ensure(Some(&own))?;
        let bundle = repo.build(self.replica.base(), &want.have)?;
        let size = bundle.bytes.len() as u64;
        let put = self
            .store
            .put_bundle(bundle.sha256.clone(), bundle.bytes, bundle.prerequisites)
            .await;
        let client_seq = self.take_client_seq();
        let frame = match put {
            Ok(()) => ClientControl::BundleReady {
                client_seq,
                request_id: want.request_id,
                sha: bundle.sha256,
            },
            Err(StoreError::Refused { code, .. }) if code == "limit_reached" => {
                ClientControl::BundleFailed {
                    client_seq,
                    request_id: want.request_id,
                    reason: BundleFailure::TooLarge,
                    bytes: size,
                }
            }
            Err(e) => return Err(e.into()),
        };
        match self.ask(client_seq, &frame).await? {
            Answer::Ack => Ok(()),
            Answer::Nack { code, message } => Err(SessionError::Refused { code, message }),
            other => Err(unexpected(&other)),
        }
    }

    async fn send_control(&mut self, frame: &ClientControl) -> Result<(), SessionError> {
        self.transport
            .send(Message::Text(
                serde_json::to_string(frame).expect("control frames are JSON"),
            ))
            .await?;
        Ok(())
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
    /// Refused while this machine lacks the Base.
    pub fn run_worktree(&self, root: &Path) -> Result<RunWorktree, SessionError> {
        let repo = self
            .replica
            .repo()
            .ok_or_else(|| ReplicaError::BaseMissing(self.replica.base().to_string()))?;
        Ok(RunWorktree::new(repo, self.replica.base(), root))
    }

    /// Reset the Run worktree to canonical state now, creating it if needed.
    /// A Run started later resets it again at its own fork.
    pub fn prepare_run(&self, worktree: &RunWorktree) -> Result<(), SessionError> {
        let fork = Fork {
            seq: self.head,
            files: self.replica.fork_files(),
            removed: self.replica.removed_paths(),
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
            removed: self.replica.removed_paths(),
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
    pub async fn finish_run(
        &mut self,
        run: &ActiveRun,
        worktree: &RunWorktree,
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
                    None => self.ensure_file(&path, true, FileKind::Text).await?,
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
                        let put = self
                            .store
                            .put_blob(file.blob.clone(), merged.content.into_bytes())
                            .await;
                        if let Err(e) = put {
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
        if let Err(e) = self.send_control(frame).await {
            self.awaiting.remove(&client_seq);
            return Err(e);
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
    /// it — binary files fetched from the thread. Called when the person first
    /// opens a file or prompts.
    pub async fn materialize(&mut self) -> Result<PathBuf, SessionError> {
        let root = self.replica.materialize()?.to_path_buf();
        for (file_id, sha) in self.replica.blobs_to_fetch() {
            self.fetch_blob(file_id, sha).await?;
        }
        Ok(root)
    }

    /// Fetch a binary file's canonical blob and write it.
    async fn fetch_blob(&mut self, file_id: u64, sha: String) -> Result<(), SessionError> {
        let bytes = self.store.get_blob(sha).await?;
        Ok(self.replica.write_blob(file_id, &bytes)?)
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
        if self.read_only().is_some() {
            return Ok(LocalChange::Ignored);
        }
        match self.replica.local_change(rel)? {
            LocalChange::Update { file_id, update } => {
                self.send_update(file_id, update).await?;
                Ok(LocalChange::Update {
                    file_id,
                    update: Vec::new(),
                })
            }
            LocalChange::Blob {
                file_id,
                sha256,
                bytes,
            } => {
                self.set_blob(file_id, &sha256, bytes).await?;
                Ok(LocalChange::Blob {
                    file_id,
                    sha256,
                    bytes: Vec::new(),
                })
            }
            LocalChange::Missing { file_id } => {
                // Deleted, or moved: which is known once the new path is seen
                // (or is not, by the time removals are settled).
                if !self.missing.contains(&file_id) {
                    self.missing.push(file_id);
                }
                Ok(LocalChange::Missing { file_id })
            }
            LocalChange::Renamed { file_id, from, to } => {
                if self.ignores(&to)? {
                    return Ok(LocalChange::Ignored);
                }
                let client_seq = self.take_client_seq();
                let rename = ClientControl::TreeRename {
                    client_seq,
                    file_id,
                    path: to.clone(),
                };
                self.expect_ack(client_seq, &rename).await?;
                self.replica.rename(file_id, &to)?;
                self.missing.retain(|id| *id != file_id);
                Ok(LocalChange::Renamed { file_id, from, to })
            }
            LocalChange::NewFile { path } => {
                // Build output, dependencies and whatever `.atlas/shareignore`
                // names never sync, wherever they are written.
                if self.ignores(&path)? {
                    return Ok(LocalChange::Ignored);
                }
                let bytes = self.replica.read_bytes(&path)?;
                let kind = kind_of(&bytes);
                // A credential created in the replica stays on this machine,
                // for the same reason it is held back at share time. Binary
                // content is judged by its name.
                let content = match kind {
                    FileKind::Text => String::from_utf8_lossy(&bytes).into_owned(),
                    FileKind::Binary => String::new(),
                };
                if let Some(reason) = secret_reason(&path, &content) {
                    tracing::info!(target: "atlas_thread_sync", ?reason, "holding back a new file that looks secret");
                    return Ok(LocalChange::Ignored);
                }
                let file_id = self.ensure_file(&path, true, kind).await?;
                match kind {
                    FileKind::Text => {
                        if let LocalChange::Update { update, .. } = self.replica.local_change(&path)? {
                            self.send_update(file_id, update).await?;
                        }
                    }
                    FileKind::Binary => {
                        let sha = bootstrap::sha256_hex(&bytes);
                        self.replica.saw_bytes(file_id, &bytes);
                        self.set_blob(file_id, &sha, bytes).await?;
                    }
                }
                Ok(LocalChange::NewFile { path })
            }
            other => Ok(other),
        }
    }

    /// Files that went missing and did not turn up elsewhere are deleted in
    /// the thread. The app's loop calls this once saves have been quiet for a
    /// moment, so a move — reported as a removal and a creation, in either
    /// order — is seen as the rename it is.
    pub async fn settle_removals(&mut self) -> Result<Vec<String>, SessionError> {
        let mut deleted = Vec::new();
        for file_id in std::mem::take(&mut self.missing) {
            if !self.replica.is_missing(file_id) {
                continue;
            }
            let path = self
                .replica
                .files()
                .find(|(id, _)| *id == file_id)
                .map(|(_, p)| p.to_string());
            let client_seq = self.take_client_seq();
            self.expect_ack(client_seq, &ClientControl::TreeDelete { client_seq, file_id })
                .await?;
            self.replica.delete(file_id)?;
            deleted.extend(path);
        }
        Ok(deleted)
    }

    /// Are removals waiting to be settled?
    pub fn removals_pending(&self) -> bool {
        !self.missing.is_empty()
    }

    /// Upload a binary file's bytes and make them canonical (ATL-403).
    async fn set_blob(&mut self, file_id: u64, sha: &str, bytes: Vec<u8>) -> Result<(), SessionError> {
        self.store.put_blob(sha.to_string(), bytes).await?;
        let client_seq = self.take_client_seq();
        let set = ClientControl::BlobSet {
            client_seq,
            file_id,
            blob: sha.to_string(),
        };
        self.expect_ack(client_seq, &set).await?;
        Ok(self.replica.set_blob(file_id, sha)?)
    }

    /// Send a frame the server answers with a plain ack.
    async fn expect_ack(&mut self, client_seq: u64, frame: &ClientControl) -> Result<(), SessionError> {
        match self.ask(client_seq, frame).await? {
            Answer::Ack => Ok(()),
            Answer::Nack { code, message } => Err(SessionError::Refused { code, message }),
            other => Err(unexpected(&other)),
        }
    }

    /// Make the sharer's uncommitted work the thread's first canonical changes:
    /// exactly what [`share::preview`] lists for `checkout` — ignored and
    /// `.atlas/shareignore`d files never appear — except files that look like
    /// secrets, which are held back and reported unless named in `include`
    /// ("include anyway"). The person's checkout is only read.
    pub async fn share_working_changes(
        &mut self,
        checkout: &Path,
        include: &[String],
    ) -> Result<ShareReport, SessionError> {
        let preview = share::preview(checkout)?;
        self.included = include.iter().cloned().collect();
        let mut report = ShareReport::default();
        for held in preview.held(include) {
            if let Some(reason) = &held.blocked {
                report.blocked.push((held.path.clone(), reason.clone()));
            }
        }
        for file in preview.uploads(include) {
            if file.deleted {
                // Deleted since the Base: the thread holds it, deleted, so
                // every replica removes the Base's copy too.
                let Some(base) = self.replica.base_bytes(&file.path)? else {
                    continue;
                };
                let file_id = self.ensure_file(&file.path, true, kind_of(&base)).await?;
                let client_seq = self.take_client_seq();
                self.expect_ack(client_seq, &ClientControl::TreeDelete { client_seq, file_id })
                    .await?;
                self.replica.delete(file_id)?;
                report.shared.push(file.path.clone());
                continue;
            }
            let target = crate::path::resolve(checkout, &file.path).map_err(ReplicaError::from)?;
            let Ok(bytes) = std::fs::read(&target) else {
                continue;
            };
            match file.kind {
                ShareKind::Text => {
                    let content = String::from_utf8_lossy(&bytes);
                    let file_id = self.ensure_file(&file.path, true, FileKind::Text).await?;
                    if let Some(update) = self.replica.set_text(file_id, &content)? {
                        self.send_update(file_id, update).await?;
                    }
                }
                ShareKind::Binary => {
                    let file_id = self.ensure_file(&file.path, true, FileKind::Binary).await?;
                    let sha = bootstrap::sha256_hex(&bytes);
                    self.set_blob(file_id, &sha, bytes).await?;
                }
            }
            report.shared.push(file.path.clone());
        }
        Ok(report)
    }

    /// Does git, or the thread's `.atlas/shareignore`, ignore `path` in the
    /// replica worktree?
    fn ignores(&self, path: &str) -> Result<bool, SessionError> {
        let root = self.replica.root();
        let ignored = git::ignored(root, &[path.to_string()], Some(&root.join(share::SHAREIGNORE)))
            .map_err(ReplicaError::from)?;
        Ok(ignored.contains(path))
    }

    /// Upload a file's Base content under the thread before its tree entry
    /// names it, so a reader without git can show the file's diff (ATL-402).
    /// `Some(None)` for a file the Base does not have; `None` when it could
    /// not be said — the upload failed, or this machine lacks the Base.
    async fn upload_base(&self, path: &str) -> Option<Option<String>> {
        if !self.replica.has_base() {
            return None;
        }
        match self.replica.base_bytes(path) {
            Ok(None) => Some(None),
            Ok(Some(bytes)) => {
                let sha = bootstrap::sha256_hex(&bytes);
                match self.store.put_blob(sha.clone(), bytes).await {
                    Ok(()) => Some(Some(sha)),
                    Err(e) => {
                        tracing::warn!(target: "atlas_thread_sync", %path, "Base blob upload failed: {e}");
                        None
                    }
                }
            }
            Err(e) => {
                tracing::warn!(target: "atlas_thread_sync", %path, "Base content unreadable: {e}");
                None
            }
        }
    }

    /// The file's id in this thread, asking the server for an entry if it has
    /// none. When `introduced`, this replica also publishes the file's Base
    /// seed: harmless if another replica already did, since seeds are
    /// byte-identical everywhere, and it lets a replica without the Base
    /// rebuild the file from the journal alone.
    async fn ensure_file(
        &mut self,
        path: &str,
        introduced: bool,
        kind: FileKind,
    ) -> Result<u64, SessionError> {
        if let Some(id) = self.replica.file_id(path) {
            return Ok(id);
        }
        // A file whose Base content looks like a secret — a key the person
        // has since removed, say — keeps that content home: no Base blob, no
        // seed on the wire. Unless they included the file anyway.
        let publish_base = introduced && !self.base_is_secret(path);
        let base_blob = if publish_base {
            self.upload_base(path).await
        } else {
            None
        };
        let client_seq = self.take_client_seq();
        self.pending_tree.insert(client_seq, (path.to_string(), kind));
        let ensure = ClientControl::TreeEnsure {
            client_seq,
            path: path.to_string(),
            kind,
            base_blob,
        };
        self.send_control(&ensure).await?;
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
        // A binary file's Base is its blob; only text is seeded.
        if publish_base && kind == FileKind::Text {
            for seed in self.replica.seed_for(path)? {
                self.send_update(file_id, seed).await?;
            }
        }
        Ok(file_id)
    }

    /// Does `path`'s Base content look like a secret the person has not
    /// chosen to share?
    fn base_is_secret(&self, path: &str) -> bool {
        if self.included.contains(path) {
            return false;
        }
        match self.replica.base_bytes(path) {
            Ok(Some(bytes)) => secret_reason(path, &String::from_utf8_lossy(&bytes)).is_some(),
            Ok(None) => false,
            // Unreadable: say nothing rather than guess.
            Err(_) => true,
        }
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

    fn is_our_bundle(&self, request_id: &str) -> bool {
        matches!(&self.bundle_request, Some((_, Some(id))) if id == request_id)
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
                        if let Some(sha) = self.replica.learn(&entry)? {
                            // A blob that cannot be fetched now is fetched at
                            // the next checkout; the change is not lost.
                            if let Err(e) = self.fetch_blob(entry.file_id, sha).await {
                                tracing::warn!(target: "atlas_thread_sync", path = %entry.path, "blob fetch failed: {e}");
                            }
                        }
                    }
                    ServerControl::Ack {
                        client_seq,
                        seq,
                        file_id,
                    } => {
                        self.saw_seq(seq);
                        self.answer(client_seq, Answer::Ack);
                        if let (Some((path, kind)), Some(file_id)) =
                            (self.pending_tree.remove(&client_seq), file_id)
                        {
                            // A file this replica knew was deleted is revived.
                            if !self.replica.add_entry(file_id, &path, kind)? {
                                self.replica.revive(file_id, &path)?;
                            }
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
                    ServerControl::BundleWanted { request_id, have } => {
                        // Waiting on the person's say-so, requests pile up;
                        // keep the newest few (the server forgets old ones).
                        if self.wanted.len() >= MAX_WANTED {
                            self.wanted.remove(0);
                        }
                        self.wanted.push(BundleWant { request_id, have });
                    }
                    ServerControl::BundlePending {
                        client_seq,
                        request_id,
                    } => {
                        if let Some((ours, id)) = &mut self.bundle_request {
                            if *ours == client_seq {
                                *id = Some(request_id);
                            }
                        }
                    }
                    ServerControl::BundleAvailable {
                        request_id, sha, ..
                    } => {
                        if self.is_our_bundle(&request_id) {
                            self.bundle_answer = Some(BundleAnswer::Available { sha });
                        }
                    }
                    ServerControl::BundleUnavailable {
                        request_id,
                        reason,
                        bytes,
                    } => {
                        if self.is_our_bundle(&request_id) {
                            self.bundle_answer = Some(BundleAnswer::Unavailable { reason, bytes });
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
