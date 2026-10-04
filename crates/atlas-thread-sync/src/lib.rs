//! Shared Threads on the desktop (ADR-0021, ADR-0022; ATL-395).
//!
//! A Shared Thread's canonical file state lives in the cloud. Every participant
//! holds a **replica** of it: a detached git worktree at the thread's Base,
//! created out of the way of their own checkout the first time they open a file
//! or prompt, plus one Yjs text document per file the thread has touched. Saves
//! from any editor become Yjs updates; updates from others are written back to
//! disk atomically, and never echoed.
//!
//! This crate is Tauri-free. The app owns tokens, links and windows; here is
//! the protocol, the replica and the loop that ties them together:
//!
//! * [`wire`] — wire protocol v1, transcribed from the server's contract;
//! * [`replica`] / [`doc`] — the worktree and the documents;
//! * [`session`] — one connection driving one replica;
//! * [`transport`] — the WebSocket, and an in-process fake server for tests;
//! * [`watch`] — saves in the worktree as thread paths;
//! * [`runs`] / [`merge`] — Runs: the Run worktree, and merging a Run's
//!   result back into canonical state (ATL-405);
//! * [`share`] — what a share uploads, and what it holds back (ATL-402);
//! * [`bootstrap`] — bringing the Base to a machine without it (ATL-402);
//! * [`store`] — the thread's blob, bundle and snapshot doors;
//! * [`run`] — the loop an app spawns per joined thread.

pub mod bootstrap;
pub mod doc;
pub mod git;
pub mod merge;
pub mod path;
pub mod replica;
pub mod runs;
pub mod secrets;
pub mod session;
pub mod share;
pub mod store;
pub mod transport;
pub mod watch;
pub mod wire;

use std::collections::HashMap;
use std::path::PathBuf;

use serde::Serialize;
use tokio::sync::{mpsc, oneshot};

pub use bootstrap::ThreadRepo;
pub use replica::{LocalChange, Replica, ReplicaError};
pub use runs::{ActiveRun, RunReport, RunSpec, RunWorktree};
pub use secrets::SecretReason;
pub use session::Verification;
pub use session::{Bootstrapped, RunView, SessionError, ShareReport, ThreadEvent, ThreadSession};
pub use share::{ShareFile, ShareKind, SharePreview};
pub use store::{FakeStore, ObjectStore, StoreError};
pub use transport::{
    Connector, FakeConnector, FakeThreadServer, FakeTransport, Message, NoReconnect, Transport,
    TransportError, WsTransport,
};

/// What the app can ask a running thread to do.
pub enum Command {
    /// Check the replica out (first file open or prompt) and start watching it.
    Materialize(oneshot::Sender<Result<PathBuf, String>>),
    /// Make sure the Run worktree at `worktree` exists and holds canonical
    /// state now, without starting a Run — so an agent session can be opened
    /// there before its first prompt.
    PrepareRun {
        worktree: PathBuf,
        reply: oneshot::Sender<Result<PathBuf, String>>,
    },
    /// Start a Run in the Run worktree at `worktree` (ATL-405).
    StartRun {
        worktree: PathBuf,
        spec: RunSpec,
        reply: oneshot::Sender<Result<RunStarted, String>>,
    },
    /// One live frame of a Run this replica started — a serialized
    /// `SessionDelta`. Best effort: the Session drain is the record.
    RunFrame { run_id: String, payload: Vec<u8> },
    /// The Run's turn ended: merge it back.
    FinishRun {
        run_id: String,
        reply: Option<oneshot::Sender<Result<RunReport, String>>>,
    },
    /// The Run will not finish: mark it interrupted.
    InterruptRun { run_id: String },
    /// Whether to send this repository's history to teammates who lack the
    /// Base (ATL-402). Off until the person agrees.
    ServeHistory(bool),
    /// Close the connection and end the loop.
    Stop,
}

/// A Run this replica started.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunStarted {
    pub run_id: String,
    pub run_no: u64,
    pub worktree: PathBuf,
}

/// What the app shows about a joined thread.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncStatus {
    pub connected: bool,
    pub role: Option<String>,
    pub head: u64,
    pub materialized: bool,
    pub worktree: PathBuf,
    pub files: usize,
    /// Files held on this machine because they now look like they contain a
    /// secret. They resume syncing once it is removed.
    pub held: Vec<String>,
    /// The thread's Runs this replica has heard of, newest first.
    pub runs: Vec<RunView>,
    /// Why this replica can only watch — no Base on this machine, a viewer's
    /// role, a closed thread — or `None` when it can edit.
    pub read_only: Option<String>,
    /// Whether this machine sends the repository's history to teammates who
    /// lack the Base, and how many are waiting for it while it does not.
    pub serves_history: bool,
    pub history_wanted: usize,
    /// Things done on the person's behalf they should hear about, newest last.
    pub notices: Vec<String>,
    /// Saves kept on this machine because it may not change the thread (a
    /// viewer, a closed thread); they go once it may (ATL-406).
    pub unsent: Vec<String>,
    /// The thread is closed.
    pub closed: bool,
    pub error: Option<String>,
}

fn status_of<T: Transport>(session: &ThreadSession<T>, error: Option<String>) -> SyncStatus {
    let replica = session.replica();
    SyncStatus {
        connected: session.is_connected(),
        role: session.role().map(|r| r.as_str().to_string()),
        head: session.head(),
        materialized: replica.is_materialized(),
        worktree: replica.root().to_path_buf(),
        files: replica.files().count(),
        held: replica.held_files(),
        runs: session.runs(),
        read_only: session.read_only(),
        serves_history: session.serves_bundles(),
        history_wanted: session.bundles_wanted(),
        notices: session.notices().to_vec(),
        unsent: session.unsent(),
        closed: session.is_closed(),
        error,
    }
}

enum Event {
    Socket(Option<Message>),
    Command(Option<Command>),
    Saved(String),
    /// Saves have been quiet: files still missing were deleted, not moved.
    Settle,
    /// Time to dial the thread again (ATL-404).
    Reconnect,
    /// Time to check the replica against the thread.
    Verify,
}

/// How long to wait before each attempt to reconnect; the last repeats.
const RECONNECT_BACKOFF: &[std::time::Duration] = &[
    std::time::Duration::from_millis(250),
    std::time::Duration::from_secs(1),
    std::time::Duration::from_secs(2),
    std::time::Duration::from_secs(5),
    std::time::Duration::from_secs(15),
    std::time::Duration::from_secs(30),
];

/// How often a connected replica checks itself against the thread.
const VERIFY_EVERY: std::time::Duration = std::time::Duration::from_secs(300);

/// Why the server closed the socket for good, for the person.
fn closed_because(code: u16) -> String {
    match code {
        1008 => "Your access to this thread ended — you were removed from the organization or the project. Your replica is kept, but it no longer syncs.".into(),
        4410 => "This thread was closed. Your replica is kept, but it no longer syncs.".into(),
        4403 => "You can no longer open this thread. Your replica is kept, but it no longer syncs.".into(),
        4400 => "This version of Atlas cannot talk to the thread. Update Atlas to keep syncing.".into(),
        other => format!("The thread closed the connection ({other})."),
    }
}

/// How long saves must be quiet before a missing file counts as deleted — a
/// move arrives as a removal and a creation, a moment apart (ATL-403).
const SETTLE_AFTER: std::time::Duration = std::time::Duration::from_millis(400);

/// Drive one joined thread until it is stopped or its socket closes.
///
/// Socket frames, app commands and saves in the worktree are taken one at a
/// time, so the replica is only ever touched from here. The watcher starts when
/// the replica is materialized — before that there is nothing on disk to watch.
///
/// The command channel is unbounded because live Run frames arrive on it from
/// the agent's emit path, which must never wait.
pub async fn run<T: Transport>(
    session: ThreadSession<T>,
    commands: mpsc::UnboundedReceiver<Command>,
    status: tokio::sync::watch::Sender<SyncStatus>,
) {
    run_with(session, commands, status, NoReconnect::<T>::default()).await;
}

/// [`run`], dialling the thread again through `connector` whenever the socket
/// drops (ATL-404): saves made meanwhile wait on disk and go out on
/// reconnect, the replica catches up from its last `seq`, and it checks
/// itself against the thread periodically and after each Run. A close the
/// server meant — access revoked, thread closed — ends the loop, and the
/// status says why.
pub async fn run_with<C: Connector>(
    mut session: ThreadSession<C::Transport>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    status: tokio::sync::watch::Sender<SyncStatus>,
    connector: C,
) {
    let mut active: HashMap<String, (ActiveRun, RunWorktree)> = HashMap::new();
    let mut watcher = None;
    let mut saves: Option<mpsc::UnboundedReceiver<String>> = None;
    if session.replica().is_materialized() {
        if let Ok((w, rx)) = watch::watch(session.replica().root()) {
            watcher = Some(w);
            saves = Some(rx);
        }
    }
    let _ = status.send(status_of(&session, None));
    let mut settle_at: Option<tokio::time::Instant> = None;
    let mut reconnect_at: Option<tokio::time::Instant> = None;
    let mut attempts = 0usize;
    let mut verify_at = tokio::time::Instant::now() + VERIFY_EVERY;
    let mut final_error: Option<String> = None;

    loop {
        let connected = session.is_connected();
        let event = tokio::select! {
            message = async {
                if connected {
                    session.next_message().await
                } else {
                    std::future::pending().await
                }
            } => Event::Socket(message),
            () = async {
                match reconnect_at {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => Event::Reconnect,
            () = tokio::time::sleep_until(verify_at) => Event::Verify,
            command = commands.recv() => Event::Command(command),
            Some(path) = async {
                match saves.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => Event::Saved(path),
            () = async {
                match settle_at {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => Event::Settle,
        };
        if matches!(event, Event::Saved(_)) {
            settle_at = Some(tokio::time::Instant::now() + SETTLE_AFTER);
        }
        let outcome = match event {
            Event::Socket(None) => {
                let code = session.close_code();
                if let Some(code) = code.filter(|c| transport::FINAL_CLOSE_CODES.contains(c)) {
                    final_error = Some(closed_because(code));
                    break;
                }
                if !connector.reconnects() {
                    break;
                }
                session.mark_disconnected();
                attempts = 0;
                reconnect_at = Some(tokio::time::Instant::now() + RECONNECT_BACKOFF[0]);
                Ok(())
            }
            Event::Reconnect => {
                reconnect_at = None;
                let dialled = match connector.connect().await {
                    Ok(transport) => session.reconnect(transport).await,
                    Err(e) => Err(e.into()),
                };
                if dialled.is_err() {
                    session.mark_disconnected();
                    attempts += 1;
                    let wait = RECONNECT_BACKOFF[attempts.min(RECONNECT_BACKOFF.len() - 1)];
                    reconnect_at = Some(tokio::time::Instant::now() + wait);
                } else {
                    attempts = 0;
                }
                dialled
            }
            Event::Verify => {
                verify_at = tokio::time::Instant::now() + VERIFY_EVERY;
                session.verify().await.map(|_| ())
            }
            Event::Command(None) | Event::Command(Some(Command::Stop)) => {
                for run_id in active.keys() {
                    let _ = session.interrupt_run(run_id).await;
                }
                break;
            }
            Event::Command(Some(Command::PrepareRun { worktree, reply }))
                if active.values().any(|(run, _)| run.worktree == worktree) =>
            {
                // A Run is working there: never reset it under the agent.
                let _ = reply.send(Ok(worktree));
                Ok(())
            }
            Event::Command(Some(Command::PrepareRun { worktree, reply })) => {
                let result = session
                    .run_worktree(&worktree)
                    .and_then(|worktree| session.prepare_run(&worktree).map(|()| worktree));
                let _ = reply.send(
                    result
                        .as_ref()
                        .map(|worktree| worktree.root().to_path_buf())
                        .map_err(ToString::to_string),
                );
                result.map(|_| ())
            }
            Event::Command(Some(Command::StartRun {
                worktree,
                spec,
                reply,
            })) => {
                let result = match session.run_worktree(&worktree) {
                    Ok(worktree) => session
                        .start_run(&worktree, spec)
                        .await
                        .map(|run| (run, worktree)),
                    Err(e) => Err(e),
                };
                let answer = result.as_ref().map(|(run, _)| RunStarted {
                    run_id: run.run_id.clone(),
                    run_no: run.run_no,
                    worktree: run.worktree.clone(),
                });
                let _ = reply.send(answer.map_err(std::string::ToString::to_string));
                result.map(|(run, worktree)| {
                    active.insert(run.run_id.clone(), (run, worktree));
                })
            }
            Event::Command(Some(Command::RunFrame { run_id, payload })) => {
                match active.get(&run_id) {
                    Some((run, _)) => {
                        session
                            .stream_run(run.run_no, wire::FrameKind::RunStream, payload)
                            .await
                    }
                    None => Ok(()),
                }
            }
            Event::Command(Some(Command::FinishRun { run_id, reply })) => {
                match active.remove(&run_id) {
                    Some((run, worktree)) => {
                        let mut result = session.finish_run(&run, &worktree).await;
                        // Each Run's end is a moment to check the replica.
                        if result.is_ok() {
                            if let Err(e) = session.verify().await {
                                tracing::warn!(target: "atlas_thread_sync", "verify after a Run: {e}");
                            }
                        }
                        let result = std::mem::replace(&mut result, Ok(RunReport::default()));
                        let text = result
                            .as_ref()
                            .map(Clone::clone)
                            .map_err(ToString::to_string);
                        if let Some(reply) = reply {
                            let _ = reply.send(text);
                        }
                        result.map(|_| ())
                    }
                    None => Ok(()),
                }
            }
            Event::Command(Some(Command::ServeHistory(on))) => {
                session.set_serve_bundles(on);
                Ok(())
            }
            Event::Command(Some(Command::InterruptRun { run_id })) => {
                match active.remove(&run_id) {
                    Some(_) => session.interrupt_run(&run_id).await,
                    None => Ok(()),
                }
            }
            Event::Socket(Some(message)) => session.receive(message).await.map(|_| ()),
            Event::Saved(path) => session.file_saved(&path).await.map(|_| ()),
            Event::Settle => {
                settle_at = None;
                session.settle_removals().await.map(|_| ())
            }
            Event::Command(Some(Command::Materialize(reply))) => {
                let result = session.materialize().await;
                if let Ok(root) = &result {
                    if watcher.is_none() {
                        match watch::watch(root) {
                            Ok((w, rx)) => {
                                watcher = Some(w);
                                saves = Some(rx);
                            }
                            Err(e) => {
                                tracing::warn!(target: "atlas_thread_sync", "watcher failed: {e}")
                            }
                        }
                    }
                }
                let _ = reply.send(
                    result
                        .as_ref()
                        .map(Clone::clone)
                        .map_err(std::string::ToString::to_string),
                );
                result.map(|_| ())
            }
        };
        let mut error = outcome.err().map(|e| e.to_string());
        // Somebody lacks the Base and this replica may hold it (ATL-402).
        // Promoted, or the thread reopened: what was held may go now.
        if session.wants_flush() {
            if let Err(e) = session.flush_unsent().await {
                error.get_or_insert_with(|| e.to_string());
            }
        }
        let wants = session.take_bundle_wants();
        if let Err(e) = session.serve_bundles(wants).await {
            error.get_or_insert_with(|| format!("could not send the Base: {e}"));
        }
        if let Some(e) = &error {
            tracing::warn!(target: "atlas_thread_sync", "thread sync: {e}");
        }
        let _ = status.send(status_of(&session, error));
    }
    drop(watcher);
    session.mark_disconnected();
    let _ = status.send(status_of(&session, final_error));
}
