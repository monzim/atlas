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
//! * [`run`] — the loop an app spawns per joined thread.

pub mod doc;
pub mod git;
pub mod merge;
pub mod path;
pub mod replica;
pub mod runs;
pub mod secrets;
pub mod session;
pub mod transport;
pub mod watch;
pub mod wire;

use std::collections::HashMap;
use std::path::PathBuf;

use serde::Serialize;
use tokio::sync::{mpsc, oneshot};

pub use replica::{LocalChange, Replica, ReplicaError};
pub use runs::{ActiveRun, RunReport, RunSpec, RunWorktree};
pub use secrets::SecretReason;
pub use session::{BlobSink, RunView, SessionError, ShareReport, ThreadEvent, ThreadSession};
pub use transport::{
    FakeThreadServer, FakeTransport, Message, Transport, TransportError, WsTransport,
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
    pub error: Option<String>,
}

fn status_of<T: Transport>(
    session: &ThreadSession<T>,
    connected: bool,
    error: Option<String>,
) -> SyncStatus {
    let replica = session.replica();
    SyncStatus {
        connected,
        role: session.role().map(|r| r.as_str().to_string()),
        head: session.head(),
        materialized: replica.is_materialized(),
        worktree: replica.root().to_path_buf(),
        files: replica.files().count(),
        held: replica.held_files(),
        runs: session.runs(),
        error,
    }
}

enum Event {
    Socket(Option<Message>),
    Command(Option<Command>),
    Saved(String),
}

/// Drive one joined thread until it is stopped or its socket closes.
///
/// Socket frames, app commands and saves in the worktree are taken one at a
/// time, so the replica is only ever touched from here. The watcher starts when
/// the replica is materialized — before that there is nothing on disk to watch.
///
/// The command channel is unbounded because live Run frames arrive on it from
/// the agent's emit path, which must never wait.
pub async fn run<T: Transport, B: BlobSink>(
    mut session: ThreadSession<T>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    status: tokio::sync::watch::Sender<SyncStatus>,
    blobs: B,
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
    let _ = status.send(status_of(&session, true, None));

    loop {
        let event = tokio::select! {
            message = session.next_message() => Event::Socket(message),
            command = commands.recv() => Event::Command(command),
            Some(path) = async {
                match saves.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => Event::Saved(path),
        };
        let outcome = match event {
            Event::Socket(None) => break,
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
                let worktree = session.run_worktree(&worktree);
                let result = session.prepare_run(&worktree);
                let _ = reply.send(
                    result
                        .as_ref()
                        .map(|()| worktree.root().to_path_buf())
                        .map_err(ToString::to_string),
                );
                result
            }
            Event::Command(Some(Command::StartRun {
                worktree,
                spec,
                reply,
            })) => {
                let worktree = session.run_worktree(&worktree);
                let result = session.start_run(&worktree, spec).await;
                let answer = result.as_ref().map(|run| RunStarted {
                    run_id: run.run_id.clone(),
                    run_no: run.run_no,
                    worktree: run.worktree.clone(),
                });
                let _ = reply.send(answer.map_err(std::string::ToString::to_string));
                result.map(|run| {
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
                        let result = session.finish_run(&run, &worktree, &blobs).await;
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
            Event::Command(Some(Command::InterruptRun { run_id })) => {
                match active.remove(&run_id) {
                    Some(_) => session.interrupt_run(&run_id).await,
                    None => Ok(()),
                }
            }
            Event::Socket(Some(message)) => session.receive(message).await.map(|_| ()),
            Event::Saved(path) => session.file_saved(&path).await.map(|_| ()),
            Event::Command(Some(Command::Materialize(reply))) => {
                let result = session.materialize();
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
        let error = outcome.err().map(|e| e.to_string());
        if let Some(e) = &error {
            tracing::warn!(target: "atlas_thread_sync", "thread sync: {e}");
        }
        let _ = status.send(status_of(&session, true, error));
    }
    drop(watcher);
    let _ = status.send(status_of(&session, false, None));
}
