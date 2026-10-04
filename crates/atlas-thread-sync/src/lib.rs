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
//! * [`run`] — the loop an app spawns per joined thread.

pub mod doc;
pub mod git;
pub mod path;
pub mod replica;
pub mod session;
pub mod transport;
pub mod watch;
pub mod wire;

use std::path::PathBuf;

use serde::Serialize;
use tokio::sync::{mpsc, oneshot};

pub use replica::{LocalChange, Replica, ReplicaError};
pub use session::{SessionError, ThreadSession};
pub use transport::{
    FakeThreadServer, FakeTransport, Message, Transport, TransportError, WsTransport,
};

/// What the app can ask a running thread to do.
pub enum Command {
    /// Check the replica out (first file open or prompt) and start watching it.
    Materialize(oneshot::Sender<Result<PathBuf, String>>),
    /// Close the connection and end the loop.
    Stop,
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
pub async fn run<T: Transport>(
    mut session: ThreadSession<T>,
    mut commands: mpsc::Receiver<Command>,
    status: tokio::sync::watch::Sender<SyncStatus>,
) {
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
            Event::Socket(None) | Event::Command(None) | Event::Command(Some(Command::Stop)) => {
                break
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
                let _ = reply.send(result.as_ref().map(Clone::clone).map_err(|e| e.to_string()));
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
