//! How a session reaches the thread socket.
//!
//! A small trait rather than the WebSocket directly, so the session's whole
//! protocol can be driven in tests against [`FakeThreadServer`] — an
//! in-process stand-in that keeps the server's rules (journal, dense `seq`,
//! idempotent resend, relay to everyone else) — with no network and no worker.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use base64::Engine as _;

use crate::store::FakeStore;
use crate::wire::{
    self, BundleFailure, ClientControl, FileVersion, Frame, FrameKind, MergedFile, Role,
    RunOutcome, ServerControl, ThreadRun, TreeEntry,
};

/// One message on the socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Text(String),
    Binary(Vec<u8>),
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("the socket is closed")]
    Closed,
    #[error("websocket: {0}")]
    Ws(String),
}

pub trait Transport: Send {
    fn send(&mut self, message: Message)
        -> impl Future<Output = Result<(), TransportError>> + Send;
    /// The next message, or `None` once the socket has closed.
    fn recv(&mut self) -> impl Future<Output = Option<Message>> + Send;
}

// ---------------------------------------------------------------------------
// The real socket
// ---------------------------------------------------------------------------

type Stream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// The thread socket at `{ws_base}/threads/ws?org=&workspace=&thread=`.
pub struct WsTransport {
    stream: Stream,
    /// The close code the server sent, if it closed us (1008 = access revoked).
    pub close_code: Option<u16>,
}

/// The socket URL for one thread, from the ingest base (`https://…` or
/// `wss://…`).
pub fn thread_socket_url(ingest_base: &str, org: &str, workspace: &str, thread: &str) -> String {
    let base = ingest_base.trim_end_matches('/');
    let base = base
        .strip_prefix("https://")
        .map(|rest| format!("wss://{rest}"))
        .or_else(|| {
            base.strip_prefix("http://")
                .map(|rest| format!("ws://{rest}"))
        })
        .unwrap_or_else(|| base.to_string());
    format!("{base}/threads/ws?org={org}&workspace={workspace}&thread={thread}")
}

impl WsTransport {
    /// Dial with the ticket in the subprotocol, the way every Atlas socket
    /// carries it (ADR-0005). The request is never logged: it holds the token.
    pub async fn connect(url: &str, token: &str) -> Result<Self, TransportError> {
        let mut request = url
            .into_client_request()
            .map_err(|e| TransportError::Ws(format!("bad url: {e}")))?;
        let protocols = format!("atlas.v1, atlas.ticket.{token}");
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_str(&protocols)
                .map_err(|_| TransportError::Ws("token is not a header value".into()))?,
        );
        let (stream, _) = tokio_tungstenite::connect_async(request)
            .await
            .map_err(|e| TransportError::Ws(e.to_string()))?;
        Ok(Self {
            stream,
            close_code: None,
        })
    }
}

impl Transport for WsTransport {
    async fn send(&mut self, message: Message) -> Result<(), TransportError> {
        let message = match message {
            Message::Text(text) => WsMessage::Text(text.into()),
            Message::Binary(bytes) => WsMessage::Binary(bytes.into()),
        };
        self.stream
            .send(message)
            .await
            .map_err(|e| TransportError::Ws(e.to_string()))
    }

    async fn recv(&mut self) -> Option<Message> {
        loop {
            match self.stream.next().await? {
                Ok(WsMessage::Text(text)) => return Some(Message::Text(text.to_string())),
                Ok(WsMessage::Binary(bytes)) => return Some(Message::Binary(bytes.to_vec())),
                Ok(WsMessage::Close(frame)) => {
                    self.close_code = frame.map(|f| u16::from(f.code));
                    return None;
                }
                Ok(_) => continue,
                Err(_) => return None,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The fake server
// ---------------------------------------------------------------------------

struct Journaled {
    seq: u64,
    tree: Option<TreeEntry>,
    frame: Option<Frame>,
    author: String,
    client: String,
    client_seq: u64,
}

#[derive(Default)]
struct Hub {
    journal: Vec<Journaled>,
    tree: Vec<TreeEntry>,
    next_conn: u64,
    conns: HashMap<u64, Conn>,
    /// Every binary frame any client sent, for tests that count echoes.
    received_updates: u64,
    runs: Vec<FakeRun>,
    /// Accepted merges by `(user, client, clientSeq)`, for idempotent resends.
    merges: HashMap<(String, String, u64), ServerControl>,
    /// Live Run frames relayed, for tests that check nothing was stored.
    run_frames_relayed: u64,
    /// Another Runner's merge to land just before the next submit is read.
    racing_merge: Option<(u64, Vec<u8>)>,
    /// The thread's object doors, shared with the tests.
    store: FakeStore,
    /// Open bundle requests: the id, and the connection that asked.
    bundle_requests: HashMap<String, u64>,
    next_request: u64,
}

struct FakeRun {
    run: ThreadRun,
    runner_client: String,
}

struct Conn {
    user: String,
    client: Option<String>,
    tx: mpsc::UnboundedSender<Message>,
}

/// An in-process thread server with the real one's rules: everything is
/// journaled before it is acknowledged or relayed, `seq` is dense across tree
/// and text changes, `(user, client, clientSeq)` is stored once, and a hello
/// is answered with a welcome, the replay after `since`, and `synced`.
#[derive(Clone, Default)]
pub struct FakeThreadServer {
    hub: Arc<Mutex<Hub>>,
}

/// One connection to a [`FakeThreadServer`].
pub struct FakeTransport {
    hub: Arc<Mutex<Hub>>,
    conn: u64,
    rx: mpsc::UnboundedReceiver<Message>,
}

impl FakeThreadServer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn connect(&self, user: &str) -> FakeTransport {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut hub = self.hub.lock().expect("hub");
        hub.next_conn += 1;
        let conn = hub.next_conn;
        hub.conns.insert(
            conn,
            Conn {
                user: user.to_string(),
                client: None,
                tx,
            },
        );
        FakeTransport {
            hub: self.hub.clone(),
            conn,
            rx,
        }
    }

    /// How many canonical updates clients have sent, resends included.
    pub fn updates_received(&self) -> u64 {
        self.hub.lock().expect("hub").received_updates
    }

    pub fn head(&self) -> u64 {
        self.hub.lock().expect("hub").journal.len() as u64
    }

    /// The Runs the server holds, by `runNo`.
    pub fn runs(&self) -> Vec<ThreadRun> {
        self.hub
            .lock()
            .expect("hub")
            .runs
            .iter()
            .map(|r| r.run.clone())
            .collect()
    }

    /// A file's merge version.
    pub fn merge_version(&self, file_id: u64) -> Option<u64> {
        self.hub
            .lock()
            .expect("hub")
            .tree
            .iter()
            .find(|e| e.file_id == file_id)
            .and_then(|e| e.merge_version)
    }

    /// Land a merge of `update` on `file_id` the moment the next
    /// `merge.submit` arrives, ahead of it — the race a Runner loses when
    /// another merge reaches the server while its own submit is on the way.
    pub fn merge_before_next_submit(&self, file_id: u64, update: Vec<u8>) {
        self.hub.lock().expect("hub").racing_merge = Some((file_id, update));
    }

    /// Live Run frames relayed so far. None of them is ever journaled.
    pub fn run_frames_relayed(&self) -> u64 {
        self.hub.lock().expect("hub").run_frames_relayed
    }

    /// The thread's object doors: what replicas upload, and download.
    pub fn store(&self) -> FakeStore {
        self.hub.lock().expect("hub").store.clone()
    }

    /// Every canonical update payload journaled, in `seq` order — to check
    /// what reached the thread.
    pub fn journaled_payloads(&self) -> Vec<Vec<u8>> {
        self.hub
            .lock()
            .expect("hub")
            .journal
            .iter()
            .filter_map(|j| j.frame.as_ref().map(|f| f.payload.clone()))
            .collect()
    }

    /// The tree as the server holds it.
    pub fn tree(&self) -> Vec<TreeEntry> {
        self.hub.lock().expect("hub").tree.clone()
    }
}

impl Hub {
    fn reply(&self, conn: u64, frame: &ServerControl) {
        if let Some(c) = self.conns.get(&conn) {
            let _ =
                c.tx.send(Message::Text(serde_json::to_string(frame).expect("json")));
        }
    }

    fn broadcast(&self, frame: &ServerControl) {
        let text = serde_json::to_string(frame).expect("json");
        for c in self.conns.values() {
            if c.client.is_some() {
                let _ = c.tx.send(Message::Text(text.clone()));
            }
        }
    }

    fn nack(&self, conn: u64, client_seq: u64, code: &str) {
        self.reply(
            conn,
            &ServerControl::Nack {
                client_seq,
                code: code.into(),
                message: code.into(),
            },
        );
    }

    fn head(&self) -> u64 {
        self.journal.len() as u64
    }

    /// The runner's socket went away: its Runs are interrupted, as the real
    /// server's heartbeat sweep does.
    fn disconnect(&mut self, conn: u64) {
        let Some(c) = self.conns.remove(&conn) else {
            return;
        };
        let Some(client) = c.client else { return };
        let mut ended = Vec::new();
        for r in &mut self.runs {
            if r.run.status == "running" && r.run.runner_id == c.user && r.runner_client == client {
                r.run.status = "interrupted".into();
                r.run.ended_at = Some(2);
                ended.push(r.run.clone());
            }
        }
        for run in ended {
            self.broadcast(&ServerControl::Run { run });
        }
    }

    /// Journal `update` as somebody else's merge: advance the version, relay
    /// it to every socket, and say `merged` — in that order, as the real
    /// server does.
    fn land_racing_merge(&mut self, file_id: u64, update: Vec<u8>) {
        let seq = self.journal.len() as u64 + 1;
        let frame = Frame {
            seq,
            ..Frame::update(file_id, 0, update)
        };
        self.journal.push(Journaled {
            seq,
            tree: None,
            frame: Some(frame.clone()),
            author: "racer".into(),
            client: "racer#merge1".into(),
            client_seq: 1,
        });
        let Some(entry) = self.tree.iter_mut().find(|e| e.file_id == file_id) else {
            return;
        };
        let version = entry.merge_version.unwrap_or(0) + 1;
        entry.merge_version = Some(version);
        let bytes = wire::encode(&frame).expect("encode");
        for c in self.conns.values() {
            if c.client.is_some() {
                let _ = c.tx.send(Message::Binary(bytes.clone()));
            }
        }
        self.broadcast(&ServerControl::Merged {
            run_id: "run-racer".into(),
            version: seq,
            files: vec![MergedFile {
                file_id,
                version,
                blob: "0".repeat(64),
            }],
        });
    }

    fn run_mut(&mut self, run_id: &str) -> Option<&mut FakeRun> {
        self.runs.iter_mut().find(|r| r.run.run_id == run_id)
    }

    fn relay(&self, from: u64, message: Message) {
        for (id, c) in &self.conns {
            if *id != from && c.client.is_some() {
                let _ = c.tx.send(message.clone());
            }
        }
    }

    fn prior(&self, user: &str, client: &str, client_seq: u64) -> Option<&Journaled> {
        self.journal
            .iter()
            .find(|j| j.author == user && j.client == client && j.client_seq == client_seq)
    }

    fn handle(&mut self, conn: u64, message: Message) {
        let Some(c) = self.conns.get(&conn) else {
            return;
        };
        let (user, client) = (c.user.clone(), c.client.clone());
        match message {
            Message::Text(text) => match serde_json::from_str::<ClientControl>(&text) {
                Ok(ClientControl::Hello {
                    client_id, since, ..
                }) => {
                    let last = self
                        .journal
                        .iter()
                        .filter(|j| j.author == user && j.client == client_id)
                        .map(|j| j.client_seq)
                        .max()
                        .unwrap_or(0);
                    self.reply(
                        conn,
                        &ServerControl::Welcome {
                            protocol: wire::PROTOCOL_VERSION,
                            thread_id: "fake-thread".into(),
                            workspace_id: "fake-workspace".into(),
                            org_id: "fake-org".into(),
                            role: Role::Participant,
                            head: self.journal.len() as u64,
                            last_client_seq: last,
                        },
                    );
                    let tx = self.conns[&conn].tx.clone();
                    for j in self.journal.iter().filter(|j| j.seq > since) {
                        let message = match (&j.tree, &j.frame) {
                            (Some(entry), _) => Message::Text(
                                serde_json::to_string(&ServerControl::Tree {
                                    seq: j.seq,
                                    entry: self
                                        .tree
                                        .iter()
                                        .find(|e| e.file_id == entry.file_id)
                                        .unwrap_or(entry)
                                        .clone(),
                                })
                                .expect("json"),
                            ),
                            (None, Some(frame)) => {
                                Message::Binary(wire::encode(frame).expect("encode"))
                            }
                            _ => continue,
                        };
                        let _ = tx.send(message);
                    }
                    let _ = tx.send(Message::Text(
                        serde_json::to_string(&ServerControl::Synced {
                            head: self.journal.len() as u64,
                        })
                        .expect("json"),
                    ));
                    if let Some(c) = self.conns.get_mut(&conn) {
                        c.client = Some(client_id);
                    }
                }
                Ok(ClientControl::BundleRequest { client_seq, have }) => {
                    if client.is_none() {
                        return;
                    }
                    self.next_request += 1;
                    let request_id = format!("request-{}", self.next_request);
                    self.reply(
                        conn,
                        &ServerControl::BundlePending {
                            client_seq,
                            request_id: request_id.clone(),
                        },
                    );
                    let held: std::collections::HashSet<&String> = have.iter().collect();
                    let cached = self
                        .store
                        .bundles()
                        .into_iter()
                        .find(|(_, b)| b.prerequisites.iter().all(|p| held.contains(p)));
                    if let Some((sha, bundle)) = cached {
                        return self.reply(
                            conn,
                            &ServerControl::BundleAvailable {
                                request_id,
                                sha,
                                bytes: bundle.bytes.len() as u64,
                            },
                        );
                    }
                    let wanted = ServerControl::BundleWanted {
                        request_id: request_id.clone(),
                        have,
                    };
                    let builders: Vec<u64> = self
                        .conns
                        .iter()
                        .filter(|(id, c)| **id != conn && c.client.is_some())
                        .map(|(id, _)| *id)
                        .collect();
                    if builders.is_empty() {
                        return self.reply(
                            conn,
                            &ServerControl::BundleUnavailable {
                                request_id,
                                reason: BundleFailure::NoReplicaOnline,
                                bytes: None,
                            },
                        );
                    }
                    self.bundle_requests.insert(request_id, conn);
                    for b in builders {
                        self.reply(b, &wanted);
                    }
                }
                Ok(ClientControl::BundleReady {
                    client_seq,
                    request_id,
                    sha,
                }) => {
                    let Some(bundle) = self.store.bundle(&sha) else {
                        return self.nack(conn, client_seq, "blob_missing");
                    };
                    self.reply(
                        conn,
                        &ServerControl::Ack {
                            client_seq,
                            seq: self.head(),
                            file_id: None,
                        },
                    );
                    if let Some(asker) = self.bundle_requests.remove(&request_id) {
                        self.reply(
                            asker,
                            &ServerControl::BundleAvailable {
                                request_id,
                                sha,
                                bytes: bundle.bytes.len() as u64,
                            },
                        );
                    }
                }
                Ok(ClientControl::BundleFailed {
                    client_seq,
                    request_id,
                    reason,
                    bytes,
                }) => {
                    self.reply(
                        conn,
                        &ServerControl::Ack {
                            client_seq,
                            seq: self.head(),
                            file_id: None,
                        },
                    );
                    if let Some(asker) = self.bundle_requests.remove(&request_id) {
                        self.reply(
                            asker,
                            &ServerControl::BundleUnavailable {
                                request_id,
                                reason,
                                bytes: Some(bytes),
                            },
                        );
                    }
                }
                Ok(ClientControl::TreeEnsure {
                    client_seq,
                    path,
                    kind,
                    ..
                }) => {
                    let Some(client) = client else { return };
                    if let Some(existing) = self.tree.iter().find(|e| e.path == path) {
                        let seq = self
                            .journal
                            .iter()
                            .find(|j| j.tree.as_ref() == Some(existing))
                            .map_or(0, |j| j.seq);
                        let file_id = existing.file_id;
                        self.reply(
                            conn,
                            &ServerControl::Ack {
                                client_seq,
                                seq,
                                file_id: Some(file_id),
                            },
                        );
                        return;
                    }
                    let seq = self.journal.len() as u64 + 1;
                    let entry = TreeEntry {
                        file_id: self.tree.len() as u64 + 1,
                        path,
                        kind,
                        merge_version: Some(0),
                    };
                    self.tree.push(entry.clone());
                    self.journal.push(Journaled {
                        seq,
                        tree: Some(entry.clone()),
                        frame: None,
                        author: user,
                        client,
                        client_seq,
                    });
                    self.reply(
                        conn,
                        &ServerControl::Ack {
                            client_seq,
                            seq,
                            file_id: Some(entry.file_id),
                        },
                    );
                    let relayed = ServerControl::Tree { seq, entry };
                    self.relay(
                        conn,
                        Message::Text(serde_json::to_string(&relayed).expect("json")),
                    );
                }
                Ok(ClientControl::RunStart {
                    client_seq,
                    run_id,
                    agent,
                    model,
                    fork_seq,
                    context_anchor,
                }) => {
                    let Some(client) = client else { return };
                    if let Some(existing) = self.runs.iter().find(|r| r.run.run_id == run_id) {
                        if existing.run.runner_id != user {
                            return self.nack(conn, client_seq, "run_conflict");
                        }
                        let run = existing.run.clone();
                        self.reply(
                            conn,
                            &ServerControl::Ack {
                                client_seq,
                                seq: self.head(),
                                file_id: None,
                            },
                        );
                        return self.reply(conn, &ServerControl::Run { run });
                    }
                    let run = ThreadRun {
                        run_id,
                        run_no: self.runs.len() as u64 + 1,
                        prompted_by: user.clone(),
                        runner_id: user,
                        agent,
                        model,
                        fork_seq,
                        context_anchor,
                        status: "running".into(),
                        started_at: 1,
                        ended_at: None,
                        merged_version: None,
                    };
                    self.runs.push(FakeRun {
                        run: run.clone(),
                        runner_client: client,
                    });
                    self.reply(
                        conn,
                        &ServerControl::Ack {
                            client_seq,
                            seq: self.head(),
                            file_id: None,
                        },
                    );
                    self.broadcast(&ServerControl::Run { run });
                }
                Ok(ClientControl::RunEnd {
                    client_seq,
                    run_id,
                    outcome,
                }) => {
                    let head = self.head();
                    let Some(r) = self.run_mut(&run_id) else {
                        return self.nack(conn, client_seq, "run_unknown");
                    };
                    if r.run.runner_id != user {
                        return self.nack(conn, client_seq, "not_runner");
                    }
                    if r.run.status == "running" {
                        r.run.status = match (outcome, r.run.merged_version) {
                            (RunOutcome::Interrupted, _) => "interrupted",
                            (RunOutcome::Completed, Some(_)) => "merged",
                            (RunOutcome::Completed, None) => "ended",
                        }
                        .into();
                        r.run.ended_at = Some(2);
                    }
                    let run = r.run.clone();
                    self.reply(
                        conn,
                        &ServerControl::Ack {
                            client_seq,
                            seq: head,
                            file_id: None,
                        },
                    );
                    self.broadcast(&ServerControl::Run { run });
                }
                Ok(ClientControl::MergeSubmit {
                    client_seq,
                    run_id,
                    files,
                }) => {
                    let Some(client) = client else { return };
                    if let Some((file_id, update)) = self.racing_merge.take() {
                        self.land_racing_merge(file_id, update);
                    }
                    let key = (user.clone(), client.clone(), client_seq);
                    if let Some(prior) = self.merges.get(&key) {
                        return self.reply(conn, &prior.clone());
                    }
                    match self.runs.iter().find(|r| r.run.run_id == run_id) {
                        None => return self.nack(conn, client_seq, "run_unknown"),
                        Some(r) if r.run.runner_id != user => {
                            return self.nack(conn, client_seq, "not_runner")
                        }
                        Some(r) if r.run.status != "running" => {
                            return self.nack(conn, client_seq, "run_unknown")
                        }
                        Some(_) => {}
                    }
                    let current = |id: u64| {
                        self.tree
                            .iter()
                            .find(|e| e.file_id == id)
                            .map(|e| e.merge_version.unwrap_or(0))
                    };
                    if files.iter().any(|f| current(f.file_id).is_none()) {
                        return self.nack(conn, client_seq, "unknown_file");
                    }
                    if files
                        .iter()
                        .any(|f| current(f.file_id) != Some(f.base_version))
                    {
                        let versions = files
                            .iter()
                            .map(|f| FileVersion {
                                file_id: f.file_id,
                                version: current(f.file_id).unwrap_or(0),
                            })
                            .collect();
                        return self.reply(
                            conn,
                            &ServerControl::MergeRejected {
                                client_seq,
                                run_id,
                                versions,
                            },
                        );
                    }
                    let mut stored = Vec::new();
                    let mut landed = Vec::new();
                    let merge_client = format!("{client}#merge{client_seq}");
                    for (i, f) in files.iter().enumerate() {
                        let Ok(update) =
                            base64::engine::general_purpose::STANDARD.decode(&f.update)
                        else {
                            return self.nack(conn, client_seq, "bad_frame");
                        };
                        let seq = self.journal.len() as u64 + 1;
                        let frame = Frame {
                            seq,
                            ..Frame::update(f.file_id, 0, update)
                        };
                        self.journal.push(Journaled {
                            seq,
                            tree: None,
                            frame: Some(frame.clone()),
                            author: user.clone(),
                            client: merge_client.clone(),
                            client_seq: i as u64 + 1,
                        });
                        let entry = self
                            .tree
                            .iter_mut()
                            .find(|e| e.file_id == f.file_id)
                            .expect("checked above");
                        let version = entry.merge_version.unwrap_or(0) + 1;
                        entry.merge_version = Some(version);
                        landed.push(FileVersion {
                            file_id: f.file_id,
                            version,
                        });
                        stored.push(frame);
                    }
                    let version = self.head();
                    if let Some(r) = self.run_mut(&run_id) {
                        r.run.merged_version = Some(version);
                    }
                    let accepted = ServerControl::MergeAccepted {
                        client_seq,
                        run_id: run_id.clone(),
                        version,
                        files: landed.clone(),
                    };
                    self.merges.insert(key, accepted.clone());
                    self.reply(conn, &accepted);
                    for frame in &stored {
                        self.relay(conn, Message::Binary(wire::encode(frame).expect("encode")));
                    }
                    let merged = ServerControl::Merged {
                        run_id,
                        version,
                        files: landed
                            .iter()
                            .zip(&files)
                            .map(|(l, f)| MergedFile {
                                file_id: l.file_id,
                                version: l.version,
                                blob: f.blob.clone(),
                            })
                            .collect(),
                    };
                    self.relay(
                        conn,
                        Message::Text(serde_json::to_string(&merged).expect("json")),
                    );
                }
                Err(_) => {}
            },
            Message::Binary(bytes) => {
                let Some(client) = client else { return };
                let Some(frame) = wire::decode(&bytes) else {
                    return;
                };
                if frame.kind == FrameKind::RunStream as u8
                    || frame.kind == FrameKind::RunFile as u8
                {
                    let ours = self.runs.iter().any(|r| {
                        r.run.run_no == frame.file_id
                            && r.run.status == "running"
                            && r.run.runner_id == user
                            && r.runner_client == client
                    });
                    if !ours {
                        return self.nack(conn, frame.client_seq, "run_unknown");
                    }
                    self.run_frames_relayed += 1;
                    let relayed = Frame {
                        seq: 0,
                        client_seq: 0,
                        ..frame
                    };
                    return self.relay(
                        conn,
                        Message::Binary(wire::encode(&relayed).expect("encode")),
                    );
                }
                if frame.kind != FrameKind::CanonicalUpdate as u8 {
                    return;
                }
                self.received_updates += 1;
                if let Some(prior) = self.prior(&user, &client, frame.client_seq) {
                    let seq = prior.seq;
                    self.reply(
                        conn,
                        &ServerControl::Ack {
                            client_seq: frame.client_seq,
                            seq,
                            file_id: None,
                        },
                    );
                    return;
                }
                let seq = self.journal.len() as u64 + 1;
                let stored = Frame {
                    seq,
                    client_seq: 0,
                    ..frame.clone()
                };
                self.journal.push(Journaled {
                    seq,
                    tree: None,
                    frame: Some(stored.clone()),
                    author: user,
                    client,
                    client_seq: frame.client_seq,
                });
                self.reply(
                    conn,
                    &ServerControl::Ack {
                        client_seq: frame.client_seq,
                        seq,
                        file_id: None,
                    },
                );
                self.relay(
                    conn,
                    Message::Binary(wire::encode(&stored).expect("encode")),
                );
            }
        }
    }
}

impl Transport for FakeTransport {
    async fn send(&mut self, message: Message) -> Result<(), TransportError> {
        let mut hub = self.hub.lock().map_err(|_| TransportError::Closed)?;
        if !hub.conns.contains_key(&self.conn) {
            return Err(TransportError::Closed);
        }
        hub.handle(self.conn, message);
        Ok(())
    }

    async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }
}

impl Drop for FakeTransport {
    fn drop(&mut self) {
        if let Ok(mut hub) = self.hub.lock() {
            hub.disconnect(self.conn);
        }
    }
}
