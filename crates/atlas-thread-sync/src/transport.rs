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

use crate::wire::{self, ClientControl, Frame, FrameKind, Role, ServerControl, TreeEntry};

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
}

impl Hub {
    fn reply(&self, conn: u64, frame: &ServerControl) {
        if let Some(c) = self.conns.get(&conn) {
            let _ =
                c.tx.send(Message::Text(serde_json::to_string(frame).expect("json")));
        }
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
                                    entry: entry.clone(),
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
                Ok(ClientControl::TreeEnsure {
                    client_seq,
                    path,
                    kind,
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
                Err(_) => {}
            },
            Message::Binary(bytes) => {
                let Some(client) = client else { return };
                let Some(frame) = wire::decode(&bytes) else {
                    return;
                };
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
            hub.conns.remove(&self.conn);
        }
    }
}
