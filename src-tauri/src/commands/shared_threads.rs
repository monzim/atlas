//! Shared Threads in the app (ATL-395): share the thread you are in, join one
//! from a link, and keep each joined thread's replica in step.
//!
//! Everything protocol-shaped lives in `atlas-thread-sync`; this module owns
//! what only the app has — the account token, the project's cloud binding, the
//! thread-metadata store, and where on disk replicas go. Only Rust holds the
//! bearer, so the renderer invokes these and renders the events on
//! [`SHARED_THREADS_EVENT`].
//!
//! # Where state lives
//!
//! * The link from a local thread to the Shared Thread it was shared as is in
//!   the thread-metadata store (`shared_thread_id`, Base, role).
//! * Which threads this machine has joined — and the stable replica id each
//!   reconnects as — is `shared-threads.json` in the app config directory, so
//!   a joined thread is rejoined at launch.
//! * Replicas are worktrees under `<app data>/shared-threads/<id>/replica`,
//!   never inside the person's own checkout.
//! * Each joined thread's Run worktree (ATL-405) is
//!   `<app data>/shared-threads/<id>/run`: one per participant on this
//!   machine, reused Run after Run so the agent session's `cwd` never changes.
//!
//! # Runs (ATL-405)
//!
//! A prompt sent in an agent session whose `cwd` is a thread's Run worktree is
//! a Run: [`begin_run`] (called from `agents_send`) forks canonical state into
//! the worktree and tells the thread, [`SharedRunMiddleware`] streams the
//! session's deltas to the thread as live Run frames, and the turn's end
//! merges the result back. Any ACP agent and the native one alike — they all
//! reach here through the same delta pipeline.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use agent_client_protocol::schema::v1 as acp;
use atlas_agent_wire::{AgentId, DeltaSink, SessionDelta, SessionDeltaEnvelope};
use atlas_bus::OutboundMiddleware;
use atlas_thread_metadata::SharedThreadLink;
use atlas_thread_sync::{
    BlobSink, Command as SyncCommand, Replica, RunReport, RunSpec, SyncStatus, ThreadEvent,
    ThreadSession, WsTransport,
};
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::sync::{mpsc, oneshot, watch};

use crate::commands::agent_host::AgentHost;

/// The window event channel for joined-thread status.
pub const SHARED_THREADS_EVENT: &str = "atlas:shared-threads";

/// The window event channel for other people's live Run frames.
pub const SHARED_RUN_FRAME_EVENT: &str = "atlas:shared-run-frame";

/// What the person sees about one Shared Thread this machine has joined.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SharedThreadEntry {
    pub shared_thread_id: String,
    pub org_id: String,
    pub workspace_id: String,
    pub title: String,
    pub base: String,
    pub role: String,
    /// The person's own checkout of the project. Only ever read.
    pub project_path: String,
    /// This replica's stable id on the wire, kept across reconnects so the
    /// server can say which of its frames it already stored.
    pub client_id: String,
    /// The link to send a teammate.
    pub link: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SharedThreadView {
    #[serde(flatten)]
    pub entry: SharedThreadEntry,
    pub status: SyncStatus,
    /// The local chat session that was shared as this thread, if it was shared
    /// from this machine — read from the thread-metadata link, so it survives
    /// a restart.
    pub session_id: Option<String>,
    /// On a share: the files that became canonical changes, and the ones held
    /// back because they look like secrets. Empty otherwise.
    pub shared_files: Vec<String>,
    pub blocked_files: Vec<BlockedFile>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockedFile {
    pub path: String,
    /// `name`, or the secret categories its content matched.
    pub reason: String,
}

struct Running {
    entry: SharedThreadEntry,
    commands: mpsc::UnboundedSender<SyncCommand>,
    status: watch::Receiver<SyncStatus>,
}

/// A Run this machine is running, keyed by the agent session behind it.
struct LiveRun {
    shared_thread_id: String,
    run_id: String,
    agent_id: AgentId,
}

impl LiveRun {
    /// The `shared_run_ended` delta for this Run: merged, ended or
    /// interrupted, with what the merge did or why it did not.
    fn ended(&self, status: &str, result: std::result::Result<RunReport, String>) -> SessionDelta {
        let (files, version, error) = match result {
            Ok(report) => (
                report.files,
                report.version,
                (!report.unuploaded.is_empty()).then(|| {
                    format!(
                        "Merged, but these files' Version copies did not upload: {}",
                        report.unuploaded.join(", ")
                    )
                }),
            ),
            Err(error) => (Vec::new(), None, Some(error)),
        };
        SessionDelta::SharedRunEnded {
            shared_thread_id: self.shared_thread_id.clone(),
            run_id: self.run_id.clone(),
            status: status.into(),
            files,
            version,
            error,
        }
    }
}

#[derive(Default)]
pub struct SharedThreadsState {
    running: Mutex<HashMap<String, Running>>,
    runs: Mutex<HashMap<String, LiveRun>>,
}

/// The delta sink, so a Run's start and end can be announced on the session
/// it belongs to — through the whole pipeline, capture included.
pub struct RunDeltaSink(pub Arc<dyn DeltaSink>);

/// One live Run frame from somebody else's Run, for the renderer.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct RunFrameEvent {
    shared_thread_id: String,
    run_no: u64,
    /// The `SessionDelta` the Runner's agent emitted, as JSON.
    delta: serde_json::Value,
}

/// An error the renderer can branch on: `code` is the server's where there is
/// one (`feature_disabled`, `workspace_local`, `limit_reached`, …), else ours.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SharedThreadError {
    pub code: String,
    pub message: String,
}

impl SharedThreadError {
    fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
        }
    }
}

type Result<T> = std::result::Result<T, SharedThreadError>;

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Share the thread behind `session_id` (an ACP session id) as a Shared
/// Thread. The project's checked-out commit becomes the Base and its
/// uncommitted work the first canonical changes; the repository itself is
/// never uploaded. Refused for a Local-mode project with `workspace_local`, so
/// the renderer can offer promotion.
#[tauri::command]
pub async fn shared_thread_share(
    app: AppHandle,
    host: State<'_, Arc<AgentHost>>,
    session_id: String,
    project_path: String,
    title: String,
) -> Result<SharedThreadView> {
    let org_id = active_org(&app)?;
    let workspace_id = cloud_workspace(&project_path, &org_id).await?;
    let base = {
        let path = PathBuf::from(&project_path);
        tauri::async_runtime::spawn_blocking(move || atlas_thread_sync::git::head_commit(&path))
            .await
            .map_err(|e| SharedThreadError::new("internal", e.to_string()))?
            .map_err(|e| {
                SharedThreadError::new(
                    "no_commit",
                    format!("This project has no commit to share from: {e}"),
                )
            })?
    };

    let token = token(&app).await?;
    let created: ServerThread = post_json(
        &format!("{}/threads", atlas_artifacts::ingest_base()),
        &token,
        &serde_json::json!({
            "orgId": org_id,
            "workspaceId": workspace_id,
            "title": title.trim(),
            "baseCommit": base,
        }),
    )
    .await?;

    let entry = SharedThreadEntry {
        link: share_link(&created.thread.id, &org_id, &workspace_id),
        shared_thread_id: created.thread.id.clone(),
        org_id,
        workspace_id,
        title: created.thread.title.clone(),
        base: created.thread.base_commit.clone(),
        role: created.role.clone().unwrap_or_else(|| "owner".into()),
        project_path: project_path.clone(),
        client_id: new_client_id(),
    };

    // Link the local thread before anything can fail on the network, so a
    // share that later loses its socket is still visibly a shared thread.
    if let Some(recorder) = host.history() {
        let store = recorder.store();
        if let Some(thread) = store.thread_for_session(&acp::SessionId::new(session_id.as_str())) {
            store.set_shared_thread(
                thread.thread_id,
                Some(SharedThreadLink {
                    shared_thread_id: entry.shared_thread_id.clone(),
                    base: entry.base.clone(),
                    role: entry.role.clone(),
                }),
            );
        }
    }

    let view = start(&app, entry, Some(PathBuf::from(project_path))).await?;
    remember(&app, &view.entry)?;
    Ok(view)
}

/// Join a Shared Thread from the link a teammate sent. The thread's Workspace
/// must be a project this machine has bound to Cloud, and its Base commit must
/// already be in that repository; nothing is checked out until
/// [`shared_thread_open`].
#[tauri::command]
pub async fn shared_thread_join(
    app: AppHandle,
    host: State<'_, Arc<AgentHost>>,
    link: String,
    project_path: Option<String>,
) -> Result<SharedThreadView> {
    let parsed = parse_link(&link)
        .ok_or_else(|| SharedThreadError::new("bad_link", "That is not a Shared Thread link."))?;
    if let Some(view) = view_of(&app, &parsed.thread) {
        return Ok(view);
    }
    let candidates = match project_path {
        Some(path) => vec![path],
        None => known_projects(&host),
    };
    let project_path = find_project(candidates, &parsed.org, &parsed.workspace)
        .await
        .ok_or_else(|| {
            SharedThreadError::new(
                "project_not_found",
                "Open the project this thread belongs to (connected to Atlas Cloud) and try again.",
            )
        })?;

    let token = token(&app).await?;
    let thread: ServerThread = get_json(
        &format!(
            "{}/threads/{}?org={}&workspace={}",
            atlas_artifacts::ingest_base(),
            parsed.thread,
            parsed.org,
            parsed.workspace
        ),
        &token,
    )
    .await?;

    let entry = SharedThreadEntry {
        link: share_link(&parsed.thread, &parsed.org, &parsed.workspace),
        shared_thread_id: parsed.thread,
        org_id: parsed.org,
        workspace_id: parsed.workspace,
        title: thread.thread.title,
        base: thread.thread.base_commit,
        role: thread.role.unwrap_or_else(|| "participant".into()),
        project_path,
        client_id: new_client_id(),
    };
    let view = start(&app, entry, None).await?;
    remember(&app, &view.entry)?;
    Ok(view)
}

/// Check the replica out, if it is not already, and answer its path. Called
/// the first time the person opens one of the thread's files or prompts in it.
#[tauri::command]
pub async fn shared_thread_open(app: AppHandle, shared_thread_id: String) -> Result<String> {
    let commands = commands_for(&app, &shared_thread_id)?;
    let (reply, answer) = oneshot::channel();
    commands
        .send(SyncCommand::Materialize(reply))
        .map_err(|_| disconnected())?;
    let root = answer
        .await
        .map_err(|_| disconnected())?
        .map_err(|e| SharedThreadError::new("checkout_failed", e))?;
    Ok(root.to_string_lossy().into_owned())
}

/// The thread's Run worktree on this machine, created if needed and holding
/// canonical state now. Open the agent session for running in the thread
/// here: every prompt sent in it is then a Run (ATL-405).
#[tauri::command]
pub async fn shared_thread_run_worktree(
    app: AppHandle,
    shared_thread_id: String,
) -> Result<String> {
    let commands = commands_for(&app, &shared_thread_id)?;
    let worktree = run_root(&app, &shared_thread_id)?;
    let (reply, answer) = oneshot::channel();
    commands
        .send(SyncCommand::PrepareRun { worktree, reply })
        .map_err(|_| disconnected())?;
    let root = answer
        .await
        .map_err(|_| disconnected())?
        .map_err(|e| SharedThreadError::new("checkout_failed", e))?;
    Ok(root.to_string_lossy().into_owned())
}

/// Every Shared Thread this machine has joined, with its live status.
#[tauri::command]
pub fn shared_thread_list(app: AppHandle) -> Vec<SharedThreadView> {
    let state = app.state::<SharedThreadsState>();
    let Ok(running) = state.running.lock() else {
        return Vec::new();
    };
    let mut views: Vec<SharedThreadView> = running
        .values()
        .map(|r| SharedThreadView {
            entry: r.entry.clone(),
            status: r.status.borrow().clone(),
            session_id: local_session(&app, &r.entry.shared_thread_id),
            shared_files: Vec::new(),
            blocked_files: Vec::new(),
        })
        .collect();
    views.sort_by(|a, b| a.entry.title.cmp(&b.entry.title));
    views
}

/// Stop syncing a thread on this machine and forget it. The replica worktree
/// is left on disk: it may hold work the person wants, and removing a worktree
/// is theirs to do.
#[tauri::command]
pub async fn shared_thread_leave(app: AppHandle, shared_thread_id: String) -> Result<()> {
    let removed = {
        let state = app.state::<SharedThreadsState>();
        let mut running = state.running.lock().map_err(|_| poisoned())?;
        running.remove(&shared_thread_id)
    };
    if let Some(running) = removed {
        let _ = running.commands.send(SyncCommand::Stop);
    }
    forget(&app, &shared_thread_id)?;
    emit_all(&app);
    Ok(())
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Manage the state and rejoin every thread this machine had joined.
///
/// Best effort, and patient: the account session is restored after this runs,
/// so the first attempts may have no token yet. A thread that still cannot
/// reconnect stays in the registry and is tried again at the next launch;
/// reconnecting mid-session is ATL-404's.
pub fn install(app: &AppHandle) {
    app.manage(SharedThreadsState::default());
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        let mut pending = registry(&app);
        for delay in [5, 30, 120] {
            if pending.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
            let mut failed = Vec::new();
            for entry in pending {
                let id = entry.shared_thread_id.clone();
                if let Err(e) = start(&app, entry.clone(), None).await {
                    tracing::warn!(target: "atlas_thread_sync", "rejoin {id}: {}", e.message);
                    failed.push(entry);
                }
            }
            pending = failed;
        }
    });
}

/// Connect, catch up, optionally share the sharer's working changes, and spawn
/// the loop that keeps the replica in step.
async fn start(
    app: &AppHandle,
    entry: SharedThreadEntry,
    share_from: Option<PathBuf>,
) -> Result<SharedThreadView> {
    let token = token(app).await?;
    let url = atlas_thread_sync::transport::thread_socket_url(
        &atlas_artifacts::ingest_base(),
        &entry.org_id,
        &entry.workspace_id,
        &entry.shared_thread_id,
    );
    let transport = WsTransport::connect(&url, &token).await.map_err(|e| {
        SharedThreadError::new("disconnected", format!("Could not reach the thread: {e}"))
    })?;

    let root = replica_root(app, &entry.shared_thread_id)?;
    let replica = Replica::new(Path::new(&entry.project_path), &entry.base, &root).map_err(|e| match e {
        atlas_thread_sync::ReplicaError::BaseMissing(_) => SharedThreadError::new(
            "base_missing",
            "This project does not have the commit the thread starts from yet. Pull or fetch it, then join again.",
        ),
        other => SharedThreadError::new("replica_failed", other.to_string()),
    })?;
    let mut session = ThreadSession::open(transport, replica, &entry.client_id)
        .await
        .map_err(session_error)?;
    let mut shared_files = Vec::new();
    let mut blocked_files = Vec::new();
    if let Some(checkout) = share_from {
        let report = session
            .share_working_changes(&checkout)
            .await
            .map_err(session_error)?;
        shared_files = report.shared;
        blocked_files = report
            .blocked
            .into_iter()
            .map(|(path, reason)| BlockedFile {
                path,
                reason: match reason {
                    atlas_thread_sync::SecretReason::Name => "name".into(),
                    atlas_thread_sync::SecretReason::Content(kinds) => kinds.join(", "),
                },
            })
            .collect();
    }

    let (commands, rx) = mpsc::unbounded_channel();
    let (status_tx, status) = watch::channel(SyncStatus::default());
    let mut updates = status.clone();
    let (events, mut heard) = mpsc::unbounded_channel();
    session.set_events(events);
    let blobs = HttpBlobs {
        org_id: entry.org_id.clone(),
        workspace_id: entry.workspace_id.clone(),
        shared_thread_id: entry.shared_thread_id.clone(),
        app: app.clone(),
    };
    tauri::async_runtime::spawn(atlas_thread_sync::run(session, rx, status_tx, blobs));
    {
        let forward = app.clone();
        let shared_thread_id = entry.shared_thread_id.clone();
        tauri::async_runtime::spawn(async move {
            while let Some(ThreadEvent::RunFrame {
                run_no, payload, ..
            }) = heard.recv().await
            {
                // A frame that is not JSON is somebody else's client's
                // business; the renderer only draws deltas.
                let Ok(delta) = serde_json::from_slice(&payload) else {
                    continue;
                };
                let _ = forward.emit(
                    SHARED_RUN_FRAME_EVENT,
                    RunFrameEvent {
                        shared_thread_id: shared_thread_id.clone(),
                        run_no,
                        delta,
                    },
                );
            }
        });
    }
    {
        let forward = app.clone();
        tauri::async_runtime::spawn(async move {
            while updates.changed().await.is_ok() {
                emit_all(&forward);
            }
            emit_all(&forward);
        });
    }

    let view = SharedThreadView {
        entry: entry.clone(),
        status: status.borrow().clone(),
        session_id: local_session(app, &entry.shared_thread_id),
        shared_files,
        blocked_files,
    };
    {
        let state = app.state::<SharedThreadsState>();
        let mut running = state.running.lock().map_err(|_| poisoned())?;
        running.insert(
            entry.shared_thread_id.clone(),
            Running {
                entry,
                commands,
                status,
            },
        );
    }
    emit_all(app);
    Ok(view)
}

fn commands_for(
    app: &AppHandle,
    shared_thread_id: &str,
) -> Result<mpsc::UnboundedSender<SyncCommand>> {
    let state = app.state::<SharedThreadsState>();
    let running = state.running.lock().map_err(|_| poisoned())?;
    running
        .get(shared_thread_id)
        .map(|r| r.commands.clone())
        .ok_or_else(|| {
            SharedThreadError::new("not_joined", "This thread is not joined on this machine.")
        })
}

fn disconnected() -> SharedThreadError {
    SharedThreadError::new("disconnected", "The thread's connection has closed.")
}

// ---------------------------------------------------------------------------
// Runs (ATL-405)
// ---------------------------------------------------------------------------

/// Called by `agents_send` before a prompt reaches the agent. When the
/// session works in a joined thread's Run worktree, the prompt is a Run: the
/// worktree is reset to canonical state at the fork and the thread is told.
/// Any other session is left alone. A refusal (the concurrent-Runs limit, a
/// viewer, a closed thread) fails the send with the server's words, so the
/// agent never works on a fork nobody will merge.
pub async fn begin_run(
    app: &AppHandle,
    agent_id: &AgentId,
    session_id: &str,
    cwd: &str,
    agent: &str,
    model: Option<&str>,
) -> std::result::Result<(), String> {
    let Some(shared_thread_id) = thread_for_run_worktree(app, cwd) else {
        return Ok(());
    };
    {
        let state = app.state::<SharedThreadsState>();
        let runs = state.runs.lock().map_err(|_| poisoned().message)?;
        if runs.contains_key(session_id) {
            // A follow-up sent mid-turn joins the Run already in flight.
            return Ok(());
        }
        // The Run worktree is one per thread on this machine: a second agent
        // session starting a Run would reset it under the first one's agent.
        if runs
            .values()
            .any(|r| r.shared_thread_id == shared_thread_id)
        {
            return Err("Another agent on this machine is already running in this thread. Wait for its turn to end, then send again.".into());
        }
    }
    let commands = commands_for(app, &shared_thread_id).map_err(|e| e.message)?;
    let (reply, answer) = oneshot::channel();
    commands
        .send(SyncCommand::StartRun {
            worktree: PathBuf::from(cwd),
            spec: RunSpec {
                run_id: RunSpec::new_id(),
                agent: agent.to_string(),
                model: model.unwrap_or("default").to_string(),
                context_anchor: None,
            },
            reply,
        })
        .map_err(|_| disconnected().message)?;
    let started = answer
        .await
        .map_err(|_| disconnected().message)?
        .map_err(|e| format!("This Shared Thread refused the Run: {e}"))?;
    tag_session(app, session_id, &shared_thread_id);
    {
        let state = app.state::<SharedThreadsState>();
        let mut runs = state.runs.lock().map_err(|_| poisoned().message)?;
        runs.insert(
            session_id.to_string(),
            LiveRun {
                shared_thread_id: shared_thread_id.clone(),
                run_id: started.run_id.clone(),
                agent_id: *agent_id,
            },
        );
    }
    emit_delta(
        app,
        agent_id,
        session_id,
        SessionDelta::SharedRunStarted {
            shared_thread_id,
            run_id: started.run_id,
            run_no: started.run_no,
        },
    );
    Ok(())
}

/// The joined thread whose Run worktree is `cwd`, if any.
fn thread_for_run_worktree(app: &AppHandle, cwd: &str) -> Option<String> {
    let state = app.state::<SharedThreadsState>();
    let running = state.running.lock().ok()?;
    let cwd = Path::new(cwd);
    running
        .keys()
        .find(|id| run_root(app, id).is_ok_and(|root| root == cwd))
        .cloned()
}

/// Link the Runner's local thread to the Shared Thread, so its Session is
/// tagged with the thread id on the normal drain.
fn tag_session(app: &AppHandle, session_id: &str, shared_thread_id: &str) {
    let Some(host) = app.try_state::<Arc<AgentHost>>() else {
        return;
    };
    let Some(recorder) = host.history() else {
        return;
    };
    let store = recorder.store();
    let Some(thread) = store.thread_for_session(&acp::SessionId::new(session_id)) else {
        return;
    };
    if thread
        .shared
        .as_ref()
        .is_some_and(|s| s.shared_thread_id == shared_thread_id)
    {
        return;
    }
    let entry = {
        let state = app.state::<SharedThreadsState>();
        let running = state.running.lock().ok();
        running.and_then(|r| r.get(shared_thread_id).map(|r| r.entry.clone()))
    };
    if let Some(entry) = entry {
        store.set_shared_thread(
            thread.thread_id,
            Some(SharedThreadLink {
                shared_thread_id: entry.shared_thread_id,
                base: entry.base,
                role: entry.role,
            }),
        );
    }
}

fn emit_delta(app: &AppHandle, agent_id: &AgentId, session_id: &str, delta: SessionDelta) {
    if let Some(sink) = app.try_state::<RunDeltaSink>() {
        sink.0.emit(SessionDeltaEnvelope {
            agent_id: *agent_id,
            session_id: session_id.to_string(),
            delta,
        });
    }
}

/// Streams a Run's deltas to its thread, and merges the Run when its turn
/// ends. A pipeline stage rather than a bus subscriber, like capture: the bus
/// drops events for a slow subscriber, and the turn's end must never be
/// missed. It only hands frames to the thread's loop, which never blocks here.
pub struct SharedRunMiddleware {
    pub app: AppHandle,
}

impl OutboundMiddleware<SessionDeltaEnvelope> for SharedRunMiddleware {
    fn on_event(&self, envelope: &SessionDeltaEnvelope) {
        if matches!(
            envelope.delta,
            SessionDelta::SharedRunStarted { .. } | SessionDelta::SharedRunEnded { .. }
        ) {
            return;
        }
        let state = self.app.state::<SharedThreadsState>();
        let Ok(mut runs) = state.runs.lock() else {
            return;
        };
        let Some(run) = runs.get(&envelope.session_id) else {
            return;
        };
        let Ok(commands) = commands_for(&self.app, &run.shared_thread_id) else {
            runs.remove(&envelope.session_id);
            return;
        };
        if let Ok(payload) = serde_json::to_vec(&envelope.delta) {
            let _ = commands.send(SyncCommand::RunFrame {
                run_id: run.run_id.clone(),
                payload,
            });
        }
        let interrupted = match &envelope.delta {
            SessionDelta::TurnFinished { .. } => false,
            SessionDelta::TurnFailed { .. } | SessionDelta::AgentDisconnected { .. } => true,
            _ => return,
        };
        let Some(run) = runs.remove(&envelope.session_id) else {
            return;
        };
        drop(runs);
        let app = self.app.clone();
        let session_id = envelope.session_id.clone();
        tauri::async_runtime::spawn(async move {
            // Tagging again at the turn's end: the local thread may not have
            // existed yet when the Run began, on a session's first prompt.
            tag_session(&app, &session_id, &run.shared_thread_id);
            let ended = if interrupted {
                let _ = commands.send(SyncCommand::InterruptRun {
                    run_id: run.run_id.clone(),
                });
                run.ended("interrupted", Ok(RunReport::default()))
            } else {
                let (reply, answer) = oneshot::channel();
                let _ = commands.send(SyncCommand::FinishRun {
                    run_id: run.run_id.clone(),
                    reply: Some(reply),
                });
                let result = answer
                    .await
                    .unwrap_or_else(|_| Err("the thread's connection closed".into()));
                let status = match &result {
                    Ok(report) if report.version.is_some() => "merged",
                    _ => "ended",
                };
                run.ended(status, result)
            };
            emit_delta(&app, &run.agent_id, &session_id, ended);
        });
    }
}

/// The thread's blob door, for a Run's Thread Version content.
struct HttpBlobs {
    app: AppHandle,
    org_id: String,
    workspace_id: String,
    shared_thread_id: String,
}

impl BlobSink for HttpBlobs {
    async fn put(&self, sha256: &str, bytes: Vec<u8>) -> std::result::Result<(), String> {
        let token = token(&self.app).await.map_err(|e| e.message)?;
        let url = format!(
            "{}/threads/{}/blobs/{sha256}?org={}&workspace={}",
            atlas_artifacts::ingest_base(),
            self.shared_thread_id,
            self.org_id,
            self.workspace_id,
        );
        let res = client()
            .map_err(|e| e.message)?
            .put(url)
            .bearer_auth(token)
            .header("content-type", "application/octet-stream")
            .body(bytes)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if res.status().is_success() {
            Ok(())
        } else {
            Err(format!("blob upload answered {}", res.status()))
        }
    }
}

fn view_of(app: &AppHandle, shared_thread_id: &str) -> Option<SharedThreadView> {
    let state = app.state::<SharedThreadsState>();
    let running = state.running.lock().ok()?;
    running.get(shared_thread_id).map(|r| SharedThreadView {
        entry: r.entry.clone(),
        status: r.status.borrow().clone(),
        session_id: local_session(app, shared_thread_id),
        shared_files: Vec::new(),
        blocked_files: Vec::new(),
    })
}

/// The ACP session of the local thread linked to `shared_thread_id`, if any.
fn local_session(app: &AppHandle, shared_thread_id: &str) -> Option<String> {
    let host = app.try_state::<Arc<AgentHost>>()?;
    let recorder = host.history()?;
    recorder
        .store()
        .threads()
        .into_iter()
        .find(|t| {
            t.shared
                .as_ref()
                .is_some_and(|s| s.shared_thread_id == shared_thread_id)
        })
        .and_then(|t| t.session_id.map(|id| id.to_string()))
}

fn emit_all(app: &AppHandle) {
    let _ = app.emit(SHARED_THREADS_EVENT, shared_thread_list(app.clone()));
}

fn session_error(e: atlas_thread_sync::SessionError) -> SharedThreadError {
    match e {
        atlas_thread_sync::SessionError::Refused { code, message } => {
            SharedThreadError { code, message }
        }
        other => SharedThreadError::new("sync_failed", other.to_string()),
    }
}

fn poisoned() -> SharedThreadError {
    SharedThreadError::new("internal", "shared-thread state is unavailable")
}

// ---------------------------------------------------------------------------
// Links, projects, tokens
// ---------------------------------------------------------------------------

struct ParsedLink {
    thread: String,
    org: String,
    workspace: String,
}

/// The web app's origin, for links. `ATLAS_APP_URL` overrides, as the other
/// bases do.
fn app_base() -> String {
    std::env::var("ATLAS_APP_URL").unwrap_or_else(|_| "https://app.tryatlas.cc".into())
}

fn share_link(thread: &str, org: &str, workspace: &str) -> String {
    format!(
        "{}/threads/{thread}?org={org}&workspace={workspace}",
        app_base().trim_end_matches('/')
    )
}

/// `…/threads/{id}?org=…&workspace=…`, from any host — the web origin today, a
/// self-hosted one tomorrow. Ids are restricted to what the server mints.
fn parse_link(link: &str) -> Option<ParsedLink> {
    let link = link.trim();
    let (path, query) = link.split_once('?')?;
    let thread = path
        .trim_end_matches('/')
        .rsplit_once("/threads/")?
        .1
        .to_string();
    let mut org = None;
    let mut workspace = None;
    for pair in query.split('&') {
        match pair.split_once('=') {
            Some(("org", v)) => org = Some(v.to_string()),
            Some(("workspace", v)) => workspace = Some(v.to_string()),
            _ => {}
        }
    }
    let ok = |s: &str| {
        !s.is_empty()
            && s.len() <= 128
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    };
    let (org, workspace) = (org?, workspace?);
    (ok(&thread) && ok(&org) && ok(&workspace)).then_some(ParsedLink {
        thread,
        org,
        workspace,
    })
}

fn active_org(app: &AppHandle) -> Result<String> {
    app.try_state::<crate::commands::artifacts_cloud::ArtifactsCloudState>()
        .and_then(|state| state.org_id())
        .ok_or_else(|| {
            SharedThreadError::new(
                "signed_out",
                "Sign in to Atlas and pick an organization to share threads.",
            )
        })
}

/// The project's cloud binding, if it is bound to Cloud in `org_id`.
fn cloud_binding(project_path: &str, org_id: &str) -> Option<atlas_checkpoint::Binding> {
    crate::commands::capture::open_reader(project_path)
        .ok()
        .flatten()
        .and_then(|store| store.binding().ok().flatten())
        .filter(|b| crate::commands::artifacts_cloud::is_cloud_bound(b, org_id))
}

/// The project's server Workspace id, if it is bound to Cloud in `org_id`.
async fn cloud_workspace(project_path: &str, org_id: &str) -> Result<String> {
    let path = project_path.to_string();
    let org = org_id.to_string();
    let binding = tauri::async_runtime::spawn_blocking(move || cloud_binding(&path, &org))
        .await
        .map_err(|e| SharedThreadError::new("internal", e.to_string()))?;
    match binding {
        Some(b) => b.remote_workspace_id.ok_or_else(|| {
            SharedThreadError::new("workspace_local", "This project is not connected to Atlas Cloud yet.")
        }),
        None => Err(SharedThreadError::new(
            "workspace_local",
            "This project is in Local mode, so nothing from it leaves your machine. Promote it to Cloud mode to share a thread.",
        )),
    }
}

/// The projects this machine has threads in, as candidates for a join.
fn known_projects(host: &AgentHost) -> Vec<String> {
    let Some(recorder) = host.history() else {
        return Vec::new();
    };
    let mut paths: Vec<String> = recorder
        .store()
        .projects()
        .into_iter()
        .flat_map(|p| {
            p.paths
                .ordered_paths()
                .map(|p| p.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
        })
        .collect();
    paths.sort();
    paths.dedup();
    paths
}

async fn find_project(candidates: Vec<String>, org: &str, workspace: &str) -> Option<String> {
    let org = org.to_string();
    let workspace = workspace.to_string();
    tauri::async_runtime::spawn_blocking(move || {
        candidates.into_iter().find(|path| {
            cloud_binding(path, &org)
                .is_some_and(|b| b.remote_workspace_id.as_deref() == Some(workspace.as_str()))
        })
    })
    .await
    .ok()
    .flatten()
}

async fn token(app: &AppHandle) -> Result<String> {
    let state = app
        .try_state::<crate::commands::auth::AuthState>()
        .ok_or_else(|| SharedThreadError::new("signed_out", "Sign in to Atlas first."))?;
    state.core().mint_access_token().await.map_err(|e| {
        SharedThreadError::new(
            "signed_out",
            format!("Could not get an access token: {e:?}"),
        )
    })
}

fn new_client_id() -> String {
    format!("desktop-{}", uuid::Uuid::new_v4().simple())
}

// ---------------------------------------------------------------------------
// The server's answers
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServerThread {
    thread: ServerThreadSummary,
    role: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServerThreadSummary {
    id: String,
    title: String,
    base_commit: String,
}

#[derive(Deserialize)]
struct ServerError {
    error: ServerErrorBody,
}

#[derive(Deserialize)]
struct ServerErrorBody {
    code: String,
    message: String,
}

async fn decode<T: serde::de::DeserializeOwned>(res: reqwest::Response) -> Result<T> {
    let status = res.status();
    let body = res
        .bytes()
        .await
        .map_err(|e| SharedThreadError::new("network", e.to_string()))?;
    if status.is_success() {
        return serde_json::from_slice(&body)
            .map_err(|e| SharedThreadError::new("bad_response", e.to_string()));
    }
    Err(match serde_json::from_slice::<ServerError>(&body) {
        Ok(e) => SharedThreadError {
            code: e.error.code,
            message: e.error.message,
        },
        Err(_) => SharedThreadError::new("http", format!("The server answered {status}.")),
    })
}

fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .map_err(|e| SharedThreadError::new("internal", e.to_string()))
}

async fn post_json<T: serde::de::DeserializeOwned>(
    url: &str,
    token: &str,
    body: &serde_json::Value,
) -> Result<T> {
    let res = client()?
        .post(url)
        .bearer_auth(token)
        .json(body)
        .send()
        .await
        .map_err(|e| SharedThreadError::new("network", e.to_string()))?;
    decode(res).await
}

async fn get_json<T: serde::de::DeserializeOwned>(url: &str, token: &str) -> Result<T> {
    let res = client()?
        .get(url)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| SharedThreadError::new("network", e.to_string()))?;
    decode(res).await
}

// ---------------------------------------------------------------------------
// The registry and replica paths
// ---------------------------------------------------------------------------

fn registry_path(app: &AppHandle) -> Option<PathBuf> {
    app.path()
        .app_config_dir()
        .ok()
        .map(|dir| dir.join("shared-threads.json"))
}

fn registry(app: &AppHandle) -> Vec<SharedThreadEntry> {
    registry_path(app)
        .and_then(|path| std::fs::read(path).ok())
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

fn write_registry(app: &AppHandle, entries: &[SharedThreadEntry]) -> Result<()> {
    let path = registry_path(app)
        .ok_or_else(|| SharedThreadError::new("internal", "no config directory"))?;
    let bytes = serde_json::to_vec_pretty(entries)
        .map_err(|e| SharedThreadError::new("internal", e.to_string()))?;
    atlas_thread_sync::replica::write_atomic(&path, &bytes)
        .map_err(|e| SharedThreadError::new("internal", e.to_string()))
}

fn remember(app: &AppHandle, entry: &SharedThreadEntry) -> Result<()> {
    let mut entries = registry(app);
    entries.retain(|e| e.shared_thread_id != entry.shared_thread_id);
    entries.push(entry.clone());
    write_registry(app, &entries)
}

fn forget(app: &AppHandle, shared_thread_id: &str) -> Result<()> {
    let mut entries = registry(app);
    entries.retain(|e| e.shared_thread_id != shared_thread_id);
    write_registry(app, &entries)
}

fn replica_root(app: &AppHandle, shared_thread_id: &str) -> Result<PathBuf> {
    Ok(thread_dir(app, shared_thread_id)?.join("replica"))
}

/// The thread's Run worktree on this machine (ATL-405).
fn run_root(app: &AppHandle, shared_thread_id: &str) -> Result<PathBuf> {
    Ok(thread_dir(app, shared_thread_id)?.join("run"))
}

fn thread_dir(app: &AppHandle, shared_thread_id: &str) -> Result<PathBuf> {
    // The id came from the server or a link; it is a path segment here, so it
    // is held to the server's own alphabet before it touches the filesystem.
    if shared_thread_id.is_empty()
        || !shared_thread_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(SharedThreadError::new(
            "bad_link",
            "That is not a Shared Thread id.",
        ));
    }
    let data = app
        .path()
        .app_data_dir()
        .map_err(|e| SharedThreadError::new("internal", e.to_string()))?;
    Ok(data.join("shared-threads").join(shared_thread_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_share_link_and_refuses_anything_else() {
        let p = parse_link("https://app.tryatlas.cc/threads/01JTHREAD?org=org_1&workspace=ws-9")
            .unwrap();
        assert_eq!(
            (p.thread.as_str(), p.org.as_str(), p.workspace.as_str()),
            ("01JTHREAD", "org_1", "ws-9")
        );
        assert!(parse_link("https://app.tryatlas.cc/threads/../../x?org=o&workspace=w").is_none());
        assert!(parse_link("https://app.tryatlas.cc/threads/abc?org=o").is_none());
        assert!(parse_link("not a link").is_none());
    }

    #[test]
    fn a_link_round_trips() {
        let link = share_link("T1", "org_1", "ws_1");
        let p = parse_link(&link).unwrap();
        assert_eq!(
            (p.thread, p.org, p.workspace),
            ("T1".into(), "org_1".into(), "ws_1".into())
        );
    }
}
