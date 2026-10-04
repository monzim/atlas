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
//! * Each joined thread's bare **thread repository** (ATL-402) is
//!   `<app data>/shared-threads/<id>/repo`: where a Base bundle is fetched when
//!   this machine lacks the Base, and built when somebody else does. It
//!   borrows the person's objects and never writes to their repository.
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
use atlas_thread_sync::store::{StoreError, StoreFuture};
use atlas_thread_sync::{
    Command as SyncCommand, Connector, ObjectStore, Replica, RunReport, RunSpec, SharePreview,
    SyncStatus, ThreadEvent, ThreadRepo, ThreadSession, TransportError, WsTransport,
};
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::sync::{mpsc, oneshot, watch};

use crate::commands::agent_host::AgentHost;

/// The window event channel for joined-thread status.
pub const SHARED_THREADS_EVENT: &str = "atlas:shared-threads";

/// The window event channel for other people's live Run frames.
pub const SHARED_RUN_FRAME_EVENT: &str = "atlas:shared-run-frame";

/// The window event channel for join requests, heard by the owner (ATL-406).
pub const SHARED_JOIN_REQUEST_EVENT: &str = "atlas:shared-thread-join-requested";

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
    /// The person's own checkout of the project, when they have one. Only
    /// ever read. `None` for somebody who joined with no copy of the
    /// repository (ATL-402).
    #[serde(default)]
    pub project_path: Option<String>,
    /// This replica's stable id on the wire, kept across reconnects so the
    /// server can say which of its frames it already stored.
    pub client_id: String,
    /// Whether this machine sends the repository's history — everything
    /// behind the Base — to teammates who lack it (ATL-402). Off until the
    /// person agrees, in the share dialog or on the thread's card.
    #[serde(default)]
    pub serve_history: bool,
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
    /// Where this thread's Run worktree is on this machine: an agent session
    /// working there runs in the thread (ATL-405), and its prompt box follows
    /// the person's role (ATL-406).
    pub run_worktree: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockedFile {
    pub path: String,
    /// `name`, or the secret categories its content matched.
    pub reason: String,
}

/// One row of the share dialog (ATL-402).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ShareFileView {
    pub path: String,
    pub kind: atlas_thread_sync::ShareKind,
    pub bytes: u64,
    pub deleted: bool,
    /// Why it is held back unless included anyway: `name`, or the secret
    /// categories its content matched. `None` when it uploads.
    pub blocked: Option<String>,
}

fn reason_text(reason: &atlas_thread_sync::SecretReason) -> String {
    match reason {
        atlas_thread_sync::SecretReason::Name => "name".into(),
        atlas_thread_sync::SecretReason::Content(kinds) => kinds.join(", "),
    }
}

fn preview_view(preview: &SharePreview) -> Vec<ShareFileView> {
    preview
        .files
        .iter()
        .map(|f| ShareFileView {
            path: f.path.clone(),
            kind: f.kind,
            bytes: f.bytes,
            deleted: f.deleted,
            blocked: f.blocked.as_ref().map(reason_text),
        })
        .collect()
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

/// Somebody asked to join, for the owner's panel (ATL-406).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct JoinRequestEvent {
    shared_thread_id: String,
    user_id: String,
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

/// What sharing `project_path` would upload — exactly the files the dialog
/// lists — and what it would hold back as secret-shaped (ATL-402). Only reads.
#[tauri::command]
pub async fn shared_thread_share_preview(project_path: String) -> Result<Vec<ShareFileView>> {
    let path = PathBuf::from(project_path);
    tauri::async_runtime::spawn_blocking(move || atlas_thread_sync::share::preview(&path))
        .await
        .map_err(|e| SharedThreadError::new("internal", e.to_string()))?
        .map(|preview| preview_view(&preview))
        .map_err(|e| SharedThreadError::new("preview_failed", e.to_string()))
}

/// Share the thread behind `session_id` (an ACP session id) as a Shared
/// Thread. The project's checked-out commit becomes the Base and its
/// uncommitted work the first canonical changes; the repository itself is
/// never uploaded. Files that look like secrets stay home unless named in
/// `include` — the dialog's per-file "include anyway". Refused for a
/// Local-mode project with `workspace_local`, so the renderer can offer
/// promotion.
#[tauri::command]
pub async fn shared_thread_share(
    app: AppHandle,
    host: State<'_, Arc<AgentHost>>,
    session_id: String,
    project_path: String,
    title: String,
    include: Option<Vec<String>>,
    serve_history: Option<bool>,
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
    // Over the plan's touched-files limit the share would be refused partway,
    // some files uploaded and the rest not: say so before anything goes.
    let include_now = include.clone().unwrap_or_default();
    let uploading = {
        let path = PathBuf::from(&project_path);
        tauri::async_runtime::spawn_blocking(move || {
            atlas_thread_sync::share::preview(&path)
                .map(|p| p.uploads(&include_now).count())
        })
        .await
        .map_err(|e| SharedThreadError::new("internal", e.to_string()))?
        .map_err(|e| SharedThreadError::new("preview_failed", e.to_string()))?
    };
    let features: ServerFeatures = get_json(
        &format!(
            "{}/threads/features?org={org_id}",
            atlas_artifacts::ingest_base()
        ),
        &token,
    )
    .await?;
    if let Some(limit) = features.limits.and_then(|l| l.touched_files) {
        if uploading as u64 > limit {
            return Err(SharedThreadError::new(
                "limit_reached",
                format!(
                    "This share would touch {uploading} files, more than your organisation's plan allows (touched files per thread: {limit}). Commit or set some aside, or add them to .atlas/shareignore."
                ),
            ));
        }
    }
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
        project_path: Some(project_path.clone()),
        client_id: new_client_id(),
        serve_history: serve_history.unwrap_or(false),
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

    let share = Share {
        checkout: PathBuf::from(project_path),
        include: include.unwrap_or_default(),
    };
    let view = start(&app, entry, Some(share)).await?;
    remember(&app, &view.entry)?;
    Ok(view)
}

/// Join a Shared Thread from the link a teammate sent. The thread's Workspace
/// should be a project this machine has bound to Cloud; without one — or when
/// that repository lacks the Base, because it was never pushed — the Base is
/// brought over as a bundle from a replica that has it (ATL-402), and the
/// person watches read-only if none can come. Nothing is checked out until
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
    // No local copy is fine: the Base comes as a bundle (ATL-402).
    let project_path = find_project(candidates, &parsed.org, &parsed.workspace).await;

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
        serve_history: false,
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

/// Send (or stop sending) this repository's history to teammates who lack the
/// thread's Base (ATL-402). A bundle is the whole history behind the Base, so
/// it is never sent without the person saying so; requests heard meanwhile
/// wait for it.
#[tauri::command]
pub async fn shared_thread_serve_history(
    app: AppHandle,
    shared_thread_id: String,
    on: bool,
) -> Result<()> {
    let commands = commands_for(&app, &shared_thread_id)?;
    commands
        .send(SyncCommand::ServeHistory(on))
        .map_err(|_| disconnected())?;
    let entry = {
        let state = app.state::<SharedThreadsState>();
        let mut running = state.running.lock().map_err(|_| poisoned())?;
        running.get_mut(&shared_thread_id).map(|r| {
            r.entry.serve_history = on;
            r.entry.clone()
        })
    };
    if let Some(entry) = entry {
        remember(&app, &entry)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The owner's panel (ATL-406)
// ---------------------------------------------------------------------------

/// What the owner manages: who is in and in what role, who is waiting, whether
/// joining needs approval, and whether the thread is open.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerView {
    pub join_policy: String,
    pub status: String,
    pub closed_at: Option<u64>,
    pub purge_at: Option<u64>,
    pub participants: Vec<Participant>,
    pub requests: Vec<JoinRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Participant {
    pub user_id: String,
    pub role: String,
    pub joined_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinRequest {
    pub user_id: String,
    pub requested_at: u64,
}

#[derive(Deserialize)]
struct ServerThreadState {
    thread: ServerThreadSettings,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServerThreadSettings {
    join_policy: String,
    status: String,
    closed_at: Option<u64>,
    purge_at: Option<u64>,
}

#[derive(Deserialize)]
struct ServerParticipants {
    participants: Vec<Participant>,
    requests: Vec<JoinRequest>,
}

fn entry_of(app: &AppHandle, shared_thread_id: &str) -> Result<SharedThreadEntry> {
    let state = app.state::<SharedThreadsState>();
    let running = state.running.lock().map_err(|_| poisoned())?;
    running
        .get(shared_thread_id)
        .map(|r| r.entry.clone())
        .ok_or_else(|| {
            SharedThreadError::new("not_joined", "This thread is not joined on this machine.")
        })
}

async fn owner_view_of(entry: &SharedThreadEntry, token: &str) -> Result<OwnerView> {
    let thread: ServerThreadState = get_json(&thread_url(entry, ""), token).await?;
    let people: ServerParticipants = get_json(&thread_url(entry, "/participants"), token).await?;
    Ok(OwnerView {
        join_policy: thread.thread.join_policy,
        status: thread.thread.status,
        closed_at: thread.thread.closed_at,
        purge_at: thread.thread.purge_at,
        participants: people.participants,
        requests: people.requests,
    })
}

/// A request to one of the thread's doors; the server's refusal, if any.
async fn send_to(request: reqwest::RequestBuilder, token: &str) -> Result<()> {
    let res = request
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| SharedThreadError::new("network", e.to_string()))?;
    if res.status() == reqwest::StatusCode::NO_CONTENT {
        return Ok(());
    }
    decode::<serde_json::Value>(res).await.map(|_| ())
}

/// The owner's view of a thread: participants, waiting requests, the join
/// policy and whether it is open.
#[tauri::command]
pub async fn shared_thread_owner_view(
    app: AppHandle,
    shared_thread_id: String,
) -> Result<OwnerView> {
    let entry = entry_of(&app, &shared_thread_id)?;
    owner_view_of(&entry, &token(&app).await?).await
}

/// Set somebody's role — `participant` approves a join request or promotes a
/// viewer; `viewer` takes edit rights away.
#[tauri::command]
pub async fn shared_thread_set_role(
    app: AppHandle,
    shared_thread_id: String,
    user_id: String,
    role: String,
) -> Result<OwnerView> {
    if role != "participant" && role != "viewer" {
        return Err(SharedThreadError::new(
            "bad_request",
            "A role is participant or viewer.",
        ));
    }
    let entry = entry_of(&app, &shared_thread_id)?;
    let token = token(&app).await?;
    let url = thread_url(&entry, &format!("/participants/{}", path_segment(&user_id)?));
    send_to(client()?.put(url).json(&serde_json::json!({ "role": role })), &token).await?;
    owner_view_of(&entry, &token).await
}

/// Decline a join request — the person stays a viewer.
#[tauri::command]
pub async fn shared_thread_decline(
    app: AppHandle,
    shared_thread_id: String,
    user_id: String,
) -> Result<OwnerView> {
    let entry = entry_of(&app, &shared_thread_id)?;
    let token = token(&app).await?;
    let url = thread_url(&entry, &format!("/participants/{}", path_segment(&user_id)?));
    send_to(client()?.delete(url), &token).await?;
    owner_view_of(&entry, &token).await
}

/// Turn "approval required" on (`approval`) or off (`auto`).
#[tauri::command]
pub async fn shared_thread_set_join_policy(
    app: AppHandle,
    shared_thread_id: String,
    join_policy: String,
) -> Result<OwnerView> {
    if join_policy != "auto" && join_policy != "approval" {
        return Err(SharedThreadError::new(
            "bad_request",
            "The join policy is auto or approval.",
        ));
    }
    let entry = entry_of(&app, &shared_thread_id)?;
    let token = token(&app).await?;
    let request = client()?
        .patch(thread_url(&entry, ""))
        .json(&serde_json::json!({ "joinPolicy": join_policy }));
    send_to(request, &token).await?;
    owner_view_of(&entry, &token).await
}

/// Close the thread — every replica turns read-only — or reopen it.
#[tauri::command]
pub async fn shared_thread_set_open(
    app: AppHandle,
    shared_thread_id: String,
    open: bool,
) -> Result<OwnerView> {
    let entry = entry_of(&app, &shared_thread_id)?;
    let token = token(&app).await?;
    let door = if open { "/reopen" } else { "/close" };
    send_to(client()?.post(thread_url(&entry, door)), &token).await?;
    owner_view_of(&entry, &token).await
}

/// A user id as one path segment. The server's ids never need escaping, and
/// anything that would is refused rather than sent.
fn path_segment(id: &str) -> Result<&str> {
    let ok = !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    if ok {
        Ok(id)
    } else {
        Err(SharedThreadError::new("bad_request", "That is not a user id."))
    }
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
            run_worktree: run_worktree_of(&app, &r.entry.shared_thread_id),
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
/// connect stays in the registry and is tried again at the next launch. Once
/// joined, a dropped socket is dialled again by the thread's own loop
/// (ATL-404).
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

/// A share's working tree, and the blocked files included anyway.
struct Share {
    checkout: PathBuf,
    include: Vec<String>,
}

/// Connect, catch up, bring the Base over if this machine lacks it, optionally
/// share the sharer's working changes, and spawn the loop that keeps the
/// replica in step.
async fn start(
    app: &AppHandle,
    entry: SharedThreadEntry,
    share: Option<Share>,
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
    let thread_repo = ThreadRepo::at(&thread_dir(app, &entry.shared_thread_id)?.join("repo"));
    let own = entry.project_path.as_deref().map(PathBuf::from);
    // The person's repository when it has the Base; else the thread
    // repository a bundle was fetched into before; else nothing yet.
    let replica = match own
        .as_deref()
        .filter(|repo| atlas_thread_sync::git::has_commit(repo, &entry.base))
    {
        Some(repo) => Replica::new(repo, &entry.base, &root),
        None if thread_repo.has(&entry.base) => {
            Replica::new(thread_repo.path(), &entry.base, &root)
        }
        None => Replica::without_base(&entry.base, &root),
    }
    .map_err(|e| SharedThreadError::new("replica_failed", e.to_string()))?;
    // With its object doors from the start: behind a compaction, catching up
    // means fetching snapshots before the thread is synced.
    let store = Arc::new(HttpStore {
        org_id: entry.org_id.clone(),
        workspace_id: entry.workspace_id.clone(),
        shared_thread_id: entry.shared_thread_id.clone(),
        app: app.clone(),
    });
    let mut session = ThreadSession::connect(transport, replica, &entry.client_id, store)
        .await
        .map_err(session_error)?;
    session.set_thread_repo(thread_repo);
    session.set_serve_bundles(entry.serve_history);
    // Under "approval required" a joiner waits as a viewer until the owner
    // says yes (ATL-406). Asking is idempotent: it answers the role as it is.
    if share.is_none() {
        match post_json::<ServerJoin>(&thread_url(&entry, "/join"), &token, &serde_json::json!({})).await {
            Ok(join) => session.set_awaiting_approval(join.pending),
            // Over the participant limit, or the like: the socket admitted
            // them as a viewer, and the status says so.
            Err(e) => tracing::info!(target: "atlas_thread_sync", "join: {}", e.message),
        }
    }
    // Without the Base the replica can only watch; a bundle fixes that. Not
    // getting one is not a failure — the status says why it is read-only.
    if !session.replica().has_base() {
        session.bootstrap(own.as_deref()).await.map_err(session_error)?;
    }
    let mut shared_files = Vec::new();
    let mut blocked_files = Vec::new();
    if let Some(share) = share {
        let report = session
            .share_working_changes(&share.checkout, &share.include)
            .await
            .map_err(session_error)?;
        shared_files = report.shared;
        blocked_files = report
            .blocked
            .into_iter()
            .map(|(path, reason)| BlockedFile {
                reason: reason_text(&reason),
                path,
            })
            .collect();
    }

    let (commands, rx) = mpsc::unbounded_channel();
    let (status_tx, status) = watch::channel(SyncStatus::default());
    let mut updates = status.clone();
    let (events, mut heard) = mpsc::unbounded_channel();
    session.set_events(events);
    let connector = WsConnector {
        app: app.clone(),
        url,
    };
    tauri::async_runtime::spawn(atlas_thread_sync::run_with(
        session, rx, status_tx, connector,
    ));
    {
        let forward = app.clone();
        let shared_thread_id = entry.shared_thread_id.clone();
        tauri::async_runtime::spawn(async move {
            while let Some(event) = heard.recv().await {
                match event {
                    ThreadEvent::RunFrame {
                        run_no, payload, ..
                    } => {
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
                    ThreadEvent::JoinRequested { user_id } => {
                        let _ = forward.emit(
                            SHARED_JOIN_REQUEST_EVENT,
                            JoinRequestEvent {
                                shared_thread_id: shared_thread_id.clone(),
                                user_id,
                            },
                        );
                    }
                }
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
        run_worktree: run_worktree_of(app, &entry.shared_thread_id),
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

/// Dials the thread socket again after a drop (ATL-404), with a token minted
/// for that dial — the one the thread was joined with may have expired.
struct WsConnector {
    app: AppHandle,
    url: String,
}

impl Connector for WsConnector {
    type Transport = WsTransport;

    async fn connect(&self) -> std::result::Result<WsTransport, TransportError> {
        let token = token(&self.app)
            .await
            .map_err(|e| TransportError::Ws(e.message))?;
        WsTransport::connect(&self.url, &token).await
    }
}

/// The thread's object doors over HTTPS: blobs (Base content, binary files,
/// Thread Version copies), Base bundles and compaction snapshots.
struct HttpStore {
    app: AppHandle,
    org_id: String,
    workspace_id: String,
    shared_thread_id: String,
}

/// Bundles are larger than anything else this module sends.
const OBJECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

impl HttpStore {
    fn url(&self, rest: &str, query: &str) -> String {
        format!(
            "{}/threads/{}/{rest}?org={}&workspace={}{query}",
            atlas_artifacts::ingest_base(),
            self.shared_thread_id,
            self.org_id,
            self.workspace_id,
        )
    }

    async fn put(&self, url: String, bytes: Vec<u8>) -> std::result::Result<(), StoreError> {
        let token = token(&self.app)
            .await
            .map_err(|e| StoreError::Failed(e.message))?;
        let res = object_client()?
            .put(url)
            .bearer_auth(token)
            .header("content-type", "application/octet-stream")
            .body(bytes)
            .send()
            .await
            .map_err(|e| StoreError::Failed(e.to_string()))?;
        store_answer(res).await.map(|_| ())
    }

    async fn get(&self, url: String) -> std::result::Result<Vec<u8>, StoreError> {
        let token = token(&self.app)
            .await
            .map_err(|e| StoreError::Failed(e.message))?;
        let res = object_client()?
            .get(url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| StoreError::Failed(e.to_string()))?;
        store_answer(res).await
    }
}

fn object_client() -> std::result::Result<reqwest::Client, StoreError> {
    reqwest::Client::builder()
        .timeout(OBJECT_TIMEOUT)
        .build()
        .map_err(|e| StoreError::Failed(e.to_string()))
}

/// A door's answer: the body, or the refusal it gave.
async fn store_answer(res: reqwest::Response) -> std::result::Result<Vec<u8>, StoreError> {
    let status = res.status();
    let body = res
        .bytes()
        .await
        .map_err(|e| StoreError::Failed(e.to_string()))?;
    if status.is_success() {
        return Ok(body.to_vec());
    }
    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(StoreError::NotFound);
    }
    Err(match serde_json::from_slice::<ServerError>(&body) {
        Ok(e) => StoreError::Refused {
            code: e.error.code,
            message: e.error.message,
        },
        Err(_) => StoreError::Failed(format!("the server answered {status}")),
    })
}

impl ObjectStore for HttpStore {
    fn put_blob(&self, sha256: String, bytes: Vec<u8>) -> StoreFuture<'_, ()> {
        Box::pin(self.put(self.url(&format!("blobs/{sha256}"), ""), bytes))
    }

    fn get_blob(&self, sha256: String) -> StoreFuture<'_, Vec<u8>> {
        Box::pin(self.get(self.url(&format!("blobs/{sha256}"), "")))
    }

    fn put_bundle(
        &self,
        sha256: String,
        bytes: Vec<u8>,
        prerequisites: Vec<String>,
    ) -> StoreFuture<'_, ()> {
        let query = if prerequisites.is_empty() {
            String::new()
        } else {
            format!("&prerequisites={}", prerequisites.join(","))
        };
        Box::pin(self.put(self.url(&format!("bundles/{sha256}"), &query), bytes))
    }

    fn get_bundle(&self, sha256: String) -> StoreFuture<'_, Vec<u8>> {
        Box::pin(self.get(self.url(&format!("bundles/{sha256}"), "")))
    }

    fn get_snapshot(&self, file_id: u64) -> StoreFuture<'_, Vec<u8>> {
        Box::pin(self.get(self.url(&format!("snapshots/{file_id}"), "")))
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
        run_worktree: run_worktree_of(app, shared_thread_id),
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
struct ServerFeatures {
    limits: Option<ServerLimits>,
}

#[derive(Deserialize)]
struct ServerLimits {
    touched_files: Option<u64>,
}

#[derive(Deserialize)]
struct ServerJoin {
    pending: bool,
}

/// `{ingest}/threads/{id}{rest}?org=&workspace=` for a joined thread.
fn thread_url(entry: &SharedThreadEntry, rest: &str) -> String {
    format!(
        "{}/threads/{}{rest}?org={}&workspace={}",
        atlas_artifacts::ingest_base(),
        entry.shared_thread_id,
        entry.org_id,
        entry.workspace_id
    )
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

fn run_worktree_of(app: &AppHandle, shared_thread_id: &str) -> String {
    run_root(app, shared_thread_id)
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default()
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
