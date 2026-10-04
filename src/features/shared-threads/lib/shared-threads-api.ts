import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";

/**
 * Shared Threads (ATL-395): one piece of work several members and agents carry
 * out together, with its file state held in the cloud. This module mirrors
 * `src-tauri/src/commands/shared_threads.rs`; only Rust holds the bearer.
 */

/** Pushed by Rust whenever a joined thread's status changes. */
export const SHARED_THREADS_EVENT = "atlas:shared-threads";

/** Pushed by Rust for every live frame of somebody else's Run (ATL-405). */
export const SHARED_RUN_FRAME_EVENT = "atlas:shared-run-frame";

/** Pushed by Rust to the owner when somebody asks to join (ATL-406). */
export const SHARED_JOIN_REQUEST_EVENT = "atlas:shared-thread-join-requested";

/** Pushed by Rust when who is here, or what they are doing, changes (ATL-407). */
export const SHARED_PRESENCE_EVENT = "atlas:shared-thread-presence";

/** Pushed by Rust when a file open in the Atlas editor changed (ATL-407). */
export const SHARED_DOC_UPDATE_EVENT = "atlas:shared-doc-update";

/** How far a replica is from the thread's head, as it says of itself. */
export type SyncState = "current" | "syncing" | "behind";

/** Somebody else on the thread (ATL-407): a desktop or the web view. */
export interface SharedPeer {
  peerId: string;
  userId: string;
  role: string;
  surface: "desktop" | "web" | (string & {});
  /** The file they are typing in. */
  typing: string | null;
  cursors: Array<{ fileId: number; path: string | null; anchor: number; head: number }>;
  /** Their Runs in flight and the file each is touching. */
  runs: Array<{ runId: string; path: string | null }>;
  sync: SyncState | null;
}

/** A replica file bound to the Atlas editor. */
export interface SharedDoc {
  sharedThreadId: string;
  fileId: number;
  /** The document now: one Yjs update, base64. */
  state: string;
}

/**
 * A Run (ATL-405): one agent turn in the thread, run by a participant in their
 * own Run worktree and merged back when it ends.
 */
export interface SharedThreadRun {
  runId: string;
  /** The thread-local number shown on its chip. */
  runNo: number;
  promptedBy: string;
  runnerId: string;
  agent: string;
  model: string;
  forkSeq: number;
  status: "running" | "merged" | "ended" | "interrupted" | "declined" | (string & {});
  startedAt: number;
  endedAt: number | null;
  mergedVersion: number | null;
  /** The paths its merge changed. */
  files: string[];
  /** The file it is touching now, as its Runner says (ATL-407). */
  currentFile: string | null;
}

/** One live frame: a `SessionDelta` the Runner's agent emitted. */
export interface SharedRunFrame {
  sharedThreadId: string;
  runNo: number;
  delta: { kind: string; delta?: string; [key: string]: unknown };
}

/** Which way a Conflict was, or is to be, resolved (ATL-410). */
export type ConflictSide = "canonical" | "run" | "both" | "edited" | "agent";

/**
 * A Conflict (ATL-410): a hunk where a Run's change and a change canonical
 * state took since the Run forked touch the same lines. Canonical state keeps
 * its version until somebody resolves it. For a binary file the three sides
 * are blob hashes and `lines` is `null`.
 */
export interface SharedThreadConflict {
  conflictId: number;
  fileId: number;
  path: string;
  runId: string;
  status: "open" | "resolved" | (string & {});
  /** Canonical state's lines, 0-based, `end` exclusive, when it was raised. */
  lines: { start: number; end: number } | null;
  binary: boolean;
  base: string | null;
  canonical: string | null;
  run: string | null;
  involved: { runs: string[]; people: string[] };
  raisedBy: string;
  raisedAt: number;
  resolution: {
    text: string;
    side: ConflictSide;
    resolvedBy: string;
    resolvedAt: number;
    version: number;
  } | null;
  /** Who ran the Run whose hunk was held, and its agent. */
  runBy: string | null;
  runAgent: string | null;
  /** Who else changed those lines, and other Runs' agents. */
  canonicalBy: string[];
  canonicalAgents: string[];
  /** The proposed result; `null` for a binary file. */
  proposed: string | null;
}

export interface SharedThreadStatus {
  connected: boolean;
  role: string | null;
  /** The newest change this replica has seen. */
  head: number;
  /** Whether the replica worktree has been checked out yet. */
  materialized: boolean;
  /** Where the replica worktree is (or will be) — never the person's own checkout. */
  worktree: string;
  files: number;
  /** Files kept on this machine because they now look like they hold a secret. */
  held: string[];
  /** The thread's Runs, newest first. */
  runs: SharedThreadRun[];
  /**
   * Why this replica can only watch — the Base never reached this machine,
   * a viewer's role, a closed thread — or `null` when it can edit.
   */
  readOnly: string | null;
  /** Whether this machine sends the repository's history to teammates who lack the Base. */
  servesHistory: boolean;
  /** Teammates waiting for that history while it is not being sent. */
  historyWanted: number;
  /**
   * What was done on the person's behalf — a file of theirs moved aside for a
   * teammate's change, a drifted replica repaired — newest last.
   */
  notices: string[];
  /** Saves kept on this machine because it may not change the thread; they go once it may. */
  unsent: string[];
  /** The thread is closed: read-only on every replica until it is reopened. */
  closed: boolean;
  /** Text files that stopped syncing: they grew past 1 MB or turned binary. */
  outgrown: string[];
  /** The thread's Conflicts, open ones first (ATL-410). */
  conflicts: SharedThreadConflict[];
  /** Everybody else here (ATL-407). */
  peers: SharedPeer[];
  /** Whether this replica is current, syncing or behind. */
  sync: SyncState | null;
  error: string | null;
}

export interface BlockedFile {
  path: string;
  /** `name`, or the secret categories the content matched. */
  reason: string;
}

/** One row of the share dialog (ATL-402): a file the share would upload. */
export interface ShareFile {
  path: string;
  /** `text` is co-edited; `binary` (or over 1 MB) syncs as whole bytes. */
  kind: "text" | "binary";
  bytes: number;
  /** Deleted in the working tree: the share deletes it in the thread. */
  deleted: boolean;
  /** Held back unless included anyway: `name`, or the secret categories matched. */
  blocked: string | null;
}

export interface SharedThreadView {
  sharedThreadId: string;
  orgId: string;
  workspaceId: string;
  title: string;
  /** The commit the thread's canonical state starts from. */
  base: string;
  role: string;
  /**
   * The person's own checkout of the project, when they have one. Only ever
   * read. `null` for somebody who joined with no copy of the repository.
   */
  projectPath: string | null;
  clientId: string;
  /** Whether this machine sends the repository's history to teammates who need it. */
  serveHistory: boolean;
  /** What to send a teammate. */
  link: string;
  status: SharedThreadStatus;
  /** The local chat session shared as this thread, when shared from this machine. */
  sessionId: string | null;
  /** On a share: what was uploaded and what was held back. Empty otherwise. */
  sharedFiles: string[];
  blockedFiles: BlockedFile[];
  /** This thread's Run worktree on this machine: a session there runs in the thread. */
  runWorktree: string;
}

/** What the owner manages (ATL-406). */
export interface OwnerView {
  joinPolicy: "auto" | "approval" | (string & {});
  status: "open" | "closed" | (string & {});
  closedAt: number | null;
  purgeAt: number | null;
  participants: Array<{ userId: string; role: string; joinedAt: number }>;
  requests: Array<{ userId: string; requestedAt: number }>;
}

/** A refusal Rust (or the server) explained. `code` is stable; branch on it. */
export interface SharedThreadError {
  code: string;
  message: string;
}

export function sharedThreadError(e: unknown): SharedThreadError {
  if (e && typeof e === "object" && "code" in e && "message" in e) {
    return {
      code: String((e as SharedThreadError).code),
      message: String((e as SharedThreadError).message),
    };
  }
  return { code: "unknown", message: e instanceof Error ? e.message : String(e) };
}

/**
 * What sharing `projectPath` would upload, and what it holds back as
 * secret-shaped. The dialog lists exactly these files. Only reads.
 */
export function previewShare(projectPath: string) {
  return invoke<ShareFile[]>("shared_thread_share_preview", { projectPath });
}

/**
 * Share the thread behind an ACP session. `include` names blocked files the
 * person chose to include anyway. Refused for a Local-mode project
 * (`workspace_local`).
 */
export function shareThread(args: {
  sessionId: string;
  projectPath: string;
  title: string;
  include: string[];
  /** Let teammates without the starting commit fetch the history from here. */
  serveHistory: boolean;
}) {
  return invoke<SharedThreadView>("shared_thread_share", args);
}

/**
 * Send (or stop sending) this repository's history to teammates who lack the
 * thread's starting commit. Never sent without the person's say-so.
 */
export function setServeHistory(sharedThreadId: string, on: boolean) {
  return invoke<void>("shared_thread_serve_history", { sharedThreadId, on });
}

/**
 * Join from a teammate's link. `projectPath` narrows which local project to
 * use; with none, or one without the thread's Base, the Base arrives as a
 * bundle — or the thread is followed read-only and `status.readOnly` says why.
 */
export function joinThread(link: string, projectPath?: string) {
  return invoke<SharedThreadView>("shared_thread_join", { link, projectPath: projectPath ?? null });
}

/** Check the replica out (first file open or prompt) and answer its path. */
export function openThread(sharedThreadId: string) {
  return invoke<string>("shared_thread_open", { sharedThreadId });
}

/**
 * The thread's Run worktree on this machine, holding canonical state now. An
 * agent session opened there runs every prompt as a Run in the thread.
 */
export function runWorktree(sharedThreadId: string) {
  return invoke<string>("shared_thread_run_worktree", { sharedThreadId });
}

export function listThreads() {
  return invoke<SharedThreadView[]>("shared_thread_list");
}

/** Stop syncing on this machine. The replica worktree stays on disk. */
export function leaveThread(sharedThreadId: string) {
  return invoke<void>("shared_thread_leave", { sharedThreadId });
}

/** The owner's view: participants, join requests, join policy, open or closed. */
export function ownerView(sharedThreadId: string) {
  return invoke<OwnerView>("shared_thread_owner_view", { sharedThreadId });
}

/** Approve a join request or promote (`participant`), or take edit rights away (`viewer`). */
export function setRole(sharedThreadId: string, userId: string, role: "participant" | "viewer") {
  return invoke<OwnerView>("shared_thread_set_role", { sharedThreadId, userId, role });
}

/** Decline a join request: they stay a viewer. */
export function declineJoin(sharedThreadId: string, userId: string) {
  return invoke<OwnerView>("shared_thread_decline", { sharedThreadId, userId });
}

/** Turn "approval required" on or off. */
export function setJoinPolicy(sharedThreadId: string, joinPolicy: "auto" | "approval") {
  return invoke<OwnerView>("shared_thread_set_join_policy", { sharedThreadId, joinPolicy });
}

/** Close the thread (read-only everywhere) or reopen it. */
export function setThreadOpen(sharedThreadId: string, open: boolean) {
  return invoke<OwnerView>("shared_thread_set_open", { sharedThreadId, open });
}

/** Resolve a Conflict on every replica; answers the Thread Version it recorded. */
export function resolveConflict(
  sharedThreadId: string,
  conflictId: number,
  side: Exclude<ConflictSide, "agent">,
  text?: string,
) {
  return invoke<number>("shared_thread_resolve_conflict", {
    sharedThreadId,
    conflictId,
    side,
    text: text ?? null,
  });
}

/**
 * Mark the thread's next Run as resolving `conflictId`, and get where to run
 * it and what to ask: the agent's rewrite of the hunk becomes the resolution.
 */
export function askAgentToResolve(sharedThreadId: string, conflictId: number) {
  return invoke<{ cwd: string; prompt: string }>("shared_thread_ask_agent_to_resolve", {
    sharedThreadId,
    conflictId,
  });
}

/** What Apply did to the person's checkout (ATL-408). */
export interface Applied {
  /** Written, created or deleted cleanly. */
  files: string[];
  /** Left with conflict markers, or kept as the checkout had them. */
  conflicted: string[];
  /** For a binary conflict: where the thread's version was put beside yours. */
  beside: string[];
  /** The stash the person's own edits went into, when they asked for it. */
  stashed: string | null;
  /** Ignored files of theirs that were in the way, moved beside themselves. */
  setAside: string[];
}

/** Apply's answer: done, or refused with what to do about it. */
export type ApplyOutcome =
  | ({ outcome: "applied" } & Applied)
  | { outcome: "dirty"; files: string[] }
  | { outcome: "conflictsOpen"; count: number };

/**
 * Write the thread's changes since its Base into this person's checkout as
 * uncommitted changes. `stash` sets aside their own uncommitted edits to the
 * same files first. Never commits, pushes or closes the thread.
 */
export function applyThread(sharedThreadId: string, stash = false) {
  return invoke<ApplyOutcome>("shared_thread_apply", { sharedThreadId, stash });
}

/**
 * Bind the Atlas editor to `path` when it is a text file of a joined thread's
 * replica; `null` for any other file.
 */
export function openSharedDoc(path: string) {
  return invoke<SharedDoc | null>("shared_thread_doc_open", { path });
}

export function closeSharedDoc(sharedThreadId: string, fileId: number) {
  return invoke<void>("shared_thread_doc_close", { sharedThreadId, fileId });
}

/** Keystrokes, as one batched Yjs update (base64). Rejects `not_syncing`. */
export function sendDocUpdate(sharedThreadId: string, fileId: number, update: string) {
  return invoke<void>("shared_thread_doc_update", { sharedThreadId, fileId, update });
}

/** The person's selections in one file, `[anchor, head]`, and whether they type there. */
export function sendCursors(
  sharedThreadId: string,
  fileId: number,
  cursors: Array<[number, number]>,
  typing: boolean,
) {
  return invoke<void>("shared_thread_cursors", { sharedThreadId, fileId, cursors, typing });
}

export function onSharedPresence(
  apply: (event: { sharedThreadId: string; peers: SharedPeer[] }) => void,
): Promise<UnlistenFn> {
  return listen<{ sharedThreadId: string; peers: SharedPeer[] }>(SHARED_PRESENCE_EVENT, (e) =>
    apply(e.payload),
  );
}

export function onSharedDocUpdate(
  apply: (event: { sharedThreadId: string; fileId: number; update: string }) => void,
): Promise<UnlistenFn> {
  return listen<{ sharedThreadId: string; fileId: number; update: string }>(
    SHARED_DOC_UPDATE_EVENT,
    (e) => apply(e.payload),
  );
}

export function onJoinRequested(
  apply: (request: { sharedThreadId: string; userId: string }) => void,
): Promise<UnlistenFn> {
  return listen<{ sharedThreadId: string; userId: string }>(SHARED_JOIN_REQUEST_EVENT, (event) =>
    apply(event.payload),
  );
}

export function onSharedRunFrame(apply: (frame: SharedRunFrame) => void): Promise<UnlistenFn> {
  return listen<SharedRunFrame>(SHARED_RUN_FRAME_EVENT, (event) => apply(event.payload));
}

export function onSharedThreadsChanged(
  apply: (threads: SharedThreadView[]) => void,
): Promise<UnlistenFn> {
  return listen<SharedThreadView[]>(SHARED_THREADS_EVENT, (event) => apply(event.payload));
}
