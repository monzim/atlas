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
}

/** One live frame: a `SessionDelta` the Runner's agent emitted. */
export interface SharedRunFrame {
  sharedThreadId: string;
  runNo: number;
  delta: { kind: string; delta?: string; [key: string]: unknown };
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

export function onSharedRunFrame(apply: (frame: SharedRunFrame) => void): Promise<UnlistenFn> {
  return listen<SharedRunFrame>(SHARED_RUN_FRAME_EVENT, (event) => apply(event.payload));
}

export function onSharedThreadsChanged(
  apply: (threads: SharedThreadView[]) => void,
): Promise<UnlistenFn> {
  return listen<SharedThreadView[]>(SHARED_THREADS_EVENT, (event) => apply(event.payload));
}
