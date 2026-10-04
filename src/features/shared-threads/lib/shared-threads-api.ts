import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";

/**
 * Shared Threads (ATL-395): one piece of work several members and agents carry
 * out together, with its file state held in the cloud. This module mirrors
 * `src-tauri/src/commands/shared_threads.rs`; only Rust holds the bearer.
 */

/** Pushed by Rust whenever a joined thread's status changes. */
export const SHARED_THREADS_EVENT = "atlas:shared-threads";

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
  error: string | null;
}

export interface BlockedFile {
  path: string;
  /** `name`, or the secret categories the content matched. */
  reason: string;
}

export interface SharedThreadView {
  sharedThreadId: string;
  orgId: string;
  workspaceId: string;
  title: string;
  /** The commit the thread's canonical state starts from. */
  base: string;
  role: string;
  /** The person's own checkout of the project. Only ever read. */
  projectPath: string;
  clientId: string;
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
    return { code: String((e as SharedThreadError).code), message: String((e as SharedThreadError).message) };
  }
  return { code: "unknown", message: e instanceof Error ? e.message : String(e) };
}

/** Share the thread behind an ACP session. Refused for a Local-mode project (`workspace_local`). */
export function shareThread(args: { sessionId: string; projectPath: string; title: string }) {
  return invoke<SharedThreadView>("shared_thread_share", args);
}

/** Join from a teammate's link. `projectPath` narrows which local project to use. */
export function joinThread(link: string, projectPath?: string) {
  return invoke<SharedThreadView>("shared_thread_join", { link, projectPath: projectPath ?? null });
}

/** Check the replica out (first file open or prompt) and answer its path. */
export function openThread(sharedThreadId: string) {
  return invoke<string>("shared_thread_open", { sharedThreadId });
}

export function listThreads() {
  return invoke<SharedThreadView[]>("shared_thread_list");
}

/** Stop syncing on this machine. The replica worktree stays on disk. */
export function leaveThread(sharedThreadId: string) {
  return invoke<void>("shared_thread_leave", { sharedThreadId });
}

export function onSharedThreadsChanged(apply: (threads: SharedThreadView[]) => void): Promise<UnlistenFn> {
  return listen<SharedThreadView[]>(SHARED_THREADS_EVENT, (event) => apply(event.payload));
}
