/**
 * The Shared Threads this machine has joined, and which local chat session was
 * shared as which (ATL-395).
 *
 * Fed by `shared_thread_list` once and then by Rust's `atlas:shared-threads`
 * pushes; a share or join answers the fresh view directly so the panel does
 * not wait for the event.
 */

import { create } from "zustand";

import { createSelectors } from "@/lib/create-selectors";

import {
  listThreads,
  onSharedRunFrame,
  onSharedThreadsChanged,
  type SharedRunFrame,
  type SharedThreadView,
} from "../lib/shared-threads-api";

/** How much of a live Run's text is kept for its chip. */
const LIVE_TAIL_CHARS = 280;

/** `${sharedThreadId}:${runNo}` — a Run's key in `live`. */
export function runKey(sharedThreadId: string, runNo: number): string {
  return `${sharedThreadId}:${runNo}`;
}

interface SharedThreadsState {
  threads: SharedThreadView[];
  /** ACP session id → Shared Thread id, for sessions shared from this machine. */
  bySession: Record<string, string>;
  /** The share/join answer, kept for its file lists until the next one. */
  lastResult: SharedThreadView | null;
  /** The tail of each live Run's text, from other people's Run frames. */
  live: Record<string, string>;
  loaded: boolean;
  load: () => Promise<void>;
  apply: (threads: SharedThreadView[]) => void;
  heard: (frame: SharedRunFrame) => void;
  shared: (sessionId: string | null, view: SharedThreadView) => void;
}

const useSharedThreadsStoreBase = create<SharedThreadsState>((set, get) => ({
  threads: [],
  bySession: {},
  lastResult: null,
  live: {},
  loaded: false,
  load: async () => {
    if (get().loaded) return;
    set({ loaded: true });
    void onSharedThreadsChanged((threads) => get().apply(threads));
    void onSharedRunFrame((frame) => get().heard(frame));
    try {
      get().apply(await listThreads());
    } catch {
      // Not signed in, or the backend is not up yet: the next push fills it.
    }
  },
  apply: (threads) => set({ threads }),
  heard: ({ sharedThreadId, runNo, delta }) => {
    // Only the answer's text is drawn on a chip; the full Run is the Runner's
    // Session, read from the timeline once it lands.
    if (delta.kind !== "text_chunk" || typeof delta.delta !== "string") return;
    const key = runKey(sharedThreadId, runNo);
    set((state) => ({
      live: {
        ...state.live,
        [key]: ((state.live[key] ?? "") + delta.delta).slice(-LIVE_TAIL_CHARS),
      },
    }));
  },
  shared: (sessionId, view) =>
    set((state) => ({
      lastResult: view,
      threads: [...state.threads.filter((t) => t.sharedThreadId !== view.sharedThreadId), view],
      bySession: sessionId
        ? { ...state.bySession, [sessionId]: view.sharedThreadId }
        : state.bySession,
    })),
}));

export const useSharedThreadsStore = createSelectors(useSharedThreadsStoreBase);
