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
  onSharedThreadsChanged,
  type SharedThreadView,
} from "../lib/shared-threads-api";

interface SharedThreadsState {
  threads: SharedThreadView[];
  /** ACP session id → Shared Thread id, for sessions shared from this machine. */
  bySession: Record<string, string>;
  /** The share/join answer, kept for its file lists until the next one. */
  lastResult: SharedThreadView | null;
  loaded: boolean;
  load: () => Promise<void>;
  apply: (threads: SharedThreadView[]) => void;
  shared: (sessionId: string | null, view: SharedThreadView) => void;
}

const useSharedThreadsStoreBase = create<SharedThreadsState>((set, get) => ({
  threads: [],
  bySession: {},
  lastResult: null,
  loaded: false,
  load: async () => {
    if (get().loaded) return;
    set({ loaded: true });
    void onSharedThreadsChanged((threads) => get().apply(threads));
    try {
      get().apply(await listThreads());
    } catch {
      // Not signed in, or the backend is not up yet: the next push fills it.
    }
  },
  apply: (threads) => set({ threads }),
  shared: (sessionId, view) =>
    set((state) => ({
      lastResult: view,
      threads: [...state.threads.filter((t) => t.sharedThreadId !== view.sharedThreadId), view],
      bySession: sessionId ? { ...state.bySession, [sessionId]: view.sharedThreadId } : state.bySession,
    })),
}));

export const useSharedThreadsStore = createSelectors(useSharedThreadsStoreBase);
