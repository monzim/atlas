import { beforeEach, describe, expect, it, vi } from "vitest";

// The store talks to Rust only through these; the live-tail logic does not.
vi.mock("../lib/shared-threads-api", () => ({
  listThreads: () => Promise.resolve([]),
  onSharedThreadsChanged: () => Promise.resolve(() => {}),
  onSharedRunFrame: () => Promise.resolve(() => {}),
}));

const { runKey, useSharedThreadsStore } = await import("./shared-threads-store");

beforeEach(() => useSharedThreadsStore.setState({ live: {} }));

describe("live Run frames (ATL-405)", () => {
  it("keeps the tail of each Run's answer text, per thread and Run", () => {
    const { heard } = useSharedThreadsStore.getState();
    heard({ sharedThreadId: "T", runNo: 1, delta: { kind: "text_chunk", delta: "Looking" } });
    heard({ sharedThreadId: "T", runNo: 1, delta: { kind: "text_chunk", delta: " at it." } });
    heard({ sharedThreadId: "T", runNo: 2, delta: { kind: "text_chunk", delta: "Other run" } });
    const { live } = useSharedThreadsStore.getState();
    expect(live[runKey("T", 1)]).toBe("Looking at it.");
    expect(live[runKey("T", 2)]).toBe("Other run");
  });

  it("ignores deltas that are not answer text, and bounds what it keeps", () => {
    const { heard } = useSharedThreadsStore.getState();
    heard({ sharedThreadId: "T", runNo: 1, delta: { kind: "tool_call_upserted" } });
    expect(useSharedThreadsStore.getState().live).toEqual({});
    heard({
      sharedThreadId: "T",
      runNo: 1,
      delta: { kind: "text_chunk", delta: "x".repeat(1000) },
    });
    expect(useSharedThreadsStore.getState().live[runKey("T", 1)]).toHaveLength(280);
  });
});
