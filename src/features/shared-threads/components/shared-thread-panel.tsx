import { useEffect, useState } from "react";
import {
  Bot,
  Check,
  Copy,
  FolderOpen,
  Link2,
  LogOut,
  Play,
  ShieldAlert,
  Users,
} from "lucide-react";

import { Badge } from "@/ui/badge";
import { Button } from "@/ui/button";
import { Input } from "@/ui/input";
import { cn } from "@/lib/utils";
import { agents, ensureAgent } from "@/features/chat/lib/agents-api";
import { openAgentSession } from "@/features/chat/lib/open-agent-session";
import { pluginIdForAgent, type AgentType } from "@/types/agent";

import {
  joinThread,
  leaveThread,
  openThread,
  runWorktree,
  shareThread,
  sharedThreadError,
  type SharedThreadError,
  type SharedThreadRun,
  type SharedThreadView,
} from "../lib/shared-threads-api";
import { runKey, useSharedThreadsStore } from "../stores/shared-threads-store";

/** What the chat pane knows about the thread it shows. */
export interface ShareTarget {
  /** The ACP session id behind the chat. `null` before the first send. */
  sessionId: string | null;
  projectPath: string | null;
  title: string;
  /** The agent the chat runs, offered for running in a joined thread. */
  agentType?: AgentType;
}

/**
 * The thread being shown, if it is shared — from this session, or from the
 * link Rust keeps in the thread metadata.
 */
export function useSharedThreadFor(sessionId: string | null): SharedThreadView | null {
  const threads = useSharedThreadsStore.use.threads();
  const bySession = useSharedThreadsStore.use.bySession();
  const load = useSharedThreadsStore.use.load();
  useEffect(() => {
    void load();
  }, [load]);
  if (!sessionId) return null;
  const id = bySession[sessionId];
  return threads.find((t) => t.sharedThreadId === id || t.sessionId === sessionId) ?? null;
}

/**
 * Share this thread, join one from a link, and see the threads this machine
 * has joined (ATL-395). Lives in the chat header's Share popover.
 */
export function SharedThreadPanel({ target }: { target: ShareTarget }) {
  const threads = useSharedThreadsStore.use.threads();
  const lastResult = useSharedThreadsStore.use.lastResult();
  const shared = useSharedThreadsStore.use.shared();
  const current = useSharedThreadFor(target.sessionId);

  const [busy, setBusy] = useState<"share" | "join" | null>(null);
  const [error, setError] = useState<SharedThreadError | null>(null);
  const [link, setLink] = useState("");

  async function share() {
    if (!target.sessionId || !target.projectPath) return;
    setBusy("share");
    setError(null);
    try {
      const view = await shareThread({
        sessionId: target.sessionId,
        projectPath: target.projectPath,
        title: target.title,
      });
      shared(target.sessionId, view);
    } catch (e) {
      setError(sharedThreadError(e));
    } finally {
      setBusy(null);
    }
  }

  async function join() {
    if (!link.trim()) return;
    setBusy("join");
    setError(null);
    try {
      const view = await joinThread(link.trim(), target.projectPath ?? undefined);
      shared(null, view);
      setLink("");
    } catch (e) {
      setError(sharedThreadError(e));
    } finally {
      setBusy(null);
    }
  }

  const others = threads.filter((t) => t.sharedThreadId !== current?.sharedThreadId);

  return (
    <div className="flex w-[340px] flex-col gap-3 p-3 text-xs">
      {current ? (
        <ThreadCard
          thread={current}
          result={lastResult?.sharedThreadId === current.sharedThreadId ? lastResult : null}
          agentType={target.agentType}
        />
      ) : (
        <section className="flex flex-col gap-2">
          <div className="flex items-center gap-2 text-[var(--foreground)]">
            <Users size={13} />
            <span className="font-medium">Share this thread</span>
          </div>
          <p className="leading-relaxed text-[var(--muted-foreground)]">
            Teammates on this project can work in it with you, live. Your checked-out commit becomes
            the starting point and your uncommitted changes are uploaded as its first changes.{" "}
            <span className="text-[var(--secondary-foreground)]">
              Your repository is not uploaded
            </span>
            , and files that look like secrets stay on this machine.
          </p>
          <Button
            size="sm"
            onClick={share}
            disabled={busy !== null || !target.sessionId || !target.projectPath}
          >
            {busy === "share" ? "Sharing…" : "Share thread"}
          </Button>
          {!target.sessionId && (
            <p className="text-[var(--muted-foreground)]">
              Send a message first — a draft has nothing to share yet.
            </p>
          )}
        </section>
      )}

      {error && <ErrorNote error={error} />}

      <div className="h-px bg-[var(--atlas-border-subtle)]" />

      <section className="flex flex-col gap-2">
        <div className="flex items-center gap-2 text-[var(--foreground)]">
          <Link2 size={13} />
          <span className="font-medium">Join from a link</span>
        </div>
        <form
          className="flex gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            void join();
          }}
        >
          <Input
            size="sm"
            value={link}
            placeholder="https://app.tryatlas.cc/threads/…"
            onChange={(e) => setLink(e.target.value)}
            className="min-w-0 flex-1 font-mono"
          />
          <Button
            size="sm"
            variant="secondary"
            type="submit"
            disabled={busy !== null || !link.trim()}
          >
            {busy === "join" ? "Joining…" : "Join"}
          </Button>
        </form>
      </section>

      {others.length > 0 && (
        <section className="flex flex-col gap-2">
          <span className="text-2xs uppercase tracking-wide text-[var(--muted-foreground)]">
            Joined on this machine
          </span>
          {others.map((t) => (
            <ThreadCard
              key={t.sharedThreadId}
              thread={t}
              result={null}
              agentType={target.agentType}
              compact
            />
          ))}
        </section>
      )}
    </div>
  );
}

function ThreadCard({
  thread,
  result,
  agentType,
  compact = false,
}: {
  thread: SharedThreadView;
  result: SharedThreadView | null;
  agentType?: AgentType;
  compact?: boolean;
}) {
  const [copied, setCopied] = useState(false);
  const [opening, setOpening] = useState(false);
  const [starting, setStarting] = useState(false);
  const [error, setError] = useState<SharedThreadError | null>(null);
  const [worktree, setWorktree] = useState<string | null>(null);
  const { status } = thread;

  async function copyLink() {
    await navigator.clipboard.writeText(thread.link);
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  }

  async function open() {
    setOpening(true);
    setError(null);
    try {
      setWorktree(await openThread(thread.sharedThreadId));
    } catch (e) {
      setError(sharedThreadError(e));
    } finally {
      setOpening(false);
    }
  }

  /**
   * Open an agent chat in this thread's Run worktree: every prompt sent there
   * is a Run — forked from canonical state, streamed live to the thread, and
   * merged back when the turn ends (ATL-405).
   */
  async function runHere() {
    setStarting(true);
    setError(null);
    try {
      const cwd = await runWorktree(thread.sharedThreadId);
      const agent = await ensureAgent(pluginIdForAgent(agentType));
      const { key } = await agents.newSession(agent.agent_id, cwd);
      await openAgentSession({
        acpSessionId: key.session_id,
        title: `${thread.title} · Runs`,
        cwd,
        agentType,
      });
    } catch (e) {
      setError(sharedThreadError(e));
    } finally {
      setStarting(false);
    }
  }

  const mayRun = thread.role !== "viewer";

  return (
    <div
      className={cn(
        "flex flex-col gap-2 rounded-md border border-[var(--atlas-border-subtle)] p-2.5",
        compact && "bg-[var(--atlas-element-hover)]",
      )}
    >
      <div className="flex items-center gap-2">
        <span className="min-w-0 flex-1 truncate font-medium text-[var(--foreground)]">
          {thread.title}
        </span>
        <Badge variant={status.connected ? "success" : "outline"}>
          {status.connected ? "Live" : "Offline"}
        </Badge>
        <Badge variant="secondary" className="capitalize">
          {thread.role}
        </Badge>
      </div>
      <div className="flex flex-wrap gap-x-3 gap-y-1 text-[var(--muted-foreground)]">
        <span className="font-mono">base {thread.base.slice(0, 8)}</span>
        <span className="tabular-nums">
          {status.files} {status.files === 1 ? "file" : "files"} changed
        </span>
        <span>{status.materialized ? "Checked out" : "Not checked out yet"}</span>
      </div>
      {status.error && <p className="text-warning">{status.error}</p>}
      {status.held.length > 0 && (
        <div className="flex flex-col gap-1 rounded bg-warning-muted p-2 text-warning">
          <span className="flex items-center gap-1.5 font-medium">
            <ShieldAlert size={12} /> Not syncing — looks like it holds a secret
          </span>
          {status.held.map((path) => (
            <span key={path} className="font-mono">
              {path}
            </span>
          ))}
          <span className="text-[var(--secondary-foreground)]">
            Remove it and save; your edits merge with the team's.
          </span>
        </div>
      )}

      {result && result.blockedFiles.length > 0 && (
        <div className="flex flex-col gap-1 rounded bg-warning-muted p-2 text-warning">
          <span className="flex items-center gap-1.5 font-medium">
            <ShieldAlert size={12} /> Kept on this machine (looks like a secret)
          </span>
          {result.blockedFiles.map((f) => (
            <span key={f.path} className="font-mono">
              {f.path}
            </span>
          ))}
        </div>
      )}
      {result && result.sharedFiles.length > 0 && (
        <p className="text-[var(--muted-foreground)]">
          Uploaded {result.sharedFiles.length} changed{" "}
          {result.sharedFiles.length === 1 ? "file" : "files"}.
        </p>
      )}

      {status.runs.length > 0 && <RunList threadId={thread.sharedThreadId} runs={status.runs} />}

      <div className="flex flex-wrap gap-1.5">
        {mayRun && (
          <Button
            size="xs"
            variant="outline"
            onClick={() => void runHere()}
            disabled={starting || !status.connected}
          >
            <Play size={11} />
            {starting ? "Preparing…" : "Run an agent here"}
          </Button>
        )}
        <Button size="xs" variant="outline" onClick={() => void copyLink()}>
          {copied ? <Check size={11} /> : <Copy size={11} />}
          {copied ? "Copied" : "Copy link"}
        </Button>
        <Button size="xs" variant="outline" onClick={() => void open()} disabled={opening}>
          <FolderOpen size={11} />
          {status.materialized ? "Show replica" : opening ? "Checking out…" : "Check out replica"}
        </Button>
        <Button
          size="xs"
          variant="ghost"
          onClick={() =>
            void leaveThread(thread.sharedThreadId).catch((e) => setError(sharedThreadError(e)))
          }
        >
          <LogOut size={11} />
          Stop syncing
        </Button>
      </div>
      {worktree && (
        <p
          className="break-all font-mono text-[var(--muted-foreground)]"
          title="Your replica — separate from your own checkout"
        >
          {worktree}
        </p>
      )}
      {error && <ErrorNote error={error} />}
    </div>
  );
}

const RUN_STATUS: Record<
  string,
  { label: string; variant: "success" | "secondary" | "outline" | "warning" }
> = {
  running: { label: "Running", variant: "secondary" },
  merged: { label: "Merged", variant: "success" },
  ended: { label: "Ended", variant: "outline" },
  interrupted: { label: "Interrupted", variant: "warning" },
  declined: { label: "Declined", variant: "outline" },
};

/** The thread's recent Runs as chips: who prompted, who ran, agent, model and files changed. */
function RunList({ threadId, runs }: { threadId: string; runs: SharedThreadRun[] }) {
  const live = useSharedThreadsStore.use.live();
  return (
    <section className="flex flex-col gap-1.5">
      <span className="text-2xs uppercase tracking-wide text-[var(--muted-foreground)]">Runs</span>
      {runs.slice(0, 5).map((run) => {
        const status = RUN_STATUS[run.status] ?? { label: run.status, variant: "outline" as const };
        const tail = run.status === "running" ? live[runKey(threadId, run.runNo)] : undefined;
        return (
          <div
            key={run.runId}
            className="flex flex-col gap-1 rounded border border-[var(--atlas-border-subtle)] px-2 py-1.5"
          >
            <div className="flex items-center gap-1.5">
              <Bot size={11} className="shrink-0 text-[var(--muted-foreground)]" />
              <span className="tabular-nums text-[var(--muted-foreground)]">#{run.runNo}</span>
              <span className="min-w-0 flex-1 truncate text-[var(--foreground)]">
                {run.agent} · <span className="font-mono">{run.model}</span>
              </span>
              <Badge variant={status.variant}>{status.label}</Badge>
            </div>
            <span className="truncate text-[var(--muted-foreground)]">
              {run.promptedBy === run.runnerId
                ? `Prompted and run by ${run.runnerId}`
                : `Prompted by ${run.promptedBy}, run by ${run.runnerId}`}
            </span>
            {run.files.length > 0 && (
              <span
                className="truncate font-mono text-[var(--secondary-foreground)]"
                title={run.files.join("\n")}
              >
                {run.files.length} {run.files.length === 1 ? "file" : "files"}:{" "}
                {run.files.join(", ")}
              </span>
            )}
            {tail && (
              <p className="line-clamp-3 whitespace-pre-wrap text-[var(--secondary-foreground)]">
                {tail}
              </p>
            )}
          </div>
        );
      })}
    </section>
  );
}

/** A refusal, with the way out when there is one. */
function ErrorNote({ error }: { error: SharedThreadError }) {
  const hint =
    error.code === "feature_disabled"
      ? "Shared Threads is not enabled for your organization yet."
      : error.code === "base_missing"
        ? "Fetch or pull so this repository has the commit the thread starts from."
        : null;
  return (
    <div className="flex flex-col gap-1.5 rounded bg-error-muted p-2 text-error">
      <span>{error.message}</span>
      {hint && <span className="text-[var(--secondary-foreground)]">{hint}</span>}
      {error.code === "workspace_local" && (
        // Promotion is a disclosure with its own confirmation step; this opens
        // that flow rather than repeating it here.
        <Button
          size="xs"
          variant="outline"
          className="self-start"
          onClick={() => window.dispatchEvent(new CustomEvent("atlas:open-capture"))}
        >
          Promote to Cloud…
        </Button>
      )}
    </div>
  );
}
