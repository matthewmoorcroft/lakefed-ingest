import { useQuery } from "@tanstack/react-query";
import { api, type Row } from "../api";
import { Notice, fmtInt, plural } from "../components";

// A large write (New's Create, Add tables, a databases file of more than 5,000 tables) is done by a background task:
// the request answers 202 with it, and the page follows its progress (GET /api/tasks/{id}) until it ends. A request id,
// made when the page starts a write, makes a retry of the same write find the same task, never a second job.

export const newRequestId = (): string =>
  typeof crypto !== "undefined" && "randomUUID" in crypto ? crypto.randomUUID()
    : `${Date.now().toString(16)}-${Math.random().toString(16).slice(2)}`;

const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** The write's answer: at once when it was small, else its task's result once the task has ended (onTask sees it as it
 *  goes). A failed task throws its error, as the request would have. */
export async function finished(answer: Row, onTask: (task: Row) => void): Promise<Row> {
  if (!answer.task) return answer;
  let task = answer.task;
  for (;;) {
    onTask(task);
    if (task.status === "succeeded") return task.result ?? {};
    if (task.status === "failed") throw new Error(task.error || "the write failed");
    await wait(2000);
    task = await api.get<Row>(`/tasks/${task.id}`);
  }
}

const KINDS: Record<string, string> = { create: "Creating the job", tables: "Adding tables", databases: "Adding databases" };

/** "Writing 52,000 of 100,200 tables", or what it ended with. */
export function TaskProgress({ task }: { task: Row | null }) {
  if (!task) return null;
  if (task.status === "failed") return <Notice tone="bad">Nothing was written: {task.error}</Notice>;
  if (task.status === "succeeded") return <Notice tone="ok">Written: {fmtInt(task.total)} {plural(task.total, "table")}.</Notice>;
  return (
    <Notice>
      {KINDS[task.kind] ?? "Writing"}: {Number(task.done) > 0 ? <>writing <b>{fmtInt(task.done)}</b> of {fmtInt(task.total)} tables</>
        : <>checking {fmtInt(task.total)} {plural(task.total, "table")}</>}
      <progress className="task-progress" max={Number(task.total) || 1} value={Number(task.done) || 0} />
      <span className="sub">In one transaction: all of it is written, or none of it. Leaving this page doesn't stop it.</span>
    </Notice>
  );
}

/** A job's background writes still running, for its page; read every few seconds while there are any. */
export function RunningTasks({ jobId }: { jobId: string }) {
  const tasks = useQuery({
    queryKey: ["job-tasks", jobId], queryFn: () => api.get<Row[]>(`/ingestion-jobs/${jobId}/tasks`),
    refetchInterval: (query) => ((query.state.data ?? []).some((t: Row) => t.status === "running") ? 3000 : 30000),
  });
  const running = (tasks.data ?? []).filter((t) => t.status === "running");
  return <>{running.map((t) => <TaskProgress key={t.id} task={t} />)}</>;
}
