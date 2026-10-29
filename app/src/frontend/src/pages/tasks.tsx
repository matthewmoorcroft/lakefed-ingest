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
 *  goes). A failed task throws its error, as the request would have, with the task attached for its progress. */
export async function finished(answer: Row, onTask: (task: Row) => void): Promise<Row> {
  if (!answer.task) return answer;
  let task = answer.task;
  for (;;) {
    onTask(task);
    if (task.status === "succeeded") return task.result ?? {};
    if (task.status === "failed") throw Object.assign(new Error(task.error || "the write failed"), { task });
    await wait(2000);
    task = await api.get<Row>(`/tasks/${task.id}`);
  }
}

/** Whether a failed write stopped part way (its task has progress): the databases written before it are kept, and the
 *  same request finishes it. */
export const resumableWrite = (error: unknown): boolean => Boolean((error as { task?: Row } | null)?.task?.progress);

const KINDS: Record<string, string> = { create: "Creating the job", tables: "Adding tables", databases: "Adding databases" };

/** "Writing 52,000 of 100,200 tables", or what it ended with. A by-database write (its task has progress) shows its
 *  databases; onResume, for a failed one, offers to finish it with the same request. */
export function TaskProgress({ task, onResume }: { task: Row | null; onResume?: () => void }) {
  if (!task) return null;
  const progress: Row | null = task.progress ?? null;
  if (task.status === "failed" && progress && !task.result) {
    const total = Number(progress.databases_total);
    return (
      <Notice tone="bad">Stopped with {fmtInt(progress.databases_written)} of {fmtInt(total)} {plural(total, "database")} written
        {task.error ? `: ${task.error}` : ""}. Those databases are kept. Sending the same write again, with the same request id,
        finishes it, and nothing is written twice.
        {onResume && <div className="toolbar"><button onClick={onResume}>Finish the write</button></div>}</Notice>
    );
  }
  if (task.status === "failed") return <Notice tone="bad">{task.result ? "Not done: " : "Nothing was written: "}{task.error}</Notice>;
  if (task.status === "succeeded") return <Notice tone="ok">Written: {fmtInt(task.total)} {plural(task.total, "table")}.</Notice>;
  if (progress) {
    const total = Number(progress.databases_total);
    return (
      <Notice>
        {KINDS[task.kind] ?? "Writing"}: {progress.stage === "checking" ? <>checking {fmtInt(total)} {plural(total, "database")}</>
          : <>writing database <b>{fmtInt(Math.min(Number(progress.databases_written) + 1, total))}</b> of {fmtInt(total)}</>}
        <progress className="task-progress" max={Number(task.total) || 1} value={Number(task.done) || 0} />
        <span className="sub">One database at a time: each database is written or not, and a stopped write can be finished by sending it
          again. Leaving this page doesn't stop it.</span>
      </Notice>
    );
  }
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
