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

/** Whether a failed write can be finished by sending it again with the same request id: one that stopped part way (its
 *  task has progress, and the databases written before it are kept), or one whose rows committed but whose Databricks
 *  jobs did not sync (its task keeps its result). */
export const resumableWrite = (error: unknown): boolean => {
  const task = (error as { task?: Row } | null)?.task;
  return Boolean(task?.progress || task?.result);
};

const KINDS: Record<string, string> = { create: "Creating the job", tables: "Adding tables", databases: "Adding databases" };

/** "Writing 52,000 of 100,200 tables", or what it ended with. A by-database write (its task has progress) shows its
 *  databases; onResume, for a failed one, offers to finish it with the same request. */
export function TaskProgress({ task, onResume }: { task: Row | null; onResume?: () => void }) {
  if (!task) return null;
  const progress: Row | null = task.progress ?? null;
  // A database-only file's size is unknown until its databases are discovered, so its task has no database count.
  const byDatabase = progress?.databases_total != null;
  if (task.status === "failed" && progress && byDatabase && !task.result) {
    const total = Number(progress.databases_total);
    return (
      <Notice tone="bad">Stopped with {fmtInt(progress.databases_written)} of {fmtInt(total)} {plural(total, "database")} written
        {task.error ? `: ${task.error}` : ""}. Those databases are kept. Sending the same write again, with the same request id,
        finishes it, and nothing is written twice.
        {onResume && <div className="toolbar"><button onClick={onResume}>Finish the write</button></div>}</Notice>
    );
  }
  if (task.status === "failed") return (
    <Notice tone="bad">{task.result ? "Not done: " : "Nothing was written: "}{task.error}
      {task.result && onResume && <div className="toolbar"><button onClick={onResume}>Finish the write</button>
        <span className="sub">Sending the same write again syncs its Databricks jobs and writes nothing twice.</span></div>}</Notice>
  );
  if (task.status === "succeeded") {
    const tables = task.result?.summary?.tables ?? task.total;
    return <Notice tone="ok">Written: {fmtInt(tables)} {plural(tables, "table")}.</Notice>;
  }
  if (progress?.stage === "discovering") {
    return (
      <Notice>
        {KINDS[task.kind] ?? "Writing"}: Discovering the tables of the databases
        <span className="sub">Each database is read through the connection by name. Leaving this page doesn't stop it.</span>
      </Notice>
    );
  }
  if (progress && byDatabase) {
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
  const total = Number(task.total);
  return (
    <Notice>
      {KINDS[task.kind] ?? "Writing"}: {Number(task.done) > 0 || progress?.stage === "writing"
        ? <>writing <b>{fmtInt(task.done)}</b>{total > 0 ? <> of {fmtInt(total)}</> : null} {plural(total > 0 ? total : Number(task.done), "table")}</>
        : <>checking {fmtInt(task.total)} {plural(task.total, "table")}</>}
      {total > 0 && <progress className="task-progress" max={total} value={Number(task.done) || 0} />}
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
