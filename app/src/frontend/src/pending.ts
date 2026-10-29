// What the job page shows while Databricks has not yet confirmed a change. Every answer comes from the server (the
// run phase on the job, and the delete state), so a button never shows an end state before the work under it has
// ended. Plain functions: the Node tests in app/tests/js read this file as it is.

export type RunPhase = "idle" | "starting" | "running" | "cancelling";

// The server's limits (ingestion_jobs.START_CONFIRM_S and CANCEL_CONFIRM_S), in minutes, for the timeout's wording.
export const START_CONFIRM_MINUTES = 15;
export const CANCEL_CONFIRM_MINUTES = 10;

export interface RunNowState {
  phase?: string;
  timed_out?: boolean;
  run_url?: string | null;
  blocked?: string | null;
  refused?: string | null;
}

export interface DeleteState {
  state: string;
  reason?: string | null;
  run_again?: boolean;
  drop_targets?: boolean;
  run?: { url?: string | null } | null;
}

export interface ButtonState {
  label: string;
  tone: "primary" | "danger";
  disabled: boolean;
  action: "run" | "cancel" | null;
  title: string | null;
}

/** The job's Run now or Cancel run button. `pending` is true while this page's own request is still in flight, so
 * the button waits from the moment it is pressed. Cancelling wins over starting, and a run only becomes Cancel run
 * once Databricks reports it running. */
export function runButton(runNow: RunNowState | undefined, pending: { starting: boolean; cancelling: boolean }): ButtonState {
  const phase = runNow?.phase ?? "idle";
  if (pending.cancelling || phase === "cancelling") {
    return { label: "Cancelling…", tone: "primary", disabled: true, action: null,
             title: "Waiting for Databricks to report every run of this job ended" };
  }
  if (pending.starting || phase === "starting") {
    return { label: "Starting…", tone: "primary", disabled: true, action: null,
             title: "Waiting for Databricks to report the run going" };
  }
  if (phase === "running") {
    return { label: "Cancel run", tone: "danger", disabled: !!runNow?.refused, action: "cancel",
             title: runNow?.refused ?? "Cancel the run of this job going now" };
  }
  return { label: "Run now", tone: "primary", disabled: !!runNow?.blocked, action: "run", title: runNow?.blocked ?? null };
}

/** Whether the page should poll quickly: a start or a cancel is pending, here or in Databricks. */
export function changeInProgress(runNow: RunNowState | undefined, pendingRequest: boolean): boolean {
  return pendingRequest || (runNow?.phase ?? "idle") !== "idle";
}

/** The plain notice when Databricks has not confirmed a start or a cancel in time, or null while it is within its
 * limit. Run now stays off after it: the page never shows a run as ended before Databricks says so. */
export function runTimeoutNotice(runNow: RunNowState | undefined): string | null {
  if (!runNow?.timed_out) return null;
  const open = runNow.run_url ? " Open the run in Databricks to check it." : "";
  if (runNow.phase === "cancelling") {
    return `Databricks has not confirmed the cancel after ${CANCEL_CONFIRM_MINUTES} minutes: its runs are still listed as going, so Run now stays off until Databricks reports them ended.${open}`;
  }
  return `Databricks has not confirmed that the run started after ${START_CONFIRM_MINUTES} minutes: it is still listed as starting, so Run now stays off until Databricks reports it running or ended.${open}`;
}

/** The notice for a deleted job whose Delete is pending or failed, with whether Run again is offered; null when the
 * job is not deleted (or its delete is finished, when it has left the list). */
export function deleteNotice(deleteState: DeleteState | null | undefined): { tone: "info" | "bad"; text: string; runAgain: boolean } | null {
  if (!deleteState) return null;
  if (deleteState.state === "deleting") {
    return { tone: "info", text: "Deleting: the job's delete job is running, and the job stays here until it has finished. "
             + "Its runs and tables can't change meanwhile.", runAgain: false };
  }
  if (deleteState.state === "failed") {
    const why = deleteState.reason ? ` ${deleteState.reason}` : "";
    return { tone: "bad", text: `Delete failed.${why}`, runAgain: !!deleteState.run_again };
  }
  return null;
}
