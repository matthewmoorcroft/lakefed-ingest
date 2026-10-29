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

/** Whether a request got no answer the app can rely on: the gateway gave up (504), or the connection dropped. The write
 * behind it may still have finished, so the page reads it as not confirmed, never as failed or done. */
export function isUnconfirmed(error: unknown): boolean {
  if ((error as { status?: number } | null)?.status === 504) return true;
  return error instanceof TypeError && /fetch|network/i.test(error.message);
}

/** A write sent as one request (a small one, not a task). It is pending until its answer, which comes once its tables
 * are written and the job's Databricks jobs are synced; an unconfirmed answer is a warning, not a failure. Null when the
 * page has nothing to say (the write is done, or it failed with its own message). */
export function requestWriteNotice(what: string, state: { pending: boolean; error: unknown }): { tone: "info" | "warn"; text: string } | null {
  if (state.pending) {
    return { tone: "info", text: `${what}: writing the tables, then syncing the job's Databricks jobs. It shows as done once both have finished.` };
  }
  if (isUnconfirmed(state.error)) {
    return { tone: "warn", text: `${what}: not confirmed. The app did not answer in time, so the write may have finished. `
             + "Check the job's tables before sending it again." };
  }
  return null;
}

/** A setup run as the server reports it (GET /api/jobs/setup/runs/{id}): pending until Databricks ends the run. */
export interface SetupRunOutcome {
  state: "pending" | "succeeded" | "failed" | "timeout";
  reason?: string | null;
  url?: string | null;
}

/** Whether Run setup job stays off: while its start is in flight, while Databricks lists the run going, and after a
 * timeout, since Databricks has not confirmed the run ended. */
export function setupRunBlocks(outcome: SetupRunOutcome | null | undefined, starting: boolean): boolean {
  return starting || outcome?.state === "pending" || outcome?.state === "timeout";
}

/** The Schema setup section's line for its run: the start in flight, a start that failed or was not confirmed, then the
 * run's own state. Null when there is nothing to say yet. */
export function setupRunNotice(outcome: SetupRunOutcome | null | undefined, start: { starting: boolean; error: unknown }):
  { tone: "info" | "ok" | "warn" | "bad"; text: string } | null {
  if (start.starting) {
    return { tone: "info", text: "Starting the setup job. It shows as done once Databricks reports its run ended." };
  }
  if (start.error) {
    if (isUnconfirmed(start.error)) {
      return { tone: "warn", text: "Setup job start not confirmed. The app did not answer in time, so the run may have started. "
               + "Check the setup job in Databricks before running it again." };
    }
    const message = start.error instanceof Error ? start.error.message : String(start.error);
    return { tone: "bad", text: `Setup job did not start: ${message}.` };
  }
  if (!outcome) return null;
  switch (outcome.state) {
    case "pending":
      return { tone: "info", text: "Setup job running. It shows as done once Databricks reports its run ended." };
    case "succeeded":
      return { tone: "ok", text: "Setup job succeeded." };
    case "failed":
      return { tone: "bad", text: `Setup job failed: ${outcome.reason || "Databricks gave no reason"}.` };
    case "timeout":
      return { tone: "warn", text: `Setup job not confirmed: ${outcome.reason || "Databricks has not reported it ended"}. `
               + "Run setup job stays off until Databricks reports it ended." };
  }
}

/** The wrapper jobs a save could not sync, as the page names them ("sales: job is locked"). Empty when all synced. */
export function syncFailureLines(results: { task_collection?: string | null; action?: string; detail?: string }[] | undefined): string[] {
  return (results ?? []).filter((r) => r.action === "error")
    .map((r) => (r.task_collection ? `${r.task_collection}: ${r.detail || "its Databricks job was not synced"}` : r.detail || "a Databricks job was not synced"));
}

/** The Edit view's line for a Save. Saved only once the table settings are written, the wrapper jobs are synced, and the
 * cluster copies run has ended; until then the page reads pending, and a failed copy or sync is said plainly. */
export function saveNotice(state: {
  pending: boolean;
  done: boolean;
  savedText: string;
  applying: boolean;
  applyTimeout?: string | null;
  applyError?: string | null;
  syncFailures?: string[];
  error?: unknown;
}): { tone: "info" | "ok" | "warn" | "bad"; text: string } | null {
  if (state.error) {
    return isUnconfirmed(state.error)
      ? { tone: "warn", text: "Saving: not confirmed. The app did not answer in time, so the save may have finished. "
          + "Reload the page to see what was saved." }
      : null;
  }
  if (state.pending) {
    return { tone: "info", text: "Saving: writing the changes and syncing the job's Databricks jobs. It shows as saved once both have finished." };
  }
  if (state.applying) {
    if (state.applyTimeout) {
      return { tone: "warn", text: `Saving: not confirmed yet. The cluster copies run has not ended: ${state.applyTimeout}. `
               + "It is not saved until Databricks reports the run ended." };
    }
    return { tone: "info", text: "Saving: applying the job cluster. The cluster copies job is making this job's copy; "
             + "it shows as saved once Databricks reports that run ended." };
  }
  if (!state.done) return null;
  if (state.applyError) {
    return { tone: "bad", text: `Saved, but the job cluster was not applied: ${state.applyError}. The job's Databricks jobs `
             + "still use the cluster they had before. Change the cluster and save again to retry." };
  }
  const failures = state.syncFailures ?? [];
  if (failures.length) {
    return { tone: "bad", text: `Saved, but ${failures.length === 1 ? "a Databricks job did" : `${failures.length} Databricks jobs did`} `
             + `not sync: ${failures.join("; ")}. Save again to retry.` };
  }
  return { tone: "ok", text: state.savedText };
}

/** A re-discovery's progress as the server reports it (rediscovery.progress): pending while its discovery run goes,
 * adding while its tables are written, then done, failed, or a timeout that is not done. */
export interface RediscoveryProgress {
  state: "pending" | "adding" | "done" | "failed" | "timeout";
  found?: number;
  added?: number;
  note?: string | null;
  reason?: string | null;
  server?: string | null;
  url?: string | null;
}

/** Whether a re-discovery is still going: its request in flight, its discovery run or table writes not ended, or a
 * timeout (not confirmed, so not done). While it is, Run it now stays off and the page polls. */
export function rediscoveryBusy(progress: RediscoveryProgress | null | undefined, requesting: boolean): boolean {
  return requesting || progress?.state === "pending" || progress?.state === "adding" || progress?.state === "timeout";
}

/** The job page's Run it now for a re-discovery: off while one is going, with the reason in its title. */
export function rediscoveryButton(progress: RediscoveryProgress | null | undefined, requesting: boolean, refused?: string | null):
  { label: string; disabled: boolean; title: string | null } {
  if (requesting) return { label: "Starting…", disabled: true, title: "Waiting for the re-discovery to start" };
  if (progress?.state === "pending") return { label: "Running…", disabled: true, title: "Waiting for Databricks to report the discovery run ended" };
  if (progress?.state === "adding") return { label: "Adding tables…", disabled: true, title: "Waiting for the tables to be written" };
  if (progress?.state === "timeout") return { label: "Run it now", disabled: true, title: "Not confirmed yet: it stays off until the re-discovery ends" };
  return { label: "Run it now", disabled: !!refused, title: refused ?? null };
}

/** The re-discovery's line on the job page: what it is doing, or how it ended. Null when none has run. */
export function rediscoveryNotice(progress: RediscoveryProgress | null | undefined, requesting: boolean, error: unknown):
  { tone: "info" | "ok" | "warn" | "bad"; text: string } | null {
  if (requesting) return { tone: "info", text: "Starting the re-discovery. It shows as done once its tables are added." };
  if (error) return { tone: "bad", text: `Re-discovery did not start: ${error instanceof Error ? error.message : String(error)}.` };
  if (!progress) return null;
  switch (progress.state) {
    case "pending":
      return { tone: "info", text: `Re-discovery running: reading ${progress.server ?? "the job's servers"} for new databases and tables. `
               + "It shows as done once Databricks reports its discovery run ended." };
    case "adding":
      return { tone: "info", text: "Re-discovery adding tables. It shows as done once the tables are written." };
    case "timeout":
      return { tone: "warn", text: `Re-discovery not confirmed: ${progress.reason || "Databricks has not reported it ended"}. `
               + "Run it now stays off until it ends." };
    case "failed":
      return { tone: "bad", text: `Re-discovery failed: ${progress.reason || "no reason was recorded"}.` };
    case "done":
      if (progress.note) return { tone: "ok", text: `Re-discovery done: ${progress.note}.` };
      return { tone: "ok", text: `Re-discovery done: found ${progress.found ?? 0} to add, added ${progress.added ?? 0}.` };
  }
}
