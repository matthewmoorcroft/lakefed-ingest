// Tests for the job page's pending-state helpers (app/src/frontend/src/pending.ts). Run with `node --test` (Node 22.18+
// strips the types in pending.ts as it loads it). frontend_pending_test.py runs this file from pytest.
import { test } from "node:test";
import assert from "node:assert/strict";
import { changeInProgress, connectionNextBlock, deleteNotice, discoveryWait, waitingSince, isUnconfirmed, requestWriteNotice, runButton, runTimeoutNotice, rediscoveryBusy, rediscoveryButton, rediscoveryNotice, saveNotice, setupRunBlocks, setupRunNotice, syncFailureLines }
  from "../../src/frontend/src/pending.ts";

const IDLE = { phase: "idle", timed_out: false, run_url: null, blocked: null, refused: null };
const NOTHING_PENDING = { starting: false, cancelling: false };

test("an idle job offers Run now, enabled", () => {
  const button = runButton(IDLE, NOTHING_PENDING);
  assert.deepEqual(button, { label: "Run now", tone: "primary", disabled: false, action: "run", title: null });
});

test("a job that the server blocks from running shows Run now off with the reason", () => {
  const button = runButton({ ...IDLE, blocked: "Another job of this schedule is going" }, NOTHING_PENDING);
  assert.equal(button.label, "Run now");
  assert.equal(button.disabled, true);
  assert.equal(button.title, "Another job of this schedule is going");
});

test("Run now reads Starting… from the moment it is pressed, before the server phase arrives", () => {
  const button = runButton(IDLE, { starting: true, cancelling: false });
  assert.equal(button.label, "Starting…");
  assert.equal(button.disabled, true);
  assert.equal(button.action, null);
});

test("Run now reads Starting… while the server says a run is starting, and stays off", () => {
  const button = runButton({ ...IDLE, phase: "starting" }, NOTHING_PENDING);
  assert.equal(button.label, "Starting…");
  assert.equal(button.disabled, true);
});

test("a run Databricks reports going turns the button into Cancel run", () => {
  const button = runButton({ ...IDLE, phase: "running", run_url: "https://dbx/run/1" }, NOTHING_PENDING);
  assert.equal(button.label, "Cancel run");
  assert.equal(button.tone, "danger");
  assert.equal(button.disabled, false);
  assert.equal(button.action, "cancel");
});

test("Cancel run is off with the refusal when the server refuses a cancel", () => {
  const button = runButton({ ...IDLE, phase: "running", refused: "You can't cancel this job's runs" }, NOTHING_PENDING);
  assert.equal(button.label, "Cancel run");
  assert.equal(button.disabled, true);
  assert.equal(button.title, "You can't cancel this job's runs");
});

test("Cancel run reads Cancelling… from the moment it is pressed, and stays off until every run has ended", () => {
  const pressed = runButton({ ...IDLE, phase: "running" }, { starting: false, cancelling: true });
  assert.equal(pressed.label, "Cancelling…");
  assert.equal(pressed.disabled, true);
  const terminating = runButton({ ...IDLE, phase: "cancelling" }, NOTHING_PENDING);
  assert.equal(terminating.label, "Cancelling…");
  assert.equal(terminating.disabled, true);
  assert.equal(terminating.action, null);
});

test("cancelling wins over starting, so a stop never shows as a start", () => {
  const button = runButton({ ...IDLE, phase: "starting" }, { starting: true, cancelling: true });
  assert.equal(button.label, "Cancelling…");
});

test("Run now is offered only when the server reports the job idle", () => {
  assert.equal(runButton(IDLE, NOTHING_PENDING).action, "run");
  assert.equal(runButton(undefined, NOTHING_PENDING).label, "Run now");
});

test("a job with no run phase at all reads as idle", () => {
  assert.equal(runButton(undefined, NOTHING_PENDING).label, "Run now");
});

test("changeInProgress is true while a request is in flight or the server shows a change", () => {
  assert.equal(changeInProgress(IDLE, true), true);
  assert.equal(changeInProgress({ ...IDLE, phase: "starting" }, false), true);
  assert.equal(changeInProgress({ ...IDLE, phase: "cancelling" }, false), true);
  assert.equal(changeInProgress({ ...IDLE, phase: "running" }, false), true);
  assert.equal(changeInProgress(IDLE, false), false);
  assert.equal(changeInProgress(undefined, false), false);
});

test("no timeout notice while the change is within its limit", () => {
  assert.equal(runTimeoutNotice(IDLE), null);
  assert.equal(runTimeoutNotice({ ...IDLE, phase: "starting" }), null);
  assert.equal(runTimeoutNotice(undefined), null);
});

test("a start Databricks has not confirmed in time says so, and that Run now stays off", () => {
  const notice = runTimeoutNotice({ ...IDLE, phase: "starting", timed_out: true });
  assert.match(notice, /after 15 minutes/);
  assert.match(notice, /Run now stays off/);
  assert.match(notice, /has not confirmed/);
  assert.doesNotMatch(notice, /Open the run/);
});

test("a cancel Databricks has not confirmed in time says so, with a link to the run when there is one", () => {
  const notice = runTimeoutNotice({ ...IDLE, phase: "cancelling", timed_out: true, run_url: "https://dbx/run/2" });
  assert.match(notice, /after 10 minutes/);
  assert.match(notice, /still listed as going/);
  assert.match(notice, /Run now stays off/);
  assert.match(notice, /Open the run in Databricks to check it\./);
});

test("the timeout never says the run has ended", () => {
  const notice = runTimeoutNotice({ ...IDLE, phase: "cancelling", timed_out: true });
  assert.doesNotMatch(notice, /has ended|stopped|cancelled\./);
});

test("the button stays off after a timeout until Databricks reports the change", () => {
  const timedOut = { ...IDLE, phase: "starting", timed_out: true };
  assert.equal(runButton(timedOut, NOTHING_PENDING).disabled, true);
  assert.equal(changeInProgress(timedOut, false), true);
});

test("no delete notice for a job that is not deleted", () => {
  assert.equal(deleteNotice(undefined), null);
  assert.equal(deleteNotice(null), null);
});

test("a deleting job says it is deleting, offers no Run again, and is not a failure", () => {
  const notice = deleteNotice({ state: "deleting", reason: null, run_again: false, run: null });
  assert.equal(notice.tone, "info");
  assert.equal(notice.runAgain, false);
  assert.match(notice.text, /^Deleting: /);
  assert.match(notice.text, /stays here until it has finished/);
});

test("a failed delete shows its reason and offers Run again when the server says so", () => {
  const notice = deleteNotice({ state: "failed", reason: "table x would not drop", run_again: true });
  assert.equal(notice.tone, "bad");
  assert.equal(notice.text, "Delete failed. table x would not drop");
  assert.equal(notice.runAgain, true);
});

test("a failed delete with no tables left shows the failure without Run again", () => {
  const notice = deleteNotice({ state: "failed", reason: "the delete job ended INTERNAL_ERROR", run_again: false });
  assert.equal(notice.runAgain, false);
  assert.equal(notice.tone, "bad");
});

test("a failed delete with no reason still says it failed", () => {
  const notice = deleteNotice({ state: "failed", reason: null, run_again: true });
  assert.equal(notice.text, "Delete failed.");
});

test("a gone delete has no notice: the job has left the list", () => {
  assert.equal(deleteNotice({ state: "gone", reason: null, run_again: false }), null);
});

test("a write sent as one request is pending until its answer, which comes after its jobs are synced", () => {
  const notice = requestWriteNotice("Adding databases", { pending: true, error: null });
  assert.equal(notice.tone, "info");
  assert.match(notice.text, /^Adding databases: writing the tables, then syncing/);
  assert.match(notice.text, /shows as done once both have finished/);
});

test("a write that finished or failed with its own message has no pending notice", () => {
  assert.equal(requestWriteNotice("Adding tables", { pending: false, error: null }), null);
  assert.equal(requestWriteNotice("Adding tables", { pending: false, error: { status: 500, message: "Not done" } }), null);
});

test("a gateway timeout is not confirmed: the write may have finished, so it is a warning and says to check first", () => {
  const notice = requestWriteNotice("Adding tables", { pending: false, error: { status: 504, message: "timeout" } });
  assert.equal(notice.tone, "warn");
  assert.match(notice.text, /^Adding tables: not confirmed/);
  assert.match(notice.text, /Check the job's tables before sending it again/);
});

test("a dropped connection is not confirmed either", () => {
  assert.equal(isUnconfirmed(new TypeError("Failed to fetch")), true);
  assert.equal(isUnconfirmed(new TypeError("Cannot read properties of undefined")), false);
  assert.equal(isUnconfirmed({ status: 500 }), false);
  assert.equal(isUnconfirmed(null), false);
});

const NO_START = { starting: false, error: null };
const SETUP_LINK = "https://h/run/41";

test("Run setup job is off from the moment it is pressed until Databricks reports the run ended", () => {
  assert.equal(setupRunBlocks(undefined, true), true);
  assert.equal(setupRunBlocks({ state: "pending", url: SETUP_LINK }, false), true);
  assert.equal(setupRunBlocks({ state: "timeout", reason: "still listed as running" }, false), true);
  assert.equal(setupRunBlocks({ state: "succeeded" }, false), false);
  assert.equal(setupRunBlocks({ state: "failed", reason: "no" }, false), false);
  assert.equal(setupRunBlocks(null, false), false);
});

test("a setup run that is starting shows pending, with no end state", () => {
  const notice = setupRunNotice(undefined, { starting: true, error: null });
  assert.equal(notice.tone, "info");
  assert.match(notice.text, /Starting the setup job/);
});

test("a setup run Databricks still lists as going shows pending", () => {
  const notice = setupRunNotice({ state: "pending", url: SETUP_LINK }, NO_START);
  assert.deepEqual(notice, { tone: "info", text: "Setup job running. It shows as done once Databricks reports its run ended." });
});

test("a setup run that ended with success shows succeeded", () => {
  assert.deepEqual(setupRunNotice({ state: "succeeded" }, NO_START), { tone: "ok", text: "Setup job succeeded." });
});

test("a setup run that failed shows failed with Databricks' message", () => {
  const notice = setupRunNotice({ state: "failed", reason: "schema migration lock timed out" }, NO_START);
  assert.deepEqual(notice, { tone: "bad", text: "Setup job failed: schema migration lock timed out." });
});

test("a setup run past its limit shows a timeout that is not done, and keeps the button off", () => {
  const reason = "Databricks has not reported it ended after 30 minutes: it is still listed as running";
  const notice = setupRunNotice({ state: "timeout", reason }, NO_START);
  assert.equal(notice.tone, "warn");
  assert.match(notice.text, /^Setup job not confirmed: Databricks has not reported it ended after 30 minutes/);
  assert.match(notice.text, /Run setup job stays off until Databricks reports it ended\.$/);
  assert.equal(setupRunBlocks({ state: "timeout", reason }, false), true);
});

test("a setup start that did not answer is not confirmed, not failed", () => {
  const notice = setupRunNotice(undefined, { starting: false, error: Object.assign(new Error("gateway"), { status: 504 }) });
  assert.equal(notice.tone, "warn");
  assert.match(notice.text, /may have started/);
});

test("a setup start the server refused shows failed to start with its message", () => {
  const notice = setupRunNotice(undefined, { starting: false, error: new Error("the setup job is not configured") });
  assert.deepEqual(notice, { tone: "bad", text: "Setup job did not start: the setup job is not configured." });
});

test("with nothing started here there is nothing to say", () => {
  assert.equal(setupRunNotice(undefined, NO_START), null);
});

const SAVED = "Saved: 2 tables changed.";

test("a save is pending while its settings are written and its wrapper jobs synced", () => {
  const notice = saveNotice({ pending: true, done: false, savedText: SAVED, applying: false });
  assert.equal(notice.tone, "info");
  assert.match(notice.text, /syncing the job's Databricks jobs/);
});

test("a save is not done while the cluster copies run is still going", () => {
  const notice = saveNotice({ pending: false, done: true, savedText: SAVED, applying: true });
  assert.equal(notice.tone, "info");
  assert.match(notice.text, /shows as saved once Databricks reports that run ended/);
});

test("a save whose cluster copies run is past its limit says not confirmed, not saved", () => {
  const notice = saveNotice({ pending: false, done: true, savedText: SAVED, applying: true,
    applyTimeout: "Databricks has not reported it ended after 30 minutes: it is still listed as running" });
  assert.equal(notice.tone, "warn");
  assert.match(notice.text, /not confirmed yet/);
  assert.match(notice.text, /It is not saved until Databricks reports the run ended\.$/);
});

test("a save is done with its summary once everything under it has finished", () => {
  assert.deepEqual(saveNotice({ pending: false, done: true, savedText: SAVED, applying: false }),
    { tone: "ok", text: SAVED });
});

test("a save whose cluster copies run failed says the cluster was not applied, plainly", () => {
  const notice = saveNotice({ pending: false, done: true, savedText: SAVED, applying: false,
    applyError: "the cluster copies run did not apply the cluster: node type unavailable" });
  assert.equal(notice.tone, "bad");
  assert.equal(notice.text, "Saved, but the job cluster was not applied: the cluster copies run did not apply the cluster: "
    + "node type unavailable. The job's Databricks jobs still use the cluster they had before. Change the cluster and save again to retry.");
});

test("a save whose wrapper jobs did not sync names each one", () => {
  const failures = syncFailureLines([{ task_collection: "sales", action: "error", detail: "job is locked" },
    { task_collection: "hr", action: "updated", detail: "" }]);
  assert.deepEqual(failures, ["sales: job is locked"]);
  const notice = saveNotice({ pending: false, done: true, savedText: SAVED, applying: false, syncFailures: failures });
  assert.deepEqual(notice, { tone: "bad", text: "Saved, but a Databricks job did not sync: sales: job is locked. Save again to retry." });
  const two = saveNotice({ pending: false, done: true, savedText: SAVED, applying: false, syncFailures: ["a: x", "b: y"] });
  assert.match(two.text, /^Saved, but 2 Databricks jobs did not sync: a: x; b: y\./);
});

test("a save whose answer never came is not confirmed, and never a failure", () => {
  const notice = saveNotice({ pending: false, done: false, savedText: SAVED, applying: false,
    error: Object.assign(new Error("gateway"), { status: 504 }) });
  assert.equal(notice.tone, "warn");
  assert.match(notice.text, /may have finished/);
});

test("a save the server refused shows no notice here, the error box carries it", () => {
  assert.equal(saveNotice({ pending: false, done: false, savedText: SAVED, applying: false, error: new Error("422") }), null);
});

const RUNNING_HERE = { state: "pending", server: "h1", url: "https://h/run/5" };

test("a re-discovery is busy from its request until its tables are written, and through a timeout", () => {
  assert.equal(rediscoveryBusy(undefined, true), true);
  assert.equal(rediscoveryBusy(RUNNING_HERE, false), true);
  assert.equal(rediscoveryBusy({ state: "adding" }, false), true);
  assert.equal(rediscoveryBusy({ state: "timeout", reason: "still listed as running" }, false), true);
  assert.equal(rediscoveryBusy({ state: "done", found: 1, added: 1 }, false), false);
  assert.equal(rediscoveryBusy({ state: "failed", reason: "no" }, false), false);
  assert.equal(rediscoveryBusy(null, false), false);
});

test("Run it now is off while the discovery run goes, and says so", () => {
  const button = rediscoveryButton(RUNNING_HERE, false);
  assert.deepEqual(button, { label: "Running…", disabled: true, title: "Waiting for Databricks to report the discovery run ended" });
});

test("Run it now is off while the tables are written", () => {
  assert.equal(rediscoveryButton({ state: "adding" }, false).label, "Adding tables…");
  assert.equal(rediscoveryButton({ state: "adding" }, false).disabled, true);
});

test("Run it now stays off after a timeout until the re-discovery ends", () => {
  const button = rediscoveryButton({ state: "timeout", reason: "still listed as running" }, false);
  assert.equal(button.disabled, true);
  assert.match(button.title, /stays off until the re-discovery ends/);
});

test("Run it now is on once the re-discovery is done or failed, and off when the job cannot change", () => {
  assert.deepEqual(rediscoveryButton({ state: "done", found: 0, added: 0 }, false), { label: "Run it now", disabled: false, title: null });
  assert.equal(rediscoveryButton({ state: "failed", reason: "x" }, false).disabled, false);
  assert.deepEqual(rediscoveryButton(null, false, "the job is being deleted"),
    { label: "Run it now", disabled: true, title: "the job is being deleted" });
});

test("a re-discovery whose discovery run goes shows pending with its server and no end state", () => {
  const notice = rediscoveryNotice(RUNNING_HERE, false, null);
  assert.equal(notice.tone, "info");
  assert.equal(notice.text, "Re-discovery running: reading h1 for new databases and tables. It shows as done once Databricks reports its discovery run ended.");
});

test("a re-discovery that is adding tables says so", () => {
  assert.deepEqual(rediscoveryNotice({ state: "adding" }, false, null),
    { tone: "info", text: "Re-discovery adding tables. It shows as done once the tables are written." });
});

test("a finished re-discovery says what it found and added", () => {
  assert.deepEqual(rediscoveryNotice({ state: "done", found: 3, added: 2 }, false, null),
    { tone: "ok", text: "Re-discovery done: found 3 to add, added 2." });
  assert.deepEqual(rediscoveryNotice({ state: "done", found: 0, added: 0, note: "the job's selection is specific tables, which add nothing" }, false, null),
    { tone: "ok", text: "Re-discovery done: the job's selection is specific tables, which add nothing." });
});

test("a failed re-discovery says why", () => {
  assert.deepEqual(rediscoveryNotice({ state: "failed", reason: "discovery run ended FAILED: no route" }, false, null),
    { tone: "bad", text: "Re-discovery failed: discovery run ended FAILED: no route." });
});

test("a timed-out re-discovery is not confirmed, not failed", () => {
  const notice = rediscoveryNotice({ state: "timeout", reason: "it has gone on for more than 6 hours" }, false, null);
  assert.equal(notice.tone, "warn");
  assert.match(notice.text, /^Re-discovery not confirmed: it has gone on for more than 6 hours\. Run it now stays off/);
});

test("a re-discovery whose start failed says so, and nothing when none has run", () => {
  assert.deepEqual(rediscoveryNotice(null, false, new Error("409 already running")),
    { tone: "bad", text: "Re-discovery did not start: 409 already running." });
  assert.equal(rediscoveryNotice(null, false, null), null);
});

// Local times, as the page shows them: 20:42 and 20:45 on 29 Oct.
const ASKED = new Date(2026, 9, 29, 20, 42, 46);
const RUNNING_FROM = new Date(2026, 9, 29, 20, 45, 31);
const LATER = new Date(2026, 9, 29, 20, 45, 30);

test("a discovery run says what it waits for, for how long and since when, from its state", () => {
  assert.equal(discoveryWait(null, ASKED, "Reading the server's databases", ASKED),
               "Starting the discovery job: 0 s so far, since 20:42");
  for (const state of ["QUEUED", "PENDING", "BLOCKED", "WAITING_FOR_RETRY"]) {
    assert.equal(discoveryWait({ state, since: ASKED }, ASKED, "Reading the server's databases", LATER),
                 "Waiting for compute: 2 min 44 s so far, since 20:42. The discovery job's compute is starting (its cluster "
                 + "stops after an hour unused), which can take a few minutes");
  }
  const reading = { state: "RUNNING", since: RUNNING_FROM };
  assert.equal(discoveryWait(reading, ASKED, "Reading the server's databases", new Date(2026, 9, 29, 20, 45, 47)),
               "Reading the server's databases: 16 s so far, since 20:45");
  assert.equal(discoveryWait({ state: "TERMINATING", since: RUNNING_FROM }, ASKED, "Reading", RUNNING_FROM),
               "Reading: 0 s so far, since 20:45");
});

test("the time a state began is kept while the state holds, and moves when it changes", () => {
  const first = waitingSince(null, "PENDING", ASKED);
  assert.deepEqual(first, { state: "PENDING", since: ASKED });
  assert.equal(waitingSince(first, "PENDING", RUNNING_FROM), first);
  assert.equal(waitingSince(first, "QUEUED", RUNNING_FROM), first, "queued and pending are one wait for compute");
  assert.deepEqual(waitingSince(first, "RUNNING", RUNNING_FROM), { state: "RUNNING", since: RUNNING_FROM });
});

test("times before ten in the morning keep their leading zero, and an hour reads as minutes", () => {
  const at = new Date(2026, 9, 29, 9, 5);
  assert.equal(discoveryWait({ state: "RUNNING", since: at }, at, "Reading", new Date(2026, 9, 29, 10, 6, 3)),
               "Reading: 61 min 3 s so far, since 09:05");
});

test("Next on the connection step says it needs a passed test, and what the test is doing while it runs", () => {
  assert.equal(connectionNextBlock(false, null), "Next needs a passed connection test: choose a connection, then Test connection");
  assert.equal(connectionNextBlock(false, "Waiting for compute: 5 s so far, since 20:42"),
               "Next needs a passed connection test. Testing now: Waiting for compute: 5 s so far, since 20:42");
  assert.equal(connectionNextBlock(false, discoveryWait({ state: "PENDING", since: ASKED }, ASKED, "Reading", LATER)),
               "Next needs a passed connection test. Testing now: Waiting for compute: 2 min 44 s so far, since 20:42");
  assert.equal(connectionNextBlock(true, null), undefined);
});
