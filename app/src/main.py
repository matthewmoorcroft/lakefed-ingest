"""lakefed_ingest operations app (optional): FastAPI backend + React frontend (built into ./static).

JSON API under /api, used by the React app; everything else serves the single-page app. Data access
runs as the app's service principal: Lakebase through lakebase.py, Jobs API through jobs.py. All SQL
is in queries.py (parameterized, unit-tested).

Run locally: `python main.py` (DATABRICKS_CONFIG_PROFILE, LAKEBASE_INSTANCE, LAKEBASE_DATABASE,
INGEST_JOB_ID, SETUP_JOB_ID, DISCOVER_JOB_ID), with the frontend built (`npm run build`) or served by
`npm run dev` (proxies /api here).
"""

from __future__ import annotations

import json
import logging
import os
import re
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
import threading
import time
import uuid
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator, model_validator

import action_states
import compute
import config_import as ci
import ingestion_jobs as ij
import job_settings as js
import job_settings_sql as jss
import jobs
import by_database
import lakebase as db
import new_job as nj
import queries as q
import rediscovery
import rollup
import run_poller
import schedules as sch


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    """The app's background work while it runs: the run poller, which keeps the databricks_run table current (every
    instance runs one, and only the one holding the poller's lock polls; RUN_POLLER=0 turns it off on this instance),
    and the re-discovery scheduler (REDISCOVERY_TICK_S; 0 leaves it off)."""
    if os.environ.get("RUN_POLLER", "1") != "0":
        run_poller.POLLER.start(on_change=_forget_shared_reads)
    if REDISCOVERY_TICK_S > 0:
        threading.Thread(target=_rediscovery_scheduler, daemon=True, name="lakefed-app-rediscovery").start()
    yield


app = FastAPI(title="lakefed_ingest operations", docs_url="/api/docs", openapi_url="/api/openapi.json",
              lifespan=_lifespan)
STATIC = Path(__file__).parent / "static"

# The routes that take a CSV of tables, and the most a request to one may carry: a whole server of 120,000 lines
# (new_job.MAX_LINES) of long select lists fits within it, and a larger upload is refused before it is read, not after.
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(64 * 1024 * 1024)))
UPLOAD_ROUTES = re.compile(r"^/api/(import|uploads|ingestion-jobs|ingestion-jobs/\d+/(tables|databases))/?$")


class UploadLimit:
    """Refuse (413) a request to an upload route whose body is over MAX_UPLOAD_BYTES: by its Content-Length when it
    gives one, else by counting the body as it arrives. Past the limit the rest is never read: the route sees an
    empty end of body, and its answer is replaced with the refusal (FastAPI would turn a raised error into a 400)."""

    # Starlette builds middleware as cls(app=app, ...) in the versions Databricks Apps installs (0.38) and as
    # cls(app, ...) in later ones, so the first parameter must be named app.
    def __init__(self, app, limit: int):
        self.inner, self.limit = app, limit

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] != "POST" or not UPLOAD_ROUTES.match(scope["path"]):
            return await self.inner(scope, receive, send)
        length = dict(scope.get("headers") or []).get(b"content-length")
        if length is not None and length.isdigit() and int(length) > self.limit:
            return await self._refuse(send)
        state = {"seen": 0, "over": False, "refused": False}

        async def counted():
            if state["over"]:
                return {"type": "http.request", "body": b"", "more_body": False}
            message = await receive()
            state["seen"] += len(message.get("body", b""))
            if state["seen"] > self.limit:
                state["over"] = True
                return {"type": "http.request", "body": b"", "more_body": False}
            return message

        async def answered(message):
            if not state["over"]:
                return await send(message)
            if not state["refused"]:
                state["refused"] = True
                await self._refuse(send)
        await self.inner(scope, counted, answered)

    async def _refuse(self, send):
        body = json.dumps({"detail": f"the upload is over {self.limit // (1024 * 1024)} MB: split the file"}).encode()
        await send({"type": "http.response.start", "status": 413,
                    "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]})
        await send({"type": "http.response.body", "body": body})


app.add_middleware(UploadLimit, limit=MAX_UPLOAD_BYTES)


@app.exception_handler(Exception)
async def _errors(request: Request, exc: Exception):
    msg = str(exc)
    hint = None
    if "permission denied" in msg.lower():
        hint = "The app's service principal may lack privileges: run the lakefed_ingest_app_grants job."
    return JSONResponse(status_code=500, content={"detail": msg[:2000], "hint": hint})


def _user(request: Request) -> str:
    return (request.headers.get("X-Forwarded-Email") or request.headers.get("X-Forwarded-User") or "").strip()


@app.middleware("http")
async def _writes_need_a_user(request: Request, call_next):
    """Every change through the API needs to know who makes it: a write with no user from the Apps proxy is refused,
    so no permission check or change log ever runs for an unknown caller. Reads stay open to anyone the proxy admits."""
    if request.method not in ("GET", "HEAD", "OPTIONS") and request.url.path.startswith("/api/") and not _user(request):
        return JSONResponse(status_code=403, content={"detail": NO_IDENTITY})
    return await call_next(request)


def _lock_collections(cur, collections):
    """Take each collection's run lock for this transaction, or refuse: changing tables while their
    collection is running could race the run's own checkpoint writes."""
    for c in sorted({c for c in collections if c}):
        cur.execute(q.collection_lock_sql(), (q.collection_lock_key(c),))
        if not cur.fetchone()[0]:
            raise HTTPException(409, f"ingestion group '{c}' is running; try again after the run finishes")


def _lock_collections_together(cur, collections):
    """_lock_collections for many collections in one statement: each one's lock, or a 409 for the first a run holds."""
    collections = sorted({c for c in collections if c})
    if not collections:
        return
    cur.execute(q.collection_locks_sql(), ([q.collection_lock_key(c) for c in collections],))
    got = {key: taken for key, taken in cur.fetchall()}
    for c in collections:
        if not got.get(q.collection_lock_key(c)):
            raise HTTPException(409, f"ingestion group '{c}' is running; try again after the run finishes")


def _refuse_mixed_sinks(cur, rows):
    """After writing control rows, in the same transaction: refuse when a sink they write would be
    claimed by a dedicated row and by other rows (the next run would stop every table on it)."""
    sinks = sorted({tuple(str(r.get(k)).lower() for k in ("sink_catalog", "sink_schema", "sink_table")) for r in rows})
    if not sinks:
        return
    cur.execute(q.mixed_sinks_for_sql(), tuple([sink[at] for sink in sinks] for at in range(3)))
    mixed = [r[0] for r in cur.fetchall()]
    if mixed:
        raise HTTPException(409, f"sink {mixed[0]} would be claimed by a dedicated row and by other rows; "
                                 "a sink is either dedicated to one source table or shared by client_id")


def _refuse_split_databases(cur, rows):
    """After writing control rows, in the same transaction: refuse when a database they belong to would
    have tables in more than one collection. Its run locks one collection, and the check job reads each
    database once for its collection, so the database's other tables would be checked and ingested apart."""
    keys = sorted({q.database_key(r) for r in rows})
    if not keys:
        return
    cur.execute(q.split_databases_sql(), (keys,))
    split = cur.fetchall()
    if split:
        _, database, collections = split[0]
        raise HTTPException(409, f"database {database} would have tables in more than one ingestion group "
                                 f"({', '.join(collections)}); move all of its tables together")


def _log(cur, user, action, control_id, collection, field=None, old=None, new=None, reseed=None, batch=None):
    as_text = lambda v: None if v is None else str(v)
    cur.execute(q.log_change_sql(), (user or None, action, control_id, collection, field, as_text(old),
                                     as_text(new), reseed, batch))


def _log_lines(cur, lines, per_statement=q.CONTROL_BATCH):
    """Many change log lines, each the values _log takes after cur, per_statement to a statement."""
    as_text = lambda v: None if v is None else str(v)
    lines = [(user or None, action, control_id, collection, field, as_text(old), as_text(new), reseed, batch)
             for user, action, control_id, collection, field, old, new, reseed, batch in lines]
    for at in range(0, len(lines), per_statement):
        chunk = lines[at:at + per_statement]
        cur.execute(q.log_changes_sql(len(chunk)), tuple(v for line in chunk for v in line))


def _taken_target(row: dict) -> str:
    return (f"another table already writes {row['sink_catalog']}.{row['sink_schema']}.{row['sink_table']}"
            + (f" for {row['client_id']}" if row.get("client_id") else ""))


def _insert_controls(cur, rows, user, batch, skip_taken=False, progress=None,
                     log_per_statement=q.CONTROL_BATCH) -> List[tuple]:
    """Insert new control rows q.CONTROL_BATCH to a statement, then their "import" change log lines, log_per_statement
    to a statement, inside the caller's transaction. A row whose target another table writes refuses the lot (409), or
    with skip_taken is left out. Returns (row, id) for each row written; progress(n) is told how many rows are done
    after each statement."""
    written = []
    for at in range(0, len(rows), q.CONTROL_BATCH):
        chunk = rows[at:at + q.CONTROL_BATCH]
        params = [q.control_params(row) for row in chunk]
        cur.execute(q.insert_controls_sql(len(chunk)), tuple(v for p in params for v in p))
        ids = {tuple(found[1:]): found[0] for found in cur.fetchall()}
        now = []
        for row, p in zip(chunk, params):
            control_id = ids.get(q.control_sink_key(p))
            if control_id is not None:
                now.append((row, control_id))
            elif not skip_taken:
                raise HTTPException(409, _taken_target(row))
        written += now
        if progress:
            progress(at + len(chunk))
    _log_lines(cur, [(user, "import", control_id, row["task_collection"], None, None, None, None, batch)
                     for row, control_id in written], log_per_statement)
    _rewrite_rollups(cur, [(row["task_collection"], row.get("src_database")) for row, _ in written])
    return written


def _touch(cur, user, jobs_changed=(), groups=()):
    """Record who last changed these ingestion jobs (or the jobs holding these groups, whose tables changed), and
    when: the job page and the Ingestion jobs list show it as Last updated."""
    if jobs_changed:
        cur.execute(q.touch_jobs_sql(), (user or None, [int(j) for j in jobs_changed]))
    wanted = sorted({g for g in groups if g})
    if wanted:
        cur.execute(q.touch_group_jobs_sql(), (user or None, wanted))


def _rewrite_rollups(cur, databases) -> None:
    """Recompute the monitoring rollups of the databases a transaction changed, given as (ingestion group, source
    database name) pairs, and of their groups, inside the caller's transaction. Each database is counted from its own
    control rows, so the rows are exact after the edit and need no deltas."""
    by_group = {}
    for group, src_database in databases:
        by_group.setdefault(group, set()).add(src_database or "")
    pairs = [(group, name) for group in sorted(by_group) for name in sorted(by_group[group])]
    for at in range(0, len(pairs), q.ROLLUP_CHUNK):
        chunk = pairs[at:at + q.ROLLUP_CHUNK]
        cur.execute(q.rollup_database_rewrite_sql(), ([group for group, _ in chunk], [name for _, name in chunk]))
    if by_group:
        cur.execute(q.rollup_group_rewrite_sql(), (sorted(by_group),))


def _rewrite_rollups_for_ids(cur, ids) -> None:
    """_rewrite_rollups for the databases of some control rows, by id."""
    if ids:
        cur.execute(q.control_databases_sql("ids"), (list(ids),))
        _rewrite_rollups(cur, cur.fetchall())


def _rewrite_rollups_for_groups(cur, groups) -> None:
    """_rewrite_rollups for every database of some ingestion groups."""
    if groups:
        cur.execute(q.control_databases_sql("groups"), (sorted(groups),))
        _rewrite_rollups(cur, cur.fetchall())


def _apply_plans(cur, rows, plans, user, batch):
    """Write planned edits (queries.plan_edit) inside the caller's transaction: each changed field, its change log
    line, a reseed where the plan needs one, then the mixed-sink and split-database checks over the rows as they
    now are."""
    by_id = {r["id"]: r for r in rows}
    before = [(by_id[p["id"]]["task_collection"], by_id[p["id"]].get("src_database")) for p in plans]
    for p in plans:
        if not p["changes"]:
            continue
        fields = list(p["changes"])
        cur.execute(q.update_control_sql(fields), [p["changes"][f][1] for f in fields] + [p["id"]])
        coll = p["changes"].get("task_collection", [None, by_id[p["id"]]["task_collection"]])[1]
        for f in fields:
            _log(cur, user, "edit", p["id"], coll, f, p["changes"][f][0], p["changes"][f][1],
                 reseed=p["reseed"], batch=batch)
        if p["reseed"]:
            cur.execute(q.flip_to_seeding_sql(), (f"reseed after edit: {p['reseed']}", [p["id"]]))
            cur.execute(q.log_reseed_sql(), (f"edit:{p['reseed']}", [p["id"]]))
    edited = [dict(by_id[p["id"]], **{f: v[1] for f, v in p["changes"].items()}) for p in plans]
    _rewrite_rollups(cur, before + [(row["task_collection"], row.get("src_database")) for row in edited])
    _refuse_mixed_sinks(cur, edited)
    _refuse_split_databases(cur, edited)


def _locked_rows(cur, ids):
    cur.execute(q.control_rows_for_update_sql(), (list(ids),))
    cols = [d.name for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    missing = set(ids) - {r["id"] for r in rows}
    if missing:
        raise HTTPException(404, f"control rows not found: {sorted(missing)}")
    return rows


# ---- context / overview -----------------------------------------------------------------------------

@app.get("/api/databricks-runs/{run_id}/open")
def open_databricks_run(run_id: int):
    """Open a Databricks job run's own page (a group run's job_run_id), as its run page link in the Jobs UI does."""
    try:
        url = run_poller.run_state_of(run_id)["url"]
    except Exception as e:
        raise HTTPException(404, f"can't find Databricks job run {run_id}: {str(e).split('. ')[0]}")
    if not url:
        raise HTTPException(404, f"Databricks job run {run_id} has no page")
    return RedirectResponse(url)


@app.get("/api/config")
def config(request: Request):
    return {"lakebase_instance": db.INSTANCE, "lakebase_database": db.DATABASE, "user": _user(request),
            "jobs": {"ingest": jobs.INGEST_JOB_ID, "setup": jobs.SETUP_JOB_ID, "discover": jobs.DISCOVER_JOB_ID},
            "workspace_url": jobs.workspace_url(), "app_principal": jobs.APP_PRINCIPAL,
            "defaults": {"partition_threshold_mb": q.DEFAULT_PARTITION_THRESHOLD_MB,
                         "partition_size_mb": q.DEFAULT_PARTITION_SIZE_MB}}


# Monitoring reads are shared by every viewer and recomputed at most once per MONITOR_CACHE_S seconds,
# so the app's load on Lakebase does not grow with open tabs. 0 turns the cache off.
MONITOR_CACHE_S = max(0, int(os.environ.get("MONITOR_CACHE_S", "15")))
_shared_reads: dict = {}
_shared_lock = threading.Lock()
_shared_computing: dict = {}     # key: the lock its one compute holds, so other requests wait for it
_shared_generation = [0]         # bumped by every committed write: a read begun before it isn't kept


def _shared(key, compute, ttl=None):
    """compute() at most once per `ttl` seconds (MONITOR_CACHE_S by default) for key, shared by every
    request. When it is due, one request computes it and the others asking meanwhile wait for that result
    instead of running the same query. A read that a write committed during is returned but not kept."""
    ttl = MONITOR_CACHE_S if ttl is None else ttl
    if not ttl:
        return compute()

    def fresh():
        hit = _shared_reads.get(key)
        return hit if hit and time.time() - hit[1] < ttl else None

    with _shared_lock:
        hit = fresh()
        if hit:
            return hit[0]
        computing = _shared_computing.setdefault(key, threading.Lock())
    with computing:
        with _shared_lock:
            hit = fresh()
            if hit:
                return hit[0]
            generation = _shared_generation[0]
        value = compute()
        with _shared_lock:
            if generation == _shared_generation[0]:
                _shared_reads[key] = (value, time.time())
    return value


def _forget_shared_reads() -> None:
    """Drop every shared read, so the pages show a change as soon as it is committed rather than up to a minute
    later."""
    with _shared_lock:
        _shared_generation[0] += 1
        _shared_reads.clear()


@contextmanager
def _write():
    """db.transaction, then the shared reads forgotten once it has committed: every write the app makes (tables,
    settings, jobs, schedules, imports) goes through it. The run poller polls again at once, so a new job's runs
    show within a poll."""
    with db.transaction() as cur:
        yield cur
    _forget_shared_reads()
    run_poller.POLLER.wake()


# ---- background writes ---------------------------------------------------------------------------------------------
# A write of more than BACKGROUND_ROWS tables (New's Create, Add tables, a databases file) takes longer than the Apps
# proxy waits (a 100,000-table Create took 7 minutes one row at a time; batched it is under a minute, still too long to
# hold a request on). The request checks what it can, records a task (app_task) and answers 202 at once; a thread
# writes the tables in the one transaction the request would have, and the pages follow the task's progress.
BACKGROUND_ROWS = int(os.environ.get("BACKGROUND_ROWS", "5000"))
TASK_BEAT_S = float(os.environ.get("TASK_BEAT_S", "2"))
# A running task's heartbeat older than this means its thread is gone: the app restarted while it wrote.
TASK_STALE_S = int(os.environ.get("TASK_STALE_S", "120"))
TASK_WRITTEN_NOT_SYNCED = ("The tables were written, but their Databricks jobs were not synced, so the task is not "
                           "done:")
TASK_ROLLED_BACK = ("the app restarted while writing these tables; its transaction was rolled back with it, so none "
                    "of them was written: send it again")
TASK_UNSYNCED = ("the tables were written, but the app restarted before the job's Databricks jobs were synced: "
                 "open the job and save it once to sync them")
TASK_PARTIAL = ("the app restarted while writing the job's databases: the ones written so far are kept, with their "
                "tables; send the same request again to finish the job")


class NotSynced(HTTPException):
    """A write that committed, but a wrapper job it changed did not sync: the write is not done, and the detail says
    what failed. Raised by a write's `after` (in its task, or in the request when the write was small)."""


def _synced(results: List[dict]) -> None:
    """Refuse a write as not done when a Databricks job it changed did not sync (the tables are written already)."""
    failures = action_states.sync_failures(results)
    if failures:
        raise NotSynced(500, f"{TASK_WRITTEN_NOT_SYNCED} {'; '.join(failures)}"[:4000])


def _start_background(work) -> None:
    """Run work on its own thread (tests run it inline instead)."""
    threading.Thread(target=work, daemon=True, name="lakefed-app-task").start()


def _error_text(e: BaseException) -> str:
    """What a failed task says: an HTTPException's detail as the request would have answered it, its errors joined."""
    if isinstance(e, HTTPException):
        detail = e.detail
        if isinstance(detail, dict) and detail.get("errors"):
            return "; ".join(str(x) for x in detail["errors"][:20])[:4000]
        return str(detail)[:4000]
    return (str(e) or type(e).__name__)[:4000]


def _task(task_id: int) -> dict:
    """A task as GET /api/tasks/{id} shows it, a stale one first ended (TASK_STALE_S)."""
    db.execute(q.stale_tasks_sql(), (TASK_UNSYNCED, TASK_ROLLED_BACK, TASK_PARTIAL, TASK_STALE_S, int(task_id), None))
    rows = db.query(q.task_sql(), (int(task_id),))
    if not rows:
        raise HTTPException(404, f"task {task_id} not found")
    return rows[0]


def _task_written(cur, task_id: Optional[int], result: dict, job_id=None, done=0) -> None:
    """The write's last statement, in its transaction: the task's result commits with the tables, so a task whose
    app restarted is known to have written them (a result) or not (none)."""
    if task_id is not None:
        cur.execute(q.task_written_sql(), (json.dumps(result, default=str), job_id, done, task_id))


def _unsynced(e: Exception, result: dict):
    """The result and error of a write whose sync after its commit failed: the tables stay written, the task is not done."""
    error = _error_text(e) if isinstance(e, NotSynced) else f"{TASK_WRITTEN_NOT_SYNCED} {_error_text(e)}"
    return {**result, "after_error": _error_text(e)}, error


def _attempt(task_id: int, write, after):
    """A task's work on the calling thread: write(progress, task_id) in its transaction, then after(result) (syncing the
    Databricks jobs) once it has committed. A beat thread records progress and the heartbeat every TASK_BEAT_S, also
    while one long statement runs. Returns (status, result, error, failure): a write that raised has no result; a sync
    that failed after the commit keeps its result, with what failed."""
    state = {"done": 0}
    stop = threading.Event()

    def beat():
        while not stop.wait(TASK_BEAT_S):
            try:
                db.execute(q.task_progress_sql(), (state["done"], task_id))
            except Exception:
                pass     # a missed beat is caught up by the next; TASK_STALE_S allows for many

    def progress(n: int) -> None:
        state["done"] = n

    threading.Thread(target=beat, daemon=True, name="lakefed-app-task-beat").start()
    result, status, error, failure = None, "failed", None, None
    try:
        result = write(progress, task_id)
        try:
            result = after(result)
            status = "succeeded"
        except Exception as e:
            result, error = _unsynced(e, result)
            failure = e
    except BaseException as e:
        error, failure = _error_text(e), e
    finally:
        stop.set()
    return status, result, error, failure


def _finish(task_id: int, status: str, result, error) -> None:
    db.execute(q.finish_task_sql(), (status, (result or {}).get("id"), json.dumps(result, default=str) if result else None,
                                     error, task_id))


def _run_task(task_id: int, write, after) -> None:
    """A background task's thread: the work (_attempt), then the task ends with what it reached."""
    status, result, error, _ = _attempt(task_id, write, after)
    _finish(task_id, status, result, error)


def _task_answer(task_id: int) -> JSONResponse:
    return JSONResponse(status_code=202, content={"task": _task(task_id), "created": False, "applied": False})


def _run_now(task_id: int, write, after):
    """A small write, in the request. Its task was recorded first, so the same request id finds it. A write that failed
    answers as the request always did; one that committed but whose Databricks sync failed answers 202 with its task,
    not done, so the page can finish it by sending the same request id again (_answer_again)."""
    status, result, error, failure = _attempt(task_id, write, after)
    _finish(task_id, status, result, error)
    if failure is not None and result is None:
        raise failure
    return result if status == "succeeded" else _task_answer(task_id)


def _finish_sync(task_id: int, stored: dict, after) -> None:
    """Sync again a write that committed (its stored result) but whose sync failed: nothing is written again."""
    try:
        status, result, error = "succeeded", after(dict(stored)), None
    except Exception as e:
        status = "failed"
        result, error = _unsynced(e, stored)
    _finish(task_id, status, result, error)


def _answer_again(task_id: int, after) -> JSONResponse:
    """The same request id sent again: the task it made answers, and nothing is written again. A task that committed but
    is not done (its Databricks sync failed, or the app restarted before it) is finished here by syncing again."""
    task = _task(task_id)
    if task["status"] == "failed" and task["result"] is not None and after is not None \
            and db.query(q.resume_synced_task_sql(), (task_id,)):
        _finish_sync(task_id, task["result"], after)
    return _task_answer(task_id)


def _write_now_or_later(kind, request_id, job_id, total, user, write, after, background: bool = False):
    """Every write is a task: recorded first (the request id is its key), then written in the request when it is small
    (_run_now), else on a background thread. Sent again with the same request id, it answers with its task (_answer_again).
    A write that first discovers its tables (background) always runs on the thread, whatever its size."""
    made = db.query(q.insert_task_sql(), (kind, request_id or uuid.uuid4().hex, job_id, int(total), None, user or None))[0]
    if not made["made"]:
        return _answer_again(made["id"], after)
    if total <= BACKGROUND_ROWS and not background:
        return _run_now(made["id"], write, after)
    _start_background(lambda: _run_task(made["id"], write, after))
    return _task_answer(made["id"])


def _made_already(body, after) -> Optional[JSONResponse]:
    """The answer to a write sent again with the request id of one that made a task: that task, before anything is
    planned again (the first write's own tables would now make the plan refuse it)."""
    if body.dry_run or not body.request_id:
        return None
    made = db.query(q.task_of_request_sql(), (body.request_id,))
    return _answer_again(made[0]["id"], after) if made else None


@app.get("/api/tasks/{task_id}")
def task(task_id: int):
    """A background write: its status (running, succeeded, failed), done of total tables, its result (what the
    request would have answered) or error."""
    return _task(task_id)


@app.get("/api/ingestion-jobs/{job_id}/tasks")
def job_tasks(job_id: int):
    """A job's background writes of the last day, newest first, so its page shows one still running."""
    db.execute(q.stale_tasks_sql(), (TASK_UNSYNCED, TASK_ROLLED_BACK, TASK_PARTIAL, TASK_STALE_S, None, int(job_id)))
    return db.query(q.job_tasks_sql(), (int(job_id),))


# ---- uploads, the sources' top level and their pages, and selections by reference -----------------------------------
# A CSV is posted once (POST /api/uploads) and kept in Lakebase; a discovery's tables are there already. New and Add
# tables show a source's servers and databases and page a database's tables from there, and Create, Add tables and
# their previews name what to take (a Selection), which the server reads back from Lakebase and plans as it would the
# same lines sent in the body.

class UploadRequest(BaseModel):
    csv: str
    # The UC connection the file reads through, chosen before the file: the file gives no connection of its own (one
    # that names another connection on any row refuses it).
    connection: str = Field(min_length=1, max_length=255)
    file_name: Optional[str] = Field(None, max_length=255)


@app.post("/api/uploads")
def upload_tables(body: UploadRequest, request: Request):
    """Keep a CSV of tables in Lakebase, one row per line, written q.CONTROL_BATCH lines to a statement, read through the
    connection it was chosen with: its header problems, or a line naming another connection, refuse it (422). A line with
    a database alone is kept to be discovered at Create; a line that can't name a table is kept as invalid, with why."""
    read = nj.read_upload(body.csv, body.connection)
    if read["errors"]:
        raise HTTPException(422, {"errors": read["errors"]})
    upload_id, lines = uuid.uuid4().hex, read["lines"]
    tables = sum(line["status"] == nj.TABLE_LINE for line in lines)
    databases = sum(line["status"] == nj.DATABASE_LINE for line in lines)
    invalid = len(lines) - tables - databases
    with _write() as cur:
        cur.execute(q.insert_upload_sql(), (upload_id, body.file_name, _user(request) or None, read["columns"],
                                            len(lines), tables, invalid))
        for at in range(0, len(lines), q.CONTROL_BATCH):
            chunk = lines[at:at + q.CONTROL_BATCH]
            cur.execute(q.insert_upload_lines_sql(len(chunk)), tuple(
                v for line in chunk for v in (upload_id, line["line"], line["server"], line["src_database"],
                                              line["src_schema"], line["src_table"], line["load_type"],
                                              json.dumps(line["fields"]), line["status"], line["errors"])))
    invalid_lines = [{"line": line["line"], "table": ".".join(line[k] for k in ("src_database", "src_schema", "src_table") if line[k]),
                      "errors": line["errors"]} for line in lines if line["status"] == nj.INVALID_LINE][:20]
    return {"upload_id": upload_id, "file_name": body.file_name, "connection": read["connection"],
            "columns": read["columns"], "lines": len(lines), "tables": tables, "databases": databases,
            "invalid": invalid, "invalid_lines": invalid_lines}


SOURCE_KINDS = ("upload", "discovery")


def _source_param(kind: str, source_id: str):
    """The parameter queries.source_rows_sql takes for a source: an upload's id (404 when there is none), or the list
    of discovery ids (comma-separated in a path)."""
    if kind == "upload":
        if not db.query(q.upload_sql(), (source_id,)):
            raise HTTPException(404, f"upload {source_id} not found")
        return source_id
    if kind == "discovery":
        return [i.strip() for i in source_id.split(",") if i.strip()]
    raise HTTPException(404, f"no source kind {kind}: {' or '.join(SOURCE_KINDS)}")


@app.get("/api/sources/{kind}/{source_id}")
def source_overview(kind: str, source_id: str, filter: str = "", offset: int = 0, limit: int = 100):
    """The top level of an upload or a discovery: its servers (databases, tables, tables by load type), its totals,
    and one page of its databases matching the filter, never its tables."""
    param = _source_param(kind, source_id)
    limit, offset = max(1, min(int(limit), 500)), max(0, int(offset))
    every = db.query(q.source_servers_sql(kind), (param, ""))
    matching = db.query(q.source_servers_sql(kind), (param, filter)) if filter else every
    out = {"kind": kind, "source_id": source_id, "filter": filter, "offset": offset, "limit": limit,
           "servers": matching,
           "totals": {"servers": len(every), "databases": sum(s["databases"] for s in every),
                      "tables": sum(s["tables"] for s in every)},
           "matched": {"databases": sum(s["databases"] for s in matching), "tables": sum(s["tables"] for s in matching)},
           "databases": db.query(q.source_databases_sql(kind), (param, filter, limit, offset))}
    if kind == "upload":
        out["upload"] = db.query(q.upload_sql(), (source_id,))[0]
        out["upload"]["databases_only"] = db.query(q.upload_databases_count_sql(), (source_id,))[0]["n"]
        out["invalid_lines"] = db.query(q.upload_invalid_sql(), (source_id, 20))
    return out


@app.get("/api/sources/{kind}/{source_id}/tables")
def source_tables(kind: str, source_id: str, server: str, database: str, search: str = "", offset: int = 0,
                  limit: int = 50):
    """One page of a database's tables in an upload or a discovery, those matching the search."""
    param = _source_param(kind, source_id)
    limit, offset = max(1, min(int(limit), 500)), max(0, int(offset))
    rows = db.query(q.source_tables_sql(kind), (param, server, database, search, limit, offset))
    return {"tables": [{k: v for k, v in r.items() if k != "total"} for r in rows],
            "total": rows[0]["total"] if rows else 0, "offset": offset, "limit": limit}


class SelectedDatabase(BaseModel):
    server: str
    database: str


class SelectedTables(SelectedDatabase):
    tables: List[str] = Field(min_length=1)     # schema.table


class Selection(BaseModel):
    """What to take from an upload or from discoveries: every database of the source matching the filter
    (whole_server), whole databases, and some tables of others."""
    upload_id: Optional[str] = None
    discovery_id: Optional[str] = None
    discovery_ids: List[str] = []
    whole_server: bool = False
    filter: str = ""
    databases: List[SelectedDatabase] = []
    tables: List[SelectedTables] = []

    @model_validator(mode="after")
    def _one_source(self):
        if bool(self.upload_id) == bool(self.discovery_id or self.discovery_ids):
            raise ValueError("name an upload_id or a discovery_id, not both")
        if not (self.whole_server or self.databases or self.tables or self.upload_id):
            raise ValueError("choose the whole server, databases or tables")
        return self

    def source(self):
        """(kind, the parameter source_rows_sql takes, the id as job_selection keeps it)."""
        if self.upload_id:
            return "upload", self.upload_id, self.upload_id
        ids = list(dict.fromkeys(([self.discovery_id] if self.discovery_id else []) + self.discovery_ids))
        return "discovery", ids, ",".join(ids)


def _selection_params(selection: Selection) -> tuple:
    """(kind, the parameter the source's queries take, source id, the parameters the selection queries take after
    it)."""
    kind, param, source_id = selection.source()
    if kind == "upload" and not db.query(q.upload_sql(), (param,)):
        raise HTTPException(404, f"upload {param} not found")
    database_keys = [q.selection_key(d.server, d.database) for d in selection.databases]
    table_keys = [q.selection_key(t.server, t.database, name) for t in selection.tables for name in t.tables]
    return kind, param, source_id, (selection.whole_server, selection.filter, database_keys, table_keys)


def _selection_parts(selection: Selection, whole_servers: List[str]) -> list:
    """What a selection was made from, part by part (job_selection): the whole servers (with the filter), the whole
    databases and the tables it names, leaving out those a whole server covers."""
    covered = lambda database: bool(selection.whole_server) and selection.filter.lower() in database.lower()
    return ([("server", server, None, None, selection.filter or None) for server in whole_servers]
            + [("database", d.server, d.database, None, None) for d in selection.databases if not covered(d.database)]
            + [("tables", t.server, t.database, list(t.tables), None) for t in selection.tables if not covered(t.database)])


SELECTED_PAGE = 1000   # the tables a selection is read from Lakebase by, one keyset page at a time


def _selected_rows(kind: str, param, selection: Selection, whole_databases: Iterable[str] = ()) -> List[dict]:
    """The tables a selection takes, read a page at a time in key order. The read stops, with a refusal, when the rows
    passed BACKGROUND_ROWS: the count before it said fewer, so the selection grew while it was read, and the request
    is sent again to be written in the background. The request holds at most BACKGROUND_ROWS rows, whatever the source
    has. whole_databases are more database keys taken whole, besides the selection's own."""
    database_keys = [q.selection_key(d.server, d.database) for d in selection.databases] + list(whole_databases)
    table_keys = [q.selection_key(t.server, t.database, name) for t in selection.tables for name in t.tables]
    rows, after = [], ("", "", -1, "", "")
    while True:
        page = db.query(q.selected_rows_page_sql(kind), (param, selection.whole_server, selection.filter, database_keys,
                                                         table_keys, *after, SELECTED_PAGE))
        if len(rows) + len(page) > BACKGROUND_ROWS:
            raise HTTPException(422, {"errors": [f"the selection grew past {BACKGROUND_ROWS:,} tables while it was "
                                                 "read: send it again, and it is written in the background"]})
        rows += page
        if len(page) < SELECTED_PAGE:
            return rows
        last = page[-1]
        after = (last["server"], last["k_database"], last["line"], last["k_schema"], last["k_table"])


def _selected(selection: Selection) -> dict:
    """What a selection takes, read back from Lakebase: {"text": the CSV of its table rows, "rows": those rows (server,
    database, schema, table, line, fields), "columns": the header the CSV is written with, "lines": each row's line in
    the uploaded file (or None), "database_lines": an upload's database-only lines ({line, database, connection}),
    "parts": the job_selection rows to keep with the job, "kind", "source_id"}. 422 when it takes neither tables nor
    database-only lines."""
    kind, param, source_id = selection.source()
    database_lines = []
    if kind == "upload":
        upload = db.query(q.upload_sql(), (param,))
        if not upload:
            raise HTTPException(404, f"upload {param} not found")
        columns = list(upload[0]["columns"])
        database_lines = [{"line": r["line"], "database": r["src_database"],
                           "connection": (r["fields"] or {}).get("src_connection") or ""}
                          for r in db.query(q.upload_database_lines_sql(), (param,))]
    else:
        columns = list(nj.DISCOVERY_COLUMNS)
    # A database-only line takes its database whole, so the file's own table rows in it are taken whatever is picked,
    # and discovery's copies of those tables are left out (_with_databases).
    whole_databases = [q.selection_key(js.server_of({"src_connection": line["connection"], "use_remote_query": True}),
                                       line["database"]) for line in database_lines]
    rows = _selected_rows(kind, param, selection, whole_databases)
    if not rows and not database_lines:
        raise HTTPException(422, {"errors": ["the selection takes no tables"]})
    whole = ([s["server"] for s in db.query(q.source_servers_sql(kind), (param, selection.filter))]
             if selection.whole_server else [])
    covered = lambda server, database: (server.lower() in {w.lower() for w in whole}
                                        and selection.filter.lower() in database.lower())
    parts = ([("server", server, None, None, selection.filter or None) for server in whole]
             + [("database", d.server, d.database, None, None) for d in selection.databases if not covered(d.server, d.database)]
             + [("tables", t.server, t.database, list(t.tables), None) for t in selection.tables
                if not covered(t.server, t.database)])
    return {"text": nj.csv_of(columns, [r["fields"] for r in rows]), "rows": rows, "columns": columns,
            "lines": [r["line"] if kind == "upload" else None for r in rows], "database_lines": database_lines,
            "parts": parts, "kind": kind, "source_id": source_id}


def _file_lines(plan: dict, selected: Optional[dict]) -> dict:
    """A plan of an upload's selection, its lines numbered as in the uploaded file (a discovered table's line is its
    database's line), with what the file's database-only lines came to."""
    if selected and selected["kind"] == "upload":
        for line in plan["lines"]:
            line["line"] = selected["lines"][line["line"] - 2]
        plan.update({k: selected[k] for k in ("covered", "discovered", "unreadable") if k in selected})
    return plan


# A file's database-only lines are read by a discovery run through the connection. The previews and the Create share
# that run for this long (per process), so the Create after a preview reads each database once.
DATABASE_READ_MEMO_S = float(os.environ.get("DATABASE_READ_MEMO_S", str(2 * 3600)))
_database_reads: dict = {}
_database_reads_lock = threading.Lock()


def _has_database_lines(selection: "Selection") -> bool:
    """Whether an upload has database-only lines: those are discovered by name, so the table counts don't cover them."""
    kind, param, _ = selection.source()
    counted = db.query(q.upload_databases_count_sql(), (param,))
    return kind == "upload" and bool(counted and counted[0]["n"])


def _resolve_databases(selected: dict, wait: bool, request: Optional[Request] = None) -> Optional[dict]:
    """The tables of a selection's database-only lines, found by name: a discovery run through the connection reads each
    database, and its tables come back as rows, as a discovery's do. None when there are no such lines, else
    {"discovering": bool, "databases": how many, "rows": the tables found ({line, server, src_database, src_schema,
    src_table, fields}), "unreadable": [{line, database, error}]}. Not waiting (a preview), a run still going answers
    discovering and the page asks again; waiting (the background Create or Add), the run is waited for."""
    lines = selected["database_lines"]
    if not lines:
        return None
    names: dict = {}
    for line in lines:
        names.setdefault(line["database"].lower(), line)
    connection = lines[0]["connection"]
    key = (connection.lower(), tuple(sorted(names)))
    read = _database_read(key)
    if read is None:
        read = _start_database_read(key, connection, [line["database"] for line in names.values()], request)
    if read["ended"] is None:
        ended = _wait_for_run(read["run_id"]) if wait else jobs.run_ended(read["run_id"])
        if ended is None:
            return {"discovering": True, "databases": len(names), "rows": [], "unreadable": []}
        read["ended"] = ended
    ended = read["ended"]
    if ended.get("result") != "SUCCESS":
        _forget_database_read(key)
        why = f"its discovery run ended {ended.get('result') or ended.get('state')}"
        if ended.get("message"):
            why += f": {ended['message']}"
        return {"discovering": False, "databases": len(names), "rows": [],
                "unreadable": [_unreadable(line, why) for line in names.values()]}
    ids = [read["discovery_id"]]
    statuses = {(s["src_database"] or "").lower(): s for s in db.query(q.discovery_statuses_sql(), (ids,))}
    unreadable = []
    for lower, line in names.items():
        status = statuses.get(lower)
        if status is None:
            unreadable.append(_unreadable(line, "the connection did not list this database"))
        elif status["status"] != "ok":
            unreadable.append(_unreadable(line, status["error"] or f"it could not be read ({status['status']})"))
    rows = [{"line": names[r["src_database"].lower()]["line"], "server": r["server"], "src_database": r["src_database"],
             "src_schema": r["src_schema"], "src_table": r["src_table"], "fields": r["fields"]}
            for r in db.query(q.source_rows_sql("discovery"), (ids,))
            if r["src_database"].lower() in names and (statuses.get(r["src_database"].lower()) or {}).get("status") == "ok"]
    return {"discovering": False, "databases": len(names), "rows": rows, "unreadable": unreadable}


def _unreadable(line: dict, error: str) -> dict:
    return {"line": line["line"], "database": line["database"], "error": error}


def _database_read(key: tuple):
    with _database_reads_lock:
        read = _database_reads.get(key)
        if read is not None and time.monotonic() - read["started"] > DATABASE_READ_MEMO_S:
            del _database_reads[key]
            read = None
        return read


def _forget_database_read(key: tuple) -> None:
    with _database_reads_lock:
        _database_reads.pop(key, None)


def _start_database_read(key: tuple, connection: str, databases: List[str], request: Optional[Request]) -> dict:
    """Starts a discovery run that reads these databases through the connection, and keeps it under the key."""
    listed = connections()
    if connection not in {c["name"] for c in listed["connections"]}:
        raise HTTPException(422, {"errors": [_connection_unseen(connection, listed.get("error"))]})
    job_id = _require(jobs.DISCOVER_JOB_ID, "discovery")
    if request is not None:
        _refuse_run_of(job_id, request)
    discovery_id = uuid.uuid4().hex[:12]
    spec = {"src_type": "sqlserver", "src_connection": connection, "kind": "tables", "databases": databases,
            "requested_by": _user(request) if request is not None else ""}
    run_id = jobs.run_now(job_id, {"discovery_id": discovery_id, "spec": json.dumps(spec)})
    read = {"run_id": run_id, "discovery_id": discovery_id, "ended": None, "started": time.monotonic()}
    with _database_reads_lock:
        _database_reads[key] = read
    return read


def _with_databases(selected: dict, resolved: Optional[dict]) -> dict:
    """The selection with the tables found for its database-only lines added after its table rows. A table the file
    names in a row of its own is left to that row (nj.discovered_tables), so the explicit row's settings are used."""
    if resolved is None:
        return selected
    kept, covered = nj.discovered_tables(resolved["rows"], {nj.table_key(row) for row in selected["rows"]})
    rows = selected["rows"] + kept
    columns = list(dict.fromkeys(selected["columns"] + list(nj.DISCOVERY_COLUMNS)))
    return {**selected, "columns": columns, "text": nj.csv_of(columns, [row["fields"] for row in rows]),
            "lines": [row["line"] for row in rows], "covered": covered, "discovered": len(kept),
            "unreadable": resolved["unreadable"]}


def _task_stage(task_id: Optional[int], stage: str) -> None:
    """A background write's stage (discovering, then writing), which its task's progress shows."""
    if task_id is not None:
        db.execute(q.task_progress_json_sql(), (json.dumps({"stage": stage}), 0, task_id))


def _keep_selection(cur, job_id: int, selected: Optional[dict], user: str) -> None:
    """In the job's transaction: what it was made from (job_selection), for a later re-discovery to follow."""
    parts = (selected or {}).get("parts") or []
    for at in range(0, len(parts), q.CONTROL_BATCH):
        chunk = parts[at:at + q.CONTROL_BATCH]
        cur.execute(q.insert_job_selection_sql(len(chunk)), tuple(
            v for scope, server, database, tables, filter_ in chunk
            for v in (int(job_id), scope, server, database, tables, filter_, selected["kind"], selected["source_id"],
                      user or None)))


@app.get("/api/ingestion-jobs/{job_id}/selection")
def job_selection(job_id: int):
    """What a job was made from, part by part: whole servers, whole databases, chosen tables."""
    return db.query(q.job_selection_sql(), (int(job_id),))


# Table states change at run cadence, so the one per-database scan they come from is shared for longer
# than running progress is.
STATES_CACHE_S = max(0, int(os.environ.get("STATES_CACHE_S", "15")))
# How often the group names with a control row are re-read (a scan of the control index), for groups with no rollup yet.
CONTROL_GROUPS_S = 600


def _states() -> List[dict]:
    """The database rows the monitoring views sum: each database's rollup row (queries.rollup_states_sql), read once
    and shared for STATES_CACHE_S. A group with no rollup row yet (its rows predate the rollups and no write or check
    has touched it) is counted from its control rows until one does."""
    return _shared("database_states", _load_states, ttl=STATES_CACHE_S)


def _load_states() -> List[dict]:
    rows = db.query(q.rollup_states_sql())
    written = {r["task_collection"] for r in db.query(q.rollup_group_names_sql())}
    unwritten = sorted(_control_groups() - written)
    if unwritten:
        rows += [rollup.from_scan(r) for r in db.query(q.database_states_for_groups_sql(), (unwritten,))]
    return rows


def _control_groups() -> set:
    """Every ingestion group with a control row, re-read at most every CONTROL_GROUPS_S seconds."""
    return _shared("control_groups",
                   lambda: {r["task_collection"] for r in db.query(q.control_group_names_sql())}, ttl=CONTROL_GROUPS_S)


def _last_runs() -> dict:
    """Each ingestion group's latest run, by group, shared as often as _running, so a run that has finished
    is not still shown running beside an empty running list."""
    return _shared("last_runs", lambda: {r["task_collection"]: r for r in db.query(q.last_runs_sql())})


def _read_at(key) -> str:
    """When the shared read under key was made: now, when it is not cached."""
    hit = _shared_reads.get(key)
    return datetime.fromtimestamp(hit[1] if hit else time.time(), timezone.utc).isoformat()


def _running() -> List[dict]:
    """queries.running_progress_sql, shared: every running group run's progress, read once per
    MONITOR_CACHE_S however many pages ask. Until the run poller's table exists, runs whose Databricks run has
    ended are closed here first (_close_ended_runs); after that the poller closes them."""
    def compute():
        if not run_poller.table_ready():
            try:
                _close_ended_runs()
            except Exception:           # the pages still show what is recorded; the next read tries again
                pass
        return db.query(q.running_progress_sql())
    return _shared("running", compute)


def _older_than(started_at, age: timedelta) -> bool:
    if started_at is None:
        return False
    when = q._when(started_at)
    return (when if when.tzinfo else when.replace(tzinfo=timezone.utc)) < datetime.now(timezone.utc) - age


def _close_ended_runs(user: Optional[str] = None) -> int:
    """Close each run recorded as running whose Databricks run has ended (cancelled in Databricks, or its cluster
    gone): failed, with how its Databricks run ended, and its events still running, as the engine closes an abandoned
    run. A run with no job run id (run by hand outside a job), whose state can't be read now, or still going in
    Databricks (queued, pending, running, terminating, or retrying) is left as it is; nothing is ever cancelled here.
    Returns how many were closed."""
    closed = 0
    for r in db.query(q.open_runs_sql()):
        job_run = str(r.get("job_run_id") or "").strip()
        if not job_run.isdigit():
            continue
        try:
            ended = jobs.run_ended(job_run)
        except Exception:
            continue
        if not ended or (ended["state"] == "NOT_FOUND" and not _older_than(r.get("started_at"),
                                                                         run_poller.NOT_FOUND_AFTER)):
            continue
        reason = run_poller.ended_reason(ended)
        detail = json.dumps({"closed_by_app": user or "reconcile", "reason": reason, "databricks_run": int(job_run),
                             "databricks_state": ended["state"], "databricks_result": ended.get("result")})
        with _write() as cur:
            cur.execute(q.close_ended_run_sql(), (detail, int(r["run_id"]), reason, reason))
        closed += 1
    return closed


# The most running job runs, and jobs needing attention, the Overview lists; Runs and Ingestion jobs list all.
OVERVIEW_LIST = 5
OVERVIEW_JOBS_PAGE = 25


def _rows_written_24h() -> dict:
    """The rows every group wrote in the last 24 hours ({rows, partial}: partial when some loads recorded no count),
    their table loads' and their batched appends' together; shared like _states."""
    def compute():
        groups = sorted({s["task_collection"] for s in _states()})
        if not groups:
            return {"rows": 0, "partial": False}
        loads = next(iter(db.query(q.rows_written_since_sql(), (groups, 24))), {})
        batched = [q.batch_rows(r["detail"]) for r in db.query(q.run_batch_rows_sql(), (24,))]
        return {"rows": int(loads.get("rows_written") or 0) + sum(rows for rows, _ in batched),
                "partial": bool(loads.get("uncounted")) or any(unknown for _, unknown in batched)}
    return _shared("rows_written_24h", compute, ttl=STATES_CACHE_S)


@app.get("/api/overview")
def overview(offset: int = 0):
    """The status summary: the running job runs and the jobs needing attention (OVERVIEW_LIST of each, with their
    totals), the totals over every enabled table with the rows written in 24 hours, and the next scheduled triggers,
    from the shared reads (_jobs_view). The ingestion jobs (a page of OVERVIEW_JOBS_PAGE, failing first) and "Not in
    any job" come too, for the totals' links."""
    view = _jobs_view()
    offset = max(0, int(offset))
    upcoming = sorted((j for j in view["jobs"] if j.get("next_start")), key=lambda j: j["next_start"])
    return {"next_triggers": [{k: j.get(k) for k in ("id", "name", "next_start", "description")}
                              for j in upcoming[:OVERVIEW_LIST]],
            "paused_jobs": sum(1 for j in view["jobs"] if j.get("paused")), "rows_24h": _rows_written_24h(),
            "jobs": view["jobs"][offset:offset + OVERVIEW_JOBS_PAGE], "jobs_total": len(view["jobs"]),
            "offset": offset, "limit": OVERVIEW_JOBS_PAGE, "not_in_job": view["not_in_job"],
            "running_job_runs": view["running_job_runs"][:OVERVIEW_LIST], "running_total": len(view["running_job_runs"]),
            "running_not_in_job": view["running_not_in_job"],
            "attention": view["attention"][:OVERVIEW_LIST], "attention_total": len(view["attention"]),
            "totals": view["totals"], "read_at": _read_at("database_states"), "cache_s": MONITOR_CACHE_S,
            "states_cache_s": STATES_CACHE_S}


# The most failed tables the failure groups read; past it they say how many they show.
ERROR_TABLES_LIMIT = 20000


@app.get("/api/errors/groups")
def error_groups(hours: int = 24):
    """Failed tables in the last `hours`, grouped by cause (queries.group_errors). The newest
    ERROR_TABLES_LIMIT tables at most; `truncated` says when more failed."""
    hours = max(1, min(int(hours), 168))

    def compute():
        rows = db.query(q.failed_tables_sql(), (hours, ERROR_TABLES_LIMIT + 1))
        tables = list(dict.fromkeys(r["control_id"] for r in rows))
        kept = set(tables[:ERROR_TABLES_LIMIT])
        shown = [r for r in rows if r["control_id"] in kept]
        return {"groups": q.group_errors(shown), "tables_shown": len(kept), "truncated": len(tables) > len(kept)}
    return _shared(("errors", hours), compute)


@app.get("/api/collections/{name}/databases")
def collection_databases(name: str):
    """A collection's source databases: state now, this run's progress, rows in first loads and in
    changes (24 h), and batched appends' totals placed on the databases they loaded."""
    def compute():
        rows = {q.server_database_key(r["server"], r["src_database"]): r for r in db.query(q.collection_database_rows_sql(), (name,))}
        running = [r for r in _running() if r["task_collection"] == name]
        batches = []
        for b in db.query(q.collection_batch_runs_sql(), (name, name)):
            total, unknown = q.batch_rows(b["detail"])
            if total or unknown:
                batches.append({"run_id": b["run_id"], "rows": total, "rows_unknown": unknown,
                                "databases": sorted(b["databases"] or []), "keys": sorted(b["keys"] or [])})
        databases = []
        for d in db.query(q.collection_databases_sql(), (name, name)):
            key = q.server_database_key(d["server"], d["src_database"])
            r = rows.get(key, {})
            d.update({k: r.get(k) for k in ("rows_first_loads", "first_loads", "first_loads_uncounted",
                                             "rows_changes_24h", "loaded_this_run", "in_flight", "failed_this_run")})
            d["batches"] = [{"run_id": b["run_id"], "rows": b["rows"]} for b in batches
                            if b["keys"] == [key]]
            databases.append(d)
        schedules, members = _schedules()
        job_names = [{"id": sc["id"], "name": sc["name"]} for sc in schedules
                     if any(m["schedule_id"] == sc["id"] and m["task_collection"] == name for m in _active(members))]
        return {"collection": name, "databases": databases, "running": running[0] if running else None, "jobs": job_names,
                "batches": batches, "read_at": datetime.now(timezone.utc).isoformat(), "cache_s": MONITOR_CACHE_S}
    return _shared(("collection", name), compute)


@app.get("/api/errors")
def errors(hours: int = 24, limit: int = 200):
    return db.query(q.recent_errors_sql(), (int(hours), int(limit)))


@app.get("/api/collections")
def collections():
    return [r["task_collection"] for r in db.query(q.collections_sql())]


# ---- ingestion jobs and job runs ----------------------------------------------------------------

def _schedules():
    """Active schedules, the deleted ones whose Delete is pending, and all their members, removed ones included (a
    member's past runs still belong to the job runs of its time in the schedule), shared like _running. _active keeps
    the current members. A pending deleted job carries its delete state (ingestion_jobs.delete_state)."""
    def compute():
        rows = _with_delete_states(db.query(q.schedules_sql()), datetime.now(timezone.utc))
        return rows, db.query(q.schedule_members_sql(), ([r["id"] for r in rows],)) if rows else []
    return _shared("schedules", compute)


def _with_delete_states(rows: List[dict], now: datetime) -> List[dict]:
    """The rows with each deleted job's delete state. One whose delete is finished (the delete job succeeded and no
    tables are left) is marked ``delete_finished`` and leaves the list: the Deleted jobs history keeps it."""
    deleted = [r for r in rows if r.get("deleted_at") is not None]
    if not deleted:
        return rows
    left = {r["schedule_id"] for r in db.query(q.deleted_tables_left_sql(), ([r["id"] for r in deleted],))}
    out = []
    for r in rows:
        if r.get("deleted_at") is None:
            out.append(r)
            continue
        state = ij.delete_state(r, r["id"] in left, _delete_run(r), now)
        if state["state"] == ij.DELETE_GONE:
            with db.transaction() as cur:
                cur.execute(q.set_schedule_flag_sql(), (["delete_finished"], True, int(r["id"])))
            continue
        out.append({**r, "delete": state})
    return out


def _deleting() -> set:
    """The groups of jobs being deleted (queries.deleting_groups_sql), shared like _schedules: they stay with their
    job, under Deleted jobs, until the delete job removes them."""
    return _shared("deleting", lambda: {r["task_collection"] for r in db.query(q.deleting_groups_sql())})


def _active(members: List[dict]) -> List[dict]:
    return [m for m in members if m.get("removed_at") is None]


def _group_stats() -> dict:
    """Each ingestion group's run-time percentiles and strictest freshness target, shared like _states."""
    def compute():
        names = sorted({s["task_collection"] for s in _states()})
        rows = db.query(q.collection_run_stats_sql(), (names, names, names, names)) if names else []
        return {r["task_collection"]: r for r in rows}
    return _shared("group_stats", compute, ttl=STATES_CACHE_S)


def _group_runs(groups: List[str], start: datetime, end: datetime) -> List[dict]:
    """The group runs of some groups that started in [start, end), with their work and outcome."""
    return _with_rows_written(db.query(q.group_runs_sql(), (sorted(groups), start, end))) if groups else []


def _schedule(job_id: int) -> dict:
    schedules, _ = _schedules()
    found = next((s for s in schedules if s["id"] == int(job_id)), None)
    if not found:
        raise HTTPException(404, f"ingestion job {job_id} not found")
    return found


def _runs_window(job: dict, start: datetime, end: datetime):
    """The window of group runs that job runs with slots in [start, end) are placed from: two slots before `start`
    (ij.place starts a slot early, and an earlier run still going at a trigger makes it skipped), and up to two
    cadences after `end` (a member's offset, then up to its next trigger)."""
    cadence = int(job["cadence_minutes"])
    return start - timedelta(minutes=2 * max(cadence, 60)), end + timedelta(minutes=2 * cadence)


def _job_runs_of(job: dict, start: datetime, end: datetime, now: datetime,
                 runs: Optional[List[dict]] = None) -> List[dict]:
    """A job's job runs whose slot started in [start, end), newest first, as ij.summarize shows them, with
    their placed members, from `runs` when given (read for _runs_window), else read here."""
    _, members = _schedules()
    mine = [m for m in members if m["schedule_id"] == job["id"]]
    cadence = int(job["cadence_minutes"])
    if runs is None:
        runs = _group_runs([m["task_collection"] for m in mine], *_runs_window(job, start, end))
    placed = [jr for jr in ij.place(job, mine, runs, start, end + timedelta(minutes=cadence), now)
              if start <= jr["slot"] < end and jr["slot"] <= now]
    running = {r["run_id"]: r for r in _running()}
    tables = {name: g["enabled_tables"] for name, g in rollup.groups(_states()).items()}
    p50 = {g: st.get("p50_s") for g, st in _group_stats().items()}
    return [dict(ij.summarize(job, jr, running, tables, p50, now), placed=jr) for jr in placed]


def _last_ran(jobs: List[dict], now: datetime) -> dict:
    """Each job's last job run that ran, however long ago, by job id (jobs whose recent triggers did not run): each
    member's newest finished group run while it was the member (one read for every job), placed into their job
    runs, the newest slot summarized (ij.earlier_last) from one more read of the runs around every job's slot. A
    job that never ran has none."""
    _, members = _schedules()
    candidates = {}
    for r in db.query(q.last_ran_group_runs_sql(), (sorted(j["id"] for j in jobs),)) if jobs else []:
        candidates.setdefault(r["schedule_id"], []).append(r)
    newest = {j["id"]: slots[0] for j in jobs for slots in [ij.newest_slots(candidates.get(j["id"]) or [], j, members)]
              if slots}
    windows = [(m["task_collection"], *_runs_window(j, newest[j["id"]], newest[j["id"]] + timedelta(seconds=1)))
               for j in jobs if j["id"] in newest for m in members if m["schedule_id"] == j["id"]]
    groups, starts, ends = (list(column) for column in zip(*windows)) if windows else ([], [], [])
    runs = _with_rows_written(db.query(q.group_runs_in_windows_sql(), (groups, starts, ends))) if windows else []
    found = {}
    for job in jobs:
        if job["id"] not in newest:
            continue
        slot = newest[job["id"]]
        start, end = _runs_window(job, slot, slot + timedelta(seconds=1))
        mine = {m["task_collection"] for m in members if m["schedule_id"] == job["id"]}
        around = [r for r in runs if r["task_collection"] in mine and start <= q._when(r["started_at"]) < end]
        last = ij.earlier_last(_job_runs_of(job, slot, slot + timedelta(seconds=1), now, runs=around))
        if last:
            found[job["id"]] = last
    return found


def _jobs_view() -> dict:
    """ingestion_jobs.jobs_view over the shared reads, with each failing job's causes from its latest job run
    with failed tables; shared like _running."""
    def compute():
        now = datetime.now(timezone.utc)
        schedules, members = _schedules()
        states, running, stats = _states(), _running(), _group_stats()
        groups = sorted({m["task_collection"] for m in members})
        start = min([ij.window(int(s["cadence_minutes"]), ij.RECENT_JOB_RUNS + 1, now) for s in schedules]
                    + [q._when(r["started_at"]) - timedelta(days=1) for r in running if r.get("started_at")], default=now)
        runs = _group_runs(groups, start, now + timedelta(seconds=1))
        deleting = _deleting()
        view = ij.jobs_view(schedules, members, states, running, runs, stats, now, deleting=deleting)
        by_id = {s["id"]: s for s in schedules}
        # A job none of whose recent triggers ran (paused overnight, say): its last job run is found before them.
        earlier = _last_ran([by_id[j["id"]] for j in view["jobs"] if j["id"] is not None and not j["last_job_run"]], now)
        latest_ids = {}
        for job in view["jobs"]:
            last = job["last_job_run"] or earlier.get(job["id"])
            latest = next((jr for jr in (job["running_job_run"], last) if jr and jr["failed_tables"]), None)
            if job["state_failing"] and latest and latest["run_ids"]:
                latest_ids[job["id"]] = set(latest["run_ids"])
        causes = {}
        if latest_ids:      # one read for every failing job's latest job run, split by job
            failures = db.query(q.run_failures_sql(), (sorted(set().union(*latest_ids.values())),))
            causes = {job_id: q.group_errors([f for f in failures if f["run_id"] in ids]) for job_id, ids in latest_ids.items()}
        if causes or earlier:
            view = ij.jobs_view(schedules, members, states, running, runs, stats, now, causes_by_job=causes,
                                earlier_last_by_job=earlier, deleting=deleting)
        return view
    return _shared("jobs_view", compute)


JOB_FILTERS = ("all", "running", "failing", "late", "paused")
# The sums a group row shows: its databases, enabled tables, late databases, and its tables by state (rollup.STATES).
GROUP_SUMS = ("databases", "enabled_tables", "late_databases") + rollup.STATES


def _job_filter(job: dict, name: str) -> bool:
    return {"all": True, "running": bool(job.get("running_job_run")), "failing": bool(job["state_failing"]),
            "late": bool(job["late_databases"]), "paused": bool(job.get("paused"))}[name]


# Columns the Ingestion jobs list sorts by, besides its own order (failing first, then late, then by name).
JOB_SORTS = ("created_at", "updated_at")


def _audit(schedule: dict) -> dict:
    """Who created a job and who last changed it (it or its groups' tables), and when; None where not recorded."""
    return {"created_by": schedule.get("created_by"), "created_at": schedule.get("created_at"),
            "updated_by": sch.updated_by(schedule), "updated_at": schedule.get("updated_at")}


@app.get("/api/ingestion-jobs")
def ingestion_jobs(filter: str = "all", search: str = "", offset: int = 0, limit: int = 25, sort: str = "",
                   descending: bool = True):
    """Every ingestion job, as on the Overview, filtered (each filter with its count) and searched on job and
    group names, a page at a time; "Not in any job" comes apart, under All. ``sort`` (created_at or updated_at)
    orders the whole list by that time, unknown times last."""
    if filter not in JOB_FILTERS:
        raise HTTPException(400, f"filter must be one of {', '.join(JOB_FILTERS)}")
    if sort and sort not in JOB_SORTS:
        raise HTTPException(400, f"sort must be one of {', '.join(JOB_SORTS)}")
    view = _jobs_view()
    audits = {s["id"]: _audit(s) for s in _schedules()[0]}
    needle = search.strip().lower()
    found = [{**j, **audits.get(j["id"], {})} for j in view["jobs"]
             if not needle or needle in str(j["name"]).lower() or any(needle in g.lower() for g in j["groups"])]
    counts = {f: sum(1 for j in found if _job_filter(j, f)) for f in JOB_FILTERS}
    rows = [j for j in found if _job_filter(j, filter)]
    if sort:
        known = sorted((j for j in rows if j.get(sort)), key=lambda j: q._when(j[sort]), reverse=descending)
        rows = known + [j for j in rows if not j.get(sort)]
    not_in_job = view["not_in_job"]
    shown_apart = filter == "all" and not_in_job and (not needle or any(needle in g.lower() for g in not_in_job["groups"]))
    offset, limit = max(0, int(offset)), max(1, min(int(limit), 100))
    return {"jobs": rows[offset:offset + limit], "total": len(rows), "offset": offset, "limit": limit,
            "not_in_job": not_in_job if shown_apart else None, "counts": counts, "read_at": _read_at("jobs_view"),
            "cache_s": MONITOR_CACHE_S}


@app.get("/api/ingestion-jobs/not-in-job")
def groups_not_in_job():
    """The groups in no ingestion job, as a page of their own: their tables by state and each group's last run, so a
    deployment with no jobs (a read-only share) still opens its groups and tables from the jobs list."""
    schedules, members = _schedules()
    _, not_in_job = rollup.jobs(_states(), schedules, members, _deleting())
    not_in_job = not_in_job or {"groups": []}
    groups, stats, last_runs = rollup.groups(_states()), _group_stats(), _last_runs()
    rows = []
    for name in sorted(not_in_job["groups"]):
        g, st, last = groups.get(name) or {}, stats.get(name) or {}, last_runs.get(name)
        rows.append({"task_collection": name, **{k: g.get(k) for k in GROUP_SUMS},
                     "last_run": dict(last, outcome=q.run_outcome(last)) if last else None,
                     "p95_s": st.get("p95_s"), "p50_s": st.get("p50_s")})
    return {**not_in_job, "name": "Not in any job", "groups_detail": rows}


@app.get("/api/ingestion-jobs/deletions")
def job_deletions():
    """The latest deleted jobs whose Delete runs the delete job, newest first: each one's delete job run (going,
    finished or failed, with its message), whether it drops the target tables, and how many of its tables are left
    (none once the delete job has finished)."""
    out = []
    for s in db.query(q.deletions_sql(), (DELETIONS_LISTED,)):
        plan = _delete_plan(s["id"])
        out.append({"id": s["id"], "name": s["name"], "deleted_at": s["deleted_at"],
                    "drop_targets": bool(sch.options_of(s).get("delete_drop_targets")), "run": _delete_run(s),
                    "tables_left": plan["tables"], "groups_left": len(plan["groups"])})
    return {"deletions": out, "configured": bool(jobs.DELETE_JOB_ID)}


@app.get("/api/ingestion-jobs/{job_id}/delete-preview")
def delete_preview(job_id: int):
    """What Delete would remove, for its dialog: the groups it deletes and those it keeps (another job holds them),
    their tables, the target tables only they write, and the target tables another job's rows also write (kept and
    named); and why it can't be deleted now, if so."""
    job, members = _load_schedule(job_id)
    active = [m for m in members if m["removed_at"] is None]
    plan = _delete_plan(job_id)
    kept_groups = sorted({m["task_collection"] for m in active} - set(plan["groups"]))
    blocker = _delete_blocker(job, active, plan["groups"])
    if not blocker and plan["tables"] and not jobs.DELETE_JOB_ID:
        blocker = NO_DELETE_JOB
    return {**plan, "groups_kept": kept_groups, "configured": bool(jobs.DELETE_JOB_ID),
            "blocked": blocker.get("message") if isinstance(blocker, dict) else blocker}


@app.get("/api/permissions")
def permissions(request: Request):
    """What the signed-in user may do, from their Databricks permissions, so each page can show a button disabled
    with the reason (every action checks again on its own call): per ingestion job, run, change and delete; per
    ingestion group in a job, changing its tables (groups in no job are open); and running each of the bundle's
    jobs the app starts by hand."""
    user = _user(request)
    schedules = db.query(q.schedules_sql())
    members = db.query(q.schedule_members_sql(), ([s["id"] for s in schedules],)) if schedules else []
    by_job = {s["id"]: _permission_refusals(s, user) for s in schedules}
    groups: dict = {}
    for s in schedules:
        for m in members:
            if m["schedule_id"] == s["id"] and m["removed_at"] is None and by_job[s["id"]]["change"]:
                groups.setdefault(m["task_collection"],
                                  f"these tables are in the ingestion job {s['name']}: {by_job[s['id']]['change']}")
    bundle = {name: (_databricks_job_refusals(job_id, user)["run"] if job_id else f"no {name} job is configured")
              for name, job_id in (("ingestion", jobs.INGEST_JOB_ID), ("setup", jobs.SETUP_JOB_ID),
                                   ("discovery", jobs.DISCOVER_JOB_ID))}
    return {"jobs": {str(sid): refusals for sid, refusals in by_job.items()}, "groups": groups, "run": bundle,
            "new_running_job": {variant: _permission_refusals({"job_variant": variant}, user)["run"]
                                for variant in jobs.schedule_variants()}}


@app.get("/api/ingestion-jobs/{job_id}")
def ingestion_job(job_id: int, request: Request):
    """One ingestion job's details: its schedule and members (wrapper jobs, next start), its tables by
    state, its last job run that ran (however long ago), the median speed over its last 24 job runs, its p95 job
    run time over 7 days, and its groups with their last group run and p95 run time. Job runs are placed once over
    7 days; the speed is over the newest 24 finished ones."""
    job = _schedule(job_id)
    now = datetime.now(timezone.utc)
    _, members = _schedules()
    mine = [m for m in _active(members) if m["schedule_id"] == job["id"]]
    sums = next(j for j in rollup.jobs(_states(), [job], members)[0])
    stats, last_runs = _group_stats(), _last_runs()
    cadence = int(job["cadence_minutes"])
    start = sch.start_minute(job)
    finished = [r for r in _job_runs_of(job, now - timedelta(days=7), now + timedelta(seconds=1), now)
                if r["outcome"]["label"] != "Running"]
    week = [r["took_s"] for r in finished if r["took_s"] is not None]
    groups = rollup.groups(_states())
    group_rows = []
    for m in sorted(mine, key=lambda m: (sch.stagger_of(m["offset_minutes"], cadence, start), m["task_collection"])):
        g, st, last = groups.get(m["task_collection"]) or {}, stats.get(m["task_collection"]) or {}, last_runs.get(m["task_collection"])
        group_rows.append({"task_collection": m["task_collection"], "offset_minutes": m["offset_minutes"],
                           "starts": sch.member_start(cadence, int(m["offset_minutes"])),
                           **{k: g.get(k) for k in GROUP_SUMS},
                           "last_run": dict(last, outcome=q.run_outcome(last)) if last else None,
                           "p95_s": st.get("p95_s"), "p50_s": st.get("p50_s"),
                           "job_url": (f"{db.workspace().config.host.rstrip('/')}/jobs/{m['job_id']}"
                                       if m.get("job_id") else None)})
    speeds = sorted(r["speed_tables_per_min"] for r in finished[:24] if r["speed_tables_per_min"] is not None)
    offsets = [sch.stagger_of(m["offset_minutes"], cadence, start) for m in mine]
    last = ij.last_finished(finished)
    last = {k: v for k, v in last.items() if k != "placed"} if last else _last_ran([job], now).get(job["id"])
    next_starts = [sch.next_run(cadence, int(m["offset_minutes"]), job.get("timezone") or "UTC", now) for m in mine]
    refusals = _permission_refusals(job, _user(request))
    return {**{k: v for k, v in sums.items()},
            "description": sch.describe(cadence, sch.start_minute(job), job.get("timezone") or "UTC"),
            "stagger_from": min(offsets) if offsets else None, "stagger_to": max(offsets) if offsets else None,
            "parameters": job.get("parameters"), "job_variant": job.get("job_variant"), "paused": job.get("paused"),
            "last_job_run": last,
            "next_start": min(next_starts).isoformat() if next_starts and not job.get("paused") else None,
            "median_speed_tables_per_min": speeds[len(speeds) // 2] if speeds else None, "speed_job_runs": len(speeds),
            "p95_took_s": sorted(week)[min(len(week) - 1, int(0.95 * len(week)))] if week else None,
            "groups_detail": group_rows, "read_at": now.isoformat(),
            # For the Run now button: why it can't start now (None when it can), whether a run is going (the button
            # is then Cancel run, which the run permission also gates), and what one run starts.
            "run_now": {"blocked": refusals["run"] or _run_now_blocker(job, mine), "refused": refusals["run"],
                        "groups": len(mine), "check": job.get("job_variant") == sch.CHECK_VARIANT,
                        **_run_phase_of(job, mine, now)},
            "delete_blocked": refusals["delete"], "change_blocked": refusals["change"],
            "delete": job.get("delete"), "rediscovery": _rediscovery_view(job, now), **_audit(job)}


def _job_groups(job_id: int) -> List[str]:
    """An ingestion job's current groups, read fresh (not from the shared schedule cache) since an edit writes
    their rows."""
    if not db.query(q.schedule_sql(), (int(job_id),)):
        raise HTTPException(404, f"ingestion job {job_id} not found")
    return sorted({m["task_collection"] for m in _active(db.query(q.schedule_members_sql(), ([int(job_id)],)))})


def _lock_job(cur, job_id: int) -> List[str]:
    """Inside an applying transaction: wait for the job's schedule lock (held by member changes and syncs too),
    then read its current groups, so a group added or removed meanwhile is never edited from a stale list."""
    cur.execute(q.schedule_lock_sql(), (q.SCHEDULE_LOCK_CLASS, q.schedule_lock_key(job_id)))
    members = db.fetch(cur, q.schedule_members_sql(), ([int(job_id)],))
    return sorted({m["task_collection"] for m in members if m["removed_at"] is None})


def _started_job(job: dict) -> Optional[str]:
    """The Databricks job an ingestion job's runs execute as: the check job on a check schedule, else its variant's
    ingestion job. Every action on the ingestion job follows the signed-in user's permission on it."""
    if (job.get("job_variant") or "default") == sch.CHECK_VARIANT:
        return jobs.CHECK_JOB_ID
    copy = sch.copy_job_id(job)
    return str(copy) if copy else jobs.ingest_variants().get(job.get("job_variant") or "default")


# Who the user is comes from the Apps proxy's X-Forwarded-Email. Without it nothing can be checked, so every
# permission check refuses.
NO_IDENTITY = "the app can't tell who you are (the Databricks Apps proxy sent no user), so it refuses this"


DELETED_JOB = ("this job is deleted: its runs and tables can't change while its delete runs or after it, and it "
               "can't be run or edited; Run again on it starts its delete job again")


def _permission_refusals(job: dict, user: str) -> dict:
    """{action: why the user may not take it, or None} for each action on an ingestion job (run, change, delete),
    from their Databricks permission on the job its runs execute as. A deleted job can't be run or changed; its
    delete stays open, for Run again."""
    refusals = _job_permission_refusals(job, user)
    if job.get("deleted_at") is None:
        return refusals
    return {**refusals, "run": DELETED_JOB, "change": DELETED_JOB}


def _job_permission_refusals(job: dict, user: str) -> dict:
    if not (user or "").strip():
        return {action: NO_IDENTITY for action in jobs.ACTION_NEEDS}
    started = _started_job(job)
    if not started:
        # An orphaned job (its variant no longer configured) has no Databricks job to ask; admins may clean it up.
        reason = "no Databricks job is configured for this job's runs"
        try:
            admin = "admins" in jobs.user_groups(user)
        except Exception:
            admin = False
        admin_only = None if admin else f"{reason}, so only a workspace admin can change or delete it"
        return {"run": reason, "change": admin_only, "delete": admin_only}
    refusals = _databricks_job_refusals(started, user)
    return {action: refusals[action] for action in jobs.ACTION_NEEDS}


def _databricks_job_refusals(job_id, user: str) -> dict:
    """{action: why the user may not take it on this Databricks job, or None}."""
    if not (user or "").strip():
        return {action: NO_IDENTITY for action in jobs.ACTION_NEEDS}
    label = f"the Databricks job {job_id}"
    try:
        level = jobs.user_job_permission(job_id, user)
    except Exception as e:  # shown on the page and refused; Databricks decides, so no answer means no
        reason = f"can't read your permission on {label}: {str(e).split('. ')[0]}"
        return {action: reason for action in jobs.ACTION_NEEDS}
    return {action: jobs.action_refusal(action, level, label) for action in jobs.ACTION_NEEDS}


def _refuse(refusal: Optional[str]) -> None:
    if refusal:
        raise HTTPException(403, refusal)


def _refuse_job(job: dict, request: Request, action: str) -> None:
    """Refuse the call unless the signed-in user may take this action on the ingestion job."""
    _refuse(_permission_refusals(job, _user(request))[action])


def _refuse_run_of(job_id, request: Request) -> None:
    """Refuse starting (or cancelling) a run of one of the bundle's Databricks jobs without Can Manage Run on it."""
    _refuse(_databricks_job_refusals(job_id, _user(request))["run"])


def _groups_refusal(groups: Iterable[str], user: str) -> Optional[str]:
    """Why the user may not change tables of these ingestion groups, or None: every ingestion job holding one of them
    needs their Can Manage. Groups in no job are open, as anyone may set up a new job. Read fresh, not shared."""
    wanted = {g for g in groups if g}
    if not wanted:
        return None
    if not (user or "").strip():
        return NO_IDENTITY
    schedules = db.query(q.schedules_sql())
    members = db.query(q.schedule_members_sql(), ([s["id"] for s in schedules],)) if schedules else []
    holding = {m["schedule_id"] for m in members if m["removed_at"] is None and m["task_collection"] in wanted}
    for s in schedules:
        if s["id"] in holding:
            refusal = _permission_refusals(s, user)["change"]
            if refusal:
                return f"these tables are in the ingestion job {s['name']}: {refusal}"
    return None


def _refuse_groups(groups: Iterable[str], request: Request) -> None:
    _refuse(_groups_refusal(groups, _user(request)))


def _refuse_rows_groups(ids: List[int], request: Request, also: Iterable[Optional[str]] = ()) -> set:
    """Refuse a change to these control rows unless the user may change every job running their groups (and the
    groups in `also`). Read and checked before the rows are locked, since the Permissions and SCIM calls can take a
    while and edits of those rows would wait on them; returns the groups checked, for _recheck_locked_rows."""
    groups = {r["task_collection"] for r in db.query(q.control_groups_sql(), (list(ids),))} | {g for g in also if g}
    _refuse_groups(groups, request)
    return groups


def _recheck_locked_rows(rows: List[dict], checked: set, request: Request) -> None:
    """A row moved to another group between the check and its lock is checked again, now under the lock."""
    moved = {r["task_collection"] for r in rows} - checked
    if moved:
        _refuse_groups(moved, request)


def _refuse_job_tables(job: dict, groups: Iterable[str], request: Request) -> None:
    """Refuse a change to a job's tables unless the user may change the job and every other job that runs one of
    the groups it touches: a group can be in more than one job, and its tables change for all of them. Checked
    before any lock is taken, as the Permissions and SCIM calls can be slow."""
    _refuse_job(job, request, "change")
    _refuse_groups(groups, request)


OUTSIDE_A_JOB = ("every ingestion group belongs to an ingestion job, and {groups} is in none: add tables through a "
                 "job's Add tables, or make a job for them with New ingestion job")


def _refuse_groups_outside_jobs(groups: Iterable[str]) -> None:
    """Refuse a write that would put tables into a group no ingestion job holds, so no group is ever outside a job.
    New ingestion job makes its groups with the job, in one transaction, and Add tables puts tables only into the
    job's own groups, so neither comes here. Read fresh, not shared."""
    wanted = {g for g in groups if g}
    if not wanted:
        return
    schedules = db.query(q.schedules_sql())
    members = db.query(q.schedule_members_sql(), ([s["id"] for s in schedules],)) if schedules else []
    held = {m["task_collection"] for m in members if m["removed_at"] is None}
    outside = sorted(wanted - held)
    if outside:
        raise HTTPException(422, OUTSIDE_A_JOB.format(groups=", ".join(outside)))


def _run_now_blocker(job: dict, members: List[dict]) -> Optional[str]:
    """Why the job can't be started by hand right now (a run of it is going, or its wrapper jobs aren't synced). The
    running runs are read after the app has closed those whose Databricks run ended (_running)."""
    if _recorded_running(members):
        return "a run of this job is already running: cancel it, or wait for it to end"
    if not members or any(not m.get("job_id") for m in members):
        return "the job's Databricks jobs are not synced yet"
    return None


def _recorded_running(members: List[dict]) -> bool:
    """Whether Lakebase has a run of one of these groups going, after closing those whose Databricks run ended."""
    groups = {m["task_collection"] for m in members}
    return any(r["task_collection"] in groups for r in _running())


@app.post("/api/ingestion-jobs/{job_id}/run")
def run_ingestion_job(job_id: int, request: Request):
    """Start one run of an ingestion job by hand, as its schedule would: its wrapper jobs (one per group, or a check
    schedule's one wrapper, which starts the check job over every group). Refused while a run of it is going."""
    job, members = _load_schedule(job_id)
    _refuse_job(job, request, "run")
    members = [m for m in members if m.get("removed_at") is None]
    blocker = _run_now_blocker(job, members)
    if blocker:
        raise HTTPException(409, blocker)
    # Lakebase sees a run only once its cluster is up; the Jobs API sees it at once. Databricks would skip a second
    # run of a wrapper (max_concurrent_runs 1) without a word, so a Run now while one is going is refused here.
    going = _active_wrapper_runs(members)
    if going:
        raise HTTPException(409, {"message": f"a run of this job is already going, started {going[0].get('started') or 'just now'}: "
                                             "Databricks would skip a second one, so wait for it to end",
                                  "run_url": going[0].get("url"), "run_id": going[0].get("run_id")})
    wrappers = sorted({int(m["job_id"]) for m in members})
    host = jobs.workspace_url()
    started = []
    for wrapper in wrappers:
        run_id = jobs.run_now(wrapper, {})
        started.append({"job_id": wrapper, "run_id": run_id, "url": f"{host}/jobs/{wrapper}/runs/{run_id}" if host else None})
    return {"started": started}


def _job_active_runs(job: dict, members: List[dict]) -> List[dict]:
    """Every Databricks run of an ingestion job that is still active, from the poller's table: its wrapper runs, its
    check job's run (check kind), and the group runs of its groups. Cancel run cancels all of them, so Cancelling
    waits for all of them to end."""
    groups = {m["task_collection"] for m in members}
    runs = _active_wrapper_runs(members)
    check_kind = (job.get("job_variant") or "default") == sch.CHECK_VARIANT
    if check_kind and jobs.CHECK_JOB_ID:
        runs += [r for r in run_poller.runs_of(jobs.CHECK_JOB_ID, active_only=True, limit=10)
                 if ij.check_run_of(r, job["id"], groups)]
    target = (sch.copy_job_id(job) or _ingest_job_id("default")) if check_kind else _wrapper_target(job)
    if target and groups:
        runs += [r for r in run_poller.runs_of(target, active_only=True, limit=50) if r.get("task_collection") in groups]
    return runs


def _active_wrapper_runs(members: List[dict]) -> List[dict]:
    """The job's wrapper jobs' runs going now (active only), newest first."""
    out = []
    for wrapper in sorted({int(m["job_id"]) for m in members if m.get("job_id")}):
        out += [r for r in run_poller.runs_of(wrapper, active_only=True, limit=5) if ij.is_active(r)]
    return sorted(out, key=lambda r: r.get("started") or "", reverse=True)


def _run_phase_of(job: dict, members: List[dict], now: datetime) -> dict:
    """Where Run now and Cancel run stand for a job (ingestion_jobs.run_phase), from its active runs. ``going`` is
    true while any run is active, so a page that asks what is going reads the same thing as the buttons."""
    phase = ij.run_phase(_job_active_runs(job, members), now)
    return {**phase, "going": phase["phase"] != ij.IDLE}


def _job_databricks_runs(job: dict, members: List[dict]) -> dict:
    """The Databricks runs of one ingestion job, as the Jobs API has them: its wrapper jobs' runs going now, its check
    job's recent runs (check kind) and the ingestion job's recent runs, of every group (main filters to the job's)."""
    groups = sorted({m["task_collection"] for m in members})
    check_kind = (job.get("job_variant") or "default") == sch.CHECK_VARIANT
    checks = [r for r in run_poller.runs_of(jobs.CHECK_JOB_ID, limit=10) if ij.check_run_of(r, job["id"], groups)] \
        if check_kind and jobs.CHECK_JOB_ID else []
    target = (sch.copy_job_id(job) or _ingest_job_id("default")) if check_kind else _wrapper_target(job)
    ingest = run_poller.runs_of(target, limit=max(25, 3 * len(groups))) if target else []
    return {"groups": groups, "wrappers": _active_wrapper_runs(members), "checks": checks, "ingest": ingest}


@app.get("/api/ingestion-jobs/{job_id}/live")
def ingestion_job_live(job_id: int):
    """The job run going now, as its Databricks runs are: the wrapper runs, the check run and what it found (check
    kind), and the group runs it started, with each one's state and, once it has recorded it, its progress. Read
    from the Jobs API, which sees a run from its start, joined with Lakebase. Nothing is going: active is false."""
    job, members = _load_schedule(job_id)
    members = [m for m in members if m.get("removed_at") is None]
    now = datetime.now(timezone.utc)
    found_runs = _job_databricks_runs(job, members)
    groups, wrappers, checks, ingest = (found_runs[k] for k in ("groups", "wrappers", "checks", "ingest"))
    check = max(checks, key=lambda r: r.get("started") or "", default=None)
    work_ids = sorted({(r.get("parameters") or {}).get("work_id") for r in ingest} - {None, ""})
    found = db.query(q.check_work_found_sql(), (work_ids,)) if check and work_ids else []
    stamped = db.query(q.database_checks_since_sql(), (groups, check["started"]))[0]["databases"] \
        if check and check.get("started") and groups else None
    return ij.live_view(wrappers, checks, ingest, groups, found, stamped, _running(), now)


@app.post("/api/ingestion-jobs/{job_id}/stop")
def stop_ingestion_job(job_id: int, request: Request):
    """Stop the job's run going now, as the Databricks job run page's Cancel run does: cancel its wrapper runs, its
    check run and the group runs of its groups (those the Jobs API lists, and any its groups' records name), then
    close the records of the runs it cancelled or found already over (the cancel itself takes Databricks a moment).
    A record whose run it could neither cancel nor read stays running. Needs what Run now needs. Nothing going: 409."""
    job, members = _load_schedule(job_id)
    _refuse_job(job, request, "run")
    members = [m for m in members if m.get("removed_at") is None]
    user = _user(request)
    found_runs = _job_databricks_runs(job, members)
    groups = set(found_runs["groups"])
    going = [dict(r, kind="wrapper") for r in found_runs["wrappers"]] + \
        [dict(r, kind="check") for r in found_runs["checks"] if ij.is_active(r)] + \
        [dict(r, kind="group") for r in found_runs["ingest"]
         if ij.is_active(r) and (r.get("parameters") or {}).get("task_collection") in groups]
    recorded = [r for r in db.query(q.open_runs_sql()) if r["task_collection"] in groups]
    if not going and not recorded:
        raise HTTPException(409, "no run of this job is going")
    jobs.forget_run_states()
    errors, over = [], set()
    listed = {int(r["run_id"]) for r in going}
    for r in recorded:
        job_run = str(r.get("job_run_id") or "").strip()
        if not job_run.isdigit() or int(job_run) in listed:
            continue
        try:        # a run the list missed (an older one on a busy ingestion job) is cancelled too
            if run_poller.ended_of(job_run):
                over.add(int(job_run))
            else:
                going.append({"kind": "group", "run_id": int(job_run),
                              "parameters": {"task_collection": r["task_collection"]}})
                listed.add(int(job_run))
        except Exception as e:
            errors.append(f"run {job_run}: {str(e).split('. ')[0][:200]}")
    cancelled = []
    for r in {int(r["run_id"]): r for r in going}.values():
        try:
            jobs.cancel(r["run_id"])
            cancelled.append({"kind": r["kind"], "run_id": r["run_id"], "job_id": r.get("job_id"), "url": r.get("url"),
                              "task_collection": (r.get("parameters") or {}).get("task_collection")})
        except Exception as e:      # one that ended meanwhile, or a refusal: the others are still stopped
            errors.append(f"run {r['run_id']}: {str(e).split('. ')[0][:200]}")
    stopped = over | {int(r["run_id"]) for r in cancelled}
    closing = [r for r in recorded
               if str(r.get("job_run_id") or "").strip().isdigit() and int(r["job_run_id"]) in stopped]
    reason = f"stopped from the app by {user}"
    detail = json.dumps({"closed_by_app": user, "reason": reason})
    batch = uuid.uuid4().hex[:12]
    with _write() as cur:
        for r in closing:
            cur.execute(q.close_ended_run_sql(), (detail, int(r["run_id"]), reason, reason))
        for group in sorted({r.get("task_collection") for r in cancelled if r.get("task_collection")} |
                            {r["task_collection"] for r in closing}) or [None]:
            _log(cur, user, "job_stop", None, group, "run", None, f"{job['name']}: {len(cancelled)} Databricks runs cancelled",
                 batch=batch)
    jobs.forget_run_states()
    return {"cancelled": cancelled, "closed": len(closing), "errors": errors}


@app.get("/api/ingestion-jobs/{job_id}/compute")
def ingestion_job_compute(job_id: int):
    """What a job's runs execute on, read live from the Jobs API: the check job (on a check schedule) and the
    ingestion job it starts, each task's compute with where every value comes from (compute.describe)."""
    job = _load_schedule(job_id)[0]
    variant = job.get("job_variant") or "default"
    ingestion = sch.copy_job_id(job) or jobs.ingest_variants().get("default" if variant == sch.CHECK_VARIANT else variant)
    started = ([("check", jobs.CHECK_JOB_ID)] if variant == sch.CHECK_VARIANT else []) + [("ingestion", ingestion)]
    out = []
    for role, started_job_id in started:
        entry = {"role": role, "job_id": started_job_id, "name": None, "compute": [], "error": None}
        if not started_job_id:
            entry["error"] = f"no {role} job is configured for this app"
        else:
            try:
                settings = jobs.job_settings(started_job_id)
                entry.update(name=settings.get("name"), compute=compute.describe(
                    settings, own_cluster=role == "ingestion" and bool(sch.copy_job_id(job))))
            except Exception as e:  # shown on the page; the rest of the job's settings still load
                entry["error"] = f"can't read job {started_job_id}: {e}"
        out.append(entry)
    return {"jobs": out}


# Where the cluster copies run has not ended this long after its start, the Edit view says so, and the save is not done.
CLUSTER_LIMIT_S = 30 * 60


@app.get("/api/ingestion-jobs/{job_id}/cluster")
def ingestion_job_cluster(job_id: int, request: Request):
    """A job's own cluster for the Edit view: what it asks for (None = the ingestion job's own), the ingestion job's
    values as the defaults, the workspace's node types, the copy it runs, and the copies job's run applying a change
    (with its outcome: pending, timed out, or not done). Once that run has ended, the job's wrapper jobs are synced to
    start the copy (or the ingestion job again); a failed copy or sync is the error, and the copy is not applied."""
    s = _load_schedule(job_id)[0]
    applying, error = None, None
    run_id = sch.options_of(s).get("cluster_copy_run_id")
    if run_id:
        state = run_poller.run_state_of(run_id)
        outcome = action_states.run_outcome(state, datetime.now(timezone.utc), CLUSTER_LIMIT_S)
        applying = {"run_id": run_id, "state": state["state"], "result": state["result"], "url": state["url"],
                    "outcome": outcome}
        if state["state"] in run_poller.ENDED_STATES or state["state"] == run_poller.NOT_FOUND:
            with _write() as cur:
                cur.execute(q.clear_schedule_option_sql(), ("cluster_copy_run_id", int(job_id)))
            if state["result"] == "SUCCESS":
                problems = action_states.sync_failures(_sync(job_id, _user(request)))
                s = _load_schedule(job_id)[0]
                if problems:
                    error = "the job's Databricks jobs did not sync: " + "; ".join(problems)
            else:
                error = f"the cluster copies run did not apply the cluster: {outcome['reason']}"
            applying = None
    choices = _cluster_choices()
    copy = sch.copy_job_id(s)
    host = db.workspace().config.host.rstrip("/")
    return {**choices, "cluster": sch.own_cluster(s), "copy_job_id": copy, "copy_url": f"{host}/jobs/{copy}" if copy else None,
            "applying": applying, "error": error or choices["error"]}


@app.get("/api/cluster-options")
def cluster_options():
    """What New offers for a job's own cluster before the job exists: the ingestion job's values as the defaults and the
    workspace's node types, as the Edit view's Job cluster shows them."""
    return {**_cluster_choices(), "cluster": None, "copy_job_id": None, "copy_url": None, "applying": None}


def _cluster_choices() -> dict:
    """The ingestion job's cluster values (the defaults a job's own cluster starts from) and the workspace's node types."""
    error = None
    try:
        bundle_cluster = ((jobs.job_settings(jobs.ingest_variants().get("default")).get("job_clusters") or [{}])[0]
                          .get("new_cluster") or {})
        defaults = {k: bundle_cluster.get(k) for k in sch.CLUSTER_KEYS}
    except Exception as e:
        defaults, error = {}, f"can't read the ingestion job's cluster: {str(e).split('. ')[0]}"
    try:
        node_types = jobs.node_type_details()
    except Exception:
        node_types = []
    return {"defaults": defaults, "node_types": node_types, "max_workers": sch.MAX_WORKERS, "error": error,
            "configured": bool(jobs.CLUSTER_COPIES_JOB_ID)}


class JobSettingsRequest(BaseModel):
    settings: dict
    dry_run: bool = True


@app.get("/api/ingestion-jobs/{job_id}/settings")
def ingestion_job_settings(job_id: int):
    """What all of a job's tables share (job_settings_sql.read: one scan, whatever the job's size), each setting
    with its value or its most common values."""
    groups = _job_groups(job_id)
    if not groups:
        return {"job_id": int(job_id), "groups": groups, "tables": 0, "settings": js.read([])}
    with _snapshot() as fetch:
        tables, settings = fetch(q.groups_row_count_sql(), (groups,))[0]["n"], jss.read(fetch, groups)
    return {"job_id": int(job_id), "groups": groups, "tables": tables, "settings": settings,
            "identity_differs": js.differing_identity_columns(settings["identity"], tables)}


@app.post("/api/ingestion-jobs/{job_id}/settings")
def edit_ingestion_job_settings(job_id: int, body: JobSettingsRequest, request: Request):
    """Preview (dry_run, the default) or apply a settings edit to every table of a job, in set-based SQL
    (job_settings_sql): templates filled per row, a reseed where a change-tracked or cursor table gets a new
    target or client_id, one change-log line per database. Anything it can't take is a 422 naming why. Applying
    is one transaction under the job's lock and every group's run lock."""
    import psycopg
    groups = _job_groups(job_id)
    if not groups:
        return {"summary": {"rows": 0, "changed": 0, "reseed": 0, "errors": 0, "applied": False}, "errors": []}

    def checked(fetch, groups):
        now = jss.read(fetch, groups)
        found = jss.preview(fetch, groups, body.settings, now)
        if found["errors"]:
            raise HTTPException(422, {"errors": found["errors"], "summary": found["summary"]})
        return now, found

    if body.dry_run:
        with _snapshot() as fetch:
            return checked(fetch, groups)[1]
    _refuse_job_tables(_load_schedule(job_id)[0], groups, request)
    try:
        with _write() as cur:
            groups = _lock_job(cur, job_id)
            _lock_collections(cur, groups)
            fetch = lambda sql, params=(): db.fetch(cur, sql, params)
            now, found = checked(fetch, groups)
            if not found["summary"]["changed"]:
                return found
            result = jss.apply(cur, groups, body.settings, now, _user(request), uuid.uuid4().hex[:12])
            _rewrite_rollups_for_groups(cur, groups)
            _touch(cur, _user(request), [job_id])
            mixed = jss.mixed_sinks(fetch, groups)
            if mixed:
                raise HTTPException(409, f"target {mixed[0]} would be claimed by a dedicated table and by others; "
                                         "a target is either one table's own or shared by client_id. Nothing was changed")
            found["summary"].update(changed=result["changed"], reseed=result["reseed"], applied=True)
            return found
    except psycopg.errors.UniqueViolation:
        raise HTTPException(409, "another control row already writes one of those targets (or that target and "
                                 "client_id); nothing was changed")
    except psycopg.errors.CheckViolation as e:
        raise HTTPException(422, {"errors": [f"a table can't take this change ({e.diag.constraint_name}); "
                                             "nothing was changed"]})


@contextmanager
def _snapshot():
    """A fetch over one read-only, repeatable-read transaction, so a read of several queries sees one moment."""
    with db.transaction() as cur:
        cur.execute("set transaction isolation level repeatable read, read only")
        yield lambda sql, params=(): db.fetch(cur, sql, params)


@app.get("/api/ingestion-jobs/{job_id}/tree")
def ingestion_job_tree(job_id: int, server: str = "", database: str = "", search: str = "", offset: int = 0,
                       limit: int = 50):
    """One level of a job's tables: its servers; with server, that server's databases; with database too, that
    database's tables. Searched and paged at each level."""
    groups = _job_groups(job_id)
    offset, limit = max(0, int(offset)), max(1, min(int(limit), 200))
    pattern = f"%{search.strip()}%"
    if not groups:
        rows, level = [], "servers"
    elif not server:
        level, rows = "servers", db.query(q.job_servers_sql(), (groups, pattern, limit, offset))
    elif not database:
        level, rows = "databases", db.query(q.job_databases_sql(), (groups, groups, server, pattern, limit, offset))
    else:
        level, rows = "tables", db.query(q.job_tables_sql(), (groups, server, database, pattern, limit, offset))
        for r in rows:
            r["columns"] = len([c for c in str(r.pop("select_list") or "").split(",") if c.strip()])
    total = rows[0]["total"] if rows else 0
    return {"level": level, "server": server or None, "database": database or None, "total": total,
            "offset": offset, "limit": limit, "items": [{k: v for k, v in r.items() if k != "total"} for r in rows]}


@app.get("/api/ingestion-jobs/{job_id}/targets")
def ingestion_job_targets(job_id: int, search: str = "", offset: int = 0, limit: int = 25):
    """A job's target tables: each with the source tables of the job writing it (and how many outside it), their
    states, the rows the run records say were written over 7 days, and a link to the table in Catalog Explorer;
    fan_in counts those written from more than one source table. Searched by name and paged."""
    groups = _job_groups(job_id)
    offset, limit = max(0, int(offset)), max(1, min(int(limit), 200))
    rows = (db.query(q.job_targets_sql(), (groups, groups, groups, f"%{search.strip()}%", limit, offset))
            if groups else [])
    host = db.workspace().config.host.rstrip("/")
    items = [{**{k: v for k, v in r.items() if k not in ("total", "fan_in_total")},
              "url": f"{host}/explore/data/{r['catalog']}/{r['schema']}/{r['table']}"} for r in rows]
    return {"total": rows[0]["total"] if rows else 0, "fan_in": rows[0]["fan_in_total"] if rows else 0,
            "offset": offset, "limit": limit, "items": items}


@app.get("/api/ingestion-jobs/{job_id}/targets/sources")
def ingestion_job_target_sources(job_id: int, target: str, offset: int = 0, limit: int = 25):
    """The job's source tables writing one target (catalog.schema.table), paged."""
    parts = target.split(".")
    if len(parts) != 3 or not all(parts):
        raise HTTPException(400, "give the target as catalog.schema.table")
    groups = _job_groups(job_id)
    offset, limit = max(0, int(offset)), max(1, min(int(limit), 200))
    rows = db.query(q.job_target_sources_sql(), (groups, groups, *parts, limit, offset)) if groups else []
    return {"target": target, "total": rows[0]["total"] if rows else 0, "offset": offset, "limit": limit,
            "items": [{k: v for k, v in r.items() if k != "total"} for r in rows]}


class JobScope(BaseModel):
    server: str = ""
    database: str = ""
    schema_name: str = Field("", alias="schema")
    table: str = ""


class JobEnableRequest(JobScope):
    enabled: bool


def _job_row_ids(cur, groups, scope: JobScope) -> List[int]:
    cur.execute(q.job_row_ids_sql(), (groups, scope.server, scope.server, scope.database, scope.database,
                                      scope.schema_name, scope.schema_name, scope.table, scope.table))
    return [r[0] for r in cur.fetchall()]


@app.post("/api/ingestion-jobs/{job_id}/enable")
def enable_job_tables(job_id: int, body: JobEnableRequest, request: Request):
    """Enable or disable a job's tables on a server, in a database, or one table in every database."""
    _refuse_job_tables(_load_schedule(job_id)[0], _job_groups(job_id), request)
    batch = uuid.uuid4().hex[:12]
    with _write() as cur:
        groups = _lock_job(cur, job_id)
        ids = _job_row_ids(cur, groups, body) if groups else []
        if not ids:
            return {"updated": 0}
        rows = _locked_rows(cur, ids)
        _lock_collections(cur, [r["task_collection"] for r in rows])
        cur.execute(q.set_enabled_sql(), (body.enabled, ids))
        _rewrite_rollups_for_ids(cur, ids)
        for r in rows:
            if bool(r["task_enabled"]) != body.enabled:
                _log(cur, _user(request), "enable" if body.enabled else "disable", r["id"], r["task_collection"],
                     "task_enabled", r["task_enabled"], body.enabled, batch=batch)
        _touch(cur, _user(request), [job_id])
    return {"updated": len(ids)}


JOB_TABLE_FIELDS = ("load_type", "primary_key", "select_list", "cursor_column", "load_partitioned", "partition_col",
                    "partition_size_mb")


class JobTableEditRequest(JobScope):
    changes: dict
    dry_run: bool = True


@app.post("/api/ingestion-jobs/{job_id}/tables/edit")
def edit_job_table(job_id: int, body: JobTableEditRequest, request: Request):
    """Change one table (schema and table) in every database of a job: its load type, key or columns. Checked
    and applied as a table edit, all or nothing, under the groups' run locks."""
    bad = sorted(set(body.changes) - set(JOB_TABLE_FIELDS))
    if bad or not body.schema_name or not body.table:
        raise HTTPException(400, f"give schema and table, and change only {', '.join(JOB_TABLE_FIELDS)}"
                                 + (f" (not {', '.join(bad)})" if bad else ""))
    groups = _job_groups(job_id)
    if not body.dry_run:
        _refuse_job_tables(_load_schedule(job_id)[0], groups, request)
    with _write() as cur:
        groups = _lock_job(cur, job_id)
        ids = _job_row_ids(cur, groups, body) if groups else []
        rows = _locked_rows(cur, ids) if ids else []
        plans = [q.plan_edit([r], body.changes, bulk=False)[0] for r in rows]
        summary = {"rows": len(plans), "changed": sum(1 for p in plans if p["changes"]),
                   "reseed": sum(1 for p in plans if p["reseed"]),
                   "errors": sum(1 for p in plans if p["errors"]), "applied": False}
        if body.dry_run or summary["errors"] or not summary["changed"]:
            cur.connection.rollback()
            return {"summary": summary, "plans": plans[:200]}
        _lock_collections(cur, [r["task_collection"] for r in rows])
        _apply_plans(cur, rows, plans, _user(request), uuid.uuid4().hex[:12])
        _touch(cur, _user(request), [job_id])
        summary["applied"] = True
        return {"summary": summary, "plans": plans[:200]}


class JobDatabasesRequest(BaseModel):
    csv: Optional[str] = None
    columns: dict = {"server": "server", "database": "database", "enabled": "enabled"}
    databases: Optional[List[dict]] = None
    groups_per_server: int = Field(1, ge=1, le=64)
    full_list: bool = False
    dry_run: bool = True
    request_id: Optional[str] = Field(None, max_length=80)


@app.post("/api/ingestion-jobs/{job_id}/databases")
def add_job_databases(job_id: int, body: JobDatabasesRequest, request: Request):
    """Preview (dry_run, the default) or apply databases into a job, from a databases file (its columns matched
    to server, database and enabled) or a list picked by hand (job_settings.plan_databases). Applying is one
    transaction under every group's run lock: new tables inserted (a taken target is skipped), enabled flags
    set, new groups added to the job; then the job's wrapper jobs are synced."""
    user = _user(request)

    def after(result):
        if result.get("new_groups"):
            _synced(_sync(job_id, user))
        return {k: v for k, v in result.items() if k != "new_groups"}

    if (made := _made_already(body, after)) is not None:
        return made
    if body.csv is not None:
        entries, errors = js.read_databases(body.csv, body.columns)
    else:
        entries = [{"line": i + 1, "server": str(d.get("server") or "").strip(),
                    "database": str(d.get("database") or "").strip(), "enabled": q._truthy(d.get("enabled", True))}
                   for i, d in enumerate(body.databases or [])]
        errors = [f"line {e['line']}: server and database are required" for e in entries
                  if not e["server"] or not e["database"]]
    if errors:
        raise HTTPException(422, {"errors": errors})
    job = _load_schedule(job_id)[0]

    def planned(rows):
        plan = js.plan_databases(rows, entries, job["name"], body.groups_per_server, body.full_list)
        if plan["errors"]:
            raise HTTPException(422, {"errors": plan["errors"]})
        summary = {"databases": len(entries), "new_tables": len(plan["new"]), "enable": len(plan["enable"]),
                   "disable": len(plan["disable"]), "new_groups": plan["new_groups"], "applied": False}
        return plan, summary

    groups = _job_groups(job_id)
    plan, summary = planned(db.query(q.groups_control_rows_sql(), (groups,)) if groups else [])
    if body.dry_run:
        return {"summary": summary}
    _refuse_job_tables(job, groups + plan["new_groups"], request)
    batch = uuid.uuid4().hex[:12]

    def write(progress, task_id):
        with _write() as cur:
            groups = _lock_job(cur, job_id)
            rows = db.fetch(cur, q.groups_control_rows_sql(for_update=True), (groups,)) if groups else []
            plan, summary = planned(rows)
            _lock_collections(cur, groups + plan["new_groups"])
            written = _insert_controls(cur, plan["new"], user, batch, skip_taken=True, progress=progress)
            off = [control_id for row, control_id in written if not row["task_enabled"]]
            if off:
                cur.execute(q.set_enabled_sql(), (False, off))
                _rewrite_rollups_for_ids(cur, off)
            for enabled, ids in ((True, plan["enable"]), (False, plan["disable"])):
                if ids:
                    cur.execute(q.set_enabled_sql(), (enabled, ids))
                    _rewrite_rollups_for_ids(cur, ids)
                    _log_lines(cur, [(user, "enable" if enabled else "disable", i, None, "task_enabled", not enabled,
                                      enabled, None, batch) for i in ids])
            _refuse_mixed_sinks(cur, plan["new"])
            _refuse_split_databases(cur, plan["new"])
            if plan["new_groups"]:
                _, members = _load_schedule(job_id, cur)
                active = [m for m in members if m["removed_at"] is None]
                offs = sch.offsets([m["task_collection"] for m in active] + plan["new_groups"],
                                   int(job["cadence_minutes"]), bool(job.get("stagger")), sch.start_minute(job))
                for g in plan["new_groups"]:
                    cur.execute(q.insert_member_sql(), (job_id, g, offs[g]))
            _touch(cur, user, [job_id])
            summary.update(applied=True, new_tables=len(written), skipped=len(plan["new"]) - len(written))
            result = {"summary": summary}
            _task_written(cur, task_id, {**result, "new_groups": plan["new_groups"]}, job_id, len(plan["new"]))
        return {**result, "new_groups": plan["new_groups"]}

    return _write_now_or_later("databases", body.request_id, job_id, len(plan["new"]), user, write, after)


class _TablesFrom(BaseModel):
    """Tables given as CSV lines in the body (a small file, as before uploads), or by reference (source)."""
    csv: Optional[str] = None
    source: Optional[Selection] = None

    @model_validator(mode="after")
    def _csv_or_source(self):
        if (self.csv is None) == (self.source is None):
            raise ValueError("give the tables as csv or as a source, not both")
        return self

    def text(self, request: Optional[Request] = None, wait: bool = False):
        """(the CSV a plan reads, the selection it came from or None). A selection's database-only lines have their
        tables discovered first: while that discovery is still running (and not waited for) the text is None and the
        selection says {"discovering": True, "databases": how many}."""
        if self.source is None:
            return self.csv, None
        selected = _selected(self.source)
        resolved = _resolve_databases(selected, wait=wait, request=request)
        if resolved is not None and resolved["discovering"]:
            return None, {"discovering": True, "databases": resolved["databases"]}
        selected = _with_databases(selected, resolved)
        return selected["text"], selected


class JobTablesRequest(_TablesFrom):
    load_type: str = "ct"
    groups_per_server: int = nj.DEFAULT_GROUPS_PER_SERVER
    dry_run: bool = True
    request_id: Optional[str] = Field(None, max_length=80)


def _write_planned_tables(cur, job: dict, plan: dict, user: str, batch: str, progress=None,
                          skip_taken: bool = False) -> List[tuple]:
    """Inside the caller's transaction, with the job's groups locked: the plan's valid lines as control rows (each with
    its change log line), their groups' run locks, the mixed-sink and split-database checks, and the job's new groups
    as members with offsets. Returns the (row, id) pairs written; skip_taken leaves out a row whose target another
    table writes, instead of refusing the lot."""
    job_id = int(job["id"])
    _lock_collections(cur, plan["groups"])
    rows = [line["row"] for line in plan["lines"] if line["status"] == "valid"]
    written = _insert_controls(cur, rows, user, batch, skip_taken=skip_taken, progress=progress)
    _refuse_mixed_sinks(cur, [row for row, _ in written])
    _refuse_split_databases(cur, [row for row, _ in written])
    if plan["new_groups"]:
        _, members = _load_schedule(job_id, cur)
        active = [m for m in members if m["removed_at"] is None]
        offs = sch.offsets([m["task_collection"] for m in active] + plan["new_groups"],
                           int(job["cadence_minutes"]), bool(job.get("stagger")), sch.start_minute(job))
        for g in plan["new_groups"]:
            cur.execute(q.insert_member_sql(), (job_id, g, offs[g]))
    return written


@app.post("/api/ingestion-jobs/{job_id}/tables")
def add_job_tables(job_id: int, body: JobTablesRequest, request: Request):
    """Preview (dry_run, the default) or add a CSV of tables to a job, as Add tables does in the Edit view: each line
    says where a table is, and the table takes the job's settings (new_job.plan_for_job). A database the job has keeps
    its group, a new one joins its server's groups, a new server gets new groups. Applying is one transaction under
    the job's lock and its groups' run locks; tables the job already writes are left; then new groups are synced. The
    tables come as CSV lines (csv) or by reference to an upload or discovery (source), which is kept with the job."""
    return _add_job_tables(job_id, body, request)


def _add_job_tables(job_id: int, body: "JobTablesRequest", request: Request, inline: bool = False, progress=None,
                    task_id=None):
    """The add itself. A file with database-only lines is first discovered by name in a background task
    (_add_after_discovery), which runs this again with inline=True, so the write happens in that task."""
    user = _user(request)

    def after(result):
        if result.get("new_groups"):
            _synced(_sync(job_id, user))
        return {k: v for k, v in result.items() if k != "new_groups"}

    if body.source is not None and not _has_database_lines(body.source):
        kind, param, source_id, rest = _selection_params(body.source)
        counted = db.query(q.selected_count_sql(kind), (param, *rest))
        if counted and counted[0]["n"] > BACKGROUND_ROWS:
            return _add_by_database(job_id, body, request, kind, param, source_id, rest, int(counted[0]["n"]))
    if not inline and (made := _made_already(body, after)) is not None:
        return made
    job = _load_schedule(job_id)[0]
    if not inline and body.source is not None and not body.dry_run and _has_database_lines(body.source):
        _refuse_job_tables(job, _job_groups(job_id), request)
        return _write_now_or_later("tables", body.request_id, job_id, 0, user,
                                   lambda progress, task_id: _add_after_discovery(job_id, body, request, progress, task_id),
                                   after, background=True)
    taken = {r["task_collection"] for r in db.query(q.collections_sql())}
    text, selected = body.text(request, wait=inline)
    if selected is not None and selected.get("discovering"):
        return {"summary": None, "applied": False, "discovering": True, "databases": selected["databases"]}

    def planned(rows):
        plan = _file_lines(nj.plan_for_job(job["name"], text, body.load_type, rows, body.groups_per_server, taken),
                           selected)
        if plan["errors"]:
            raise HTTPException(422, {"errors": plan["errors"]})
        return plan

    groups = _job_groups(job_id)
    plan = planned(db.query(q.groups_control_rows_sql(), (groups,)) if groups else [])
    if body.dry_run:
        return {"summary": nj.summary(plan), "applied": False}
    _refuse_job_tables(job, groups + plan["groups"], request)
    batch = uuid.uuid4().hex[:12]

    def write(progress, task_id):
        with _write() as cur:
            groups = _lock_job(cur, job_id)
            plan = planned(db.fetch(cur, q.groups_control_rows_sql(for_update=True), (groups,)) if groups else [])
            summary = nj.summary(plan)
            if summary["invalid"]:
                return {"summary": summary, "applied": False, "new_groups": []}
            written = _write_planned_tables(cur, job, plan, user, batch, progress=progress)
            _keep_selection(cur, job_id, selected, user)
            _touch(cur, user, [job_id])
            result = {"summary": summary, "applied": True}
            _task_written(cur, task_id, {**result, "new_groups": plan["new_groups"]}, job_id, len(written))
        return {**result, "new_groups": plan["new_groups"]}

    if inline:
        return write(progress, task_id)
    valid = sum(line["status"] == "valid" for line in plan["lines"])
    return _write_now_or_later("tables", body.request_id, job_id, valid, user, write, after)


def _add_after_discovery(job_id: int, body: "JobTablesRequest", request: Request, progress, task_id) -> dict:
    """The background half of an add with database-only lines: their tables are discovered by name, then the add is
    written as an inline one is."""
    _task_stage(task_id, "discovering")
    _resolve_databases(_selected(body.source), wait=True, request=request)
    _task_stage(task_id, "writing")
    return _add_job_tables(job_id, body, request, inline=True, progress=progress, task_id=task_id)


def _instant(value: str, name: str) -> datetime:
    """A request's date and time, with its UTC offset (as the app's own links give it), or 400 saying what is wrong
    with it rather than a server error."""
    try:
        at = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        raise HTTPException(400, f"{name} must be a date and time such as 2026-10-29T13:00:00+00:00, not {value!r}")
    if at.tzinfo is None:
        raise HTTPException(400, f"{name} must say its UTC offset, such as 2026-10-29T13:00:00+00:00")
    return at


@app.get("/api/ingestion-jobs/{job_id}/runs")
def ingestion_job_runs(job_id: int, before: Optional[str] = None, limit: int = 25):
    """A job's job runs, newest first, `limit` slots before `before` (now when not given); `next_before` is
    the oldest slot shown, for the next page. Placed by the job's current schedule."""
    job = _schedule(job_id)
    now = datetime.now(timezone.utc)
    end = _instant(before, "before") if before else now + timedelta(seconds=1)
    limit = max(1, min(int(limit), 100))
    runs = _job_runs_of(job, end - timedelta(minutes=int(job["cadence_minutes"]) * limit), end, now)
    return {"job_runs": [{k: v for k, v in r.items() if k != "placed"} for r in runs[:limit]],
            "next_before": runs[-1]["slot"] if len(runs) >= limit else None, "job_id": job["id"], "job_name": job["name"]}


@app.get("/api/ingestion-jobs/{job_id}/runs/{slot}")
def ingestion_job_run(job_id: int, slot: str):
    """One job run: its summary (progress over all the job's tables, speed, expected end), its group runs
    (members, waiting ones with their start), its databases (failing first) and its failures by cause, with
    how many job runs before it also had failed tables."""
    job = _schedule(job_id)
    now = datetime.now(timezone.utc)
    at = _instant(slot, "the job run's time")
    cadence = int(job["cadence_minutes"])
    runs = _job_runs_of(job, at - timedelta(minutes=cadence * ij.RECENT_JOB_RUNS), at + timedelta(seconds=1), now)
    found = next((r for r in runs if r["slot"] == at.isoformat()), None)
    if not found:
        raise HTTPException(404, f"no job run of {job['name']} at {slot}")
    earlier = [r for r in runs if r["slot"] < found["slot"]]
    running = {r["run_id"]: r for r in _running()}
    tables = {name: g["enabled_tables"] for name, g in rollup.groups(_states()).items()}
    ids = found["run_ids"]
    return {**{k: v for k, v in found.items() if k != "placed"},
            "group_runs": ij.member_views(found["placed"], running, tables, now),
            "databases": db.query(q.job_run_databases_sql(), (ids,)) if ids else [],
            "failures": q.group_errors(db.query(q.run_failures_sql(), (ids,))) if ids and found["failed_tables"] else [],
            "failed_before": ij.consecutive_failed(earlier), "cadence_minutes": cadence, "paused": job.get("paused"),
            "read_at": now.isoformat(), "cache_s": MONITOR_CACHE_S}


# How far back Runs reads, at most, to fill a page an outcome filter has thinned.
JOB_RUNS_HISTORY = timedelta(days=30)


@app.get("/api/job-runs")
def job_runs_across(job: Optional[str] = None, outcome: Optional[str] = None, before: Optional[str] = None,
                    limit: int = 25):
    """Job runs of every job (or one, by id), running first, then newest first, `limit` a page before
    `before`, with outcome counts over the windows read. With an outcome filter, older windows are read until
    the page is full or JOB_RUNS_HISTORY is reached. job=none lists the group runs of the groups in no job
    instead, with today's run columns."""
    now = datetime.now(timezone.utc)
    end = _instant(before, "before") if before else now + timedelta(seconds=1)
    limit = max(1, min(int(limit), 100))
    schedules, members = _schedules()
    if job == "none":
        unscheduled = sorted({s["task_collection"] for s in _states()} - {m["task_collection"] for m in _active(members)}
                             - _deleting())
        rows = _with_rows_written(db.query(q.group_runs_before_sql(), (unscheduled, end, limit + 1))) if unscheduled else []
        more = len(rows) > limit
        rows = rows[:limit]
        return {"group_runs": rows, "job_runs": [], "counts": {}, "next_before": rows[-1]["started_at"] if more else None}
    chosen = [s for s in schedules if job is None or str(s["id"]) == str(job)]
    found, counts, start = [], {}, end
    while chosen:
        window_start = min(start - timedelta(minutes=int(s["cadence_minutes"]) * limit) for s in chosen)
        batch = []
        for s in chosen:
            batch += _job_runs_of(s, window_start, start, now)
        for r in batch:
            counts[r["outcome"]["label"]] = counts.get(r["outcome"]["label"], 0) + 1
        found += [r for r in batch if r["outcome"]["label"] != "Running" and (not outcome or r["outcome"]["label"] == outcome)]
        start = window_start
        if not outcome or len(found) > limit or end - start >= JOB_RUNS_HISTORY:
            break
    found.sort(key=lambda r: r["slot"], reverse=True)
    rest = found[:limit]
    # Running job runs come from the Overview's read, so one overrunning its window is still listed.
    running = [r for r in _jobs_view()["running_job_runs"] if (job is None or str(r["job_id"]) == str(job))
               and (not outcome or outcome == "Running")] if not before else []
    more = len(found) > limit or (not outcome and len(found) >= limit)
    return {"job_runs": [{k: v for k, v in r.items() if k != "placed"} for r in running + rest],
            "counts": counts, "next_before": rest[-1]["slot"] if rest and more else None,
            "jobs": [{"id": s["id"], "name": s["name"]} for s in schedules]}


# ---- sources and one database -------------------------------------------------------------------

@app.get("/api/sources")
def sources():
    """Source instances (rollup.sources): how each is read, its ingestion jobs, its tables by state and the
    tables discovered on it with no control row yet, plus readable UC connections with nothing registered.
    Shared like _states; a failure to list UC connections only drops the "nothing registered" rows."""
    def compute():
        schedules, members = _schedules()
        try:
            listed, listed_error = _connections(), None
        except Exception as e:
            listed, listed_error = [], str(e)
        view = rollup.sources(_states(), listed, db.query(q.not_ingested_sql()), schedules, members, _deleting())
        for instance in view["instances"]:
            for j in instance["jobs"]:
                if j.get("cadence_minutes"):
                    j["description"] = sch.describe(int(j["cadence_minutes"]), sch.start_minute(j), j.get("timezone") or "UTC")
        return {**view, "connections_error": listed_error, "read_at": datetime.now(timezone.utc).isoformat(),
                "cache_s": STATES_CACHE_S}
    return _shared("sources", compute, ttl=STATES_CACHE_S)


DATABASE_TABLES_PAGE = 100


@app.get("/api/collections/{name}/databases/{server}/{database}")
def one_database(name: str, server: str, database: str, search: str = "", offset: int = 0):
    """One database of an ingestion group: its tables by state now, the group's jobs and its running group
    run, and its tables (DATABASE_TABLES_PAGE a page, failing first, then in flight, then to go) with their
    part in that run. The server "-" stands for a blank one."""
    server = "" if server == "-" else server
    counts = db.query(q.database_counts_sql(), (name, server, database))[0]
    if not counts["control_rows"]:
        raise HTTPException(404, f"no database {database} on {server or '(no server)'} in ingestion group {name}")
    sums = {k: counts[k] for k in ("tables", "no_sla_tables", "state_current", "state_late", "state_first_load",
                                    "state_failing")}
    running = next((r for r in _running() if r["task_collection"] == name), None)
    schedules, members = _schedules()
    job_names = [{"id": s["id"], "name": s["name"]} for s in schedules
                 if any(m["schedule_id"] == s["id"] and m["task_collection"] == name for m in _active(members))]
    pattern = f"%{search.strip()}%" if search.strip() else "%"
    offset = max(0, int(offset))
    rows = db.query(q.database_tables_sql(), (name, server, database, running["run_id"] if running else None, pattern,
                                              DATABASE_TABLES_PAGE, offset))
    total = db.query(q.database_tables_count_sql(), (name, server, database, pattern))[0]["tables"]
    enabled = sums.pop("tables")     # "tables" in the answer is this page of them
    return {"task_collection": name, "server": server, "src_database": database, **sums, "enabled_tables": enabled,
            "jobs": job_names, "running": running, "tables": rows, "total": total, "offset": offset,
            "limit": DATABASE_TABLES_PAGE, "read_at": datetime.now(timezone.utc).isoformat()}


# ---- runs ---------------------------------------------------------------------------------------

@app.get("/api/runs")
def runs(collection: Optional[str] = None, limit: int = 200):
    """Recent runs, each with the rows it wrote: its table loads' counts plus its batched appends' total;
    rows_partial when some loads recorded no count."""
    sql, params = q.runs_sql(collection or None, limit=limit)
    return _with_rows_written(db.query(sql, params))


def _with_rows_written(out: List[dict]) -> List[dict]:
    """Each run with the rows it wrote, its work (queries.run_work_sql): first table start to last finish,
    and tables loaded, its outcome and its speed. Running runs take their progress from _running."""
    if out:
        work = {r["run_id"]: r for r in db.query(q.run_work_sql(), ([r["run_id"] for r in out],))}
        progress = {p["run_id"]: p for p in _running()} if any(r.get("status") == "running" for r in out) else {}
        for r in out:
            w = work.get(r["run_id"], {})
            r.update({k: w.get(k) for k in ("work_started_at", "work_finished_at", "loaded")})
            batched, batched_unknown = q.batch_rows(r.get("detail"))
            r["rows_written"] = int(w.get("rows_written") or 0) + batched
            # Partial when a batch had members without a count, or a load outside the batches had none (a full
            # reload, say): either leaves part of the total unknown.
            r["rows_partial"] = bool(batched_unknown) or bool(w.get("uncounted"))
            q.with_outcome(r, progress.get(r["run_id"]))
            r["speed_tables_per_min"] = None if r["ended_without_finishing"] else q.work_speed(r)
    return out


@app.get("/api/runs/{run_id}")
def run_detail(run_id: int):
    """One run: its row, outcome and rows written, and while it is running its live progress (the
    overview's counts), speed over the last minutes and about how long it has left. A check job's own record of
    a check comes with what it found (check), else check is None."""
    found = _with_rows_written(db.query(q.run_sql(), (int(run_id),)))
    if not found:
        raise HTTPException(404, f"run {run_id} not found")
    run = found[0]
    schedules, members = _schedules()
    key = run.get("work_id") if sch.is_check(run) else sch.launch_of(run)
    first = ({r["task_collection"]: r["started_at"] for r in db.query(q.first_started_sql(), (key, f"{key}:%"))}
             if key else {})
    run["job_runs"] = ij.job_runs_of_run(run, schedules, members, first_started=first)
    run["check"] = ij.check_found(run) if sch.is_check(run) else None
    if run["status"] == "running":
        progress = next((r for r in _running() if r["run_id"] == run["run_id"]), None)
        now = datetime.now(timezone.utc)
        run.update(progress=progress, live_speed=progress and q.live_speed(progress, now),
                   time_left=progress and q.time_left(progress, now))
        q.with_outcome(run, progress)
    return run


@app.get("/api/runs/{run_id}/databases")
def run_databases(run_id: int):
    return db.query(q.run_databases_sql(), (int(run_id),))


@app.get("/api/runs/{run_id}/events")
def run_events(run_id: int):
    return db.query(q.run_events_sql(), (int(run_id),))


# The most in-flight tables the group run page lists.
IN_FLIGHT_LIMIT = 50


@app.get("/api/runs/{run_id}/in-flight")
def run_in_flight(run_id: int):
    """The tables a group run is loading now, longest first (at most IN_FLIGHT_LIMIT), and how many there are."""
    tables = db.query(q.in_flight_sql(), (int(run_id), IN_FLIGHT_LIMIT))
    total = len(tables) if len(tables) < IN_FLIGHT_LIMIT else db.query(q.in_flight_count_sql(), (int(run_id),))[0]["tables"]
    return {"tables": tables, "total": total, "limit": IN_FLIGHT_LIMIT}


# ---- tables -------------------------------------------------------------------------------------

TABLES_PAGE = 100


@app.get("/api/tables")
def tables(collection: Optional[str] = None, search: Optional[str] = None, status: str = "all",
           database: Optional[str] = None, after: Optional[str] = None, limit: int = 0):
    """Control rows with their checkpoint, TABLES_PAGE at a time (queries.TABLES_ORDER). `after` is the
    previous page's `next_after` (JSON); `database` filters on one database exactly."""
    if status not in q.TABLE_FILTERS:
        raise HTTPException(400, f"status must be one of {q.TABLE_FILTERS}")
    limit = max(1, min(int(limit or TABLES_PAGE), 500))
    cursor = json.loads(after) if after else None
    if cursor is not None and (not isinstance(cursor, list) or len(cursor) != 5):
        raise HTTPException(400, "after must be the previous page's next_after")
    sql, params = q.tables_sql(collection or None, search or None, status, limit + 1, database=database, after=cursor)
    rows = db.query(sql, params)
    last = rows[limit - 1] if len(rows) > limit else None      # one row more than a page says there is a next page
    rows = rows[:limit]
    return {"tables": rows, "limit": limit,
            "next_after": [last["task_collection"], last.get("src_database") or "", last["src_schema"], last["src_table"],
                           last["id"]] if last else None}


class Ids(BaseModel):
    ids: List[int] = Field(min_length=1)


class EnableRequest(Ids):
    enabled: bool


@app.post("/api/tables/enable")
def set_enabled(body: EnableRequest, request: Request):
    batch = uuid.uuid4().hex[:12]
    checked = _refuse_rows_groups(body.ids, request)
    with _write() as cur:
        rows = _locked_rows(cur, body.ids)
        _recheck_locked_rows(rows, checked, request)
        _lock_collections(cur, [r["task_collection"] for r in rows])
        cur.execute(q.set_enabled_sql(), (body.enabled, body.ids))
        _rewrite_rollups_for_ids(cur, body.ids)
        for r in rows:
            if bool(r["task_enabled"]) != body.enabled:
                _log(cur, _user(request), "enable" if body.enabled else "disable", r["id"], r["task_collection"],
                     "task_enabled", r["task_enabled"], body.enabled, batch=batch)
        _touch(cur, _user(request), groups=[r["task_collection"] for r in rows])
    return {"updated": len(rows)}


@app.post("/api/tables/reseed")
def reseed(body: Ids, request: Request):
    batch = uuid.uuid4().hex[:12]
    checked = _refuse_rows_groups(body.ids, request)
    with _write() as cur:
        rows = _locked_rows(cur, body.ids)
        _recheck_locked_rows(rows, checked, request)
        _lock_collections(cur, [r["task_collection"] for r in rows])
        cur.execute(q.force_reseed_sql(), (body.ids,))
        n = cur.rowcount
        _rewrite_rollups_for_ids(cur, body.ids)
        cur.execute(q.log_reseed_sql(), ("manual", body.ids))
        for r in rows:
            _log(cur, _user(request), "reseed", r["id"], r["task_collection"], reseed="manual", batch=batch)
        _touch(cur, _user(request), groups=[r["task_collection"] for r in rows])
    return {"updated": n}


@app.post("/api/tables")
def add_table(row: dict, request: Request):
    _refuse_groups_outside_jobs([row.get("task_collection")])
    _refuse_groups([row.get("task_collection")], request)
    errs = q.validate_control_row(row)
    if errs:
        raise HTTPException(422, {"errors": errs})
    with _write() as cur:
        _lock_collections(cur, [row.get("task_collection")])
        cur.execute(q.insert_control_sql(), q.control_params(row))
        new_id = cur.fetchone()[0]
        _rewrite_rollups(cur, [(row.get("task_collection"), row.get("src_database"))])
        _refuse_mixed_sinks(cur, [row])
        _refuse_split_databases(cur, [row])
        _log(cur, _user(request), "add", new_id, row.get("task_collection"))
        _touch(cur, _user(request), groups=[row.get("task_collection")])
    return {"id": new_id}


@app.get("/api/tables/{control_id}")
def table_detail(control_id: int):
    rows = db.query(q.control_row_sql(), (int(control_id),))
    if not rows:
        raise HTTPException(404, f"control row {control_id} not found")
    cp = db.query(q.checkpoint_for_sql(), (int(control_id),))
    csql, cparams = q.changes_sql(int(control_id), 100)
    return {"control": rows[0], "checkpoint": cp[0] if cp else None, "changes": db.query(csql, cparams)}


class EditRequest(BaseModel):
    ids: List[int] = Field(min_length=1)
    changes: dict = Field(min_length=1)
    dry_run: bool = True


@app.post("/api/tables/edit")
def edit_tables(body: EditRequest, request: Request):
    """Preview (dry_run, the default) or apply an edit to one or many control rows. Applying is all
    or nothing, in one transaction: it refuses while an affected collection is running, flips tables
    whose source, sink or key changed to seeding, checks sink ownership, and logs every change."""
    import psycopg
    bulk = len(body.ids) > 1
    batch = uuid.uuid4().hex[:12]
    new_coll = body.changes.get("task_collection")
    checked = set()
    if not body.dry_run:
        _refuse_groups_outside_jobs([new_coll])
        checked = _refuse_rows_groups(body.ids, request, also=[new_coll])
    try:
        with _write() as cur:
            rows = _locked_rows(cur, body.ids)
            plans = q.plan_edit(rows, body.changes, bulk=bulk)
            summary = {"rows": len(plans), "changed": sum(1 for p in plans if p["changes"]),
                       "reseed": sum(1 for p in plans if p["reseed"]),
                       "errors": sum(1 for p in plans if p["errors"]), "applied": False}
            if body.dry_run or summary["errors"] or not summary["changed"]:
                cur.connection.rollback()
                return {"summary": summary, "plans": plans}
            _recheck_locked_rows(rows, checked, request)
            _lock_collections(cur, [r["task_collection"] for r in rows] + ([new_coll] if new_coll else []))
            _apply_plans(cur, rows, plans, _user(request), batch)
            _touch(cur, _user(request), groups=[r["task_collection"] for r in rows] + [new_coll])
            summary["applied"] = True
            return {"summary": summary, "plans": plans}
    except psycopg.errors.UniqueViolation:
        raise HTTPException(409, "another control row already writes that sink (or that sink and client_id); "
                                 "nothing was changed")


@app.get("/api/changes")
def changes(control_id: Optional[int] = None, limit: int = 200):
    sql, params = q.changes_sql(control_id, limit)
    return db.query(sql, params)


# ---- jobs ---------------------------------------------------------------------------------------

def _require(job_id, name):
    if not job_id:
        raise HTTPException(503, f"{name} job id isn't configured for this app")
    return job_id


@app.get("/api/jobs/ingest")
def ingest_job():
    jid = _require(jobs.INGEST_JOB_ID, "ingestion")
    active, recent = run_poller.runs_of(jid, active_only=True, limit=50), run_poller.runs_of(jid, limit=25)
    # A run a wrapper job started (trigger RUN_JOB_TASK) came from a schedule its collection was in
    # when the run started.
    joined = _scheduled_collections(with_added=True)
    at = lambda v: datetime.fromisoformat(v) if isinstance(v, str) else v
    for r in active + recent:
        r["schedule"] = ", ".join(n for n, added in joined.get(r["task_collection"], [])
                                  if r["started"] and at(r["started"]) >= at(added)) or None \
            if r["trigger"] == "RUN_JOB_TASK" else None
    return {"job": jobs.job_summary(jid), "active": active, "recent": recent}


class RunRequest(BaseModel):
    task_collection: str = Field(min_length=1)
    parallelism: int = Field(default=192, ge=1, le=256)


@app.post("/api/jobs/ingest/run")
def run_ingest(body: RunRequest, request: Request):
    _refuse_run_of(_require(jobs.INGEST_JOB_ID, "ingestion"), request)
    rid = jobs.run_now(_require(jobs.INGEST_JOB_ID, "ingestion"),
                       {"task_collection": body.task_collection, "parallelism": str(body.parallelism)})
    return {"run_id": rid}


@app.post("/api/jobs/runs/{run_id}/cancel")
def cancel_run(run_id: int, request: Request):
    """Cancel a run of one of the app's own Databricks jobs, for a user with Can Manage Run on that job."""
    job_id = run_poller.run_state_of(run_id)["job_id"]
    if str(job_id) not in {str(j) for j in _bundle_job_ids() if j}:
        raise HTTPException(403, "the app only cancels runs of its own Databricks jobs")
    _refuse_run_of(job_id, request)
    jobs.cancel(run_id)
    return {"cancelled": run_id}


@app.post("/api/jobs/setup/run")
def run_setup(request: Request):
    _refuse_run_of(_require(jobs.SETUP_JOB_ID, "setup"), request)
    return {"run_id": jobs.run_now(_require(jobs.SETUP_JOB_ID, "setup"), {})}


# Where Databricks has not reported a setup run ended by this long after its start, the page says so, and it is not done.
SETUP_LIMIT_S = 30 * 60


@app.get("/api/jobs/setup/runs/active")
def setup_run_active():
    """The setup run Databricks lists going, if any (the run poller's table), so a page opened during a run keeps its
    button off. Its outcome is read from GET /api/jobs/setup/runs/{run_id}."""
    runs = run_poller.runs_of(_require(jobs.SETUP_JOB_ID, "setup"), active_only=True, limit=1)
    return {"run_id": int(runs[0]["run_id"]) if runs else None}


@app.get("/api/jobs/setup/runs/{run_id}")
def setup_run(run_id: int):
    """A setup run's outcome for the Schema setup section: pending while Databricks lists it going (the run poller's
    table), a timeout past SETUP_LIMIT_S, then succeeded, or failed with Databricks' message. Only the setup job's runs."""
    try:
        state = run_poller.run_state_of(run_id)
    except Exception as e:
        if jobs.is_not_found(e):
            raise HTTPException(404, f"setup run {run_id} not found")
        raise
    if str(state.get("job_id")) != str(_require(jobs.SETUP_JOB_ID, "setup")):
        raise HTTPException(404, f"setup run {run_id} not found")
    return {"run_id": int(run_id), "outcome": action_states.run_outcome(state, datetime.now(timezone.utc), SETUP_LIMIT_S)}


def _bundle_job_ids() -> list:
    return [*jobs.ingest_variants().values(), jobs.CHECK_JOB_ID, jobs.SETUP_JOB_ID, jobs.DISCOVER_JOB_ID]


# ---- bulk import --------------------------------------------------------------------------------

class DiscoverRequest(BaseModel):
    src_host: str = Field(min_length=1)
    src_port: Optional[int] = None
    databases: str = Field(min_length=1, description="comma- or newline-separated")
    secret_key_template: str = Field(default="{database}", min_length=1)


@app.post("/api/discoveries")
def start_discovery(body: DiscoverRequest, request: Request):
    names = q.parse_databases(body.databases)
    if not names:
        raise HTTPException(422, "at least one database is required")
    _refuse_run_of(_require(jobs.DISCOVER_JOB_ID, "discovery"), request)
    did = uuid.uuid4().hex[:12]
    spec = {"src_type": "sqlserver", "src_host": body.src_host.strip(), "src_port": body.src_port or None,
            "databases": names, "secret_key_template": body.secret_key_template.strip(),
            "requested_by": _user(request)}
    rid = jobs.run_now(_require(jobs.DISCOVER_JOB_ID, "discovery"), {"discovery_id": did, "spec": json.dumps(spec)})
    return {"discovery_id": did, "run_id": rid, "databases": len(names)}


class SourceDiscoverRequest(BaseModel):
    """A discovery through a Unity Catalog connection, as Add tables' Discover on a source starts it: list the
    server's databases (Test connection), or read the tables of some databases or of the whole server."""
    src_connection: str = Field(min_length=1)
    kind: str = Field(default="tables", pattern="^(list|tables)$")
    databases: List[str] = []
    whole_server: bool = False
    # A database name filter for listing the server, as SQL Server's LIKE reads it (discovery.pattern_errors).
    pattern: str = Field(default="", max_length=128, pattern=r"^[A-Za-z0-9_%\-\[\]\.$#@]*$")
    # For an Azure SQL server whose login can't open master: a Unity Catalog service credential with Reader on the
    # server, and the server's resource id. The discovery job then lists the databases through Azure
    # (discovery.azure_databases); the app never calls Azure. Both or neither.
    azure_credential: str = Field(default="", max_length=255, pattern=r"^[A-Za-z0-9_\-]*$")
    azure_resource_id: str = Field(default="", max_length=512, pattern=r"^(/subscriptions/[0-9A-Za-z-]+/resourceGroups/"
                                   r"[-\w.()]+/providers/Microsoft\.Sql/(servers|managedInstances)/[a-z0-9][a-z0-9-]*)?$")


def _connection_unseen(name: str, listing_error: Optional[str]) -> str:
    """Why the app can't use a connection, naming the grant it lacks: the app lists connections as its own service
    principal, which sees only those it has USE CONNECTION on (the connection is not the bundle's, so its owner
    grants it)."""
    who = f"its service principal ({jobs.APP_PRINCIPAL})" if jobs.APP_PRINCIPAL else "its service principal"
    why = f"the app can't list connections ({listing_error.split('. ')[0]})" if listing_error else \
        f"the app can't see the connection '{name}'"
    return (f"{why}: {who} needs USE CONNECTION on '{name}'. Its owner grants it in Catalog Explorer: Connections, "
            f"{name}, Permissions, Grant, USE CONNECTION to the app; then Refresh the list.")


@app.post("/api/discoveries/source")
def start_source_discovery(body: SourceDiscoverRequest, request: Request):
    names = list(dict.fromkeys(d.strip() for d in body.databases if d.strip()))
    if body.kind == "tables" and not body.whole_server and not names:
        raise HTTPException(422, "choose the whole server or at least one database")
    if bool(body.azure_credential) != bool(body.azure_resource_id):
        raise HTTPException(422, "listing through Azure takes both a service credential and the server's resource id")
    listed = connections()
    if body.src_connection not in {c["name"] for c in listed["connections"]}:
        raise HTTPException(404, _connection_unseen(body.src_connection, listed.get("error")))
    job_id = _require(jobs.DISCOVER_JOB_ID, "discovery")
    _refuse_run_of(job_id, request)
    did = uuid.uuid4().hex[:12]
    spec = {"src_type": "sqlserver", "src_connection": body.src_connection, "kind": body.kind,
            "databases": "*" if body.kind == "tables" and body.whole_server else names, "requested_by": _user(request),
            **({"pattern": body.pattern} if body.pattern else {}),
            **({"azure_credential": body.azure_credential, "azure_resource_id": body.azure_resource_id}
               if body.azure_credential else {})}
    if body.kind == "list":
        spec.pop("databases")
    rid = jobs.run_now(job_id, {"discovery_id": did, "spec": json.dumps(spec)})
    return {"discovery_id": did, "run_id": rid}


@app.get("/api/discoveries/{discovery_id}/tables")
def discovery_tables(discovery_id: str):
    """The tables a discovery found, with the settings it recommends for each: what Add tables turns into lines."""
    keep = ("src_database", "src_schema", "src_table", "select_list", "primary_key", "load_type", "cursor_column",
            "row_count", "size_mb", "ct_enabled", "excluded_columns", "notes", "src_connection")
    return [{k: r.get(k) for k in keep} for r in db.query(q.discovered_tables_sql(), (discovery_id,))]


@app.get("/api/discoveries/runs/{run_id}")
def discovery_run(run_id: int):
    """State of a discovery job run, so the page can tell a run that ended without results from one
    still starting. Only runs of the discovery job."""
    st = run_poller.run_state_of(run_id)
    if str(st["job_id"]) != str(jobs.DISCOVER_JOB_ID):
        raise HTTPException(404, f"run {run_id} is not a discovery run")
    return st


@app.get("/api/discoveries")
def discoveries(limit: int = 50):
    return db.query(q.discoveries_sql(), (int(limit),))


@app.get("/api/discoveries/{discovery_id}/databases")
def discovery_databases(discovery_id: str):
    return db.query(q.discovery_databases_sql(), (discovery_id,))


@app.get("/api/discoveries/{discovery_id}/listing")
def discovery_listing(discovery_id: str, filter: str = "", offset: int = 0, limit: int = 100):
    """One page of a discovery's databases matching the filter, by name (100 at a time), with the discovery's totals:
    total (its databases), matched (those the filter keeps). The Discover step pages through it rather than holding
    every database in the browser."""
    limit, offset = max(1, min(int(limit), 500)), max(0, int(offset))
    totals = db.query(q.discovery_totals_sql(), (filter, discovery_id))[0]
    return {"discovery_id": discovery_id, "filter": filter, "offset": offset, "limit": limit,
            "total": totals["total"], "matched": totals["matched"],
            "databases": db.query(q.discovery_listing_sql(), (discovery_id, filter, limit, offset))}


class Shared(BaseModel):
    sink_catalog: str = "main"
    sink_schema: str = "bronze_{database}"
    sink_table: str = "{table}"
    client_id: str = ""
    freshness_sla_minutes: Optional[int] = Field(default=None, gt=0)
    partition_threshold_mb: float = q.DEFAULT_PARTITION_THRESHOLD_MB
    partition_size_mb: int = q.DEFAULT_PARTITION_SIZE_MB
    include_default: bool = True


class ReviewRequest(BaseModel):
    shared: Shared = Shared()


@app.post("/api/discoveries/{discovery_id}/review")
def review(discovery_id: str, body: ReviewRequest):
    """Review grid rows computed from the discovery and the shared fields."""
    discovered = db.query(q.discovered_tables_sql(), (discovery_id,))
    return [q.public_review_row(r) for r in q.review_rows(discovered, body.shared.model_dump())]


class RegisterRequest(BaseModel):
    task_collection: str = Field(min_length=1)
    shared: Shared = Shared()
    edits: List[dict] = []
    dry_run: bool = False


@app.post("/api/discoveries/{discovery_id}/register")
def register(discovery_id: str, body: RegisterRequest, request: Request):
    """Validate (dry_run) or register the included tables. Rows are rebuilt from the stored discovery
    and only the reviewer's editable fields are applied (queries.merge_review_edits)."""
    discovered = db.query(q.discovered_tables_sql(), (discovery_id,))
    rows = q.merge_review_edits(q.review_rows(discovered, body.shared.model_dump()), body.edits)
    chosen = [r for r in rows if r.get("include")]
    results, valid = [], []
    for r in chosen:
        row = q.control_row_from_review(r, body.task_collection)
        errs = q.validate_control_row(row)
        name = ".".join(q.review_key(r))
        if errs:
            results.append({"table": name, "status": "invalid", "errors": errs})
        else:
            valid.append((name, row))
    if not body.dry_run and valid:
        _refuse_groups_outside_jobs([body.task_collection])
        _refuse_groups([body.task_collection], request)
        batch = uuid.uuid4().hex[:12]
        with _write() as cur:
            _lock_collections(cur, [body.task_collection])
            for name, row in valid:
                cur.execute(q.insert_control_if_absent_sql(), q.control_params(row))
                res = cur.fetchone()
                if res:
                    _log(cur, _user(request), "register", res[0], body.task_collection, batch=batch)
                results.append({"table": name, "status": "registered" if res else "skipped",
                                "id": res[0] if res else None,
                                "errors": [] if res else ["sink already used by another control row"]})
            _rewrite_rollups(cur, [(row.get("task_collection"), row.get("src_database")) for _, row in valid])
            _refuse_mixed_sinks(cur, [row for _, row in valid])     # all or nothing, like an edit
            _refuse_split_databases(cur, [row for _, row in valid])
            _touch(cur, _user(request), groups=[body.task_collection])
    else:
        results += [{"table": name, "status": "valid", "errors": []} for name, _ in valid]
    counts = {}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    return {"selected": len(chosen), "counts": counts, "results": results}


# ---- configuration import ----------------------------------------------------------------------

class ImportRequest(BaseModel):
    task_collection: str = Field(min_length=1, max_length=128)
    settings: dict = {}
    csv: str
    dry_run: bool = True

    @field_validator("task_collection")
    @classmethod
    def _named(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("name the ingestion group: a blank name would put the tables in no group")
        return v.strip()


@app.post("/api/import")
def import_csv(body: ImportRequest, request: Request):
    """Preview (dry_run) or apply a CSV of tables into one ingestion group, with the group's settings
    (config_import.plan_import). Apply plans again inside one transaction, after taking the group's run lock and
    reading its rows there, so the settings it inherits are the rows' as they are; it needs every line valid, then
    inserts each line's row unless its sink is taken (skipped), logs it as an import, and runs the mixed-sink
    check, as Bulk import's register does. Tables the group already has are skipped."""
    group = body.task_collection.strip()

    def planned(rows):
        plan = ci.plan_import(body.csv, group, body.settings, rows)
        if plan["errors"]:
            raise HTTPException(422, {"errors": plan["errors"]})
        return plan

    def counted(lines):
        counts: dict = {}
        for line in lines:
            counts[line["status"]] = counts.get(line["status"], 0) + 1
        return counts

    if body.dry_run:
        plan = planned(db.query(q.collection_control_rows_sql(), (group,)))
    else:
        _refuse_groups_outside_jobs([group])
        _refuse_groups([group], request)
        batch = uuid.uuid4().hex[:12]
        with _write() as cur:
            _lock_collections(cur, [group])
            plan = planned(db.fetch(cur, q.collection_control_rows_sql(), (group,)))
            invalid = counted(plan["lines"]).get("invalid")
            if invalid:
                raise HTTPException(422, {"errors": ["fix the invalid lines first"], "invalid": invalid})
            for line in plan["lines"]:
                if line["status"] == "registered":
                    line["status"] = "skipped"
                    continue
                cur.execute(q.insert_control_if_absent_sql(), q.control_params(line["row"]))
                res = cur.fetchone()
                line["status"] = "imported" if res else "skipped"
                if res:
                    line["id"] = res[0]
                    _log(cur, _user(request), "import", res[0], group, batch=batch)
                else:
                    line["errors"] = ["sink already used by another control row"]
            imported = [line["row"] for line in plan["lines"] if line["status"] == "imported"]
            _rewrite_rollups(cur, [(row.get("task_collection"), row.get("src_database")) for row in imported])
            _refuse_mixed_sinks(cur, imported)
            _refuse_split_databases(cur, imported)
            _touch(cur, _user(request), groups=[group])
    return {"task_collection": group, "settings": plan["settings"], "counts": counted(plan["lines"]),
            "lines": [{k: v for k, v in line.items() if k != "row"} for line in plan["lines"]]}


@app.get("/api/collections/{name}/settings")
def collection_settings(name: str):
    """An ingestion group's settings, from its rows: each one's shared value, or its mixed values with row counts."""
    rows = db.query(q.collection_control_rows_sql(), (name,))
    return {"task_collection": name, "tables": len(rows), "settings": ci.setting_summary(ci.group_settings(rows))}


@app.get("/api/collections/{name}/export.csv")
def collection_export(name: str):
    """An ingestion group's rows as the import CSV."""
    rows = db.query(q.collection_control_rows_sql(), (name,))
    return Response(ci.export_csv(rows), media_type="text/csv",
                    headers={"Content-Disposition": f'attachment; filename="{q.identifier(name)}.csv"'})


# ---- schedules ----------------------------------------------------------------------------------

class ScheduleBody(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    cadence_minutes: int
    timezone: str = "UTC"
    parameters: dict = {}
    job_variant: str = "default"
    stagger: bool = True
    paused: bool = False
    notify_emails: Optional[str] = None
    options: dict = {}
    members: List[str] = Field(min_length=1)

    @field_validator("timezone")
    @classmethod
    def _tz(cls, v: str) -> str:
        return (v or "").strip() or "UTC"      # stored as given: a blank one would break every page

    @model_validator(mode="after")
    def _utc_for_minutes(self):
        self.timezone = sch.timezone_for(self.cadence_minutes, self.timezone)
        return self


def _ingest_job_id(variant: str) -> Optional[int]:
    """The job a schedule's wrappers start: the ingestion job of its variant, or the check job."""
    jid = jobs.schedule_variants().get(variant or "default")
    return int(jid) if jid else None


def _wrapper_target(schedule: dict) -> Optional[int]:
    """The job a schedule's wrappers start: its own copy of the ingestion job when it has its own cluster (a check
    schedule's wrapper still starts the check job, and passes the copy on), else its variant's job."""
    variant = schedule.get("job_variant") or "default"
    copy = sch.copy_job_id(schedule)
    return copy if copy and variant != sch.CHECK_VARIANT else _ingest_job_id(variant)


def _variant_job_id(variant: str) -> int:
    jid = _ingest_job_id(variant)
    if jid is None:
        raise HTTPException(503, f"no Databricks ingestion job configured for variant '{variant}'")
    return jid


def _owner() -> str:
    """The wrapper jobs' owner tag: this app's Lakebase database (other deployments' jobs keep out)."""
    return f"{db.INSTANCE}/{db.DATABASE}"


def _load_schedule(sid: int, cur=None):
    get = (lambda sql, p: db.fetch(cur, sql, p)) if cur is not None else db.query
    rows = get(q.schedule_sql(), (int(sid),))
    if not rows:
        raise HTTPException(404, f"schedule {sid} not found")
    members = get(q.schedule_members_sql(), ([int(sid)],))
    return rows[0], members


def _validate_schedule(body: ScheduleBody):
    known = [r["task_collection"] for r in db.query(q.collections_sql())]
    errs = sch.validate(body.model_dump(), known, jobs.schedule_variants())
    if errs:
        raise HTTPException(422, {"errors": errs})


# Options only the app's own steps write: who last changed the job, the copies job's copy and the app's run
# applying it, and a deleted job's delete job run and whether it drops the target tables. A request's values for them are ignored, and the stored ones kept.
MANAGED_OPTIONS = ("updated_by", sch.COPY_OPTION, "cluster_copy_run_id", "delete_run_id", "delete_drop_targets",
                   rediscovery.RECORD)


def _requested_options(body: "ScheduleBody") -> dict:
    return {k: v for k, v in (body.options or {}).items() if k not in MANAGED_OPTIONS}


def _refuse_unknown_node_types(cluster: Optional[dict]) -> None:
    """A job's own cluster may only name node types this workspace has."""
    wanted = [cluster.get(k) for k in ("driver_node_type_id", "node_type_id")] if cluster else []
    wanted = [n for n in wanted if n]
    if not wanted:
        return
    try:
        known = set(jobs.node_types())
    except Exception as e:  # without the list, nothing is checked: refuse rather than start an unknown cluster
        raise HTTPException(503, f"can't read this workspace's node types: {str(e).split('. ')[0]}")
    unknown = [n for n in wanted if n not in known]
    if unknown:
        raise HTTPException(422, {"errors": [f"options.cluster: {unknown[0]} is not a node type in this workspace"]})


def _apply_cluster(sid: int) -> Optional[str]:
    """Start the copies job for a schedule (it makes, updates or deletes the schedule's copy of the ingestion job)
    and record its run, which the cluster endpoint follows. Returns why it couldn't start, or None."""
    if not jobs.CLUSTER_COPIES_JOB_ID:
        return "no cluster copies job is configured for this app, so the job keeps the ingestion job's cluster"
    try:
        run_id = jobs.start_cluster_copies(sid)
    except Exception as e:
        return f"the cluster copies job didn't start: {str(e).split('. ')[0]}"
    with _write() as cur:
        cur.execute(q.set_schedule_option_sql(), (["cluster_copy_run_id"], run_id, int(sid)))
    return None


def _created(result: dict, user: str, options: dict) -> dict:
    """A new job's wrapper jobs synced, and its own cluster copy started, once its rows are committed. Either failing is
    not done (NotSynced): the job's rows are written, and the detail says what did not sync or start."""
    sid = result["id"]
    synced = _sync(sid, user)
    cluster = _apply_cluster(sid) if sch.own_cluster({"options": options}) else None
    _synced(synced)
    if cluster:
        raise NotSynced(500, f"{TASK_WRITTEN_NOT_SYNCED} {cluster}"[:4000])
    return {**result, "sync": synced, "cluster_error": None}


def _sync(sid: int, user: str) -> List[dict]:
    """Bring the schedule's wrapper jobs in line with Lakebase and record the results. Runs under the
    schedule's sync lock, with the members read after taking it, so concurrent syncs don't both create a
    job. A failed sync never raises once the schedule's change is committed: errors are per member."""
    batch = uuid.uuid4().hex[:12]
    with _write() as cur:
        cur.execute(q.schedule_lock_sql(), (q.SCHEDULE_LOCK_CLASS, q.schedule_lock_key(sid)))
        s, members = _load_schedule(sid, cur)
        active = [m for m in members if m["removed_at"] is None]
        removed = [m for m in members if m["delete_pending"]]
        results = sch.sync(s, active, removed, jobs.schedule_client(), _wrapper_target(s),
                           jobs.SCHEDULE_JOB_VIEW_GROUP, _owner())
        for r in results:
            if r.member_id is not None:
                if r.action == "error":
                    cur.execute(q.member_sync_failed_sql(), (r.detail, r.member_id))
                else:
                    cur.execute(q.member_synced_sql(), (r.job_id, r.member_id))
            if r.action in ("created", "adopted", "updated", "deleted"):
                _log(cur, user, "schedule_sync", None, r.task_collection or None, "job_id", None,
                     f"{r.action} job {r.job_id}" + (f" ({r.detail})" if r.detail else ""), batch=batch)
    return [r.__dict__ for r in results]


def _members_view(s: dict, members: List[dict], stats: dict, skipped: dict, now: datetime) -> List[dict]:
    out = []
    for m in members:
        st = stats.get(m["task_collection"], {})
        p95 = st.get("p95_s")
        out.append({
            **m,
            "when": sch.describe(int(s["cadence_minutes"]), int(m["offset_minutes"]), s["timezone"]),
            "next_run": None if s["paused"] else
            sch.next_run(int(s["cadence_minutes"]), int(m["offset_minutes"]), s["timezone"], now).isoformat(),
            "job_url": f"{db.workspace().config.host.rstrip('/')}/jobs/{m['job_id']}" if m.get("job_id") else None,
            **{k: st.get(k) for k in ("last_run_id", "last_status", "last_started", "last_duration_s", "p50_s",
                                      "p95_s", "runs_24h", "failed_24h", "lock_skipped_24h", "min_sla_minutes",
                                      "enabled_tables")},
            "skipped_24h": skipped.get(m["task_collection"]),
            "warnings": sch.warnings(int(s["cadence_minutes"]), p95, st.get("min_sla_minutes"),
                                     skipped.get(m["task_collection"]) or 0),
        })
    return out


_skipped = sch.SkippedRuns()


def _skipped_starts(job_id, since: datetime, now: datetime, client=None) -> Optional[List[int]]:
    """Start times (ms) of a wrapper job's skipped runs since `since`; None if they can't be read. From the run
    poller's table, or from the Jobs API until the table exists."""
    if run_poller.table_ready():
        try:
            return run_poller.skipped_starts_of(job_id, since, now)
        except Exception:
            return None
    try:
        return _skipped.starts(client or jobs.schedule_client(), job_id, int(since.timestamp() * 1000),
                               int(now.timestamp() * 1000))
    except Exception:
        return None


def _skipped_counts(members: List[dict]) -> dict:
    """Skipped triggers per member over 24 hours, from the wrapper jobs' runs."""
    now = datetime.now(timezone.utc)
    client, out = None, {}
    for m in members:
        if m.get("job_id"):
            client = client or jobs.schedule_client()
            starts = _skipped_starts(m["job_id"], now - timedelta(hours=24), now, client)
            out[m["task_collection"]] = None if starts is None else len(starts)
    return out


@app.get("/api/schedules")
def list_schedules(details: bool = True):
    """Schedules with their members: when each runs next, its wrapper job, recent runs and warnings."""
    now = datetime.now(timezone.utc)
    rows = _with_delete_states(db.query(q.schedules_sql()), now)
    members = db.query(q.schedule_members_sql(), ([r["id"] for r in rows],)) if rows else []
    active = [m for m in members if m["removed_at"] is None]
    colls = sorted({m["task_collection"] for m in active})
    stats = {r["task_collection"]: r for r in
             (db.query(q.collection_run_stats_sql(), (colls, colls, colls, colls)) if colls else [])}
    skipped = _skipped_counts(active) if details else {}
    return [{**s, "description": sch.describe(int(s["cadence_minutes"]), sch.start_minute(s), s["timezone"]),
             "members": _members_view(s, [m for m in active if m["schedule_id"] == s["id"]], stats, skipped, now)}
            for s in rows]


def _scheduled_collections(with_added: bool = False) -> dict:
    """{collection: [names of the active schedules it's in]} (or [(name, added_at)])."""
    scheduled = {}
    rows = db.query(q.schedules_sql())
    if rows:
        names = {r["id"]: r["name"] for r in rows}
        for m in db.query(q.schedule_members_sql(), (list(names),)):
            if m["removed_at"] is None:
                name = names[m["schedule_id"]]
                scheduled.setdefault(m["task_collection"], []).append((name, m["added_at"]) if with_added else name)
    return scheduled


@app.get("/api/schedules/options")
def schedule_options():
    scheduled = _scheduled_collections()
    try:     # the ingestion job's own defaults, shown as the editor's placeholders
        defaults = {k: v for k, v in jobs.job_summary(jobs.INGEST_JOB_ID)["parameters"].items() if k in sch.PARAMETERS}
    except Exception:
        defaults = {}
    return {"cadences": sch.CADENCES, "parameters": list(sch.PARAMETERS), "flags": sch.FLAGS, "defaults": defaults,
            "variants": sorted(jobs.schedule_variants()),
            "collections": [r["task_collection"] for r in db.query(q.collections_sql())], "scheduled": scheduled,
            "view_group": jobs.SCHEDULE_JOB_VIEW_GROUP}


@app.post("/api/schedules")
def create_schedule(body: ScheduleBody, request: Request):
    import psycopg
    body.options = _requested_options(body)
    _validate_schedule(body)
    _refuse_unknown_node_types(sch.own_cluster(body.model_dump()))
    user = _user(request)
    # A check schedule's one wrapper fires every member at once.
    body.stagger = body.stagger and body.job_variant != sch.CHECK_VARIANT
    offs = sch.offsets(body.members, body.cadence_minutes, body.stagger, sch.start_minute(body.model_dump()))
    if not body.paused:   # anyone may set up a job, but one that runs needs the right to run what it starts
        _refuse(_permission_refusals({"job_variant": body.job_variant}, user)["run"])
    batch = uuid.uuid4().hex[:12]
    try:
        with _write() as cur:
            cur.execute(q.insert_schedule_sql(), (body.name.strip(), body.cadence_minutes, body.timezone,
                                                  json.dumps(body.parameters), body.job_variant or "default",
                                                  body.stagger, body.paused, body.notify_emails or None,
                                                  json.dumps(body.options) if body.options else None, user or None))
            sid = cur.fetchone()[0]
            _touch(cur, user, [sid])
            for c in sorted(body.members):
                cur.execute(q.insert_member_sql(), (sid, c, offs[c]))
                _log(cur, user, "schedule_create", None, c, "schedule", None,
                     f"{body.name} ({sch.describe(body.cadence_minutes, offs[c], body.timezone)})", batch=batch)
    except psycopg.errors.UniqueViolation:
        raise HTTPException(409, f"a schedule named '{body.name}' already exists")
    synced = _sync(sid, user)
    cluster = _apply_cluster(sid) if sch.own_cluster(body.model_dump()) else None
    return {"id": sid, "sync": synced, "cluster_error": cluster}


class NewJobRun(BaseModel):
    """When a new job runs, as the Edit view's When it runs and Advanced configuration set it."""
    cadence_minutes: int = 60
    timezone: str = "UTC"
    paused: bool = True
    notify_emails: Optional[str] = None
    parameters: dict = {}
    options: dict = {}
    job_variant: str = "default"
    stagger: bool = True


class NewJobRequest(_TablesFrom):
    name: str = Field(min_length=1, max_length=80)
    settings: dict = {}
    groups_per_server: int = nj.DEFAULT_GROUPS_PER_SERVER
    run: NewJobRun = NewJobRun()
    dry_run: bool = True
    # Made when the page opens the Create, so a retry of the same Create finds the task the first one made.
    request_id: Optional[str] = Field(None, max_length=80)


def _source_columns(kind: str, param) -> List[str]:
    """The CSV columns a source's rows are written as (what a plan reads): an upload's header, a discovery's columns."""
    if kind == "upload":
        return list(db.query(q.upload_sql(), (param,))[0]["columns"])
    return list(nj.DISCOVERY_COLUMNS)


def _units_of(kind: str, param, selection: "Selection"):
    """The databases a selection takes, one at a time: a whole server's databases (matching its filter) are read a page
    at a time from Lakebase, then the named ones (by_database.selection_units)."""
    keys = ()
    if selection.whole_server:
        pages = by_database.database_pages(lambda after: db.query(
            q.source_databases_after_sql(kind), (param, selection.filter, *after, by_database.DATABASE_PAGE)))
        keys = ((d["server"], d["src_database"]) for d in pages)
    return by_database.selection_units(selection, keys)


@dataclass(slots=True, kw_only=True)
class ChunkJob:
    """The job a by-database write puts its databases into: its schedule, who asked and the batch their log lines carry,
    the schedule's cadence and time zone (for the create lines' text), each group's offset, the task whose progress each
    transaction records, the write's stage clock, and whether a member it adds is logged (Create logs them as
    schedule_create; Add tables never has)."""
    sid: int
    name: str
    user: str
    batch: str
    cadence_minutes: int
    timezone: str
    offsets: dict
    task_id: int
    stages: by_database.Stages
    log_members: bool = True


def _schedule_create_line(job: ChunkJob, group: str) -> tuple:
    """The change log line for a member a by-database write added to the job's schedule: the group, and the schedule's
    cadence and start in words."""
    described = sch.describe(job.cadence_minutes, job.offsets.get(group, 0), job.timezone)
    return (job.user, "schedule_create", None, group, "schedule", None, f"{job.name} ({described})", None, job.batch)


def _write_chunk(cur, plans: List[dict], job: ChunkJob, state: dict) -> None:
    """The writes of one transaction, for the databases of a chunk (their plans), in the caller's transaction: each
    group's run lock once, the valid lines the chunk's databases do not have yet (a resumed write skips the rows a
    committed transaction wrote; a table the job already writes is left) with their change log lines in one statement,
    the mixed and split checks once, the members its groups are missing, and the task's progress. The tables counted
    are the valid lines reached, so a resumed write reports the whole."""
    groups = sorted({group for plan in plans for group in plan["groups"]})
    _lock_collections_together(cur, groups)
    lines = [(line, line["row"]) for plan in plans for line in plan["lines"] if line["status"] == "valid"]
    names = {str(row.get("src_database") or "") for _, row in lines}
    databases = sorted(names | {name.lower() for name in names})
    have = set()
    if groups and databases:
        cur.execute(q.job_database_sinks_chunk_sql(), (groups, databases))
        have = {tuple(found) for found in cur.fetchall()}
    todo = [row for line, row in lines
            if (str(row.get("src_database") or "").lower(), line["sink"], str(row.get("client_id") or ""),
                row["task_collection"]) not in have]
    if todo:
        _insert_controls(cur, todo, job.user, job.batch, log_per_statement=by_database.TABLES_PER_TRANSACTION)
        _refuse_mixed_sinks(cur, todo)
        _refuse_split_databases(cur, todo)
    if groups:
        cur.execute(q.insert_members_if_absent_sql(),
                    (job.sid, groups, [job.offsets.get(group, 0) for group in groups]))
        created = sorted(found[0] for found in cur.fetchall())
        if job.log_members:
            _log_lines(cur, [_schedule_create_line(job, group) for group in created])
    state["databases_written"] += len(plans)
    state["tables_written"] += len(lines)
    state["seconds"] = job.stages.seconds()
    cur.execute(q.task_progress_json_sql(), (json.dumps(state), state["tables_written"], job.task_id))


def _rows_of_units(kind, param, units: List) -> List[Tuple[object, List[dict]]]:
    """Each unit with the table rows it takes, read for all of the units in one or two statements. The databases a
    source listing returned for an upload are read by their exact names (an index range each); the rest, which a
    user named in any case, are read without regard to case."""
    if not units:
        return []
    rows = []
    exact = [unit for unit in units if kind == "upload" and unit.exact]
    if exact:
        rows += db.query(q.database_rows_exact_sql(),
                         ([unit.server for unit in exact], [unit.database for unit in exact], param))
    named = [unit for unit in units if not (kind == "upload" and unit.exact)]
    if named:
        keys = [by_database.read_key(unit.server, unit.database) for unit in named]
        rows += db.query(q.database_rows_chunk_sql(kind), (param, keys))
    return by_database.rows_by_unit(units, rows)


def _create_by_database(body: NewJobRequest, request: Request, selection: Selection, kind, param, source_id, rest):
    """A big job, by reference to more tables than BACKGROUND_ROWS: its groups are sized by tables (by_database), and it
    is written in a background task, one database at a time, each database's rows in its own transaction. First every
    database is checked (read and planned, nothing written); then the job is made (its schedule and what it was chosen
    from, in one transaction), and each database is written with its group's run lock, its group's member, and the
    task's progress, all committed together. The job's Databricks jobs are synced once, at the end. A restart leaves
    the databases written so far, and the same request id finishes the job: a database a job already has is skipped,
    so it is never written twice (the task's progress is what the page shows)."""
    import psycopg
    name = body.name.strip()
    run = body.run.model_copy()
    run.stagger = run.stagger and run.job_variant != sch.CHECK_VARIANT
    user = _user(request)
    settings = nj.settings_of(body.settings)
    errors = nj.given_errors(body.settings) + nj.setting_errors(settings, require_target=not body.dry_run)
    if not name:
        errors.insert(0, "name the job")
    servers = db.query(q.selected_servers_sql(kind), (param, *rest))
    if not servers:
        errors.append("the selection takes no tables")
    groups_of = by_database.groups_by_server(servers)
    all_groups = [g for s in servers for g in nj.group_names(name, s["server"], groups_of[s["server"]])]
    timezone_ = sch.timezone_for(run.cadence_minutes, run.timezone)
    if not errors:
        errors += sch.validate({**run.model_dump(), "name": name, "members": all_groups, "timezone": timezone_},
                               all_groups, jobs.schedule_variants())
    if errors:
        raise HTTPException(422, {"errors": errors})
    summary = by_database.summary_of(name, servers, settings["load_type"])
    if body.dry_run:
        return {"summary": summary, "settings": settings, "created": False, "target_missing": _target_missing(settings)}
    if not run.paused:     # anyone may set up a job, but one that runs needs the right to run what it starts
        _refuse(_permission_refusals({"job_variant": run.job_variant}, user)["run"])
    options = {k: v for k, v in (run.options or {}).items() if k not in MANAGED_OPTIONS}
    _refuse_unknown_node_types(sch.own_cluster({"options": options}))
    offs = sch.offsets(all_groups, run.cadence_minutes, run.stagger,
                       sch.start_minute({"options": options, "cadence_minutes": run.cadence_minutes}))
    taken = {r["task_collection"] for r in db.query(q.collections_sql())}
    columns = _source_columns(kind, param)
    selection_parts = _selection_parts(selection, [s["server"] for s in servers] if selection.whole_server else [])
    batch = uuid.uuid4().hex[:12]
    databases_total = sum(s["databases"] for s in servers)

    def units():
        return _units_of(kind, param, selection)

    def plan_of(rows, own, stages):
        with stages.timed("plan"):
            return by_database.plan_database(name, body.settings, rows, columns, groups_of, taken, own)

    def read_of(chunk, stages):
        with stages.timed("read"):
            return _rows_of_units(kind, param, chunk)

    def check(own, stages, keep):
        """Every database planned, nothing written: the file's and the settings' problems, and the groups in use.
        Returns each database that has tables, in order, for the write to pack into transactions; each keeps its plan
        when keep is True, so the write does not plan it again."""
        errors_found, counted = [], []
        for read_batch in by_database.read_batches(list(units())):
            for unit, rows in read_of(read_batch, stages):
                if not rows:
                    continue
                plan = plan_of(rows, own, stages)
                if plan["errors"]:
                    raise HTTPException(422, {"errors": plan["errors"][:20]})
                for line in plan["lines"]:
                    if line["status"] == "invalid":
                        errors_found.append(f"{unit.database} {line['table']}: {'; '.join(line['errors'])}")
                counted.append(by_database.Checked(unit, len(rows), plan if keep else None))
        if errors_found:
            raise HTTPException(422, {"errors": errors_found[:20] + (
                [f"{len(errors_found)} lines cannot be taken"] if len(errors_found) > 20 else [])})
        return counted

    def write(progress, task_id):
        task = db.query(q.task_sql(), (task_id,))[0]
        sid = task["job_id"]
        own = set(_job_groups(sid)) if sid else set()
        stages = by_database.Stages()
        state = {"stage": "checking", "databases_total": databases_total, "databases_written": 0, "tables_written": 0,
                 "seconds": stages.seconds()}
        db.execute(q.task_progress_json_sql(), (json.dumps(state), 0, task_id))
        keep = by_database.keeps_plans(summary["tables"])
        counted = check(own, stages, keep)
        stages.begin_write()
        state["stage"] = "writing"
        if sid is None:
            try:
                with _write() as cur:
                    cur.execute(q.insert_schedule_sql(), (name, run.cadence_minutes, timezone_, json.dumps(run.parameters),
                                                          run.job_variant or "default", run.stagger, run.paused,
                                                          run.notify_emails or None, json.dumps(options) if options else None,
                                                          user or None))
                    sid = cur.fetchone()[0]
                    _keep_selection(cur, sid, {"parts": selection_parts, "kind": kind, "source_id": source_id}, user)
                    _touch(cur, user, [sid])
                    cur.execute(q.task_job_sql(), (sid, task_id))
            except psycopg.errors.UniqueViolation:
                raise HTTPException(409, f"an ingestion job named '{name}' already exists")
        job = ChunkJob(sid=sid, name=name, user=user, batch=batch, cadence_minutes=run.cadence_minutes,
                       timezone=timezone_, offsets=offs, task_id=task_id, stages=stages)
        for chunk in by_database.transactions_of(counted):
            plans = [checked.plan for checked in chunk] if keep else [
                plan_of(rows, own, stages) for _, rows in read_of([checked.unit for checked in chunk], stages) if rows]
            if plans:
                with _write() as cur:
                    _write_chunk(cur, plans, job, state)
        result = {"id": sid, "summary": summary, "settings": settings, "created": True}
        with _write() as cur:
            _task_written(cur, task_id, result, sid, state["tables_written"])
        return result

    def after(result):
        return _created(result, user, options)

    made = db.query(q.insert_task_sql(), ("create", body.request_id or uuid.uuid4().hex, None,
                                          summary["tables"], None, user or None))[0]
    if made["made"] or db.query(q.resume_task_sql(), (made["id"],)):
        _start_background(lambda: _run_task(made["id"], write, after))
        return _task_answer(made["id"])
    return _answer_again(made["id"], after)


def _add_by_database(job_id: int, body: "JobTablesRequest", request: Request, kind, param, source_id, rest, total: int):
    """Add tables to a job by reference when the source has more than BACKGROUND_ROWS of them: written in a background
    task, like _create_by_database, its databases read in batches and packed into transactions under the job's schedule
    lock and their groups' run locks, with the groups' members and the task's progress committed with them. The job's
    settings, groups and tables (new_job.job_inputs, read once when the request arrives) place each database; a table
    the job already writes is left. Every database is checked first (nothing written), and the job's Databricks jobs
    are synced once at the end. The same request id after a stop finishes the job, skipping the rows already in."""
    job = _load_schedule(job_id)[0]
    selection = body.source
    groups = _job_groups(job_id)
    _refuse_job_tables(job, groups, request)
    inputs = nj.job_inputs(job["name"], db.query(q.groups_control_rows_sql(), (groups,)) if groups else [],
                           body.groups_per_server)
    if inputs["errors"]:
        raise HTTPException(422, {"errors": inputs["errors"]})
    taken = {r["task_collection"] for r in db.query(q.collections_sql())}
    columns = _source_columns(kind, param)
    user, batch = _user(request), uuid.uuid4().hex[:12]
    given = {**inputs["given"], "load_type": body.load_type}
    servers = db.query(q.selected_servers_sql(kind), (param, *rest))
    databases_total = sum(s["databases"] for s in servers)
    parts = _selection_parts(selection, [s["server"] for s in servers] if selection.whole_server else [])

    def plan_of(rows, stages):
        with stages.timed("plan"):
            return by_database.plan_database(job["name"], given, rows, columns, {}, taken, set(groups),
                                             group_of=inputs["group_of"], have=inputs["have"])

    def read_of(chunk, stages):
        with stages.timed("read"):
            return _rows_of_units(kind, param, chunk)

    def check(stages, keep):
        """Every database planned, nothing written: the lines that cannot be taken, and the groups the job would add.
        Returns those totals, and each database with its table count, in order, for the write to pack into
        transactions."""
        invalid, tables, in_job, new_groups, counted = [], 0, 0, set(), []
        for read_batch in by_database.read_batches(list(_units_of(kind, param, selection))):
            for unit, rows in read_of(read_batch, stages):
                if not rows:
                    continue
                plan = plan_of(rows, stages)
                if plan["errors"]:
                    raise HTTPException(422, {"errors": plan["errors"][:20]})
                for line in plan["lines"]:
                    if line["status"] == "invalid":
                        invalid.append(f"{unit.database} {line['table']}: {'; '.join(line['errors'])}")
                    elif line["status"] == "in_job":
                        in_job += 1
                    else:
                        tables += 1
                new_groups |= set(plan["groups"]) - set(groups)
                counted.append(by_database.Checked(unit, len(rows), plan if keep else None))
        if invalid:
            raise HTTPException(422, {"errors": invalid[:20] + (
                [f"{len(invalid)} lines cannot be taken"] if len(invalid) > 20 else [])})
        return {"tables": tables, "in_job": in_job, "new_groups": sorted(new_groups)}, counted

    def write(progress, task_id):
        stages = by_database.Stages()
        state = {"stage": "checking", "databases_total": databases_total, "databases_written": 0, "tables_written": 0,
                 "seconds": stages.seconds()}
        db.execute(q.task_progress_json_sql(), (json.dumps(state), 0, task_id))
        keep = by_database.keeps_plans(total)
        found, counted = check(stages, keep)
        stages.begin_write()
        active = [m for m in _load_schedule(job_id)[1] if m["removed_at"] is None]
        offs = sch.offsets([m["task_collection"] for m in active] + found["new_groups"], int(job["cadence_minutes"]),
                           bool(job.get("stagger")), sch.start_minute(job))
        state["stage"] = "writing"
        chunk_job = ChunkJob(sid=job_id, name=job["name"], user=user, batch=batch,
                             cadence_minutes=int(job["cadence_minutes"]), timezone=job["timezone"], offsets=offs,
                             task_id=task_id, stages=stages, log_members=False)
        for chunk in by_database.transactions_of(counted):
            plans = [checked.plan for checked in chunk] if keep else [
                plan_of(rows, stages) for _, rows in read_of([checked.unit for checked in chunk], stages) if rows]
            if plans:
                with _write() as cur:
                    _lock_job(cur, job_id)
                    _write_chunk(cur, plans, chunk_job, state)
        result = {"summary": {**found, "tables": state["tables_written"], "by_database": True, "checked": True},
                  "applied": True, "databases": state["databases_written"]}
        with _write() as cur:
            _keep_selection(cur, job_id, {"parts": parts, "kind": kind, "source_id": source_id}, user)
            _touch(cur, user, [job_id])
            _task_written(cur, task_id, {**result, "new_groups": found["new_groups"]}, job_id, state["tables_written"])
        return {**result, "new_groups": found["new_groups"]}

    def after(result):
        if result.get("new_groups"):
            _synced(_sync(job_id, user))
        return {k: v for k, v in result.items() if k != "new_groups"}

    if body.dry_run:
        return {"summary": {"tables": total, "by_database": True, "checked": False, "new_groups": []},
                "applied": False}
    made = db.query(q.insert_task_sql(), ("tables", body.request_id or uuid.uuid4().hex, job_id, total, None,
                                          user or None))[0]
    if made["made"] or db.query(q.resume_task_sql(), (made["id"],)):
        _start_background(lambda: _run_task(made["id"], write, after))
        return _task_answer(made["id"])
    return _answer_again(made["id"], after)


@app.post("/api/ingestion-jobs")
def create_ingestion_job(body: NewJobRequest, request: Request):
    """Preview (dry_run, the default) or create an ingestion job in one go, as New ingestion job does: its tables from
    a CSV of where each one is (new_job.plan), written with the job's settings into groups per server, and the job
    (a schedule with those groups). Creating is one transaction: the schedule, every table (all or nothing: a target
    another table writes refuses the lot), the groups' members; then its Databricks jobs are synced. The tables come
    as CSV lines (csv) or by reference to an upload or discovery (source), which is kept with the job. A source of more
    than BACKGROUND_ROWS tables is written database by database in a background task (_create_by_database)."""
    if body.source is not None and not _has_database_lines(body.source):
        kind, param, source_id, rest = _selection_params(body.source)
        counted = db.query(q.selected_count_sql(kind), (param, *rest))
        if counted and counted[0]["n"] > BACKGROUND_ROWS:
            return _create_by_database(body, request, body.source, kind, param, source_id, rest)
    user = _user(request)
    create_options = {k: v for k, v in (body.run.options or {}).items() if k not in MANAGED_OPTIONS}

    def after_create(result):
        return _created(result, user, create_options)

    if (made := _made_already(body, after_create)) is not None:
        return made
    return _create_job(body, request)


def _target_missing(settings: dict) -> List[str]:
    """The target fields a preview's settings leave blank, by name, for the page to say what Create needs."""
    return [label for key, label in js.TARGET_SETTINGS if not str(settings.get(key) or "").strip()]


def _create_job(body: NewJobRequest, request: Request, inline: bool = False, progress=None, task_id=None):
    """The Create itself. A file with database-only lines is first discovered by name in a background task
    (_create_after_discovery), which runs this again with inline=True, so the write happens in that task."""
    user = _user(request)
    create_options = {k: v for k, v in (body.run.options or {}).items() if k not in MANAGED_OPTIONS}
    if not inline and body.source is not None and not body.dry_run and _has_database_lines(body.source):
        if not body.run.paused:
            _refuse(_permission_refusals({"job_variant": body.run.job_variant}, user)["run"])
        return _write_now_or_later("create", body.request_id, None, 0, user,
                                   lambda progress, task_id: _create_after_discovery(body, request, progress, task_id),
                                   lambda result: _created(result, user, create_options) if result.get("created")
                                   else result, background=True)
    import psycopg
    name = body.name.strip()
    run = body.run.model_copy()
    run.stagger = run.stagger and run.job_variant != sch.CHECK_VARIANT
    taken = {r["task_collection"] for r in db.query(q.collections_sql())}
    text, selected = body.text(request, wait=inline)
    if selected is not None and selected.get("discovering"):
        return {"discovering": True, "summary": None, "settings": {}, "created": False, "databases": selected["databases"]}

    def planned():
        plan = _file_lines(nj.plan(name, text, body.settings, body.groups_per_server, taken,
                                   require_target=not body.dry_run), selected)
        errors = list(plan["errors"])
        if not errors and plan["groups"]:
            schedule = {**run.model_dump(), "name": name, "members": plan["groups"],
                        "timezone": sch.timezone_for(run.cadence_minutes, run.timezone)}
            errors += sch.validate(schedule, plan["groups"], jobs.schedule_variants())
        if errors:
            raise HTTPException(422, {"errors": errors})
        return plan

    plan = planned()
    summary = nj.summary(plan)
    if body.dry_run or summary["invalid"]:
        return {"summary": summary, "settings": plan["settings"], "created": False,
                "target_missing": _target_missing(plan["settings"])}
    if not run.paused:     # anyone may set up a job, but one that runs needs the right to run what it starts
        _refuse(_permission_refusals({"job_variant": run.job_variant}, user)["run"])
    options = create_options
    _refuse_unknown_node_types(sch.own_cluster({"options": options}))
    timezone_ = sch.timezone_for(run.cadence_minutes, run.timezone)
    offs = sch.offsets(plan["groups"], run.cadence_minutes, run.stagger, sch.start_minute({"options": options, "cadence_minutes": run.cadence_minutes}))
    batch = uuid.uuid4().hex[:12]
    rows = [line["row"] for line in plan["lines"]]

    def write(progress, task_id):
        try:
            with _write() as cur:
                _lock_collections(cur, plan["groups"])
                cur.execute(q.insert_schedule_sql(), (name, run.cadence_minutes, timezone_, json.dumps(run.parameters),
                                                      run.job_variant or "default", run.stagger, run.paused,
                                                      run.notify_emails or None, json.dumps(options) if options else None,
                                                      user or None))
                sid = cur.fetchone()[0]
                _insert_controls(cur, rows, user, batch, progress=progress)
                _refuse_mixed_sinks(cur, rows)
                _refuse_split_databases(cur, rows)
                for group in plan["groups"]:
                    cur.execute(q.insert_member_sql(), (sid, group, offs[group]))
                _log_lines(cur, [(user, "schedule_create", None, group, "schedule", None,
                                  f"{name} ({sch.describe(run.cadence_minutes, offs[group], timezone_)})", None, batch)
                                 for group in plan["groups"]])
                _keep_selection(cur, sid, selected, user)
                _touch(cur, user, [sid])
                result = {"id": sid, "summary": summary, "settings": plan["settings"], "created": True}
                _task_written(cur, task_id, result, sid, len(rows))
        except psycopg.errors.UniqueViolation:
            raise HTTPException(409, f"an ingestion job named '{name}' already exists")
        return result

    def after(result):
        return _created(result, user, options)

    if inline:
        return write(progress, task_id)
    return _write_now_or_later("create", body.request_id, None, len(rows), user, write, after)


def _create_after_discovery(body: NewJobRequest, request: Request, progress, task_id) -> dict:
    """The background half of a Create with database-only lines: their tables are discovered by name (the task's stage
    says so), then the Create is written as an inline one is."""
    _task_stage(task_id, "discovering")
    _resolve_databases(_selected(body.source), wait=True, request=request)
    _task_stage(task_id, "writing")
    return _create_job(body, request, inline=True, progress=progress, task_id=task_id)


@app.post("/api/schedules/{sid}")
def update_schedule(sid: int, body: ScheduleBody, request: Request):
    """Edit a schedule: fields, and members added or removed. Offsets are recomputed; jobs are synced. A change to its
    own cluster starts the copies job, which makes, updates or deletes its copy of the ingestion job."""
    import psycopg
    sent_options = "options" in body.model_fields_set   # read first: assigning body.options marks it as sent
    body.options = _requested_options(body)
    _validate_schedule(body)
    _refuse_unknown_node_types(sch.own_cluster(body.model_dump()))
    user = _user(request)
    s, members = _load_schedule(sid)
    if s["deleted_at"] is not None:
        raise HTTPException(409, "schedule was deleted")
    _refuse_job(s, request, "change")
    if (body.job_variant or "default") != (s["job_variant"] or "default") and not body.paused:
        _refuse(_permission_refusals({"job_variant": body.job_variant}, user)["run"])
    body.stagger = body.stagger and body.job_variant != sch.CHECK_VARIANT
    new = body.model_dump()
    new["notify_emails"] = new["notify_emails"] or None
    new["job_variant"] = new["job_variant"] or "default"
    stored = sch.options_of(s)
    # A body without options (an older page) keeps the stored ones; an explicit {} clears them.
    new["options"] = (new["options"] or None) if sent_options else (stored or None)
    kept = {k: stored[k] for k in MANAGED_OPTIONS if k in stored}
    if kept:
        new["options"] = {**(new["options"] or {}), **kept}
    as_json = lambda v: json.loads(v) if isinstance(v, str) else (v or {})
    cluster_changed = sch.own_cluster(new) != sch.own_cluster(s)
    changed = [f for f in q.SCHEDULE_FIELDS if (s[f] or None) != (new[f] or None) and not
               (f in q.JSON_SCHEDULE_FIELDS and as_json(s[f]) == as_json(new[f]))]
    active = {m["task_collection"]: m for m in members if m["removed_at"] is None}
    added = sorted(set(body.members) - set(active))
    gone = sorted(set(active) - set(body.members))
    with_tables = sorted(set(gone) & {r["task_collection"] for r in db.query(q.collections_sql())})
    if with_tables:
        raise HTTPException(422, f"every ingestion group belongs to an ingestion job: {', '.join(with_tables)} still "
                                 "has tables, so it can't leave this job")
    offs = sch.offsets(body.members, body.cadence_minutes, body.stagger, sch.start_minute(new))
    batch = uuid.uuid4().hex[:12]
    try:
        with _write() as cur:
            # Serialized with job-wide edits (_lock_job) and syncs, which read the members under this lock.
            cur.execute(q.schedule_lock_sql(), (q.SCHEDULE_LOCK_CLASS, q.schedule_lock_key(sid)))
            if changed:
                vals = [(json.dumps(new[f]) if new[f] is not None else None) if f in q.JSON_SCHEDULE_FIELDS else new[f]
                        for f in changed]
                cur.execute(q.update_schedule_sql(changed), vals + [int(sid)])
                for f in changed:
                    _log(cur, user, "schedule_edit", None, None, f"{s['name']}.{f}", s[f], new[f], batch=batch)
            if gone:
                cur.execute(q.remove_members_sql(), ([active[c]["id"] for c in gone],))
                for c in gone:
                    _log(cur, user, "schedule_edit", None, c, f"{s['name']}.members", c, None, batch=batch)
            for c in added:
                cur.execute(q.insert_member_sql(), (int(sid), c, offs[c]))
                _log(cur, user, "schedule_edit", None, c, f"{s['name']}.members", None, c, batch=batch)
            for c, m in active.items():
                if c in offs and int(m["offset_minutes"]) != offs[c]:
                    cur.execute(q.update_member_offset_sql(), (offs[c], m["id"]))
            if changed or gone or added:
                _touch(cur, user, [sid])
    except psycopg.errors.UniqueViolation:
        raise HTTPException(409, f"a schedule named '{body.name}' already exists")
    synced = _sync(sid, user)
    cluster = _apply_cluster(sid) if cluster_changed else None
    return {"id": sid, "changed": changed, "added": added, "removed": gone, "sync": synced, "cluster_error": cluster,
            "cluster_started": cluster_changed and cluster is None}


def _set_paused(sid: int, paused: bool, request: Request):
    user = _user(request)
    s, _ = _load_schedule(sid)
    if s["deleted_at"] is not None:
        raise HTTPException(409, "schedule was deleted")
    _refuse_job(s, request, "change")
    if bool(s["paused"]) != paused:
        with _write() as cur:
            cur.execute(q.update_schedule_sql(["paused"]), (paused, int(sid)))
            _log(cur, user, "schedule_pause" if paused else "schedule_resume", None, None, f"{s['name']}.paused",
                 s["paused"], paused)
            _touch(cur, user, [sid])
    return {"id": sid, "paused": paused, "sync": _sync(sid, user)}


@app.post("/api/schedules/{sid}/pause")
def pause_schedule(sid: int, request: Request):
    return _set_paused(sid, True, request)


@app.post("/api/schedules/{sid}/resume")
def resume_schedule(sid: int, request: Request):
    return _set_paused(sid, False, request)


class DeleteJobBody(BaseModel):
    drop_targets: bool = False


@app.post("/api/schedules/{sid}/delete")
def delete_schedule(sid: int, request: Request, body: Optional[DeleteJobBody] = None):
    """Delete a schedule (an ingestion job): it is marked deleted (the row is kept), its wrapper jobs are deleted, and
    the delete job starts, which deletes its groups' tables and their state as the deployer (the app has no DELETE
    privilege) and, with ``drop_targets``, the target tables only those groups write. Run history and the change log
    stay. Refused while a run of the job is going. On a job already deleted, it starts the delete job again (Run
    again on Ingestion jobs), with the flag it was deleted with unless the body sets it."""
    user = _user(request)
    s, members = _load_schedule(sid)
    _refuse_job(s, request, "delete")
    active = [m for m in members if m["removed_at"] is None]
    plan = _delete_plan(sid)
    blocker = _delete_blocker(s, active, plan["groups"])
    if blocker:
        raise HTTPException(409, blocker)
    if plan["tables"] and not jobs.DELETE_JOB_ID:
        raise HTTPException(409, NO_DELETE_JOB)
    if s.get("deleted_at") is not None:
        if not plan["tables"]:
            return {"id": sid, "deleted": True, "sync": [], "delete_run": None}
        drop = body.drop_targets if body else bool(sch.options_of(s).get("delete_drop_targets"))
        with _write() as cur:
            _record_delete(cur, sid, drop)
        return {"id": sid, "deleted": True, "sync": [], "delete_run": _start_delete_job(sid, drop)}
    drop = bool(body and body.drop_targets)
    with _write() as cur:
        cur.execute(q.delete_schedule_sql(), (int(sid),))
        if active:
            cur.execute(q.remove_members_sql(), ([m["id"] for m in active],))   # their jobs are deleted by the sync below
        if plan["tables"]:
            _record_delete(cur, sid, drop)
        _log(cur, user, "schedule_delete", None, None, s["name"], None, "drop_targets" if drop else None)
    synced = _sync(sid, user)
    if sch.options_of(s).get(sch.COPY_OPTION):     # its copy of the ingestion job goes too
        _apply_cluster(sid)
    return {"id": sid, "deleted": True, "sync": synced,
            "delete_run": _start_delete_job(sid, drop) if plan["tables"] else None}


NO_DELETE_JOB = ("no delete job is configured for this app, so the job's groups and their tables would stay behind "
                 "outside any job: deploy the engine bundle, then the app, and delete again")
# Deleted jobs the Ingestion jobs page lists with their delete job run.
DELETIONS_LISTED = 10


def _delete_plan(sid: int) -> dict:
    """What a job's Delete removes, read fresh: the groups no other live job holds, their tables (control rows), the
    target tables only they write, and those another group's rows also write (kept, the first DELETE_KEPT_LISTED
    named)."""
    groups = [r["task_collection"] for r in db.query(q.delete_groups_sql(), (int(sid), int(sid)))]
    if not groups:
        return {"groups": [], "tables": 0, "targets": 0, "kept": 0, "kept_names": []}
    row = db.query(q.delete_targets_sql(), (groups, groups, groups))[0]
    names = row.get("kept_names") or []
    return {"groups": groups, "tables": int(row.get("tables") or 0), "targets": int(row.get("targets") or 0),
            "kept": int(row.get("kept") or 0), "kept_names": json.loads(names) if isinstance(names, str) else list(names)}


def _delete_blocker(job: dict, active: List[dict], groups: List[str]):
    """Why the job can't be deleted right now: a run of it, or of a group it deletes, is going (Lakebase sees a run
    once its cluster is up, the Jobs API from its start), a background write to it is still running (its heartbeat is
    fresh), or its delete job is still running."""
    if any(r["task_collection"] in {m["task_collection"] for m in active} | set(groups) for r in _running()):
        return "a run of this job is going: wait for it to end, or stop it, then delete the job"
    if db.query(q.running_task_sql(), (int(job["id"]), TASK_STALE_S)):
        return "a write to this job is still running (adding or creating tables): wait for it to finish, then delete the job"
    going = _active_wrapper_runs(active)
    if going:
        return {"message": f"a run of this job is going, started {going[0].get('started') or 'just now'}: wait for it "
                           "to end, or stop it, then delete the job",
                "run_url": going[0].get("url"), "run_id": going[0].get("run_id")}
    run = _delete_run(job)
    if run and ij.is_active(run):
        return {"message": "this job's delete is still running", "run_url": run.get("url"), "run_id": run.get("run_id")}
    return None


def _record_delete(cur, sid: int, drop: bool) -> None:
    """Record that the job's Delete runs the delete job, and with which flag. The earlier run, its start error and its
    finished mark are forgotten, so the job shows Deleting until this run's state is known."""
    cur.execute(q.set_schedule_flag_sql(), (["delete_drop_targets"], bool(drop), int(sid)))
    for key in ("delete_run_id", "delete_start_error", "delete_finished"):
        cur.execute(q.clear_schedule_option_sql(), (key, int(sid)))


def _start_delete_job(sid: int, drop: bool) -> dict:
    """Start the delete job for a deleted job and record its run. A start that fails is reported, not raised: the job
    is deleted already, and Run again on Ingestion jobs starts it later."""
    try:
        run_id = jobs.start_delete_job(sid, drop)
    except Exception as e:
        error = f"the delete job didn't start: {str(e).split('. ')[0]}"
        with _write() as cur:
            cur.execute(q.set_schedule_text_option_sql(), (["delete_start_error"], error, int(sid)))
        return {"run_id": None, "url": None, "error": error}
    with _write() as cur:
        cur.execute(q.set_schedule_option_sql(), (["delete_run_id"], run_id, int(sid)))
    host = jobs.workspace_url()
    return {"run_id": run_id, "url": f"{host}/jobs/{jobs.DELETE_JOB_ID}/runs/{run_id}" if host else None, "error": None}


def _delete_run(job: dict) -> Optional[dict]:
    """The job's latest delete job run, as the Jobs API has it, or None (not started, or unreadable: error set)."""
    run_id = sch.options_of(job).get("delete_run_id")
    if not run_id:
        return None
    try:
        return run_poller.run_state_of(run_id)
    except Exception as e:
        return {"run_id": run_id, "state": None, "result": None, "url": None,
                "message": f"can't read the delete job's run: {str(e).split('. ')[0]}"}


@app.post("/api/schedules/{sid}/sync")
def sync_schedule(sid: int, request: Request):
    """Re-sync the wrapper jobs (recreates missing ones and undoes edits made outside the app)."""
    s = _load_schedule(sid)[0]
    # A deleted job keeps its sync: it is how a wrapper job whose delete failed is deleted again, so only the
    # change permission is checked here, not the deleted-job refusal.
    _refuse(_job_permission_refusals(s, _user(request))["change"])
    cluster = _apply_cluster(sid) if sch.own_cluster(s) else None     # copies the ingestion job again
    return {"id": sid, "sync": _sync(sid, _user(request)), "cluster_error": cluster}


@app.get("/api/schedules/{sid}/check")
def check_schedule(sid: int):
    """Drift report for the schedule's wrapper jobs; changes nothing."""
    s, members = _load_schedule(sid)
    active = [m for m in members if m["removed_at"] is None]
    target = _wrapper_target(s)
    if target is None:
        raise HTTPException(503, f"no Databricks ingestion job configured for variant '{s['job_variant']}'")
    return sch.check(s, active, jobs.schedule_client(), target, _owner())


def _timeline_run(job_run: dict, now: datetime) -> dict:
    """One job run as a bar on the Timeline: from its first group run's start to its last one's finish (to now while
    running; a trigger that did not run has no span), its outcome, and each group with its group run, for the hover."""
    members = job_run["placed"]["members"]
    runs = [m["run"] for m in members if m.get("run")]
    finishes = [q._when(r["finished_at"]) for r in runs if r.get("finished_at")]
    running = job_run["outcome"]["label"] == "Running"
    ended = now if running else (max(finishes) if finishes and job_run["started_at"] else None)
    return {**{k: job_run.get(k) for k in ("slot", "started_at", "outcome", "ran", "members", "failed_tables", "took_s")},
            "ended_at": ended.isoformat() if ended else None,
            "groups": [{"group": m["task_collection"], "status": (m.get("run") or {}).get("status"),
                        "run_id": (m.get("run") or {}).get("run_id")} for m in members]}


@app.get("/api/timeline")
def timeline(hours: int = 24):
    """The last `hours` as the Jobs UI's run timeline, one lane per ingestion job: its job runs (each with its span,
    outcome and groups; a paused job's triggers that did not run left out), its planned triggers (the current
    schedule, since it or the group last changed) and the triggers Databricks skipped because the previous run was
    still going (from the wrapper jobs' runs). The group runs of groups in no job make one more lane."""
    hours = max(1, min(int(hours), 168))
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=hours)
    schedules, members = _schedules()
    active = _active(members)
    client = None
    lanes = []
    for s in sorted(schedules, key=lambda s: str(s["name"]).lower()):
        planned, skipped = set(), []
        for m in (m for m in active if m["schedule_id"] == s["id"]):
            if not s["paused"]:
                since = max([start] + [q._when(v) for v in (m.get("added_at"), s.get("updated_at")) if v])
                planned |= {t.isoformat() for t in sch.fire_times(int(s["cadence_minutes"]), int(m["offset_minutes"]),
                                                                   s["timezone"], since, end)}
            if m.get("job_id"):
                client = client or jobs.schedule_client()
                skipped += [jobs.ts(t) for t in (_skipped_starts(m["job_id"], start, end, client) or [])]
        lanes.append({"job_id": s["id"], "name": s["name"], "paused": bool(s["paused"]),
                      "description": sch.describe(int(s["cadence_minutes"]), sch.start_minute(s), s["timezone"] or "UTC"),
                      # A paused job's triggers that did not run are left out, as Runs folds them away.
                      "runs": [_timeline_run(jr, end) for jr in _job_runs_of(s, start, end, end)
                               if not (jr["outcome"]["label"] == "Not run" and jr["outcome"].get("paused"))],
                      "planned": sorted(planned), "skipped": sorted(skipped)})
    unscheduled = sorted({st["task_collection"] for st in _states()} - {m["task_collection"] for m in active})
    group_runs = [{k: r.get(k) for k in ("run_id", "task_collection", "status", "started_at", "finished_at")}
                  for r in _group_runs(unscheduled, start, end)] if unscheduled else []
    return {"start": start.isoformat(), "end": end.isoformat(), "jobs": lanes,
            "not_in_job": {"groups": unscheduled, "group_runs": group_runs} if group_runs else None}


# ---- UC connections (bulk import) ----------------------------------------------------------------

DISCOVERABLE_CONNECTION_TYPES = {"SQLSERVER"}
# Connection types lakefed can read: its source adapters (queries.SRC_TYPES). HTTP and other connections are left out.
READABLE_CONNECTION_TYPES = {t.upper() for t in q.SRC_TYPES}


def _connections() -> List[dict]:
    """The database connections of a type lakefed can read that this app's identity can see, with host and port
    from their options."""
    out = []
    for c in db.workspace().connections.list():
        options = c.options or {}
        kind = getattr(c.connection_type, "value", c.connection_type)
        if str(kind or "").upper() not in READABLE_CONNECTION_TYPES:
            continue
        out.append({"name": c.name, "connection_type": kind, "host": options.get("host"), "port": options.get("port"),
                    "owner": c.owner, "comment": c.comment, "discoverable": kind in DISCOVERABLE_CONNECTION_TYPES})
    return sorted(out, key=lambda c: c["name"])


@app.get("/api/connections")
def connections(fresh: bool = False):
    """UC connections with what lakefed tracks on each (control rows naming the connection or its host).
    A listing error (no rights on any connection, for example) is returned, not raised, so the page
    still offers discovery by host and secret. `fresh` lists them again now (the picker's Refresh, after a
    connection is made in Catalog Explorer) instead of from the shared read."""
    if fresh:
        with _shared_lock:
            _shared_reads.pop("connections", None)
    def compute():
        try:
            found = _connections()
        except Exception as e:                          # the app's identity may have no rights on connections
            return {"connections": [], "error": str(e)[:500]}
        states = _states()
        for c in found:
            c["tracked"] = rollup.tracked(states, c["name"], c["host"], c.get("port"), c.get("connection_type"))
        return {"connections": found, "error": None}
    return _shared("connections", compute)


@app.get("/api/connections/{name}/databases")
def connection_databases(name: str):
    """The databases lakefed tracks on one connection (by its name or its host)."""
    found = next((c for c in connections()["connections"] if c["name"] == name), None)
    if not found:
        raise HTTPException(404, f"connection '{name}' not found")
    return rollup.connection_databases(_states(), name, found["host"], found.get("port"), found.get("connection_type"))


# ---- single-page app ----------------------------------------------------------------------------

if (STATIC / "assets").is_dir():
    app.mount("/assets", StaticFiles(directory=STATIC / "assets"), name="assets")


@app.get("/{path:path}", include_in_schema=False)
def spa(path: str):
    if path.startswith("api/"):
        raise HTTPException(404)
    # Only files inside static/: the path is decoded, so "%2e%2e/" must not climb out of it.
    target = (STATIC / path).resolve()
    if path and target.is_file() and target.is_relative_to(STATIC.resolve()):
        return FileResponse(target)
    index = STATIC / "index.html"
    if not index.is_file():
        return JSONResponse(status_code=503, content={"detail": "frontend not built (npm run build)"})
    return FileResponse(index)


# ---- re-discovery: a job's selection read again each day at its hour (rediscovery.py) ----------------------------------
# The scheduler is a thread of the app, so it runs while the app does. A job that is due is claimed in Lakebase first
# (its record becomes running), so one start runs however many ticks or app instances look; the re-discovery then runs
# on its own thread, and its tables are written by the path Add tables takes, as the change log's re-discovery user.
REDISCOVERY_TICK_S = float(os.environ.get("REDISCOVERY_TICK_S", "300"))   # 0 turns the scheduler off
REDISCOVERY_POLL_S = float(os.environ.get("REDISCOVERY_POLL_S", "30"))
REDISCOVERY_MAX_WAIT_S = float(os.environ.get("REDISCOVERY_MAX_WAIT_S", str(6 * 3600)))
REDISCOVERY_USER = "re-discovery"


def _rediscovery_scheduler() -> None:
    while True:
        time.sleep(REDISCOVERY_TICK_S)
        try:
            _rediscovery_tick()
        except Exception:
            logging.getLogger(__name__).exception("re-discovery tick failed")


def _rediscovery_tick(now: Optional[datetime] = None) -> List[int]:
    """Start the re-discovery of each job that is due now, each on its own thread. Returns the jobs started."""
    now = now or datetime.now(timezone.utc)
    started = []
    for job in db.query(q.schedules_sql()):
        if not rediscovery.is_due(sch.options_of(job), now, bool(job["paused"])):
            continue
        claimed = db.query(q.claim_rediscovery_sql(),
                           (json.dumps(rediscovery.running_record(now)), job["id"], rediscovery.STALE_S))
        if claimed:
            started.append(job["id"])
            _start_background(lambda sid=job["id"]: _run_rediscovery(sid, now))
    return started


@app.post("/api/ingestion-jobs/{job_id}/rediscover")
def rediscover_now(job_id: int, request: Request):
    """Re-discover a job now, the run the daily hour starts, on its own thread. It takes the same claim as the daily run,
    so a run already going (not yet stale) refuses it: the job page's Run now stays off until that run has ended."""
    s = _load_schedule(job_id)[0]
    _refuse(_job_permission_refusals(s, _user(request))["change"])
    now = datetime.now(timezone.utc)
    claimed = db.query(q.claim_rediscovery_sql(),
                       (json.dumps(rediscovery.running_record(now)), int(job_id), rediscovery.STALE_S))
    if not claimed:
        raise HTTPException(409, "a re-discovery of this job is already running")
    _start_background(lambda: _run_rediscovery(int(job_id), now))
    return {"id": int(job_id), "started_at": now.isoformat()}


def _run_rediscovery(sid: int, started: datetime) -> None:
    """A job's re-discovery on its own thread. Its outcome (what was found and added, what vanished, the errors) or why
    it did not run is recorded on the job, even when it failed. A 409 because a group is running is 'deferred': the
    next re-discovery is the next day's. Each phase is recorded as it starts, so the job page can show where it is."""
    record = rediscovery.running_record(started)
    stop = threading.Event()

    def beat():
        while not stop.wait(rediscovery.BEAT_S):
            try:
                db.execute(q.beat_rediscovery_sql(), (datetime.now(timezone.utc).isoformat(), int(sid)))
            except Exception:
                pass     # a missed beat is caught up by the next; STALE_S allows for several

    def report(**fields) -> None:
        record.update(fields, heartbeat_at=datetime.now(timezone.utc).isoformat())
        db.execute(q.set_rediscovery_sql(), (json.dumps(record, default=str), int(sid)))

    threading.Thread(target=beat, daemon=True, name="lakefed-rediscovery-beat").start()
    try:
        record.update(status="done", **_rediscover(sid, report))
    except HTTPException as e:
        deferred = e.status_code == 409 and "running" in str(e.detail)
        record.update(status="deferred" if deferred else "failed", error=_error_text(e))
    except Exception as e:
        record.update(status="failed", error=_error_text(e))
    finally:
        stop.set()
    for progress_key in ("phase", "run_id", "server"):
        record.pop(progress_key, None)
    record["finished_at"] = datetime.now(timezone.utc).isoformat()
    db.execute(q.set_rediscovery_sql(), (json.dumps(record, default=str), int(sid)))
    _forget_shared_reads()


def _rediscovery_view(job: dict, now: datetime) -> dict:
    """The job page's re-discovery: the hour, the last record and its progress. While a discovery run goes, the run
    poller's state for it decides whether that is pending or past its wait (a timeout, not done)."""
    options = sch.options_of(job)
    last = (options or {}).get(rediscovery.RECORD) or None
    outcome = None
    if last and last.get("phase") == rediscovery.DISCOVERING and last.get("run_id"):
        try:
            run = run_poller.run_state_of(last["run_id"])
        except Exception:
            run = None
        outcome = action_states.run_outcome(run, now, REDISCOVERY_MAX_WAIT_S)
    return rediscovery.view(options, now, outcome)


def _rediscover(sid: int, report=lambda **fields: None) -> dict:
    """Read a job's selection's servers again (one discovery run each), then add what the rules allow: in one
    transaction under the job's lock and its groups' run locks, through the path Add tables takes. The job's new groups
    are synced after the commit."""
    job, _ = _load_schedule(sid)
    selection = db.query(q.job_selection_sql(), (int(sid),))
    if not selection:
        return {"found": 0, "added": 0, "note": "the job was not made from a selection, so there is nothing to read again"}
    discovery_parts = [p for p in selection if p["source_kind"] == "discovery"]
    if not discovery_parts:
        return {"found": 0, "added": 0, "note": "the job was made from an uploaded CSV, which has no source to read again"}
    if not any(p["scope"] != "tables" for p in discovery_parts):
        return {"found": 0, "added": 0, "note": "the job's selection is specific tables, which add nothing"}
    ids = sorted({i for p in discovery_parts for i in str(p["source_id"] or "").split(",") if i})
    readings, errors = rediscovery.specs(discovery_parts, db.query(q.selection_discoveries_sql(), (ids,)))
    new_ids = []
    for server, spec in readings:
        discovery_id = uuid.uuid4().hex[:12]
        run_id = jobs.run_now(_require(jobs.DISCOVER_JOB_ID, "discovery"),
                              {"discovery_id": discovery_id, "spec": json.dumps(spec)})
        new_ids.append(discovery_id)
        report(phase=rediscovery.DISCOVERING, run_id=run_id, server=server)
        ended = _wait_for_run(run_id)
        if ended.get("result") != "SUCCESS":
            errors.append(f"{server}: its discovery run ended {ended.get('result') or ended.get('state')}"
                          + (f": {ended['message']}" if ended.get("message") else ""))
    statuses = db.query(q.discovery_statuses_sql(), (new_ids,)) if new_ids else []
    found = db.query(q.source_rows_sql("discovery"), (new_ids,)) if new_ids else []
    taken = {r["task_collection"] for r in db.query(q.collections_sql())}
    report(phase=rediscovery.ADDING, run_id=None, server=None)
    user, batch = REDISCOVERY_USER, uuid.uuid4().hex[:12]

    def write() -> tuple:
        with _write() as cur:
            groups = _lock_job(cur, sid)
            job_rows = db.fetch(cur, q.groups_control_rows_sql(for_update=True), (groups,)) if groups else []
            picked = rediscovery.wanted(discovery_parts, found, statuses, job_rows)
            limit = rediscovery.REPORT_LIMIT
            outcome = {"found": len(picked["add"]), "added": 0, "skipped": 0,
                       "new_databases": picked["new_databases"][:limit],
                       "vanished": {"count": len(picked["vanished"]), "tables": picked["vanished"][:limit]},
                       "errors": (errors + [f"{e['database']}: {e['error']}" for e in picked["errors"]])[:limit],
                       "discovery_ids": new_ids}
            if not picked["add"]:
                return outcome, []
            plan = nj.plan_for_job(job["name"], nj.csv_of(nj.DISCOVERY_COLUMNS, [r["fields"] for r in picked["add"]]),
                                   "ct", job_rows, nj.DEFAULT_GROUPS_PER_SERVER, taken)
            if plan["errors"]:
                raise HTTPException(422, {"errors": plan["errors"]})
            written = _write_planned_tables(cur, job, plan, user, batch, skip_taken=True)
            if written:
                _touch(cur, user, [sid])
            outcome["added"] = len(written)
            outcome["skipped"] = len(picked["add"]) - len(written)
            outcome["skipped_tables"] = [{"table": line["table"], "errors": line["errors"]}
                                         for line in plan["lines"] if line["status"] == "invalid"][:limit]
            return outcome, plan["new_groups"]

    outcome, new_groups = write()
    if new_groups:
        _sync(sid, user)
    return outcome


def _wait_for_run(run_id: int) -> dict:
    """A Databricks run's end (jobs.run_ended), polled every REDISCOVERY_POLL_S until REDISCOVERY_MAX_WAIT_S."""
    waited = 0.0
    while (ended := jobs.run_ended(run_id)) is None:
        if waited >= REDISCOVERY_MAX_WAIT_S:
            raise HTTPException(504, f"discovery run {run_id} is still going after {int(waited)} seconds")
        time.sleep(REDISCOVERY_POLL_S)
        waited += REDISCOVERY_POLL_S
    return ended


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("DATABRICKS_APP_PORT", "8000")))
