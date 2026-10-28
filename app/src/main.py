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
import os
from contextlib import contextmanager
import threading
import time
import uuid
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator, model_validator

import compute
import config_import as ci
import ingestion_jobs as ij
import job_settings as js
import job_settings_sql as jss
import jobs
import lakebase as db
import queries as q
import rollup
import schedules as sch

app = FastAPI(title="lakefed_ingest operations", docs_url="/api/docs", openapi_url="/api/openapi.json")
STATIC = Path(__file__).parent / "static"


@app.exception_handler(Exception)
async def _errors(request: Request, exc: Exception):
    msg = str(exc)
    hint = None
    if "permission denied" in msg.lower():
        hint = "The app's service principal may lack privileges: run the lakefed_ingest_app_grants job."
    return JSONResponse(status_code=500, content={"detail": msg[:2000], "hint": hint})


def _user(request: Request) -> str:
    return request.headers.get("X-Forwarded-Email") or request.headers.get("X-Forwarded-User") or ""


def _lock_collections(cur, collections):
    """Take each collection's run lock for this transaction, or refuse: changing tables while their
    collection is running could race the run's own checkpoint writes."""
    for c in sorted({c for c in collections if c}):
        cur.execute(q.collection_lock_sql(), (q.collection_lock_key(c),))
        if not cur.fetchone()[0]:
            raise HTTPException(409, f"ingestion group '{c}' is running; try again after the run finishes")


def _refuse_mixed_sinks(cur, rows):
    """After writing control rows, in the same transaction: refuse when a sink they write would be
    claimed by a dedicated row and by other rows (the next run would stop every table on it)."""
    sinks = sorted({".".join(str(r.get(k)).lower() for k in ("sink_catalog", "sink_schema", "sink_table"))
                    for r in rows})
    if not sinks:
        return
    cur.execute(q.mixed_sinks_for_sql(), (sinks,))
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


def _apply_plans(cur, rows, plans, user, batch):
    """Write planned edits (queries.plan_edit) inside the caller's transaction: each changed field, its change log
    line, a reseed where the plan needs one, then the mixed-sink and split-database checks over the rows as they
    now are."""
    by_id = {r["id"]: r for r in rows}
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

@app.get("/api/config")
def config(request: Request):
    return {"lakebase_instance": db.INSTANCE, "lakebase_database": db.DATABASE, "user": _user(request),
            "jobs": {"ingest": jobs.INGEST_JOB_ID, "setup": jobs.SETUP_JOB_ID, "discover": jobs.DISCOVER_JOB_ID},
            "defaults": {"partition_threshold_mb": q.DEFAULT_PARTITION_THRESHOLD_MB,
                         "partition_size_mb": q.DEFAULT_PARTITION_SIZE_MB}}


# Monitoring reads are shared by every viewer and recomputed at most once per MONITOR_CACHE_S seconds,
# so the app's load on Lakebase does not grow with open tabs. 0 turns the cache off.
MONITOR_CACHE_S = max(0, int(os.environ.get("MONITOR_CACHE_S", "15")))
_shared_reads: dict = {}
_shared_lock = threading.Lock()


def _shared(key, compute, ttl=None):
    """compute() at most once per `ttl` seconds (MONITOR_CACHE_S by default) for key, shared by every
    request."""
    ttl = MONITOR_CACHE_S if ttl is None else ttl
    if ttl:
        with _shared_lock:
            hit = _shared_reads.get(key)
            if hit and time.time() - hit[1] < ttl:
                return hit[0]
    value = compute()
    if ttl:
        with _shared_lock:
            _shared_reads[key] = (value, time.time())
    return value


# Table states change at run cadence, so the one per-database scan they come from is shared for longer
# than running progress is.
STATES_CACHE_S = max(0, int(os.environ.get("STATES_CACHE_S", "60")))


def _states() -> List[dict]:
    """queries.database_states_sql, shared: the one scan of the control table the monitoring views sum."""
    return _shared("database_states", lambda: db.query(q.database_states_sql()), ttl=STATES_CACHE_S)


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
    MONITOR_CACHE_S however many pages ask."""
    return _shared("running", lambda: db.query(q.running_progress_sql()))


# The most running job runs, and jobs needing attention, the Overview lists; Runs and Ingestion jobs list all.
OVERVIEW_LIST = 5
OVERVIEW_JOBS_PAGE = 25


@app.get("/api/overview")
def overview(offset: int = 0):
    """The ingestion jobs (a page of OVERVIEW_JOBS_PAGE, failing first) with their Status, "Not in any job",
    the running job runs and the jobs needing attention (OVERVIEW_LIST of each, with their totals), and the
    totals over every enabled table, from the shared reads (_jobs_view)."""
    view = _jobs_view()
    offset = max(0, int(offset))
    return {"jobs": view["jobs"][offset:offset + OVERVIEW_JOBS_PAGE], "jobs_total": len(view["jobs"]),
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
    """Active schedules and all their members, removed ones included (a member's past runs still belong to
    the job runs of its time in the schedule), shared like _running. _active keeps the current members."""
    def compute():
        rows = db.query(q.schedules_sql())
        return rows, db.query(q.schedule_members_sql(), ([r["id"] for r in rows],)) if rows else []
    return _shared("schedules", compute)


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
        view = ij.jobs_view(schedules, members, states, running, runs, stats, now)
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
                                earlier_last_by_job=earlier)
        return view
    return _shared("jobs_view", compute)


JOB_FILTERS = ("all", "running", "failing", "late", "paused")


def _job_filter(job: dict, name: str) -> bool:
    return {"all": True, "running": bool(job.get("running_job_run")), "failing": bool(job["state_failing"]),
            "late": bool(job["state_late"]), "paused": bool(job.get("paused"))}[name]


@app.get("/api/ingestion-jobs")
def ingestion_jobs(filter: str = "all", search: str = "", offset: int = 0, limit: int = 25):
    """Every ingestion job, as on the Overview, filtered (each filter with its count) and searched on job and
    group names, a page at a time; "Not in any job" comes apart, under All."""
    if filter not in JOB_FILTERS:
        raise HTTPException(400, f"filter must be one of {', '.join(JOB_FILTERS)}")
    view = _jobs_view()
    needle = search.strip().lower()
    found = [j for j in view["jobs"]
             if not needle or needle in str(j["name"]).lower() or any(needle in g.lower() for g in j["groups"])]
    counts = {f: sum(1 for j in found if _job_filter(j, f)) for f in JOB_FILTERS}
    rows = [j for j in found if _job_filter(j, filter)]
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
    _, not_in_job = rollup.jobs(_states(), schedules, members)
    not_in_job = not_in_job or {"groups": []}
    groups, stats, last_runs = rollup.groups(_states()), _group_stats(), _last_runs()
    rows = []
    for name in sorted(not_in_job["groups"]):
        g, st, last = groups.get(name) or {}, stats.get(name) or {}, last_runs.get(name)
        rows.append({"task_collection": name, **{k: g.get(k) for k in ("databases", "enabled_tables") + rollup.STATES},
                     "last_run": dict(last, outcome=q.run_outcome(last)) if last else None,
                     "p95_s": st.get("p95_s"), "p50_s": st.get("p50_s")})
    return {**not_in_job, "name": "Not in any job", "groups_detail": rows}


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
                           **{k: g.get(k) for k in ("databases", "enabled_tables") + rollup.STATES},
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
            # For the Run now button: why it can't start now (None when it can), and what one run starts.
            "run_now": {"blocked": refusals["run"] or _run_now_blocker(job, mine),
                        "groups": len(mine), "check": job.get("job_variant") == sch.CHECK_VARIANT},
            "delete_blocked": refusals["delete"]}


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
    ingestion job. Run now and Delete follow the signed-in user's permission on it."""
    if (job.get("job_variant") or "default") == sch.CHECK_VARIANT:
        return jobs.CHECK_JOB_ID
    return jobs.ingest_variants().get(job.get("job_variant") or "default")


def _permission_refusals(job: dict, user: str) -> dict:
    """{action: why the user may not take it, or None} for Run now and Delete, from their Databricks permission."""
    started = _started_job(job)
    if not started:
        # An orphaned job (its variant no longer configured) has no Databricks job to ask; admins may clean it up.
        reason = "no Databricks job is configured for this job's runs"
        try:
            admin = "admins" in jobs.user_groups(user)
        except Exception:
            admin = False
        return {"run": reason, "delete": None if admin else f"{reason}, so only a workspace admin can delete it"}
    try:
        level = jobs.user_job_permission(started, user)
    except Exception as e:  # shown on the page and refused; Databricks decides, so no answer means no
        reason = f"can't read your permission on the Databricks job {started}: {str(e).split('. ')[0]}"
        return {"run": reason, "delete": reason}
    return {action: jobs.action_refusal(action, level, f"the Databricks job {started}") for action in ("run", "delete")}


def _run_now_blocker(job: dict, members: List[dict]) -> Optional[str]:
    """Why the job can't be started by hand right now (a run of it is going, or its wrapper jobs aren't synced)."""
    groups = {m["task_collection"] for m in members}
    if any(r["task_collection"] in groups for r in _running()):
        return "a run of this job is already running"
    if not members or any(not m.get("job_id") for m in members):
        return "the job's Databricks jobs are not synced yet"
    return None


@app.post("/api/ingestion-jobs/{job_id}/run")
def run_ingestion_job(job_id: int, request: Request):
    """Start one run of an ingestion job by hand, as its schedule would: its wrapper jobs (one per group, or a check
    schedule's one wrapper, which starts the check job over every group). Refused while a run of it is going."""
    job, members = _load_schedule(job_id)
    refusal = _permission_refusals(job, _user(request))["run"]
    if refusal:
        raise HTTPException(403, refusal)
    members = [m for m in members if m.get("removed_at") is None]
    blocker = _run_now_blocker(job, members)
    if blocker:
        raise HTTPException(409, blocker)
    wrappers = sorted({int(m["job_id"]) for m in members})
    return {"started": [jobs.run_now(wrapper, {}) for wrapper in wrappers]}


@app.get("/api/ingestion-jobs/{job_id}/compute")
def ingestion_job_compute(job_id: int):
    """What a job's runs execute on, read live from the Jobs API: the check job (on a check schedule) and the
    ingestion job it starts, each task's compute with where every value comes from (compute.describe)."""
    job = _load_schedule(job_id)[0]
    variant = job.get("job_variant") or "default"
    if variant == sch.CHECK_VARIANT:
        started = [("check", jobs.CHECK_JOB_ID), ("ingestion", jobs.ingest_variants().get("default"))]
    else:
        started = [("ingestion", jobs.ingest_variants().get(variant))]
    out = []
    for role, started_job_id in started:
        entry = {"role": role, "job_id": started_job_id, "name": None, "compute": [], "error": None}
        if not started_job_id:
            entry["error"] = f"no {role} job is configured for this app"
        else:
            try:
                settings = jobs.job_settings(started_job_id)
                entry.update(name=settings.get("name"), compute=compute.describe(settings))
            except Exception as e:  # shown on the page; the rest of the job's settings still load
                entry["error"] = f"can't read job {started_job_id}: {e}"
        out.append(entry)
    return {"jobs": out}


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
    try:
        with db.transaction() as cur:
            groups = _lock_job(cur, job_id)
            _lock_collections(cur, groups)
            fetch = lambda sql, params=(): db.fetch(cur, sql, params)
            now, found = checked(fetch, groups)
            if not found["summary"]["changed"]:
                return found
            result = jss.apply(cur, groups, body.settings, now, _user(request), uuid.uuid4().hex[:12])
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
    _job_groups(job_id)
    batch = uuid.uuid4().hex[:12]
    with db.transaction() as cur:
        groups = _lock_job(cur, job_id)
        ids = _job_row_ids(cur, groups, body) if groups else []
        if not ids:
            return {"updated": 0}
        rows = _locked_rows(cur, ids)
        _lock_collections(cur, [r["task_collection"] for r in rows])
        cur.execute(q.set_enabled_sql(), (body.enabled, ids))
        for r in rows:
            if bool(r["task_enabled"]) != body.enabled:
                _log(cur, _user(request), "enable" if body.enabled else "disable", r["id"], r["task_collection"],
                     "task_enabled", r["task_enabled"], body.enabled, batch=batch)
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
    _job_groups(job_id)
    with db.transaction() as cur:
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
        summary["applied"] = True
        return {"summary": summary, "plans": plans[:200]}


class JobDatabasesRequest(BaseModel):
    csv: Optional[str] = None
    columns: dict = {"server": "server", "database": "database", "enabled": "enabled"}
    databases: Optional[List[dict]] = None
    groups_per_server: int = Field(1, ge=1, le=64)
    full_list: bool = False
    dry_run: bool = True


@app.post("/api/ingestion-jobs/{job_id}/databases")
def add_job_databases(job_id: int, body: JobDatabasesRequest, request: Request):
    """Preview (dry_run, the default) or apply databases into a job, from a databases file (its columns matched
    to server, database and enabled) or a list picked by hand (job_settings.plan_databases). Applying is one
    transaction under every group's run lock: new tables inserted (a taken target is skipped), enabled flags
    set, new groups added to the job; then the job's wrapper jobs are synced."""
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
    if body.dry_run:
        plan, summary = planned(db.query(q.groups_control_rows_sql(), (groups,)) if groups else [])
        return {"summary": summary}
    user, batch = _user(request), uuid.uuid4().hex[:12]
    with db.transaction() as cur:
        groups = _lock_job(cur, job_id)
        rows = db.fetch(cur, q.groups_control_rows_sql(for_update=True), (groups,)) if groups else []
        plan, summary = planned(rows)
        _lock_collections(cur, groups + plan["new_groups"])
        inserted = 0
        for row in plan["new"]:
            cur.execute(q.insert_control_if_absent_sql(), q.control_params(row))
            res = cur.fetchone()
            if res:
                inserted += 1
                if not row["task_enabled"]:
                    cur.execute(q.set_enabled_sql(), (False, [res[0]]))
                _log(cur, user, "import", res[0], row["task_collection"], batch=batch)
        for enabled, ids in ((True, plan["enable"]), (False, plan["disable"])):
            if ids:
                cur.execute(q.set_enabled_sql(), (enabled, ids))
                for i in ids:
                    _log(cur, user, "enable" if enabled else "disable", i, None, "task_enabled", not enabled, enabled,
                         batch=batch)
        _refuse_mixed_sinks(cur, plan["new"])
        _refuse_split_databases(cur, plan["new"])
        if plan["new_groups"]:
            _, members = _load_schedule(job_id, cur)
            active = [m for m in members if m["removed_at"] is None]
            offs = sch.offsets([m["task_collection"] for m in active] + plan["new_groups"],
                               int(job["cadence_minutes"]), bool(job.get("stagger")), sch.start_minute(job))
            for g in plan["new_groups"]:
                cur.execute(q.insert_member_sql(), (job_id, g, offs[g]))
        summary.update(applied=True, new_tables=inserted, skipped=len(plan["new"]) - inserted)
    if plan["new_groups"]:
        _sync(job_id, user)
    return {"summary": summary}


@app.get("/api/ingestion-jobs/{job_id}/runs")
def ingestion_job_runs(job_id: int, before: Optional[str] = None, limit: int = 25):
    """A job's job runs, newest first, `limit` slots before `before` (now when not given); `next_before` is
    the oldest slot shown, for the next page. Placed by the job's current schedule."""
    job = _schedule(job_id)
    now = datetime.now(timezone.utc)
    end = datetime.fromisoformat(before) if before else now + timedelta(seconds=1)
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
    at = datetime.fromisoformat(slot)
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
    end = datetime.fromisoformat(before) if before else now + timedelta(seconds=1)
    limit = max(1, min(int(limit), 100))
    schedules, members = _schedules()
    if job == "none":
        unscheduled = sorted({s["task_collection"] for s in _states()} - {m["task_collection"] for m in _active(members)})
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
        view = rollup.sources(_states(), listed, db.query(q.not_ingested_sql()), schedules, members)
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
    states = [s for s in _states() if s["task_collection"] == name and s["server"] == server
              and s["src_database"] == database]
    if not states:
        raise HTTPException(404, f"no database {database} on {server or '(no server)'} in ingestion group {name}")
    sums = {k: sum(q._count(s, k) for s in states) for k in ("tables", "no_sla_tables") + rollup.STATES}
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
            r["rows_partial"] = bool(batched_unknown) if batched else bool(w.get("uncounted"))
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
    with db.transaction() as cur:
        rows = _locked_rows(cur, body.ids)
        _lock_collections(cur, [r["task_collection"] for r in rows])
        cur.execute(q.set_enabled_sql(), (body.enabled, body.ids))
        for r in rows:
            if bool(r["task_enabled"]) != body.enabled:
                _log(cur, _user(request), "enable" if body.enabled else "disable", r["id"], r["task_collection"],
                     "task_enabled", r["task_enabled"], body.enabled, batch=batch)
    return {"updated": len(rows)}


@app.post("/api/tables/reseed")
def reseed(body: Ids, request: Request):
    batch = uuid.uuid4().hex[:12]
    with db.transaction() as cur:
        rows = _locked_rows(cur, body.ids)
        _lock_collections(cur, [r["task_collection"] for r in rows])
        cur.execute(q.force_reseed_sql(), (body.ids,))
        n = cur.rowcount
        cur.execute(q.log_reseed_sql(), ("manual", body.ids))
        for r in rows:
            _log(cur, _user(request), "reseed", r["id"], r["task_collection"], reseed="manual", batch=batch)
    return {"updated": n}


@app.post("/api/tables")
def add_table(row: dict, request: Request):
    errs = q.validate_control_row(row)
    if errs:
        raise HTTPException(422, {"errors": errs})
    with db.transaction() as cur:
        _lock_collections(cur, [row.get("task_collection")])
        cur.execute(q.insert_control_sql(), q.control_params(row))
        new_id = cur.fetchone()[0]
        _refuse_mixed_sinks(cur, [row])
        _refuse_split_databases(cur, [row])
        _log(cur, _user(request), "add", new_id, row.get("task_collection"))
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
    try:
        with db.transaction() as cur:
            rows = _locked_rows(cur, body.ids)
            plans = q.plan_edit(rows, body.changes, bulk=bulk)
            summary = {"rows": len(plans), "changed": sum(1 for p in plans if p["changes"]),
                       "reseed": sum(1 for p in plans if p["reseed"]),
                       "errors": sum(1 for p in plans if p["errors"]), "applied": False}
            if body.dry_run or summary["errors"] or not summary["changed"]:
                cur.connection.rollback()
                return {"summary": summary, "plans": plans}
            new_coll = body.changes.get("task_collection")
            _lock_collections(cur, [r["task_collection"] for r in rows] + ([new_coll] if new_coll else []))
            _apply_plans(cur, rows, plans, _user(request), batch)
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
    active, recent = jobs.runs(jid, active_only=True, limit=50), jobs.runs(jid, active_only=False, limit=25)
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
    parallelism: int = Field(default=32, ge=1, le=256)


@app.post("/api/jobs/ingest/run")
def run_ingest(body: RunRequest):
    rid = jobs.run_now(_require(jobs.INGEST_JOB_ID, "ingestion"),
                       {"task_collection": body.task_collection, "parallelism": str(body.parallelism)})
    return {"run_id": rid}


@app.post("/api/jobs/runs/{run_id}/cancel")
def cancel_run(run_id: int):
    jobs.cancel(run_id)
    return {"cancelled": run_id}


@app.post("/api/jobs/setup/run")
def run_setup():
    return {"run_id": jobs.run_now(_require(jobs.SETUP_JOB_ID, "setup"), {})}


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
    did = uuid.uuid4().hex[:12]
    spec = {"src_type": "sqlserver", "src_host": body.src_host.strip(), "src_port": body.src_port or None,
            "databases": names, "secret_key_template": body.secret_key_template.strip(),
            "requested_by": _user(request)}
    rid = jobs.run_now(_require(jobs.DISCOVER_JOB_ID, "discovery"), {"discovery_id": did, "spec": json.dumps(spec)})
    return {"discovery_id": did, "run_id": rid, "databases": len(names)}


@app.get("/api/discoveries/runs/{run_id}")
def discovery_run(run_id: int):
    """State of a discovery job run, so the page can tell a run that ended without results from one
    still starting. Only runs of the discovery job."""
    st = jobs.run_state(run_id)
    if str(st["job_id"]) != str(jobs.DISCOVER_JOB_ID):
        raise HTTPException(404, f"run {run_id} is not a discovery run")
    return st


@app.get("/api/discoveries")
def discoveries(limit: int = 50):
    return db.query(q.discoveries_sql(), (int(limit),))


@app.get("/api/discoveries/{discovery_id}/databases")
def discovery_databases(discovery_id: str):
    return db.query(q.discovery_databases_sql(), (discovery_id,))


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
        batch = uuid.uuid4().hex[:12]
        with db.transaction() as cur:
            _lock_collections(cur, [body.task_collection])
            for name, row in valid:
                cur.execute(q.insert_control_if_absent_sql(), q.control_params(row))
                res = cur.fetchone()
                if res:
                    _log(cur, _user(request), "register", res[0], body.task_collection, batch=batch)
                results.append({"table": name, "status": "registered" if res else "skipped",
                                "id": res[0] if res else None,
                                "errors": [] if res else ["sink already used by another control row"]})
            _refuse_mixed_sinks(cur, [row for _, row in valid])     # all or nothing, like an edit
            _refuse_split_databases(cur, [row for _, row in valid])
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
        batch = uuid.uuid4().hex[:12]
        with db.transaction() as cur:
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
            _refuse_mixed_sinks(cur, [line["row"] for line in plan["lines"] if line["status"] == "imported"])
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


def _sync(sid: int, user: str) -> List[dict]:
    """Bring the schedule's wrapper jobs in line with Lakebase and record the results. Runs under the
    schedule's sync lock, with the members read after taking it, so concurrent syncs don't both create a
    job. A failed sync never raises once the schedule's change is committed: errors are per member."""
    batch = uuid.uuid4().hex[:12]
    with db.transaction() as cur:
        cur.execute(q.schedule_lock_sql(), (q.SCHEDULE_LOCK_CLASS, q.schedule_lock_key(sid)))
        s, members = _load_schedule(sid, cur)
        active = [m for m in members if m["removed_at"] is None]
        removed = [m for m in members if m["delete_pending"]]
        results = sch.sync(s, active, removed, jobs.schedule_client(), _ingest_job_id(s["job_variant"]),
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
    """Start times (ms) of a wrapper job's skipped runs since `since`; None if the Jobs API failed."""
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
    rows = db.query(q.schedules_sql())
    members = db.query(q.schedule_members_sql(), ([r["id"] for r in rows],)) if rows else []
    active = [m for m in members if m["removed_at"] is None]
    colls = sorted({m["task_collection"] for m in active})
    stats = {r["task_collection"]: r for r in
             (db.query(q.collection_run_stats_sql(), (colls, colls, colls, colls)) if colls else [])}
    skipped = _skipped_counts(active) if details else {}
    now = datetime.now(timezone.utc)
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
    _validate_schedule(body)
    user = _user(request)
    # A check schedule's one wrapper fires every member at once.
    body.stagger = body.stagger and body.job_variant != sch.CHECK_VARIANT
    offs = sch.offsets(body.members, body.cadence_minutes, body.stagger, sch.start_minute(body.model_dump()))
    batch = uuid.uuid4().hex[:12]
    try:
        with db.transaction() as cur:
            cur.execute(q.insert_schedule_sql(), (body.name.strip(), body.cadence_minutes, body.timezone,
                                                  json.dumps(body.parameters), body.job_variant or "default",
                                                  body.stagger, body.paused, body.notify_emails or None,
                                                  json.dumps(body.options) if body.options else None, user or None))
            sid = cur.fetchone()[0]
            for c in sorted(body.members):
                cur.execute(q.insert_member_sql(), (sid, c, offs[c]))
                _log(cur, user, "schedule_create", None, c, "schedule", None,
                     f"{body.name} ({sch.describe(body.cadence_minutes, offs[c], body.timezone)})", batch=batch)
    except psycopg.errors.UniqueViolation:
        raise HTTPException(409, f"a schedule named '{body.name}' already exists")
    return {"id": sid, "sync": _sync(sid, user)}


@app.post("/api/schedules/{sid}")
def update_schedule(sid: int, body: ScheduleBody, request: Request):
    """Edit a schedule: fields, and members added or removed. Offsets are recomputed; jobs are synced."""
    import psycopg
    _validate_schedule(body)
    user = _user(request)
    s, members = _load_schedule(sid)
    if s["deleted_at"] is not None:
        raise HTTPException(409, "schedule was deleted")
    body.stagger = body.stagger and body.job_variant != sch.CHECK_VARIANT
    new = body.model_dump()
    new["notify_emails"] = new["notify_emails"] or None
    new["job_variant"] = new["job_variant"] or "default"
    # A body without options (an older page) keeps the stored ones; an explicit {} clears them.
    new["options"] = (new["options"] or None) if "options" in body.model_fields_set else s.get("options")
    changed = [f for f in q.SCHEDULE_FIELDS if (s[f] or None) != (new[f] or None) and not
               (f in q.JSON_SCHEDULE_FIELDS and (s[f] or {}) == (new[f] or {}))]
    active = {m["task_collection"]: m for m in members if m["removed_at"] is None}
    added = sorted(set(body.members) - set(active))
    gone = sorted(set(active) - set(body.members))
    offs = sch.offsets(body.members, body.cadence_minutes, body.stagger, sch.start_minute(new))
    batch = uuid.uuid4().hex[:12]
    try:
        with db.transaction() as cur:
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
    except psycopg.errors.UniqueViolation:
        raise HTTPException(409, f"a schedule named '{body.name}' already exists")
    return {"id": sid, "changed": changed, "added": added, "removed": gone, "sync": _sync(sid, user)}


def _set_paused(sid: int, paused: bool, request: Request):
    user = _user(request)
    s, _ = _load_schedule(sid)
    if s["deleted_at"] is not None:
        raise HTTPException(409, "schedule was deleted")
    if bool(s["paused"]) != paused:
        with db.transaction() as cur:
            cur.execute(q.update_schedule_sql(["paused"]), (paused, int(sid)))
            _log(cur, user, "schedule_pause" if paused else "schedule_resume", None, None, f"{s['name']}.paused",
                 s["paused"], paused)
    return {"id": sid, "paused": paused, "sync": _sync(sid, user)}


@app.post("/api/schedules/{sid}/pause")
def pause_schedule(sid: int, request: Request):
    return _set_paused(sid, True, request)


@app.post("/api/schedules/{sid}/resume")
def resume_schedule(sid: int, request: Request):
    return _set_paused(sid, False, request)


@app.post("/api/schedules/{sid}/delete")
def delete_schedule(sid: int, request: Request):
    """Delete a schedule (an ingestion job) and its wrapper jobs; the schedule row is kept, marked deleted. Its groups
    move to Not in any job, and their tables, control rows, checkpoints and run history all stay."""
    user = _user(request)
    s, members = _load_schedule(sid)
    refusal = _permission_refusals(s, user)["delete"]
    if refusal:
        raise HTTPException(403, refusal)
    active = [m["id"] for m in members if m["removed_at"] is None]
    with db.transaction() as cur:
        cur.execute(q.delete_schedule_sql(), (int(sid),))
        if active:
            cur.execute(q.remove_members_sql(), (active,))   # their jobs are deleted by the sync below
        _log(cur, user, "schedule_delete", None, None, s["name"], None, None)
    return {"id": sid, "deleted": True, "sync": _sync(sid, user)}


@app.post("/api/schedules/{sid}/sync")
def sync_schedule(sid: int, request: Request):
    """Re-sync the wrapper jobs (recreates missing ones and undoes edits made outside the app)."""
    return {"id": sid, "sync": _sync(sid, _user(request))}


@app.get("/api/schedules/{sid}/check")
def check_schedule(sid: int):
    """Drift report for the schedule's wrapper jobs; changes nothing."""
    s, members = _load_schedule(sid)
    active = [m for m in members if m["removed_at"] is None]
    return sch.check(s, active, jobs.schedule_client(), _variant_job_id(s["job_variant"]), _owner())


@app.get("/api/timeline")
def timeline(hours: int = 24):
    """Runs per collection in the last `hours`, planned triggers of scheduled collections, and skipped
    triggers (from the wrapper jobs' runs)."""
    hours = max(1, min(int(hours), 168))
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=hours)
    runs = db.query(q.timeline_runs_sql(), (end, start))
    scheds = db.query(q.schedules_sql())
    members = db.query(q.schedule_members_sql(), ([r["id"] for r in scheds],)) if scheds else []
    by_id = {r["id"]: r for r in scheds}
    rows = {}
    for r in runs:
        rows.setdefault(r["task_collection"], {"task_collection": r["task_collection"], "runs": [], "planned": [],
                                               "skipped": [], "schedules": []})["runs"].append(r)
    client = None
    for m in members:
        if m["removed_at"] is not None:
            continue
        s = by_id[m["schedule_id"]]
        row = rows.setdefault(m["task_collection"], {"task_collection": m["task_collection"], "runs": [],
                                                     "planned": [], "skipped": [], "schedules": []})
        row["schedules"].append(f"{s['name']} ({sch.describe(int(s['cadence_minutes']), int(m['offset_minutes']), s['timezone'])})")
        if not s["paused"]:
            # The current schedule's triggers, from when the collection joined or the schedule last changed.
            since = max([start] + [datetime.fromisoformat(v) for v in (m.get("added_at"), s.get("updated_at")) if v])
            row["planned"] += [t.isoformat() for t in
                               sch.fire_times(int(s["cadence_minutes"]), int(m["offset_minutes"]), s["timezone"], since, end)]
        if m.get("job_id"):
            client = client or jobs.schedule_client()
            row["skipped"] += [jobs.ts(t) for t in (_skipped_starts(m["job_id"], start, end, client) or [])]
    return {"start": start.isoformat(), "end": end.isoformat(),
            "collections": [rows[k] for k in sorted(rows)]}


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
def connections():
    """UC connections with what lakefed tracks on each (control rows naming the connection or its host).
    A listing error (no rights on any connection, for example) is returned, not raised, so the page
    still offers discovery by host and secret."""
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("DATABRICKS_APP_PORT", "8000")))
