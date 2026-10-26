"""Ingestion jobs (schedules) and their job runs, as the Overview, the Ingestion jobs list, Runs and the job
pages show them. Pure: main.py reads the schedules, the shared table states (rollup), the running pass and the
group runs of a window, and this module places and sums them, so it is tested without a database.

A job run is one trigger of a schedule (schedules.job_runs places group runs by cadence slot). Placement is
by the schedule as it is now, so it is exact for scheduled runs; a run started by hand inside a slot is an
extra run, and a cluster start slower than the cadence can land a run in the next slot."""
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

import queries as q
import rollup
import schedules as sch

# How many of a job's latest job runs the Overview places: enough to say "in each of the last N job runs".
RECENT_JOB_RUNS = 6


def _when(value):
    return datetime.fromisoformat(value) if isinstance(value, str) else value


def window(cadence: int, slots: int, end: datetime) -> datetime:
    """The start of a window holding a job's last `slots` triggers before `end`, at most 7 days long."""
    return end - min(timedelta(minutes=cadence * slots), timedelta(days=7))


def place(job: dict, members: List[dict], runs: List[dict], start: datetime, end: datetime, now: datetime):
    """The job's job runs whose slot starts in [start, end), newest first (schedules.job_runs), with their
    members' runs. Triggers are placed from one slot earlier, so a window starting mid-slot keeps every
    member of its first job run (a member's offset is under a cadence, and under an hour past hourly)."""
    mine = [m for m in members if m["schedule_id"] == job["id"]]
    groups = {m["task_collection"] for m in mine}
    cadence = int(job["cadence_minutes"])
    placed = sch.job_runs(cadence, job.get("timezone") or "UTC", mine, [r for r in runs if r["task_collection"] in groups],
                          start - timedelta(minutes=max(cadence, 60)), end, now)
    return [jr for jr in placed if jr["slot"] >= start]


def current_and_past(job: dict, members: List[dict], runs: List[dict], start: datetime, now: datetime) -> List[dict]:
    """The job runs whose slot started in [start, now], newest first. Placed through the end of the current
    slot, so its members whose start is still ahead are in it as waiting."""
    cadence = int(job["cadence_minutes"])
    return [jr for jr in place(job, members, runs, start, now + timedelta(minutes=cadence), now) if jr["slot"] <= now]


def _progress(member: dict, running_by_run: dict, group_tables: int) -> dict:
    """One member's part of its job run's progress bar, in the shape of RunDetail's Progress: a running
    group run from the running pass, a finished one from its totals (its ok counts its unchanged tables),
    and one not started yet as all to go."""
    run = member.get("run")
    if run and run.get("status") == "running":
        p = running_by_run.get(run.get("run_id")) or {}
        loaded = q._count(p, "tables_ok")
        return {"total": q._count(p, "enabled_tables") or group_tables, "loaded": loaded,
                "unchanged": max(q._count(p, "tables_checked") - loaded, 0),
                "in_flight": q._count(p, "tables_running"), "failed": q._count(p, "tables_failed")}
    if run and run.get("status") != "skipped" and run.get("total") is not None:
        unchanged = q._count(run, "skipped")
        return {"total": q._count(run, "total"), "unchanged": unchanged,
                "loaded": max(q._count(run, "ok") - unchanged, 0), "in_flight": 0, "failed": q._count(run, "failed")}
    return {"total": group_tables, "unchanged": 0, "loaded": 0, "in_flight": 0, "failed": 0}


def _sum(parts: Iterable[dict]) -> dict:
    out = {"total": 0, "unchanged": 0, "loaded": 0, "in_flight": 0, "failed": 0}
    for p in parts:
        for k in out:
            out[k] += p[k]
    return out


def summarize(job: dict, job_run: dict, running_by_run: dict, group_tables: Dict[str, int],
              p50_by_group: Dict[str, Optional[float]], now: datetime) -> dict:
    """One job run as its rows and pages show it: outcome, group runs (ran, of members), took (first group
    run start to last finish), tables loaded, speed over its work span (first table start to last finish
    across its group runs), rows written, parallelism, clusters, failed tables, its progress bar over all
    the job's tables, and while running its speed now, expected end, running for and typical run time.
    Group runs come with their work (main._with_rows_written)."""
    members = job_run["members"]
    runs = [m["run"] for m in members if m.get("run")] + [r for m in members for r in m.get("extra") or []]
    ran = [r for r in runs if r.get("status") != "skipped"]
    outcome = q.job_run_outcome(members)
    if outcome["label"] == "Not run" and job.get("paused"):
        outcome = dict(outcome, note="the job is paused")
    starts = [_when(r["started_at"]) for r in ran if r.get("started_at")]
    finishes = [_when(r["finished_at"]) for r in ran if r.get("finished_at")]
    work_starts = [_when(r["work_started_at"]) for r in ran if r.get("work_started_at")]
    work_ends = [_when(r["work_finished_at"]) for r in ran if r.get("work_finished_at")]
    loaded = sum(q.loaded_tables(r) for r in ran)
    running = outcome["label"] == "Running"
    speed = None
    if work_starts and work_ends and loaded and not running:
        speed = round(loaded / (max((max(work_ends) - min(work_starts)).total_seconds(), 1) / 60), 1)
    progress = _sum(_progress(m, running_by_run, group_tables.get(m["task_collection"], 0)) for m in members)
    out = {
        "slot": job_run["slot"].isoformat(), "job_id": job["id"], "job_name": job["name"], "outcome": outcome,
        "members": len(members), "ran": len(ran), "extra": sum(len(m.get("extra") or []) for m in members),
        "started_at": min(starts).isoformat() if starts else None,
        "took_s": (round((max(finishes) - min(starts)).total_seconds())
                   if starts and finishes and not running and outcome["label"] != "Ended without finishing" else None),
        "loaded": loaded, "speed_tables_per_min": speed,
        "rows_written": sum(q._count(r, "rows_written") for r in ran),
        "rows_partial": any(r.get("rows_partial") for r in ran),
        "parallelism": sorted({int(r["parallelism"]) for r in ran if r.get("parallelism") is not None}),
        "clusters": len({r["cluster_id"] for r in ran if r.get("cluster_id")}),
        "failed_tables": sum(q._count(r, "failed") for r in ran if r.get("status") != "running")
        + sum(q._count(running_by_run.get(r.get("run_id")) or {}, "tables_failed") for r in ran
              if r.get("status") == "running"),
        "run_ids": [r["run_id"] for r in runs if r.get("run_id") is not None],
        "progress": progress,
    }
    if running:
        live = [q.live_speed(running_by_run[r["run_id"]], now) for r in ran
                if r.get("status") == "running" and r.get("run_id") in running_by_run]
        live = [s for s in live if s]
        end = q.job_run_expected_end(members, running_by_run, p50_by_group, now)
        typical = [m["offset_minutes"] * 60 + p50_by_group[m["task_collection"]] for m in members
                   if p50_by_group.get(m["task_collection"]) is not None]
        out.update({
            "speed_now": ({"tables_per_min": round(sum(s["tables_per_min"] for s in live), 1),
                           "rows_per_min": round(sum(s["rows_per_min"] for s in live), 1)} if live else None),
            "expected_end": end.isoformat() if end else None,
            "running_s": round((now - min(starts)).total_seconds()) if starts else None,
            "typical_s": round(max(typical)) if typical else None,
            "waiting": sum(1 for m in members if m["state"] == "waiting"),
            "running_group_runs": sum(1 for r in ran if r.get("status") == "running"),
        })
    return out


def member_views(job_run: dict, running_by_run: dict, group_tables: Dict[str, int], now: datetime) -> List[dict]:
    """A job run's group runs, one row per member: running first, then waiting, then finished. Each with
    its state, start or planned start, took or running for, progress, speed, parallelism, cluster and
    failed tables; extra runs follow their member."""
    rows = []
    for m in job_run["members"]:
        for i, run in enumerate([m.get("run")] + list(m.get("extra") or [])):
            if i and not run:
                continue
            p = running_by_run.get((run or {}).get("run_id")) if run else None
            if run:
                status = q.run_outcome(run, p if run.get("status") == "running" else None)
            elif m["state"] == "waiting":
                status = {"label": f"Waiting, starts {_when(m['fire_at']).isoformat()}", "tone": "", "note": None}
            elif m["state"] == "skipped":
                status = {"label": "Skipped: still running", "tone": "neutral", "note": "the previous group run was still going"}
            else:
                status = {"label": "No run", "tone": "neutral", "note": None}
            live = run and run.get("status") == "running"
            rows.append({
                "task_collection": m["task_collection"], "offset_minutes": m["offset_minutes"],
                "fire_at": _when(m["fire_at"]).isoformat(), "state": m["state"], "extra": bool(i),
                "status": status, "run_id": (run or {}).get("run_id"), "started_at": (run or {}).get("started_at"),
                "duration_s": (run or {}).get("duration_s"),
                "progress": _progress(dict(m, run=run), running_by_run, group_tables.get(m["task_collection"], 0)),
                "speed_tables_per_min": (q.live_speed(p, now) or {}).get("tables_per_min") if live and p
                else (run or {}).get("speed_tables_per_min"),
                "parallelism": (run or {}).get("parallelism"), "cluster_id": (run or {}).get("cluster_id"),
                "failed": q._count(p or {}, "tables_failed") if live else q._count(run or {}, "failed"),
            })
    return sorted(rows, key=lambda r: (0 if r["status"]["tone"] == "run" else 1 if r["state"] == "waiting" else 2,
                                       r["offset_minutes"], r["task_collection"], r["extra"]))


def status(job: dict, running: Optional[dict], last: Optional[dict], top_cause: Optional[str],
           warnings: List[str]) -> dict:
    """A job's Status cell: its running state and its issues together. Running (with failing tables, the
    progress, expected end and top cause), else failing (paused or not), late while paused, late, paused,
    ended without finishing, or current. A paused job whose tables are only late shows late, not failing."""
    failing, late = q._count(job, "state_failing"), q._count(job, "state_late")
    if running:
        p = running["progress"]
        checked = p["unchanged"] + p["loaded"]
        parts = [f"{100 * checked // p['total'] if p['total'] else 0}% checked"]
        if failing:
            parts.append(f"{failing:,} {'table' if failing == 1 else 'tables'} failing")
        return {"label": "Running", "tone": "run", "detail": parts, "expected_end": running.get("expected_end"),
                "cause": top_cause if failing else None}
    paused = {"detail": ["paused"], "paused_since": job.get("updated_at")} if job.get("paused") else {"detail": []}
    if failing:
        return {"label": f"Failing: {failing:,} {'table' if failing == 1 else 'tables'}", "tone": "bad", **paused,
                "cause": top_cause}
    if job.get("paused") and late:
        return {"label": f"Late: {late:,} {'table' if late == 1 else 'tables'}", "tone": "warn", **paused, "cause": None}
    if late:
        return {"label": f"Late: {late:,} {'table' if late == 1 else 'tables'}", "tone": "warn",
                "detail": warnings[:1], "cause": None}
    if job.get("paused"):
        return {"label": "Paused", "tone": "neutral", "detail": [], "cause": None}
    if last and last["outcome"]["label"] == "Ended without finishing":
        return {"label": "Ended without finishing", "tone": "bad", "detail": [], "cause": None}
    return {"label": "Current", "tone": "ok", "detail": [], "cause": None}


def consecutive_failed(summaries: List[dict]) -> int:
    """How many of a job's latest finished job runs in a row had failed tables (newest first)."""
    count = 0
    for s in summaries:
        if s["outcome"]["label"] == "Running":
            continue
        if not s["failed_tables"]:
            break
        count += 1
    return count


def jobs_view(schedules: List[dict], members: List[dict], states: List[dict], running: List[dict],
              runs: List[dict], stats_by_group: Dict[str, dict], now: datetime,
              causes_by_job: Optional[Dict[int, List[dict]]] = None) -> dict:
    """Every ingestion job as a row (sums, cadence, next start, last finished job run, the running one, and
    Status), "Not in any job", the running job runs, and the jobs that need attention. `runs` are the group
    runs of the last RECENT_JOB_RUNS triggers of every job, with their work; `stats_by_group` each group's
    run-time percentiles and strictest freshness target (queries.collection_run_stats_sql); `causes_by_job`
    the grouped failures (queries.group_errors) of each job's last job run with failed tables. Running group
    runs of groups in no job are listed apart, as they belong to no job run."""
    causes_by_job = causes_by_job or {}
    p50_by_group = {g: st.get("p50_s") for g, st in stats_by_group.items()}
    job_sums, not_in_job = rollup.jobs(states, schedules, members)
    groups = rollup.groups(states)
    group_tables = {name: g["enabled_tables"] for name, g in groups.items()}
    running_by_run = {r["run_id"]: r for r in running}
    active = [m for m in members if m.get("removed_at") is None]
    rows, running_job_runs = [], []
    for s, sums in zip(schedules, job_sums):
        cadence = int(s["cadence_minutes"])
        start = window(cadence, RECENT_JOB_RUNS, now)
        member_groups = {m["task_collection"] for m in active if m["schedule_id"] == s["id"]}
        oldest_running = [_when(r["started_at"]) for r in running if r["task_collection"] in member_groups and r.get("started_at")]
        if oldest_running:     # a group run overrunning the window still belongs to its job run
            start = min(start, min(oldest_running) - timedelta(minutes=cadence))
        placed = current_and_past(s, active, runs, start, now)
        summaries = [summarize(s, jr, running_by_run, group_tables, p50_by_group, now) for jr in placed]
        current = next((x for x in summaries if x["outcome"]["label"] == "Running"), None)
        last = next((x for x in summaries if x["outcome"]["label"] != "Running"), None)
        mine = [m for m in active if m["schedule_id"] == s["id"]]
        causes = causes_by_job.get(s["id"]) or []
        warnings = [w for m in mine for w in sch.warnings(cadence, (stats_by_group.get(m["task_collection"]) or {}).get("p95_s"),
                                                           (stats_by_group.get(m["task_collection"]) or {}).get("min_sla_minutes"))]
        next_starts = [sch.next_run(cadence, int(m["offset_minutes"]), s.get("timezone") or "UTC", now) for m in mine]
        row = {**sums, "description": sch.describe(cadence, 0, s.get("timezone") or "UTC"),
               "next_start": min(next_starts).isoformat() if next_starts and not s.get("paused") else None,
               "last_job_run": last, "running_job_run": current,
               "failed_in_a_row": consecutive_failed(summaries), "causes": causes[:3]}
        row["status"] = status(row, current, last, causes[0]["label"] if causes else None, warnings)
        rows.append(row)
        running_job_runs += [x for x in summaries if x["outcome"]["label"] == "Running"]
    rows.sort(key=lambda j: (-1 if j["running_job_run"] and j["state_failing"] else 0 if j["state_failing"] else
                             1 if j["state_late"] else 2, str(j["name"]).lower()))
    attention = [j for j in rows if j["state_failing"]
                 or (j["last_job_run"] and j["last_job_run"]["outcome"]["label"] == "Ended without finishing")]
    attention.sort(key=lambda j: (-q._count(j, "state_failing"), str(j["name"]).lower()))
    running_job_runs.sort(key=lambda r: r.get("started_at") or r["slot"])
    totals = {**{k: sum(q._count(g, k) for g in groups.values()) for k in ("enabled_tables", "no_sla_tables") + rollup.STATES},
              "groups": len(groups), "databases": len({(s["server"], s["src_database"]) for s in states
                                                        if q._count(s, "tables")}),
              "servers": len({s["server"] for s in states if q._count(s, "tables")}),
              "jobs": len(rows), "running_jobs": sum(1 for j in rows if j["running_job_run"]),
              "failing_jobs": sum(1 for j in rows if j["state_failing"]),
              "late_jobs": sum(1 for j in rows if j["state_late"])}
    unscheduled = set((not_in_job or {}).get("groups") or [])
    running_not_in_job = [dict(r, outcome=q.run_outcome({"status": "running"}, r)) for r in running
                          if r["task_collection"] in unscheduled]
    return {"jobs": rows, "not_in_job": not_in_job, "running_job_runs": running_job_runs,
            "running_not_in_job": running_not_in_job, "attention": attention, "totals": totals}


def job_runs_of_run(run: dict, schedules: List[dict], members: List[dict]) -> List[dict]:
    """The job runs a group run belongs to, one per active schedule its group is in: the slot of its
    member's latest trigger at or before the run's start."""
    started = _when(run.get("started_at"))
    out = []
    if started is None:
        return out
    for s in schedules:
        for m in members:
            if m["schedule_id"] != s["id"] or m["task_collection"] != run["task_collection"] or m.get("removed_at"):
                continue
            cadence, offset, tz = int(s["cadence_minutes"]), int(m["offset_minutes"]), s.get("timezone") or "UTC"
            fires = sch.fire_times(cadence, offset, tz, started - timedelta(minutes=cadence + 1), started + timedelta(seconds=1))
            if fires:
                out.append({"job_id": s["id"], "job_name": s["name"], "slot": sch.slot_of(fires[-1], offset, tz).isoformat()})
    return out
