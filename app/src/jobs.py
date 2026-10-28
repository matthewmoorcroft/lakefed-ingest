"""Databricks Jobs operations for the app (the app's service principal holds CAN_MANAGE_RUN on the
ingestion, setup and discovery jobs through the app's `job` resources: view, run, and cancel; not edit)."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import List, Optional

from lakebase import workspace

INGEST_JOB_ID = os.environ.get("INGEST_JOB_ID")
SETUP_JOB_ID = os.environ.get("SETUP_JOB_ID")
DISCOVER_JOB_ID = os.environ.get("DISCOVER_JOB_ID")
# The check job (lakefed_ingest_check): a schedule of the check variant starts it with all its members.
CHECK_JOB_ID = os.environ.get("CHECK_JOB_ID")
# Group given CAN_VIEW on the wrapper jobs the app creates for schedules (blank = owner and admins only).
SCHEDULE_JOB_VIEW_GROUP = os.environ.get("SCHEDULE_JOB_VIEW_GROUP", "users") or None


def ingest_variants() -> dict:
    """Ingestion jobs a schedule can start: 'default' (INGEST_JOB_ID), plus INGEST_JOB_ID_<NAME> variants
    (e.g. a larger cluster). The app needs CAN_MANAGE_RUN on each (an app `job` resource)."""
    out = {"default": INGEST_JOB_ID} if INGEST_JOB_ID else {}
    for k, v in os.environ.items():
        if k.startswith("INGEST_JOB_ID_") and v:
            out[k[len("INGEST_JOB_ID_"):].lower()] = v
    return out


def schedule_variants() -> dict:
    """What a schedule's wrappers can start: the ingestion job variants, plus ``check`` (one wrapper for the
    whole schedule, starting the check job with every member) when CHECK_JOB_ID is set."""
    import schedules
    out = ingest_variants()
    if CHECK_JOB_ID:
        out[schedules.CHECK_VARIANT] = CHECK_JOB_ID
    return out


def schedule_client():
    """Jobs API client for schedule sync and the wrapper jobs' runs."""
    import schedules
    return schedules.JobsClient(workspace())


def ts(ms) -> Optional[str]:
    """Epoch milliseconds (as the Jobs API reports times) to ISO 8601 UTC."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat() if ms else None


def workspace_url() -> str:
    """The workspace's address, for links out of the app to a run or a cluster; empty when it can't be read."""
    try:
        return workspace().config.host.rstrip("/")
    except Exception:
        return ""


def job_summary(job_id) -> dict:
    """Name, schedule and default parameters of a job."""
    j = workspace().jobs.get(int(job_id))
    s = j.settings
    sched = s.schedule
    params = {p.name: p.default for p in (s.parameters or [])}
    return {
        "job_id": j.job_id,
        "name": s.name,
        "schedule": sched.quartz_cron_expression if sched else None,
        "timezone": sched.timezone_id if sched else None,
        "schedule_status": (sched.pause_status.value if sched and sched.pause_status else None),
        "default_task_collection": params.get("task_collection"),
        "parameters": params,
        "url": f"{workspace_url()}/jobs/{j.job_id}",
    }


# Every action on an existing job follows Databricks' own job permissions for the signed-in user: the levels in
# Databricks' order, what each action needs (as the Jobs UI asks: run from Can Manage Run, change settings or the
# schedule and delete from Can Manage), and how a level reads on the page.
PERMISSION_RANK = {"CAN_VIEW": 1, "CAN_MANAGE_RUN": 2, "CAN_MANAGE": 3, "IS_OWNER": 3}
ACTION_NEEDS = {"run": ("starting a run", "CAN_MANAGE_RUN"), "change": ("changing the job or its tables", "CAN_MANAGE"),
                "delete": ("deleting", "CAN_MANAGE")}
PERMISSION_LABEL = {"CAN_VIEW": "Can View", "CAN_MANAGE_RUN": "Can Manage Run", "CAN_MANAGE": "Can Manage",
                    "IS_OWNER": "Is Owner"}


def highest_permission(acl: List[dict], user: str, groups: List[str]) -> Optional[str]:
    """The user's highest level on a job from its access list (Permissions API entries as dicts), counting entries
    for the user and for any of their groups; a workspace admin manages every job."""
    if "admins" in groups:
        return "CAN_MANAGE"
    email, member_of = (user or "").strip().lower(), set(groups) | {"users"}
    levels = [p["permission_level"] for entry in acl
              if (entry.get("user_name") or "").lower() == email or entry.get("group_name") in member_of
              for p in entry.get("all_permissions") or []]
    return max(levels, key=lambda level: PERMISSION_RANK.get(level, 0), default=None)


def action_refusal(action: str, level: Optional[str], job_label: str) -> Optional[str]:
    """Why a user with this level on the job may not take the action, or None when they may."""
    doing, needs = ACTION_NEEDS[action]
    if PERMISSION_RANK.get(level or "", 0) >= PERMISSION_RANK[needs]:
        return None
    have = f"you have {PERMISSION_LABEL.get(level, level)}" if level else "you have no access to it"
    return f"{doing} needs {PERMISSION_LABEL[needs]} on {job_label}; {have}"


# A page asks for every button's permission at once and each action checks again, so reads are kept briefly: a grant
# or revoke in Databricks shows here within this many seconds.
PERMISSION_CACHE_S = 20
_permission_cache: dict = {}


def _cached(key, read):
    now = time.monotonic()
    hit = _permission_cache.get(key)
    if hit and now - hit[0] < PERMISSION_CACHE_S:
        return hit[1]
    value = read()
    _permission_cache[key] = (now, value)
    return value


def user_groups(user: str) -> List[str]:
    """The workspace groups of a user, by the email the Apps proxy forwards."""
    def read():
        found = workspace().users.list(filter=f'userName eq "{user}"', attributes="groups")
        return [g.display for u in found for g in (u.groups or []) if g.display]
    return _cached(("groups", user), read)


def user_job_permission(job_id, user: str) -> Optional[str]:
    """The signed-in user's highest permission on a Databricks job, read by the app's service principal (which
    needs Can Manage on the job to read its access list)."""
    def read():
        acl = workspace().permissions.get("jobs", str(int(job_id))).access_control_list or []
        return [entry.as_dict() for entry in acl]
    return highest_permission(_cached(("acl", str(int(job_id))), read), user, user_groups(user))


def job_settings(job_id) -> dict:
    """A job's settings as the Jobs API returns them (a plain dict), for compute.describe."""
    return workspace().jobs.get(int(job_id)).settings.as_dict()


def runs(job_id, active_only: bool = False, limit: int = 25) -> List[dict]:
    rows = []
    # The Jobs API caps a page at 25 runs; the SDK pages through, and we stop at `limit`.
    for r in workspace().jobs.list_runs(job_id=int(job_id), active_only=active_only, limit=min(limit, 25)):
        params = {p.name: p.value for p in (getattr(r, "job_parameters", None) or [])}
        st = r.state
        rows.append({
            "run_id": r.run_id,
            "task_collection": params.get("task_collection"),
            "state": st.life_cycle_state.value if st and st.life_cycle_state else None,
            "result": st.result_state.value if st and st.result_state else None,
            "started": ts(r.start_time),
            "ended": ts(r.end_time),
            "trigger": r.trigger.value if r.trigger else None,
            "url": r.run_page_url,
        })
        if len(rows) >= limit:
            break
    return rows


def run_now(job_id, job_parameters: dict) -> int:
    waiter = workspace().jobs.run_now(job_id=int(job_id), job_parameters=job_parameters)
    resp = getattr(waiter, "response", waiter)
    return int(resp.run_id)


def run_state(run_id) -> dict:
    """A run's state: life cycle, result, the state message and its page."""
    r = workspace().jobs.get_run(int(run_id))
    st = r.state
    return {"run_id": r.run_id, "job_id": r.job_id,
            "state": st.life_cycle_state.value if st and st.life_cycle_state else None,
            "result": st.result_state.value if st and st.result_state else None,
            "message": st.state_message if st else None, "url": r.run_page_url}


def cancel(run_id) -> None:
    workspace().jobs.cancel_run(run_id=int(run_id))
