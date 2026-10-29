"""Schedule endpoints (main.py) with a fake Lakebase and a fake Jobs API: create, edit, pause, delete."""
import contextlib
from datetime import datetime, timezone

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
import queries as q  # noqa: E402
import schedules as sch  # noqa: E402

from schedules_test import FakeJobs  # noqa: E402


class Store:
    """A tiny in-memory stand-in for the schedule tables, driven by the SQL main.py runs."""

    def __init__(self):
        self.schedules, self.members, self.log, self.unique, self.locks = {}, {}, [], False, []
        self.with_tables = {"c1", "c2", "c3"}           # groups with control rows

    def now(self):
        return datetime.now(timezone.utc).isoformat()

    def run(self, sql, params):
        if sql in (q.touch_jobs_sql(), q.touch_group_jobs_sql()):
            import json
            user, keys = params
            ids = keys if sql == q.touch_jobs_sql() else {m["schedule_id"] for m in self.members.values()
                                                          if m["task_collection"] in keys and m["removed_at"] is None}
            for sid in ids:
                row = self.schedules[sid]
                row.update(updated_at=self.now(), options=json.dumps({**sch.options_of(row), "updated_by": user}))
            return []
        if sql in (q.set_schedule_option_sql(), q.clear_schedule_option_sql()):
            import json
            row = self.schedules[params[-1]]
            options = sch.options_of(row)
            if sql == q.set_schedule_option_sql():
                options[params[0][0]] = params[1]
            else:
                options.pop(params[0], None)
            row["options"] = json.dumps(options)
            return []
        if sql.startswith(f"insert into {q.SCHEMA}.schedule ("):
            if self.unique:
                import psycopg
                raise psycopg.errors.UniqueViolation("dup")
            sid = len(self.schedules) + 1
            cols = q.SCHEDULE_FIELDS + ("created_by",)
            row = dict(zip(cols, params), id=sid, deleted_at=None, created_at=self.now(), updated_at=self.now())
            import json
            row["parameters"] = json.loads(row["parameters"])
            self.schedules[sid] = row
            return [(sid,)]
        if sql.startswith(f"insert into {q.SCHEMA}.schedule_member"):
            mid = len(self.members) + 1
            self.members[mid] = dict(id=mid, schedule_id=params[0], task_collection=params[1], offset_minutes=params[2],
                                     job_id=None, synced_at=None, sync_error=None, added_at=self.now(), removed_at=None)
            return [(mid,)]
        if sql.startswith(f"update {q.SCHEMA}.schedule set deleted_at"):
            self.schedules[params[0]]["deleted_at"] = self.now()
        elif sql.startswith(f"update {q.SCHEMA}.schedule set"):
            import json
            fields = [p.split(" = ")[0] for p in sql.split(" set ")[1].split(", updated_at")[0].split(", ")]
            for f, v in zip(fields, params[:-1]):
                self.schedules[params[-1]][f] = json.loads(v) if f == "parameters" else v
        elif sql.startswith(f"update {q.SCHEMA}.schedule_member set removed_at"):
            for mid in params[0]:
                self.members[mid]["removed_at"] = self.now()
        elif sql.startswith(f"update {q.SCHEMA}.schedule_member set offset_minutes"):
            self.members[params[1]]["offset_minutes"] = params[0]
        elif sql == q.member_synced_sql():
            self.members[params[1]].update(job_id=params[0], sync_error=None, synced_at=self.now())
        elif sql == q.member_sync_failed_sql():
            self.members[params[1]]["sync_error"] = params[0]
        elif sql == q.schedule_lock_sql():
            self.locks.append(params)
        elif "control_change" in sql:
            self.log.append(params)
        return []

    def query(self, sql, params=()):
        if "from lakefed_ingest.control" in sql and "distinct task_collection" in sql:
            return [{"task_collection": c} for c in sorted(self.with_tables)]
        if sql == q.schedules_sql():
            return [dict(s) for s in self.schedules.values() if s["deleted_at"] is None]
        if sql == q.schedule_sql():
            s = self.schedules.get(params[0])
            return [dict(s)] if s else []
        if sql == q.schedule_members_sql():
            pending = lambda m: bool(m["removed_at"] and m["job_id"] and (m["synced_at"] is None or m["synced_at"] < m["removed_at"]))
            return [dict(m, delete_pending=pending(m)) for m in self.members.values() if m["schedule_id"] in params[0]]
        return []


class Cur:
    def __init__(self, store):
        self.store, self._rows = store, []

    def execute(self, sql, params=()):
        self._rows = self.store.run(sql, params)

    def fetchone(self):
        return self._rows[0] if self._rows else None


@pytest.fixture
def api(monkeypatch):
    store, jobs_api = Store(), FakeJobs()
    monkeypatch.setattr(main.db, "query", store.query)
    monkeypatch.setattr(main.db, "fetch", lambda cur, sql, params=(): store.query(sql, params))
    monkeypatch.setattr(main.db, "transaction", contextlib.contextmanager(lambda: (yield Cur(store))))
    monkeypatch.setattr(main.db, "workspace", lambda: type("W", (), {"config": type("C", (), {"host": "https://h"})()})())
    monkeypatch.setattr(main.jobs, "schedule_client", lambda: jobs_api)
    monkeypatch.setattr(main.jobs, "ingest_variants", lambda: {"default": "555"})
    # Run now and Delete ask Databricks for the user's permission: the signed-in operator manages the job here.
    monkeypatch.setattr(main.jobs, "user_job_permission", lambda job_id, user: "CAN_MANAGE")
    monkeypatch.setattr(main.jobs, "SCHEDULE_JOB_VIEW_GROUP", None)
    monkeypatch.setattr(q, "collections_sql", lambda: "select distinct task_collection from lakefed_ingest.control")
    c = TestClient(main.app, headers={"X-Forwarded-Email": "op@example.com"})
    c.store, c.jobs = store, jobs_api
    return c


BODY = dict(name="Prod 15", cadence_minutes=15, timezone="UTC", parameters={"parallelism": "16"},
            stagger=True, paused=False, members=["c1", "c2"])


def test_create_writes_rows_creates_one_job_per_member_and_logs(api):
    r = api.post("/api/schedules", json=BODY)
    assert r.status_code == 200, r.text
    assert sorted(x["action"] for x in r.json()["sync"]) == ["created", "created"]
    members = {m["task_collection"]: m for m in api.store.members.values()}
    assert {c: m["offset_minutes"] for c, m in members.items()} == {"c1": 0, "c2": 7}
    assert all(m["job_id"] in api.jobs.jobs for m in members.values())
    job = api.jobs.jobs[members["c2"]["job_id"]]
    assert job["schedule"]["quartz_cron_expression"] == "0 7/15 * * * ?"
    assert job["tasks"][0]["run_job_task"] == {"job_id": 555, "job_parameters": {"task_collection": "c2", "parallelism": "16"}}
    actions = [p[1] for p in api.store.log]
    assert actions.count("schedule_create") == 2 and actions.count("schedule_sync") == 2

    listed = api.get("/api/schedules").json()
    assert listed[0]["name"] == "Prod 15" and len(listed[0]["members"]) == 2
    m = {x["task_collection"]: x for x in listed[0]["members"]}
    assert m["c2"]["when"] == "Every 15 minutes, starting at 7 minutes past the hour (UTC)" and m["c2"]["next_run"] and m["c2"]["job_url"].startswith("https://h/jobs/")


def test_validation_and_duplicate_name(api):
    r = api.post("/api/schedules", json=dict(BODY, cadence_minutes=7, members=["c1", "nope"]))
    assert r.status_code == 422 and "cadence_minutes" in r.text and "unknown ingestion groups nope" in r.text
    api.store.unique = True
    assert api.post("/api/schedules", json=BODY).status_code == 409


def test_edit_adds_and_removes_members_restaggers_and_resets_jobs(api):
    sid = api.post("/api/schedules", json=BODY).json()["id"]
    api.store.with_tables.discard("c1")      # its tables have moved on, so it may leave the job
    before = {m["task_collection"]: m["job_id"] for m in api.store.members.values()}
    r = api.post(f"/api/schedules/{sid}", json=dict(BODY, cadence_minutes=30, members=["c2", "c3"])).json()
    assert r["changed"] == ["cadence_minutes"] and r["added"] == ["c3"] and r["removed"] == ["c1"]
    actions = {x["task_collection"]: x["action"] for x in r["sync"]}
    assert actions == {"c2": "updated", "c3": "created", "c1": "deleted"}
    assert before["c1"] not in api.jobs.jobs                                      # removed member's job is gone
    active = {m["task_collection"]: m for m in api.store.members.values() if m["removed_at"] is None}
    assert {c: m["offset_minutes"] for c, m in active.items()} == {"c2": 0, "c3": 15}
    assert api.jobs.jobs[active["c3"]["job_id"]]["schedule"]["quartz_cron_expression"] == "0 15/30 * * * ?"
    again = api.post(f"/api/schedules/{sid}/sync").json()["sync"]                 # nothing left to do
    assert {x["action"] for x in again} == {"unchanged"}


def test_pause_resume_and_delete(api):
    sid = api.post("/api/schedules", json=BODY).json()["id"]
    api.post(f"/api/schedules/{sid}/pause")
    assert all(j["schedule"]["pause_status"] == "PAUSED" for j in api.jobs.jobs.values())
    assert api.get("/api/schedules").json()[0]["members"][0]["next_run"] is None
    api.post(f"/api/schedules/{sid}/resume")
    assert all(j["schedule"]["pause_status"] == "UNPAUSED" for j in api.jobs.jobs.values())
    r = api.post(f"/api/schedules/{sid}/delete").json()
    assert sorted(x["action"] for x in r["sync"]) == ["deleted", "deleted"] and api.jobs.jobs == {}
    assert api.get("/api/schedules").json() == []
    assert api.post(f"/api/schedules/{sid}/pause").status_code == 409


def test_check_reports_drift_without_changing_anything(api):
    sid = api.post("/api/schedules", json=BODY).json()["id"]
    jid = next(m["job_id"] for m in api.store.members.values() if m["task_collection"] == "c1")
    api.jobs.jobs[jid] = dict(api.jobs.jobs[jid], max_concurrent_runs=4)
    status = api.get(f"/api/schedules/{sid}/check").json()
    assert status["c1"] == {"status": "drifted", "fields": ["max_concurrent_runs"]} and status["c2"]["status"] == "in_sync"
    assert api.jobs.jobs[jid]["max_concurrent_runs"] == 4


def test_the_timeline_has_one_lane_per_job_with_its_planned_triggers(api, monkeypatch):
    api.post("/api/schedules", json=BODY)
    lanes = api.get("/api/timeline?hours=2").json()["jobs"]
    assert [(lane["name"], lane["paused"]) for lane in lanes] == [("Prod 15", False)]
    assert len(lanes[0]["planned"]) <= 2      # only since the schedule began
    assert not any(r["started_at"] for r in lanes[0]["runs"])       # no group run has started (the current slot may be open)
    api.store.members[1]["added_at"] = api.store.members[2]["added_at"] = "2020-01-01T00:00:00+00:00"
    api.store.schedules[1]["updated_at"] = "2020-01-01T00:00:00+00:00"
    main._shared_reads.clear()      # the schedules are a shared read
    lane, = api.get("/api/timeline?hours=2").json()["jobs"]
    assert 14 <= len(lane["planned"]) <= 18     # both groups' triggers, staggered 7 minutes apart, in one lane
    assert lane["description"].startswith("Every 15 minutes")


def test_a_group_with_tables_cant_leave_its_job(api):
    sid = api.post("/api/schedules", json=BODY).json()["id"]
    r = api.post(f"/api/schedules/{sid}", json=dict(BODY, members=["c2"]))
    assert r.status_code == 422 and "c1 still has tables" in r.text
    assert {m["task_collection"] for m in api.store.members.values() if m["removed_at"] is None} == {"c1", "c2"}


def test_sync_takes_the_schedule_lock_and_a_failed_delete_is_retried(api):
    sid = api.post("/api/schedules", json=BODY).json()["id"]
    assert api.store.locks and api.store.locks[-1] == (q.SCHEDULE_LOCK_CLASS, sid)
    c1 = next(m for m in api.store.members.values() if m["task_collection"] == "c1")
    api.store.with_tables.discard("c1")
    real = api.jobs.delete
    api.jobs.delete = lambda job_id: (_ for _ in ()).throw(RuntimeError("503 temporarily unavailable"))
    r = api.post(f"/api/schedules/{sid}", json=dict(BODY, members=["c2"])).json()
    assert {x["task_collection"]: x["action"] for x in r["sync"]}["c1"] == "error"
    assert c1["job_id"] in api.jobs.jobs and "503" in c1["sync_error"]
    api.jobs.delete = real
    again = api.post(f"/api/schedules/{sid}/sync").json()["sync"]          # the delete is still pending
    assert ("c1", "deleted") in {(x["task_collection"], x["action"]) for x in again}
    assert c1["job_id"] not in api.jobs.jobs


def test_delete_without_the_variant_still_deletes_the_jobs(api, monkeypatch):
    sid = api.post("/api/schedules", json=BODY).json()["id"]
    monkeypatch.setattr(main.jobs, "ingest_variants", lambda: {})
    monkeypatch.setattr(main.jobs, "user_groups", lambda user: ["users"])
    assert api.post(f"/api/schedules/{sid}/delete").status_code == 403      # only an admin cleans up an orphan
    monkeypatch.setattr(main.jobs, "user_groups", lambda user: ["admins"])
    r = api.post(f"/api/schedules/{sid}/delete")
    assert r.status_code == 200 and api.jobs.jobs == {}
    assert api.post(f"/api/schedules/{sid}/sync").status_code == 200


def test_a_blank_time_zone_is_stored_as_utc(api):
    api.post("/api/schedules", json=dict(BODY, timezone="  "))
    assert api.store.schedules[1]["timezone"] == "UTC"
    assert api.get("/api/schedules").status_code == 200 and api.get("/api/timeline").status_code == 200


def test_ingest_runs_started_by_a_wrapper_show_their_schedule(api, monkeypatch):
    api.post("/api/schedules", json=BODY)
    monkeypatch.setattr(main.jobs, "INGEST_JOB_ID", "555")
    monkeypatch.setattr(main.jobs, "job_summary", lambda jid: {"job_id": 555, "parameters": {"parallelism": "32", "db_name": "x"}})
    monkeypatch.setattr(main.jobs, "runs", lambda jid, active_only=False, limit=25: [
        {"run_id": 1, "task_collection": "c1", "trigger": "RUN_JOB_TASK", "started": "2999-01-01T00:00:00+00:00"},
        {"run_id": 2, "task_collection": "c1", "trigger": "ONE_TIME", "started": "2999-01-01T00:00:00+00:00"},
        {"run_id": 3, "task_collection": "c1", "trigger": "RUN_JOB_TASK", "started": "2020-01-01T00:00:00+00:00"}])
    r = api.get("/api/jobs/ingest").json()
    assert [x["schedule"] for x in r["recent"]] == ["Prod 15", None, None]     # 3 ran before c1 joined
    opts = api.get("/api/schedules/options").json()
    assert opts["defaults"] == {"parallelism": "32"} and "table_retries" in opts["parameters"]
    assert opts["scheduled"] == {"c1": ["Prod 15"], "c2": ["Prod 15"]}


def test_a_schedule_body_keeps_minute_schedules_in_utc():
    import main
    body = main.ScheduleBody(name="n", cadence_minutes=30, timezone="Europe/London", members=["c1"])
    assert body.timezone == "UTC"
    daily = main.ScheduleBody(name="n", cadence_minutes=1440, timezone="Europe/London", members=["c1"])
    assert daily.timezone == "Europe/London"


def test_a_check_schedule_is_offered_only_with_the_check_job_and_syncs_one_job(api, monkeypatch):
    assert "check" not in api.get("/api/schedules/options").json()["variants"]
    monkeypatch.setattr(main.jobs, "CHECK_JOB_ID", "777")
    assert api.get("/api/schedules/options").json()["variants"] == ["check", "default"]
    r = api.post("/api/schedules", json=dict(BODY, job_variant="check"))
    assert r.status_code == 200, r.text
    created = [s for s in api.jobs.jobs.values() if s["tags"].get(main.sch.TAG_COLLECTION) == main.sch.GROUP_TAG]
    assert len(created) == 1 and len(api.jobs.jobs) == 1
    assert created[0]["tasks"][0]["run_job_task"] == {"job_id": 777, "job_parameters": {
        "schedule_id": str(r.json()["id"]), "task_collections": "c1,c2", "parallelism": "16"}}
    # The one wrapper fires every member at once: a stagger asked for (the form keeps the last choice) is not kept.
    assert api.store.schedules[r.json()["id"]]["stagger"] is False
    assert {m["offset_minutes"] for m in api.store.members.values()} == {0}


def test_a_jobs_compute_lists_what_its_schedule_starts(api, monkeypatch):
    ingest = {"name": "lakefed_ingest", "tasks": [{"task_key": "ingest", "job_cluster_key": "c"}],
              "job_clusters": [{"job_cluster_key": "c", "new_cluster": {"node_type_id": "Standard_D8ds_v5",
                                                                        "num_workers": 1}}]}
    check = {"name": "lakefed_ingest_check", "tasks": [{"task_key": "check", "environment_key": "e"}]}
    monkeypatch.setattr(main.jobs, "job_settings", lambda job_id: {"555": ingest, "777": check}[str(job_id)])
    sid = api.post("/api/schedules", json=BODY).json()["id"]
    found = api.get(f"/api/ingestion-jobs/{sid}/compute").json()
    assert [(j["role"], j["job_id"], [t["kind"] for t in j["compute"]]) for j in found["jobs"]] == [
        ("ingestion", "555", ["job cluster"])]
    monkeypatch.setattr(main.jobs, "CHECK_JOB_ID", "777")
    monkeypatch.setattr(main.jobs, "INGEST_JOB_ID", "555")
    sid = api.post("/api/schedules", json=dict(BODY, name="Group", job_variant="check")).json()["id"]
    found = api.get(f"/api/ingestion-jobs/{sid}/compute").json()
    assert [(j["role"], [t["kind"] for t in j["compute"]]) for j in found["jobs"]] == [
        ("check", ["serverless"]), ("ingestion", ["job cluster"])]


def test_a_jobs_compute_says_so_when_a_job_cannot_be_read(api, monkeypatch):
    def unreadable(job_id):
        raise RuntimeError("no access")
    monkeypatch.setattr(main.jobs, "job_settings", unreadable)
    sid = api.post("/api/schedules", json=BODY).json()["id"]
    [job] = api.get(f"/api/ingestion-jobs/{sid}/compute").json()["jobs"]
    assert (job["compute"], job["error"]) == ([], "can't read job 555: no access")


def _run_now_setup(api, monkeypatch, body):
    started = []
    monkeypatch.setattr(main.jobs, "run_now", lambda job_id, params: started.append(int(job_id)) or 900 + len(started))
    monkeypatch.setattr(main, "_running", lambda: [])
    sid = api.post("/api/schedules", json=body).json()["id"]
    return sid, started


def test_run_now_starts_each_members_wrapper_as_the_schedule_would(api, monkeypatch):
    monkeypatch.setattr(main.jobs, "user_job_permission", lambda job_id, user: "CAN_MANAGE")
    sid, started = _run_now_setup(api, monkeypatch, BODY)
    r = api.post(f"/api/ingestion-jobs/{sid}/run")
    assert r.status_code == 200, r.text
    wrappers = {m["job_id"] for m in api.store.members.values()}
    assert set(started) == wrappers and len(started) == 2 and r.json()["started"] == [901, 902]


def test_run_now_on_a_check_schedule_starts_its_one_wrapper(api, monkeypatch):
    monkeypatch.setattr(main.jobs, "user_job_permission", lambda job_id, user: "CAN_MANAGE")
    monkeypatch.setattr(main.jobs, "CHECK_JOB_ID", "777")
    sid, started = _run_now_setup(api, monkeypatch, dict(BODY, job_variant="check"))
    assert api.post(f"/api/ingestion-jobs/{sid}/run").status_code == 200 and len(started) == 1


def test_run_now_is_refused_while_a_run_of_the_job_is_going_or_when_turned_off(api, monkeypatch):
    sid, started = _run_now_setup(api, monkeypatch, BODY)
    monkeypatch.setattr(main.jobs, "user_job_permission", lambda job_id, user: "CAN_VIEW")
    r = api.post(f"/api/ingestion-jobs/{sid}/run")
    assert r.status_code == 403 and r.json()["detail"] == (
        "starting a run needs Can Manage Run on the Databricks job 555; you have Can View")
    monkeypatch.setattr(main.jobs, "user_job_permission", lambda job_id, user: "CAN_MANAGE_RUN")
    monkeypatch.setattr(main, "_running", lambda: [{"task_collection": "c1", "run_id": 5}])
    r = api.post(f"/api/ingestion-jobs/{sid}/run")
    assert r.status_code == 409 and "already running" in r.json()["detail"] and started == []


def test_deleting_a_job_needs_can_manage_on_its_databricks_job(api, monkeypatch):
    sid = api.post("/api/schedules", json=BODY).json()["id"]
    monkeypatch.setattr(main.jobs, "user_job_permission", lambda job_id, user: "CAN_MANAGE_RUN")
    r = api.post(f"/api/schedules/{sid}/delete")
    assert r.status_code == 403 and api.store.schedules[sid].get("deleted_at") is None
    monkeypatch.setattr(main.jobs, "user_job_permission", lambda job_id, user: "IS_OWNER")
    assert api.post(f"/api/schedules/{sid}/delete").status_code == 200


CHANGE_REFUSAL = "changing the job or its tables needs Can Manage on the Databricks job 555; you have Can Manage Run"


def test_changing_pausing_resuming_or_syncing_a_job_needs_can_manage_but_running_it_does_not(api, monkeypatch):
    sid, started = _run_now_setup(api, monkeypatch, BODY)
    monkeypatch.setattr(main.jobs, "user_job_permission", lambda job_id, user: "CAN_MANAGE_RUN")
    for call in (lambda: api.post(f"/api/schedules/{sid}", json=dict(BODY, cadence_minutes=60)),
                 lambda: api.post(f"/api/schedules/{sid}/pause"), lambda: api.post(f"/api/schedules/{sid}/resume"),
                 lambda: api.post(f"/api/schedules/{sid}/sync")):
        r = call()
        assert (r.status_code, r.json()["detail"]) == (403, CHANGE_REFUSAL)
    assert api.store.schedules[sid]["cadence_minutes"] == 15 and not api.store.schedules[sid]["paused"]
    assert api.post(f"/api/ingestion-jobs/{sid}/run").status_code == 200 and len(started) == 2
    detail = api.get(f"/api/ingestion-jobs/{sid}").json()
    assert (detail["change_blocked"], detail["run_now"]["blocked"]) == (CHANGE_REFUSAL, None)


def test_anyone_may_set_up_a_paused_job_but_one_that_runs_needs_the_right_to_run_what_it_starts(api, monkeypatch):
    monkeypatch.setattr(main.jobs, "user_job_permission", lambda job_id, user: "CAN_VIEW")
    r = api.post("/api/schedules", json=BODY)
    assert r.status_code == 403 and r.json()["detail"].startswith("starting a run needs Can Manage Run")
    assert not api.store.schedules
    assert api.post("/api/schedules", json=dict(BODY, paused=True)).status_code == 200


def test_tables_of_a_group_in_a_job_need_can_manage_on_that_job_and_groups_in_no_job_stay_open(api, monkeypatch):
    api.post("/api/schedules", json=BODY)
    monkeypatch.setattr(main.jobs, "user_job_permission", lambda job_id, user: "CAN_MANAGE_RUN")
    assert main._groups_refusal(["c1", "c3"], "op@example.com") == (
        f"these tables are in the ingestion job Prod 15: {CHANGE_REFUSAL}")
    assert main._groups_refusal(["c3"], "op@example.com") is None
    r = api.post("/api/tables", json={"task_collection": "c2", "src_type": "sqlserver", "src_connection": "conn",
                                      "use_remote_query": True, "src_database": "d", "src_schema": "dbo",
                                      "src_table": "t", "sink_catalog": "c", "sink_schema": "s", "sink_table": "t",
                                      "load_type": "full"})
    assert r.status_code == 403 and "Prod 15" in r.json()["detail"]


def test_the_permissions_answer_lists_what_the_user_may_not_do_on_each_job_group_and_bundle_job(api, monkeypatch):
    sid = api.post("/api/schedules", json=BODY).json()["id"]
    monkeypatch.setattr(main.jobs, "SETUP_JOB_ID", "556")
    monkeypatch.setattr(main.jobs, "DISCOVER_JOB_ID", None)
    monkeypatch.setattr(main.jobs, "user_job_permission",
                        lambda job_id, user: "CAN_MANAGE_RUN" if str(job_id) == "555" else "CAN_VIEW")
    answer = api.get("/api/permissions").json()
    assert answer["jobs"][str(sid)] == {"run": None, "change": CHANGE_REFUSAL,
                                        "delete": CHANGE_REFUSAL.replace("changing the job or its tables", "deleting")}
    assert set(answer["groups"]) == {"c1", "c2"}
    assert answer["run"]["setup"] == "starting a run needs Can Manage Run on the Databricks job 556; you have Can View"
    assert answer["run"]["discovery"] == "no discovery job is configured"
    assert answer["new_running_job"]["default"] is None
    assert api.post("/api/jobs/setup/run").status_code == 403


def test_a_job_records_who_created_it_and_who_last_changed_it_and_a_sent_name_is_ignored(api, monkeypatch):
    sid = api.post("/api/schedules", json=dict(BODY, paused=True)).json()["id"]
    row = api.store.schedules[sid]
    assert (row["created_by"], sch.updated_by(row)) == ("op@example.com", "op@example.com")
    other = TestClient(main.app, headers={"X-Forwarded-Email": "second@example.com"})
    r = other.post(f"/api/schedules/{sid}", json=dict(BODY, paused=True, cadence_minutes=60,
                                                     options={"updated_by": "someone@else.com"}))
    assert r.status_code == 200 and sch.updated_by(api.store.schedules[sid]) == "second@example.com"
    other.post(f"/api/schedules/{sid}/resume")
    assert sch.updated_by(api.store.schedules[sid]) == "second@example.com"


BIG = {"driver_node_type_id": "Standard_D32ds_v5", "node_type_id": "Standard_D16ds_v5", "num_workers": 4}


def _cluster_setup(monkeypatch):
    started = []
    monkeypatch.setattr(main.jobs, "CLUSTER_COPIES_JOB_ID", "700")
    details = [{"id": n, "memory_gb": gb, "cores": cores} for n, gb, cores in
               (("Standard_D16ds_v5", 64, 16), ("Standard_D32ds_v5", 128, 32), ("Standard_D8ds_v5", 32, 8))]
    monkeypatch.setattr(main.jobs, "node_type_details", lambda: details)
    monkeypatch.setattr(main.jobs, "node_types", lambda: [d["id"] for d in details])
    monkeypatch.setattr(main.jobs, "start_cluster_copies", lambda sid: started.append(sid) or 8000 + len(started))
    return started


def test_a_cluster_of_its_own_starts_the_copies_job_and_a_forged_copy_id_is_ignored(api, monkeypatch):
    started = _cluster_setup(monkeypatch)
    sid = api.post("/api/schedules", json=BODY).json()["id"]
    assert started == []
    body = dict(BODY, options={"cluster": BIG, "ingest_copy_job_id": 1})
    r = api.post(f"/api/schedules/{sid}", json=body)
    assert r.status_code == 200 and r.json()["cluster_error"] is None and started == [sid]
    options = sch.options_of(api.store.schedules[sid])
    assert options == {"cluster": BIG, "cluster_copy_run_id": 8001, "updated_by": "op@example.com"}
    # Saving again with the same cluster starts nothing; the wrappers keep the bundle's job until the copy exists.
    assert api.post(f"/api/schedules/{sid}", json=body).status_code == 200 and started == [sid]
    assert main._wrapper_target(api.store.schedules[sid]) == 555


def test_new_offers_the_job_cluster_fields_before_the_job_exists(api, monkeypatch):
    _cluster_setup(monkeypatch)
    monkeypatch.setattr(main.jobs, "job_settings", lambda job_id: {"job_clusters": [{"new_cluster": {
        "driver_node_type_id": "Standard_D16ds_v5", "node_type_id": "Standard_D8ds_v5", "num_workers": 1}}]})
    r = api.get("/api/cluster-options")
    assert r.status_code == 200
    got = r.json()
    assert got["defaults"] == {"driver_node_type_id": "Standard_D16ds_v5", "node_type_id": "Standard_D8ds_v5", "num_workers": 1}
    assert got["node_types"] and got["cluster"] is None and got["applying"] is None and got["error"] is None


def test_a_cluster_must_name_this_workspaces_node_types_and_at_most_eight_workers(api, monkeypatch):
    _cluster_setup(monkeypatch)
    sid = api.post("/api/schedules", json=BODY).json()["id"]
    r = api.post(f"/api/schedules/{sid}", json=dict(BODY, options={"cluster": dict(BIG, node_type_id="Standard_X9")}))
    assert r.status_code == 422 and "Standard_X9 is not a node type" in r.text
    r = api.post(f"/api/schedules/{sid}", json=dict(BODY, options={"cluster": dict(BIG, num_workers=9)}))
    assert r.status_code == 422 and "0 to 8 workers" in r.text


def test_once_its_copy_exists_a_job_runs_it_and_its_permissions_are_the_copys(api, monkeypatch):
    sid = api.post("/api/schedules", json=BODY).json()["id"]
    api.store.schedules[sid]["options"] = '{"cluster": {"num_workers": 4}, "ingest_copy_job_id": 4242}'
    assert main._wrapper_target(api.store.schedules[sid]) == 4242
    asked = []
    monkeypatch.setattr(main.jobs, "user_job_permission", lambda job_id, user: asked.append(str(job_id)) or "CAN_MANAGE")
    main._permission_refusals(api.store.schedules[sid], "op@example.com")
    assert asked == ["4242"]
    # Back at the defaults, the recorded copy is no longer used, before the copies job deletes it.
    api.store.schedules[sid]["options"] = '{"ingest_copy_job_id": 4242}'
    assert main._wrapper_target(api.store.schedules[sid]) == 555


def test_a_check_schedule_with_its_own_cluster_passes_its_copy_to_the_check_job():
    schedule = dict(id=3, name="Check", cadence_minutes=60, timezone="UTC", parameters={}, job_variant="check",
                    paused=False, options={"cluster": {"num_workers": 2}, "ingest_copy_job_id": 4242})
    settings = sch.group_job_settings(schedule, ["c1", "c2"], 777)
    task = settings["tasks"][0]["run_job_task"]
    assert task["job_id"] == 777 and task["job_parameters"]["ingest_job_id"] == "4242"
    assert "ingest_job_id" not in sch.group_job_settings(dict(schedule, options={}), ["c1"], 777)["tasks"][0][
        "run_job_task"]["job_parameters"]


def test_other_variants_cant_have_a_cluster_of_their_own():
    assert sch.cluster_problems({"num_workers": 2}, "large") == [
        "options.cluster: the large variant runs its own Databricks job, whose cluster is set there"]
    assert sch.cluster_problems({"num_workers": 2, "spark_version": "x"}, "default") == [
        "options.cluster: unknown key spark_version"]


def test_the_edit_view_follows_the_copies_run_and_syncs_the_wrappers_once_it_has_ended(api, monkeypatch):
    _cluster_setup(monkeypatch)
    sid = api.post("/api/schedules", json=BODY).json()["id"]
    api.store.schedules[sid]["options"] = '{"cluster": {"num_workers": 4}, "cluster_copy_run_id": 8001}'
    state = {"state": "RUNNING", "result": None, "message": "", "url": "https://h/run/8001"}
    monkeypatch.setattr(main.jobs, "run_state", lambda run_id: dict(state, run_id=run_id))
    monkeypatch.setattr(main.jobs, "job_settings", lambda job_id: {"job_clusters": [{"new_cluster": {
        "driver_node_type_id": "Standard_D16ds_v5", "node_type_id": "Standard_D8ds_v5", "num_workers": 1}}]})
    synced = []
    monkeypatch.setattr(main, "_sync", lambda job_id, user: synced.append(job_id) or [])
    body = api.get(f"/api/ingestion-jobs/{sid}/cluster").json()
    assert body["applying"]["state"] == "RUNNING" and synced == []
    assert body["defaults"] == {"driver_node_type_id": "Standard_D16ds_v5", "node_type_id": "Standard_D8ds_v5",
                                "num_workers": 1} and body["max_workers"] == 8
    assert body["node_types"][0] == {"id": "Standard_D16ds_v5", "memory_gb": 64, "cores": 16}
    # The copies job records the copy, then its run ends: the next read syncs the wrappers to it.
    api.store.schedules[sid]["options"] = ('{"cluster": {"num_workers": 4}, "cluster_copy_run_id": 8001, '
                                           '"ingest_copy_job_id": 4242}')
    state.update(state="TERMINATED", result="SUCCESS")
    body = api.get(f"/api/ingestion-jobs/{sid}/cluster").json()
    assert body["applying"] is None and body["copy_job_id"] == 4242 and synced == [sid]
    assert "cluster_copy_run_id" not in sch.options_of(api.store.schedules[sid])


def test_a_save_without_options_keeps_the_jobs_start_time(api):
    daily = dict(BODY, cadence_minutes=1440, paused=True)
    sid = api.post("/api/schedules", json=dict(daily, options={"start_minute": 150})).json()["id"]
    assert api.post(f"/api/schedules/{sid}", json=daily).status_code == 200
    assert sch.options_of(api.store.schedules[sid])["start_minute"] == 150


def test_a_malformed_or_zoneless_date_in_a_request_is_a_bad_request_not_a_server_error(api):
    sid = api.post("/api/schedules", json=BODY).json()["id"]
    for path in (f"/api/ingestion-jobs/{sid}/runs?before=yesterday", "/api/job-runs?before=2026-13-40",
                 f"/api/ingestion-jobs/{sid}/runs/not-a-time", "/api/job-runs?before=2026-10-29T13:00:00"):
        r = api.get(path)
        assert r.status_code == 400, (path, r.status_code)
        assert "UTC offset" in r.text or "date and time" in r.text
