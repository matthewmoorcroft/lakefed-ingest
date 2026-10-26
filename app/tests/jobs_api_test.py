"""The Overview and the ingestion job endpoints, with a fake database and a fixed clock."""
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
import queries as q  # noqa: E402

UTC = timezone.utc
NOW = datetime(2026, 10, 26, 14, 10, tzinfo=UTC)
ADDED = datetime(2026, 10, 1, tzinfo=UTC)


class FixedClock(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz else NOW.replace(tzinfo=None)


def state(group, tables, current=0, late=0, failing=0):
    return dict(server="srv1", src_connection=None, src_host="h", task_collection=group, src_database=f"{group}_db",
                tables=tables, state_current=current, state_late=late, state_first_load=0, state_failing=failing,
                no_sla_tables=0)


def group_run(run_id, group, started, status="completed", failed=0):
    return dict(run_id=run_id, task_collection=group, status=status, started_at=started.isoformat(),
                finished_at=None if status == "running" else (started + timedelta(minutes=5)).isoformat(),
                duration_s=300, total=10, ok=10 - failed, failed=failed, skipped=8, reseeded=0, job_run_id=None,
                cluster_id=f"cl-{run_id}", parallelism=32, detail=None)


SCHEDULES = [dict(id=1, name="East hourly", cadence_minutes=60, timezone="UTC", paused=False, updated_at=ADDED,
                  parameters={}, job_variant="default", stagger=True),
             dict(id=2, name="Nightly", cadence_minutes=1440, timezone="UTC", paused=True, updated_at=ADDED,
                  parameters={}, job_variant="default", stagger=False)]
MEMBERS = [dict(id=1, schedule_id=1, task_collection="c1", offset_minutes=0, job_id=None, added_at=ADDED, removed_at=None),
           dict(id=2, schedule_id=1, task_collection="c2", offset_minutes=30, job_id=None, added_at=ADDED, removed_at=None),
           dict(id=3, schedule_id=2, task_collection="c3", offset_minutes=0, job_id=None, added_at=ADDED, removed_at=None)]
RUNS = [group_run(9, "c1", datetime(2026, 10, 26, 13, 0, 20, tzinfo=UTC), status="failed", failed=2),
        group_run(10, "c1", datetime(2026, 10, 26, 13, 40, tzinfo=UTC)),
        group_run(8, "c2", datetime(2026, 10, 26, 13, 30, 10, tzinfo=UTC))]


@pytest.fixture
def client(monkeypatch):
    answers = {
        q.schedules_sql(): SCHEDULES, q.database_states_sql(): [state("c1", 10, current=8, failing=2),
                                                               state("c2", 5, current=5), state("c3", 4, late=4),
                                                               state("c4", 3, current=3)],
        q.running_progress_sql(): [], q.last_runs_sql(): [],
        q.collection_run_stats_sql(): [dict(task_collection="c1", p50_s=300, p95_s=400),
                                       dict(task_collection="c2", p50_s=300, p95_s=400)],
        q.run_failures_sql(): [dict(task_collection="c1", run_id=9, control_id=1, server="srv1", src_database="c1_db",
                                    src_schema="dbo", src_table="t1", error="connection reset", finished_at=NOW.isoformat()),
                               dict(task_collection="c1", run_id=9, control_id=2, server="srv1", src_database="c1_db",
                                    src_schema="dbo", src_table="t2", error="connection reset", finished_at=NOW.isoformat())],
        q.job_run_databases_sql(): [dict(task_collection="c1", server="srv1", src_database="c1_db", loaded=8, failed=2)],
    }

    def query(sql, params=()):
        if sql == q.schedule_members_sql():
            return [m for m in MEMBERS if m["schedule_id"] in params[0]]
        if sql == q.group_runs_sql():
            groups, start, end = params
            return [dict(r) for r in RUNS if r["task_collection"] in groups
                    and start <= datetime.fromisoformat(r["started_at"]) < end]
        return [dict(r) for r in answers.get(sql, [])]
    monkeypatch.setattr(main.db, "query", query)
    monkeypatch.setattr(main, "datetime", FixedClock)
    monkeypatch.setattr(main, "MONITOR_CACHE_S", 0)
    monkeypatch.setattr(main, "STATES_CACHE_S", 0)
    return TestClient(main.app)


def test_the_overview_lists_jobs_not_in_any_job_and_attention_without_a_paused_late_job(client):
    body = client.get("/api/overview").json()
    assert [j["name"] for j in body["jobs"]] == ["East hourly", "Nightly"] and body["jobs_total"] == 2
    east = body["jobs"][0]
    assert east["last_job_run"]["outcome"]["label"] == "Completed with failures"
    assert east["status"]["label"] == "Running" and "2 tables failing" in east["status"]["detail"]     # c2 starts at 14:30
    assert east["status"]["cause"] == "Could not connect to the source database"
    assert body["running_total"] == 1 and body["running_job_runs"][0]["waiting"] == 2     # c1 starting, c2 at 14:30
    assert [j["name"] for j in body["attention"]] == ["East hourly"] and body["attention_total"] == 1
    assert body["not_in_job"]["groups"] == ["c4"]
    assert body["totals"]["enabled_tables"] == 22 and "collections" not in body and "servers" not in body


def test_a_jobs_runs_page_by_slot_and_mark_extra_runs(client):
    body = client.get("/api/ingestion-jobs/1/runs?limit=3").json()
    slots = [(r["slot"][11:16], r["outcome"]["label"], r["extra"]) for r in body["job_runs"]]
    assert slots[:2] == [("14:00", "Running", 0), ("13:00", "Completed with failures", 1)]
    assert body["job_runs"][1]["clusters"] == 3 and body["job_runs"][1]["parallelism"] == [32]


def test_a_job_run_lists_its_waiting_members_with_their_start(client):
    body = client.get("/api/ingestion-jobs/1/runs/2026-10-26T14:00:00+00:00").json()
    waiting = [g for g in body["group_runs"] if g["state"] == "waiting"]
    assert [(g["task_collection"], g["fire_at"]) for g in waiting] == [("c2", "2026-10-26T14:30:00+00:00")]
    assert client.get("/api/ingestion-jobs/1/runs/2026-10-26T14:30:00+00:00").status_code == 404
    done = client.get("/api/ingestion-jobs/1/runs/2026-10-26T13:00:00+00:00").json()
    assert done["databases"][0]["failed"] == 2 and done["failures"][0]["tables"] == 2


def test_the_ingestion_jobs_list_filters_with_counts_and_ends_with_not_in_any_job(client):
    body = client.get("/api/ingestion-jobs").json()
    assert [j["name"] for j in body["jobs"]] == ["East hourly", "Nightly"] and body["total"] == 2
    assert body["not_in_job"]["groups"] == ["c4"]
    assert body["counts"] == {"all": 2, "running": 1, "failing": 1, "late": 1, "paused": 1}
    assert [j["name"] for j in client.get("/api/ingestion-jobs?filter=late").json()["jobs"]] == ["Nightly"]
    assert [j["name"] for j in client.get("/api/ingestion-jobs?search=c3").json()["jobs"]] == ["Nightly"]


def test_a_job_run_finds_a_group_run_that_started_near_the_end_of_its_slot(client):
    RUNS.append(group_run(11, "c2", datetime(2026, 10, 26, 12, 10, tzinfo=UTC)))     # late, after c2's 11:30 trigger
    try:
        body = client.get("/api/ingestion-jobs/1/runs/2026-10-26T11:00:00+00:00").json()
    finally:
        RUNS.pop()
    assert [g["run_id"] for g in body["group_runs"] if g["task_collection"] == "c2"] == [11]


def test_runs_across_jobs_pin_the_running_job_runs_and_count_outcomes(client):
    body = client.get("/api/job-runs").json()
    labels = [(r["job_name"], r["slot"][11:16], r["outcome"]["label"]) for r in body["job_runs"]][:3]
    assert labels[0] == ("East hourly", "14:00", "Running") and ("East hourly", "13:00", "Completed with failures") in labels
    assert body["counts"]["Completed with failures"] == 1
    none = client.get("/api/job-runs?job=none").json()
    assert none["job_runs"] == [] and "group_runs" in none


def test_sources_list_a_uc_connection_with_nothing_registered(client, monkeypatch):
    class Connection:
        def __init__(self, name, host):
            self.name, self.connection_type, self.options, self.owner, self.comment = name, "SQLSERVER", {"host": host}, "o", None

    class Workspace:
        class connections:
            @staticmethod
            def list():
                return [Connection("unused_conn", "nobody.example.com")]
    monkeypatch.setattr(main.db, "workspace", lambda: Workspace())
    body = client.get("/api/sources").json()
    empty = [i for i in body["instances"] if i["nothing_registered"]]
    assert [i["server"] for i in empty] == ["unused_conn"] and body["nothing_registered"] == 1
    assert {i["server"] for i in body["instances"] if not i["nothing_registered"]} == {"srv1"}
    on_srv1 = next(i for i in body["instances"] if i["server"] == "srv1")
    assert {j["name"]: j.get("description") for j in on_srv1["jobs"] if j["id"]}["East hourly"] == "hourly at :00"


def test_one_database_lists_its_tables_with_the_group_and_its_job(client, monkeypatch):
    calls = []
    real = main.db.query

    def query(sql, params=()):
        if sql == q.database_tables_sql():
            calls.append(params)
            return [dict(id=1, src_schema="dbo", src_table="t1", state="failing")]
        if sql == q.database_tables_count_sql():
            return [dict(tables=1)]
        return real(sql, params)
    monkeypatch.setattr(main.db, "query", query)
    body = client.get("/api/collections/c1/databases/srv1/c1_db?search=t1").json()
    assert (body["tables"][0]["state"], body["total"], body["jobs"]) == ("failing", 1, [{"id": 1, "name": "East hourly"}])
    assert (body["enabled_tables"], body["state_current"], body["state_failing"]) == (10, 8, 2)     # the database's, not the page's
    assert calls[0][:5] == ("c1", "srv1", "c1_db", None, "%t1%")
    assert client.get("/api/collections/c1/databases/srv9/c1_db").status_code == 404


def test_an_outcome_filter_reads_older_windows_until_it_finds_matches(client):
    RUNS.append(group_run(13, "c1", datetime(2026, 10, 26, 9, 1, tzinfo=UTC), status="failed", failed=4))
    try:
        body = client.get("/api/job-runs", params={"outcome": "Completed with failures", "limit": 1,
                                                    "before": "2026-10-26T12:00:00+00:00"}).json()
    finally:
        RUNS.pop()
    assert [r["slot"][11:16] for r in body["job_runs"]] == ["09:00"]


def test_runs_of_groups_in_no_job_are_read_from_the_database_before_a_time(client, monkeypatch):
    calls, real = [], main.db.query

    def query(sql, params=()):
        if sql == q.group_runs_before_sql():
            calls.append(params)
            return [group_run(30, "c4", datetime(2026, 10, 26, 8, 0, tzinfo=UTC))]
        return real(sql, params)
    monkeypatch.setattr(main.db, "query", query)
    body = client.get("/api/job-runs", params={"job": "none", "before": "2026-10-26T12:00:00+00:00"}).json()
    assert [r["run_id"] for r in body["group_runs"]] == [30] and calls[0][0] == ["c4"]
