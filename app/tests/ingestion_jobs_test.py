"""Ingestion jobs and job runs as the pages show them (ingestion_jobs.py), from fixed reads."""
from datetime import datetime, timedelta, timezone

import ingestion_jobs as ij

UTC = timezone.utc
NOW = datetime(2026, 10, 26, 14, 10, tzinfo=UTC)
ADDED = datetime(2026, 10, 1, tzinfo=UTC)

SCHEDULES = [dict(id=1, name="East hourly", cadence_minutes=60, timezone="UTC", paused=False, updated_at=ADDED),
             dict(id=2, name="Nightly", cadence_minutes=1440, timezone="UTC", paused=True,
                  updated_at=datetime(2026, 10, 26, 9, 2, tzinfo=UTC))]
MEMBERS = [dict(schedule_id=1, task_collection="c1", offset_minutes=0, added_at=ADDED, removed_at=None),
           dict(schedule_id=1, task_collection="c2", offset_minutes=30, added_at=ADDED, removed_at=None),
           dict(schedule_id=2, task_collection="c3", offset_minutes=0, added_at=ADDED, removed_at=None)]


def state(group, tables, current=0, late=0, failing=0, server="srv1", database=None):
    return dict(server=server, task_collection=group, src_database=database or f"{group}_db", tables=tables,
                state_current=current, state_late=late, state_first_load=0, state_failing=failing, no_sla_tables=0)


STATES = [state("c1", 10, current=8, failing=2), state("c2", 5, current=5), state("c3", 4, late=4),
          state("c4", 3, current=3, server="srv2")]


def run(run_id, group, started, status="completed", minutes=10, **extra):
    return dict(run_id=run_id, task_collection=group, status=status, started_at=started.isoformat(),
                finished_at=None if status == "running" else (started + timedelta(minutes=minutes)).isoformat(),
                total=extra.pop("total", None if status == "running" else 10), ok=extra.pop("ok", None),
                skipped=extra.pop("skipped", 0), failed=extra.pop("failed", 0), detail=None, **extra)


RUNS = [run(9, "c1", datetime(2026, 10, 26, 13, 0, 20, tzinfo=UTC), status="failed", failed=2, ok=8, skipped=6,
            work_started_at=datetime(2026, 10, 26, 13, 1, tzinfo=UTC), work_finished_at=datetime(2026, 10, 26, 13, 3, tzinfo=UTC)),
        run(8, "c2", datetime(2026, 10, 26, 13, 30, 10, tzinfo=UTC), total=5, ok=5, skipped=5),
        run(10, "c1", datetime(2026, 10, 26, 14, 0, 20, tzinfo=UTC), status="running")]
RUNNING = [dict(run_id=10, task_collection="c1", enabled_tables=10, tables_checked=4, tables_ok=2, tables_running=1,
                tables_failed=0, work_started_at=None, ok_last_5m=0, elapsed_s=580, p50_s=600),
           dict(run_id=11, task_collection="c4", enabled_tables=3, tables_checked=0, tables_ok=0, tables_running=0,
                tables_failed=0, work_started_at=None, ok_last_5m=0, elapsed_s=60, p50_s=None)]
STATS = {"c1": dict(p50_s=600, p95_s=900), "c2": dict(p50_s=300, p95_s=400), "c3": dict(p50_s=None)}


def view():
    return ij.jobs_view(SCHEDULES, MEMBERS, STATES, RUNNING, RUNS, STATS, NOW,
                        causes_by_job={1: [dict(label="Could not connect to the source database", tables=2)]})


def test_jobs_come_failing_first_with_their_status_and_last_job_run():
    v = view()
    east, nightly = v["jobs"]
    assert (east["name"], nightly["name"]) == ("East hourly", "Nightly")
    assert east["status"]["label"] == "Running" and "2 tables failing" in east["status"]["detail"]
    assert east["status"]["cause"] == "Could not connect to the source database"
    last = east["last_job_run"]
    assert (last["slot"], last["outcome"]["label"], last["failed_tables"]) == ("2026-10-26T13:00:00+00:00",
                                                                               "Completed with failures", 2)
    assert last["took_s"] == 2390 and last["loaded"] == 2 + 0 and last["ran"] == 2
    assert (nightly["status"]["label"], nightly["status"]["detail"]) == ("Late: 4 tables", ["paused"])


def test_a_running_job_run_counts_every_tables_of_the_job_and_its_waiting_groups():
    current = view()["jobs"][0]["running_job_run"]
    assert current["progress"] == {"total": 15, "unchanged": 2, "loaded": 2, "in_flight": 1, "failed": 0}
    assert (current["waiting"], current["running_group_runs"]) == (1, 1)
    assert current["expected_end"] == "2026-10-26T14:35:00+00:00"     # c2 starts at 14:30 and typically takes 5 min
    assert current["typical_s"] == 30 * 60 + 300


def test_a_paused_job_with_late_tables_needs_no_attention_and_a_group_in_no_job_is_listed_apart():
    v = view()
    assert [j["name"] for j in v["attention"]] == ["East hourly"]
    assert v["not_in_job"]["groups"] == ["c4"]
    assert [r["run_id"] for r in v["running_not_in_job"]] == [11]
    assert {k: v["totals"][k] for k in ("jobs", "running_jobs", "failing_jobs", "late_jobs")} == \
        {"jobs": 2, "running_jobs": 1, "failing_jobs": 1, "late_jobs": 1}


def test_a_jobs_runs_in_a_row_with_failed_tables():
    assert ij.consecutive_failed([dict(outcome={"label": "Running"}, failed_tables=0),
                                  dict(outcome={"label": "Completed with failures"}, failed_tables=3),
                                  dict(outcome={"label": "Completed with failures"}, failed_tables=1),
                                  dict(outcome={"label": "Completed"}, failed_tables=0),
                                  dict(outcome={"label": "Completed with failures"}, failed_tables=1)]) == 2


def test_a_group_run_knows_its_job_runs():
    found = ij.job_runs_of_run(dict(task_collection="c2", started_at="2026-10-26T13:31:00+00:00"), SCHEDULES, MEMBERS)
    assert found == [{"job_id": 1, "job_name": "East hourly", "slot": "2026-10-26T13:00:00+00:00"}]
    assert ij.job_runs_of_run(dict(task_collection="c4", started_at="2026-10-26T13:31:00+00:00"), SCHEDULES, MEMBERS) == []


def test_a_group_run_running_longer_than_the_window_still_shows_as_running():
    old_start = NOW - timedelta(days=2)
    running = [dict(RUNNING[0], run_id=20, elapsed_s=2 * 86400, started_at=old_start.isoformat())]
    runs = RUNS[:2] + [run(20, "c1", old_start, status="running")]
    v = ij.jobs_view(SCHEDULES, MEMBERS, STATES, running, runs, STATS, NOW)
    assert [r["run_ids"] for r in v["running_job_runs"]] == [[20], []]     # the 14:00 one waits for c2


def test_a_paused_job_with_failing_tables_shows_failing_and_says_it_is_paused():
    paused_since = datetime(2026, 10, 26, 9, 2, tzinfo=UTC)
    got = ij.status(dict(state_failing=3, state_late=9, paused=True, updated_at=paused_since), None, None, "Timed out", [])
    assert (got["label"], got["detail"], got["paused_since"], got["cause"]) == ("Failing: 3 tables", ["paused"], paused_since, "Timed out")
    late = ij.status(dict(state_failing=0, state_late=9, paused=True, updated_at=paused_since), None, None, None, [])
    assert (late["label"], late["detail"]) == ("Late: 9 tables", ["paused"])


def test_a_window_starting_mid_slot_never_cuts_a_job_run_short():
    start = datetime(2026, 10, 26, 13, 10, tzinfo=UTC)     # after c1's 13:00 trigger, before c2's 13:30
    end = datetime(2026, 10, 26, 15, 0, tzinfo=UTC)
    placed = ij.place(SCHEDULES[0], MEMBERS, RUNS, start, end, NOW)
    assert [(jr["slot"].hour, len(jr["members"])) for jr in placed] == [(14, 2)]
    whole = ij.place(SCHEDULES[0], MEMBERS, RUNS, datetime(2026, 10, 26, 13, 0, tzinfo=UTC), end, NOW)
    assert [(jr["slot"].hour, len(jr["members"])) for jr in whole] == [(14, 2), (13, 2)]
