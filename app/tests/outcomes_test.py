"""A group run's outcome, speed and time left (queries.run_outcome, work_speed, live_speed, time_left)."""
import json
from datetime import datetime, timedelta, timezone

import queries as q

NOW = datetime(2026, 10, 26, 14, 0, tzinfo=timezone.utc)


def test_outcomes_name_what_the_engine_recorded():
    assert q.run_outcome(dict(status="completed", failed=0))["label"] == "Completed"
    with_failures = q.run_outcome(dict(status="failed", failed=3, detail=json.dumps({"batch_rows": 0})))
    assert (with_failures["label"], with_failures["tone"], with_failures["note"]) == ("Completed with failures", "warn",
                                                                                       "3 tables failed")
    ended = q.run_outcome(dict(status="failed", failed=0, detail=json.dumps({"closed_by_run": 41, "reason": "x"})))
    assert (ended["label"], ended["tone"], ended["note"]) == ("Ended without finishing", "bad", "closed by run 41")
    assert q.run_outcome(dict(status="failed", failed=0, detail="not json"))["label"] == "Failed"
    assert q.run_outcome(dict(status="skipped"))["label"] == "Skipped: already running"
    assert q.run_outcome(dict(status="running"), dict(tables_ok=0, tables_failed=0, tables_running=0,
                                                      tables_checked=0))["label"] == "Detecting changes"
    assert q.run_outcome(dict(status="running"), dict(tables_ok=1, tables_checked=1))["label"] == "Loading changed tables"


def test_a_run_another_run_closed_has_no_duration():
    run = dict(status="failed", failed=0, duration_s=86000, detail=json.dumps({"closed_by_run": 41}))
    assert q.with_outcome(run)["duration_s"] is None


def test_speed_is_over_the_work_span_only():
    start = NOW - timedelta(minutes=10)
    assert q.work_speed(dict(loaded=50, work_started_at=start, work_finished_at=start + timedelta(minutes=5))) == 10.0
    assert q.work_speed(dict(loaded=0, work_started_at=None, work_finished_at=None)) is None


def test_live_speed_waits_for_three_minutes_of_loading_and_counts_only_time_spent_loading():
    early = dict(work_started_at=NOW - timedelta(minutes=2), ok_last_5m=40, rows_last_5m=4000)
    assert q.live_speed(early, NOW) is None
    later = dict(work_started_at=NOW - timedelta(minutes=4), ok_last_5m=40, rows_last_5m=4000)
    assert q.live_speed(later, NOW) == {"tables_per_min": 10.0, "rows_per_min": 1000.0}
    settled = dict(later, work_started_at=NOW - timedelta(minutes=30))
    assert q.live_speed(settled, NOW) == {"tables_per_min": 8.0, "rows_per_min": 800.0}


def test_time_left_from_the_rate_while_loading_else_from_the_typical_run():
    loading = dict(enabled_tables=1000, tables_checked=500, tables_running=50, tables_failed=50, ok_last_5m=100,
                   work_started_at=NOW - timedelta(minutes=10), elapsed_s=900, p50_s=1200)
    assert q.time_left(loading, NOW) == {"seconds": 1200, "basis": "rate"}
    detecting = dict(enabled_tables=1000, tables_checked=0, tables_running=0, tables_failed=0, ok_last_5m=0,
                     work_started_at=None, elapsed_s=300, p50_s=1200)
    assert q.time_left(detecting, NOW) == {"seconds": 900, "basis": "typical"}
    assert q.time_left(dict(detecting, p50_s=None), NOW) is None
    assert q.time_left(dict(detecting, elapsed_s=5000), NOW) == {"seconds": 0, "basis": "typical"}
