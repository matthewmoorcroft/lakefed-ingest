"""A deleted ingestion job's Delete as its state: deleting while its delete job runs, failed with the reason, or gone
(ingestion_jobs.delete_state), and the Status cell it shows (ingestion_jobs.delete_status). Plain functions, no database."""
from datetime import datetime, timedelta, timezone

import ingestion_jobs as ij

UTC = timezone.utc
DELETED_AT = datetime(2026, 10, 29, 12, 0, tzinfo=UTC)
JUST_NOW = DELETED_AT + timedelta(seconds=30)
LATER = DELETED_AT + timedelta(hours=1)


def test_a_job_whose_delete_job_is_running_is_deleting_even_with_its_tables_already_gone():
    run = {"run_id": 9, "state": "RUNNING", "result": None, "message": None, "url": "u"}
    state = ij.delete_state(_job(), tables_left=False, run=run, now=LATER)
    assert (state["state"], state["run_again"], state["run"]) == (ij.DELETING, False, run)


def test_a_queued_or_pending_delete_job_is_deleting():
    for life_cycle in ("QUEUED", "PENDING"):
        run = {"run_id": 9, "state": life_cycle, "result": None, "message": None}
        assert ij.delete_state(_job(), tables_left=True, run=run, now=LATER)["state"] == ij.DELETING


def test_a_delete_job_not_recorded_yet_is_starting_for_a_few_minutes_only():
    assert ij.delete_state(_job(), True, None, JUST_NOW)["state"] == ij.DELETING
    late = ij.delete_state(_job(), True, None, DELETED_AT + timedelta(minutes=10))
    assert (late["state"], late["reason"], late["run_again"]) == (
        ij.DELETE_FAILED, "its delete job run was never recorded", True)


def test_a_succeeded_delete_with_no_tables_left_is_gone():
    run = {"run_id": 9, "state": "TERMINATED", "result": "SUCCESS", "message": None}
    assert ij.delete_state(_job(), tables_left=False, run=run, now=LATER)["state"] == ij.DELETE_GONE


def test_a_job_with_no_tables_and_no_run_is_gone():
    assert ij.delete_state(_job(), tables_left=False, run=None, now=LATER)["state"] == ij.DELETE_GONE


def test_a_succeeded_delete_that_left_tables_is_failed_and_can_run_again():
    run = {"run_id": 9, "state": "TERMINATED", "result": "SUCCESS", "message": None}
    state = ij.delete_state(_job(), tables_left=True, run=run, now=LATER)
    assert (state["state"], state["run_again"]) == (ij.DELETE_FAILED, True)


def test_a_failed_delete_shows_the_run_message_and_runs_again_only_while_tables_remain():
    run = {"run_id": 9, "state": "TERMINATED", "result": "FAILED", "message": "table x would not drop"}
    with_tables = ij.delete_state(_job(), tables_left=True, run=run, now=LATER)
    assert (with_tables["state"], with_tables["reason"], with_tables["run_again"]) == (
        ij.DELETE_FAILED, "table x would not drop", True)
    rows_gone = ij.delete_state(_job(), tables_left=False, run=run, now=LATER)
    assert (rows_gone["state"], rows_gone["run_again"]) == (ij.DELETE_FAILED, False)


def test_a_failed_delete_without_a_message_names_its_result():
    run = {"run_id": 9, "state": "INTERNAL_ERROR", "result": None, "message": None}
    assert ij.delete_state(_job(), tables_left=True, run=run, now=LATER)["reason"] == \
        "the delete job ended INTERNAL_ERROR"


def test_a_start_that_failed_is_failed_with_its_reason_and_can_run_again():
    job = _job(delete_start_error="the delete job didn't start: 403 forbidden")
    state = ij.delete_state(job, tables_left=True, run=None, now=JUST_NOW)
    assert (state["state"], state["reason"], state["run_again"]) == (
        ij.DELETE_FAILED, "the delete job didn't start: 403 forbidden", True)


def test_the_status_cell_of_a_pending_or_failed_delete():
    assert ij.delete_status({"state": ij.DELETING, "reason": None, "run": None}) == {
        "label": "Deleting", "tone": "run", "detail": ["starting its delete job"], "cause": None}
    assert ij.delete_status({"state": ij.DELETING, "reason": None, "run": {"run_id": 9}})["detail"] == [
        "its delete job is running"]
    assert ij.delete_status({"state": ij.DELETE_FAILED, "reason": "403 forbidden", "run": None}) == {
        "label": "Delete failed", "tone": "bad", "detail": ["403 forbidden"], "cause": None}


def _job(**options):
    """A deleted schedule row, with the options a Delete records."""
    return {"id": 1, "name": "East hourly", "deleted_at": DELETED_AT.isoformat(),
            "options": {"delete_drop_targets": True, **options}}
