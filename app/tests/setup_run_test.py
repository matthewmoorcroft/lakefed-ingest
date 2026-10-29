"""The Schema setup section's run (GET /api/jobs/setup/runs/{id}): pending while Databricks lists the setup run going,
succeeded or failed with its message once it has ended, and a timeout that is not done past SETUP_LIMIT_S. Only the
setup job's runs are read. The run's state is stubbed; no workspace is reached."""
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

import main
import run_poller

SETUP_JOB = "9"
NOW = datetime.now(timezone.utc)


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(main.jobs, "SETUP_JOB_ID", SETUP_JOB)
    return TestClient(main.app)


def _state(life_cycle, result=None, started=NOW - timedelta(minutes=3), message=None, job_id=SETUP_JOB):
    return {"run_id": 41, "job_id": job_id, "state": life_cycle, "result": result, "message": message,
            "url": "https://h/run/41", "started": started}


def _answer(monkeypatch, state):
    monkeypatch.setattr(run_poller, "run_state_of", lambda run_id: state)


def test_a_setup_run_still_going_is_pending(client, monkeypatch):
    _answer(monkeypatch, _state("RUNNING"))
    outcome = client.get("/api/jobs/setup/runs/41").json()["outcome"]
    assert (outcome["state"], outcome["reason"], outcome["url"]) == ("pending", None, "https://h/run/41")


def test_a_setup_run_not_recorded_yet_is_pending(client, monkeypatch):
    _answer(monkeypatch, _state("PENDING", started=None))
    assert client.get("/api/jobs/setup/runs/41").json()["outcome"]["state"] == "pending"


def test_a_setup_run_that_succeeded_is_succeeded(client, monkeypatch):
    _answer(monkeypatch, _state("TERMINATED", "SUCCESS"))
    assert client.get("/api/jobs/setup/runs/41").json()["outcome"]["state"] == "succeeded"


def test_a_setup_run_that_failed_is_failed_with_databricks_message(client, monkeypatch):
    _answer(monkeypatch, _state("TERMINATED", "FAILED", message="schema migration lock timed out"))
    outcome = client.get("/api/jobs/setup/runs/41").json()["outcome"]
    assert (outcome["state"], outcome["reason"]) == ("failed", "schema migration lock timed out")


def test_a_setup_run_still_listed_past_its_limit_is_a_timeout_and_not_done(client, monkeypatch):
    _answer(monkeypatch, _state("RUNNING", started=NOW - timedelta(minutes=31)))
    outcome = client.get("/api/jobs/setup/runs/41").json()["outcome"]
    assert outcome["state"] == "timeout"
    assert outcome["reason"] == "Databricks has not reported it ended after 30 minutes: it is still listed as running"


def test_another_jobs_run_is_not_a_setup_run(client, monkeypatch):
    _answer(monkeypatch, _state("TERMINATED", "SUCCESS", job_id="77"))
    assert client.get("/api/jobs/setup/runs/41").status_code == 404


def test_a_run_databricks_does_not_have_is_a_404(client, monkeypatch):
    def missing(run_id):
        raise LookupError("no such run")
    monkeypatch.setattr(run_poller, "run_state_of", missing)
    monkeypatch.setattr(main.jobs, "is_not_found", lambda error: True)
    assert client.get("/api/jobs/setup/runs/41").status_code == 404


def test_the_setup_run_databricks_lists_going_is_read_from_the_run_table(client, monkeypatch):
    asked = []
    monkeypatch.setattr(run_poller, "runs_of", lambda job_id, active_only=False, limit=25: asked.append(
        (str(job_id), active_only, limit)) or [{"run_id": 41, "state": "RUNNING"}])
    assert client.get("/api/jobs/setup/runs/active").json() == {"run_id": 41}
    assert asked == [(SETUP_JOB, True, 1)]


def test_no_setup_run_going_answers_no_run(client, monkeypatch):
    monkeypatch.setattr(run_poller, "runs_of", lambda job_id, active_only=False, limit=25: [])
    assert client.get("/api/jobs/setup/runs/active").json() == {"run_id": None}

