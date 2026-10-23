"""Starting lakefed's own ingest runs for the collections the check found work in, with run-now and no For each."""
import json
import re
from pathlib import Path

import pytest

import launcher


class _Jobs:
    """A fake of the Jobs API calls the launcher makes. ``script`` gives each collection's run the
    states its polls return, one per poll (the last one repeats): RUNNING, or a result state."""

    def __init__(self, script=None, active=(), start_errors=(), poll_errors=()):
        self.script = dict(script or {})
        self.active = list(active)
        self.start_errors = set(start_errors)
        self.poll_errors = dict(poll_errors)
        self.started, self.cancelled, self.polls = [], [], []
        self.running_now, self.most_running = set(), 0
        self._runs = {}

    def run_now(self, job_id, job_parameters, idempotency_token):
        collection = job_parameters["task_collection"]
        if collection in self.start_errors:
            raise RuntimeError(f"cannot start {collection}")
        run_id = 100 + len(self.started)
        self.started.append({"job_id": job_id, "job_parameters": dict(job_parameters), "token": idempotency_token,
                             "run_id": run_id})
        self._runs[run_id] = {"collection": collection, "states": list(self.script.get(collection, ["SUCCESS"]))}
        self.running_now.add(run_id)
        self.most_running = max(self.most_running, len(self.running_now))
        return run_id

    def get_run(self, run_id):
        self.polls.append(run_id)
        run = self._runs[run_id]
        if self.poll_errors.get(run["collection"], 0) > 0:
            self.poll_errors[run["collection"]] -= 1
            raise RuntimeError("temporarily unavailable")
        state = run["states"].pop(0) if len(run["states"]) > 1 else run["states"][0]
        if state in ("RUNNING", "PENDING"):
            return {"life_cycle_state": state, "result_state": None, "state_message": "",
                    "run_page_url": f"https://ws/run/{run_id}"}
        self.running_now.discard(run_id)
        if state == "SKIPPED":
            return {"life_cycle_state": "SKIPPED", "result_state": None, "state_message": "max concurrent runs",
                    "run_page_url": f"https://ws/run/{run_id}"}
        return {"life_cycle_state": "TERMINATED", "result_state": state, "state_message": f"{state.lower()} msg",
                "run_page_url": f"https://ws/run/{run_id}"}

    def cancel_run(self, run_id):
        self.cancelled.append(run_id)
        self.running_now.discard(run_id)

    def active_runs(self, job_id):
        return [{"run_id": 1, "job_parameters": {"task_collection": c}} for c in self.active]

    def max_concurrent_runs(self, job_id):
        return 100


class _Clock:
    """A clock that a fake sleep moves on."""

    def __init__(self):
        self.now, self.sleeps = 0.0, []

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds

    def __call__(self):
        return self.now


def _per_collection(**work):
    """run_check's per_collection: each collection's databases with work, as [source, database] pairs."""
    return [{"collection": c, "work": [["s1", db] for db in dbs]} for c, dbs in work.items()]


def test_one_run_per_collection_with_work_given_its_work_list():
    requests = launcher.run_requests(_per_collection(c2=["db9", "db3"], c1=["db1"], c3=[]),
                                     parameters={"parallelism": "192", "freshness_interval_s": "0"}, launch_id="555")
    assert [r["collection"] for r in requests] == ["c1", "c2"]          # c3 had no work: no run
    assert requests[1] == {"collection": "c2", "databases": 2, "work": [("s1", "db3"), ("s1", "db9")],
                           "work_id": "555:c2",
                           "job_parameters": {"parallelism": "192", "freshness_interval_s": "0",
                                              "task_collection": "c2", "work_id": "555:c2"}}


def test_runs_get_only_the_ingest_jobs_parameters_that_are_set():
    values = {"parallelism": "192", "freshness_interval_s": "0", "db_name": "", "spark_conf": "{}", "work": "x"}
    assert launcher.run_parameters(values) == {"parallelism": "192", "freshness_interval_s": "0", "spark_conf": "{}"}


def test_the_ingest_job_declares_every_parameter_the_launcher_passes():
    job = (Path(__file__).parent.parent / "resources" / "lakefed_ingest.yml").read_text()
    declared = set(re.findall(r"^\s+- name: (\w+)$", job, re.M))
    assert set(launcher.RUN_PARAMETERS) | {"task_collection", "work_id"} <= declared
    assert 'work_id: "{{job.parameters.work_id}}"' in job     # handed on to ingest.ipynb


def _code_cells(notebook):
    nb = json.loads((Path(__file__).parent.parent / "src" / "lakefed_ingest" / notebook).read_text())
    return [c["source"] for c in nb["cells"] if c["cell_type"] == "code"]


def test_check_launch_sets_every_run_parameter_and_compiles():
    cells = _code_cells("check_launch.ipynb")
    code = "\n".join(cells)
    assert all(f'"{name}"' in code for name in launcher.RUN_PARAMETERS)
    assert {"task_collections", "ingest_job_id", "max_running", "wait", "confirm_s", "poll_s", "timeout_s",
            "launch_id"} <= set(re.findall(r'widget\("(\w+)"', code))
    for cell in cells:
        if not cell.lstrip().startswith("%"):
            compile(cell, "check_launch.ipynb", "exec")
    ingest_widgets = set(re.findall(r'widget\("(\w+)"', "\n".join(_code_cells("ingest.ipynb"))))
    assert set(launcher.RUN_PARAMETERS) | {"task_collection", "work_id"} == ingest_widgets


def test_no_work_starts_nothing():
    jobs, clock = _Jobs(), _Clock()
    summary = launcher.launch(jobs, 7, launcher.run_requests(_per_collection(c1=[])), sleep=clock.sleep, clock=clock)
    assert jobs.started == [] and summary["runs"] == [] and clock.sleeps == []
    assert launcher.failures(summary) == []


def test_the_token_is_stable_per_attempt_and_collection_and_fits_the_api():
    # One token per attempt of the launching step and collection: the start call's own retries get
    # back the same run, and a retried or repaired step starts afresh (active runs are skipped).
    token = launcher.idempotency_token("555", "c1")
    assert token == launcher.idempotency_token("555", "c1")
    assert token != launcher.idempotency_token("556", "c1") != launcher.idempotency_token("555", "c2")
    assert len(token) <= 64
    assert len(launcher.idempotency_token("5" * 40, "a_collection_with_a_long_name" * 4)) <= 64


def test_waits_for_every_run_and_lets_the_others_finish_after_a_failure():
    jobs, clock = _Jobs({"c1": ["RUNNING", "FAILED"], "c2": ["RUNNING", "RUNNING", "RUNNING", "SUCCESS"]}), _Clock()
    requests = launcher.run_requests(_per_collection(c1=["a"], c2=["b"]))
    summary = launcher.launch(jobs, 7, requests, wait=True, launch_id="555", poll_s=30, sleep=clock.sleep, clock=clock)
    assert [s["token"] for s in jobs.started] == [launcher.idempotency_token("555", "c1"),
                                                  launcher.idempotency_token("555", "c2")]
    assert jobs.cancelled == []                            # c1's failure does not stop c2
    by = {r["collection"]: r for r in summary["runs"]}
    assert by["c1"]["outcome"] == "failed" and by["c1"]["run_id"] == 100 and by["c1"]["url"] == "https://ws/run/100"
    assert by["c2"]["outcome"] == "succeeded"
    assert summary["counts"] == {"failed": 1, "succeeded": 1}
    assert set(clock.sleeps) == {30}
    failed = launcher.failures(summary)
    assert [f["collection"] for f in failed] == ["c1"]
    assert "c1" in launcher.failure_message(failed) and "https://ws/run/100" in launcher.failure_message(failed)


def test_at_most_max_running_at_once_and_the_next_starts_as_one_ends():
    jobs, clock = _Jobs({"c1": ["RUNNING", "SUCCESS"], "c2": ["RUNNING", "RUNNING", "RUNNING", "SUCCESS"],
                         "c3": ["SUCCESS"]}), _Clock()
    requests = launcher.run_requests(_per_collection(c1=["a"], c2=["b"], c3=["c"]))
    summary = launcher.launch(jobs, 7, requests, wait=True, max_running=2, sleep=clock.sleep, clock=clock)
    assert jobs.most_running == 2
    assert [s["job_parameters"]["task_collection"] for s in jobs.started] == ["c1", "c2", "c3"]
    assert summary["counts"] == {"succeeded": 3}


def test_a_run_databricks_skipped_or_could_not_start_is_reported_and_fails_the_step():
    jobs, clock = _Jobs({"c1": ["SKIPPED"], "c3": ["MAXIMUM_CONCURRENT_RUNS_REACHED"]}, start_errors={"c2"}), _Clock()
    requests = launcher.run_requests(_per_collection(c1=["a"], c2=["b"], c3=["c"], c4=["d"]))
    summary = launcher.launch(jobs, 7, requests, wait=True, sleep=clock.sleep, clock=clock)
    by = {r["collection"]: r for r in summary["runs"]}
    assert by["c1"]["outcome"] == "skipped" and by["c3"]["outcome"] == "skipped"
    assert by["c2"]["outcome"] == "start_failed" and "cannot start c2" in by["c2"]["state_message"]
    assert by["c2"]["run_id"] is None
    assert by["c4"]["outcome"] == "succeeded"
    assert sorted(f["collection"] for f in launcher.failures(summary)) == ["c1", "c2", "c3"]


def test_a_collection_with_a_run_going_is_not_started_again():
    jobs, clock = _Jobs(active=["c1"]), _Clock()
    requests = launcher.run_requests(_per_collection(c1=["a"], c2=["b"]))
    summary = launcher.launch(jobs, 7, requests, already_running=launcher.active_collections(jobs.active_runs(7)),
                              sleep=clock.sleep, clock=clock)
    assert [s["job_parameters"]["task_collection"] for s in jobs.started] == ["c2"]
    by = {r["collection"]: r for r in summary["runs"]}
    assert by["c1"]["outcome"] == "already_running" and by["c1"]["run_id"] is None
    assert launcher.failures(summary) == []                # its own run carries on with that work


def test_active_collections_reads_the_runs_parameters():
    runs = [{"run_id": 1, "job_parameters": {"task_collection": "c1", "parallelism": "8"}},
            {"run_id": 2, "job_parameters": {}}, {"run_id": 3, "job_parameters": {"task_collection": " c2 "}}]
    assert launcher.active_collections(runs) == {"c1", "c2"}


def test_a_stopped_step_cancels_the_runs_it_started_that_are_still_going():
    jobs = _Jobs({"c1": ["SUCCESS"], "c2": ["RUNNING"], "c3": ["RUNNING"]})
    polls = []

    def sleep(seconds):
        polls.append(seconds)
        if len(polls) == 2:
            raise KeyboardInterrupt("cancelled")           # the step is cancelled while it waits

    requests = launcher.run_requests(_per_collection(c1=["a"], c2=["b"], c3=["c"]))
    with pytest.raises(KeyboardInterrupt):
        launcher.launch(jobs, 7, requests, wait=True, sleep=sleep, clock=lambda: 0.0)
    assert sorted(jobs.cancelled) == [101, 102]            # c1 had finished; c2 and c3 are cancelled


def test_a_timeout_cancels_what_is_still_going_and_fails_the_step():
    jobs, clock = _Jobs({"c1": ["RUNNING", "SUCCESS"], "c2": ["RUNNING"]}), _Clock()
    requests = launcher.run_requests(_per_collection(c1=["a"], c2=["b"]))
    summary = launcher.launch(jobs, 7, requests, wait=True, poll_s=60, timeout_s=150, sleep=clock.sleep, clock=clock)
    by = {r["collection"]: r for r in summary["runs"]}
    assert by["c1"]["outcome"] == "succeeded"
    assert by["c2"]["outcome"] == "timed_out" and jobs.cancelled == [101]
    assert [f["collection"] for f in launcher.failures(summary)] == ["c2"]


def test_a_failed_poll_is_tried_again_on_the_next_one():
    jobs, clock = _Jobs({"c1": ["RUNNING", "SUCCESS"]}, poll_errors={"c1": 2}), _Clock()
    summary = launcher.launch(jobs, 7, launcher.run_requests(_per_collection(c1=["a"])), wait=True, sleep=clock.sleep,
                              clock=clock)
    assert summary["runs"][0]["outcome"] == "succeeded" and summary["runs"][0]["poll_errors"] == 2
    assert jobs.cancelled == []


def test_by_default_every_run_is_started_and_the_check_ends_once_they_have_started():
    jobs, clock = _Jobs({"c1": ["PENDING"], "c2": ["PENDING", "RUNNING"], "c3": ["RUNNING"]}), _Clock()
    requests = launcher.run_requests(_per_collection(c1=["a"], c2=["b"], c3=["c"]))
    summary = launcher.launch(jobs, 7, requests, max_running=1, confirm_s=30, sleep=clock.sleep, clock=clock)
    assert len(jobs.started) == 3                            # max_running is for waiting only
    assert summary["counts"] == {"started": 3} and launcher.failures(summary) == []
    assert clock.now == 30 and set(clock.sleeps) == {5}    # watched for confirm_s, then the check ends
    assert jobs.cancelled == []


def test_a_run_databricks_skips_at_once_is_caught_before_the_check_ends():
    # The ingestion job's queue is off: at its max_concurrent_runs, Databricks skips a new run.
    jobs, clock = _Jobs({"c1": ["RUNNING"], "c2": ["SKIPPED"]}), _Clock()
    requests = launcher.run_requests(_per_collection(c1=["a"], c2=["b"]))
    summary = launcher.launch(jobs, 7, requests, confirm_s=30, sleep=clock.sleep, clock=clock)
    by = {r["collection"]: r for r in summary["runs"]}
    assert by["c1"]["outcome"] == "started" and by["c2"]["outcome"] == "skipped"
    assert [f["collection"] for f in launcher.failures(summary)] == ["c2"]
    assert "c2: skipped" in launcher.failure_message(launcher.failures(summary))


def test_it_starts_no_more_runs_than_the_ingestion_job_has_room_for():
    jobs, clock = _Jobs({"c1": ["RUNNING"], "c2": ["RUNNING"], "c3": ["RUNNING"]}, active=["c9"] * 98), _Clock()
    room = launcher.capacity(jobs.max_concurrent_runs(7), len(jobs.active_runs(7)))
    assert room == 2 and launcher.capacity(100, 140) == 0
    requests = launcher.run_requests(_per_collection(c1=["a"], c2=["b"], c3=["c"]))
    summary = launcher.launch(jobs, 7, requests, room=room, confirm_s=0, sleep=clock.sleep, clock=clock)
    assert [s["job_parameters"]["task_collection"] for s in jobs.started] == ["c1", "c2"]
    by = {r["collection"]: r for r in summary["runs"]}
    assert by["c3"]["outcome"] == "not_started" and "maximum concurrent runs" in by["c3"]["state_message"]
    assert [f["collection"] for f in launcher.failures(summary)] == ["c3"]


def test_the_sdk_adapter_maps_the_jobs_api():
    class _State:
        def __init__(self, life, result, message):
            self.life_cycle_state = type("E", (), {"value": life})()
            self.result_state = type("E", (), {"value": result})() if result else None
            self.state_message = message

    class _Run:
        def __init__(self, run_id, collection):
            self.run_id, self.run_page_url = run_id, f"https://ws/run/{run_id}"
            self.state = _State("TERMINATED", "SUCCESS", "")
            self.job_parameters = [type("P", (), {"name": "task_collection", "value": collection})()]

    class _Wait:
        run_id = 42

    calls = []

    class _JobsApi:
        def get(self, job_id):
            return type("J", (), {"settings": type("S", (), {"max_concurrent_runs": 100})()})()

        def run_now(self, **kw):
            calls.append(("run_now", kw))
            return _Wait()

        def get_run(self, run_id):
            return _Run(run_id, "c1")

        def cancel_run(self, run_id):
            calls.append(("cancel_run", run_id))

        def list_runs(self, **kw):
            calls.append(("list_runs", kw))
            return iter([_Run(5, "c9")])

    sdk = launcher.SdkJobs(type("W", (), {"jobs": _JobsApi()})())
    assert sdk.run_now(7, {"task_collection": "c1"}, "tok") == 42
    assert calls[0] == ("run_now", {"job_id": 7, "job_parameters": {"task_collection": "c1"},
                                    "idempotency_token": "tok"})
    assert sdk.get_run(42) == {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS", "state_message": "",
                               "run_page_url": "https://ws/run/42"}
    sdk.cancel_run(42)
    assert calls[1] == ("cancel_run", 42)
    assert sdk.active_runs(7) == [{"run_id": 5, "job_parameters": {"task_collection": "c9"}}]
    assert calls[2] == ("list_runs", {"job_id": 7, "active_only": True})
    assert sdk.max_concurrent_runs(7) == 100


def test_the_check_job_feeds_every_widget_and_brings_its_own_packages():
    job = (Path(__file__).parent.parent / "resources" / "lakefed_ingest_check.yml").read_text()
    passed = set(re.findall(r"^\s+([a-z_]+): ", job.split("base_parameters:")[1].split("environments:")[0], re.M))
    widgets = set(re.findall(r'widget\("(\w+)"', "\n".join(_code_cells("check_launch.ipynb"))))
    tuning_only = {"check_parallelism", "force_databases", "wait", "confirm_s", "max_running", "poll_s", "timeout_s"}
    assert (widgets | set(launcher.RUN_PARAMETERS)) - tuning_only == passed   # the rest keep their notebook defaults
    assert '"psycopg[binary]"' in job and "environment_key: lakebase_client" in job
    assert not any(c.lstrip().startswith("%pip") for c in _code_cells("check_launch.ipynb"))
    assert "for_each_task" not in job
