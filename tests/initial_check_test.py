"""The check: which databases of a group of collections need an ingest, and the work list of each."""
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import ct_engine
import initial_check as ic
import planner
import state_store as ss

NOW = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)
SOURCE = "conn|0|conn"   # state_store.database_source of _cfg: connection-backed, so its blank port stays 0


def _cfg(cid, db, table, collection="c1"):
    return {"id": cid, "task_collection": collection, "src_connection": "conn", "use_remote_query": True,
            "src_database": db, "src_schema": "dbo", "src_table": table, "select_list": "id, v",
            "primary_key": "id", "src_type": "sqlserver", "load_type": "ct",
            "sink_catalog": "main", "sink_schema": "bronze", "sink_table": f"{db}_{table}"}


def _sinks(configs):
    """The sink tables that exist for these rows: every one of them, unless a test drops one."""
    return {ct_engine.sink_lookup_key(c) for c in configs}


def _cp(v, phase=ss.INCREMENTAL, **kw):
    return {"position": None if v is None else str(v), "phase": phase, "status": "ok", "select_list": "id, v",
            "strategy": "ct", "ct_begin_version": 7, "last_success_at": NOW - timedelta(hours=1), **kw}


def _recorded(configs, checkpoints):
    """The checkpoints with each row's identity recorded, as a run under the identity backstop leaves them."""
    by_id = {c["id"]: c for c in configs}
    names = ("source_identity", "sink_identity", "merge_key")
    return {cid: {**cp, **dict(zip(names, ss.identity_values(by_id[cid])))} for cid, cp in checkpoints.items()}


def _detector(versions, changed=frozenset(), calls=None):
    """detect_one for the fakes: each database's CT version, and the tables with a change."""
    def detect_one(members):
        db = members[0][0]["src_database"]
        if calls is not None:
            calls.append(db)
        cur = versions[db]
        state = lambda c: (cur, {("dbo", m["src_table"]): (1, 7) for m, _ in members})
        detect = lambda c, items: ({cid: (1, int(cid in changed)) for cid, *_ in items}, [], 1)
        return planner.detect_group(members, None, detect, database_state=state)
    return detect_one


def test_only_databases_that_changed_or_were_never_loaded_need_an_ingest():
    configs = [_cfg(1, "quiet", "a"), _cfg(2, "quiet", "b"),
               _cfg(3, "busy", "a"), _cfg(4, "busy", "b"),
               _cfg(5, "new", "a"), _cfg(6, "new", "b"),
               _cfg(7, "moved", "a"),
               _cfg(8, "down", "a")]
    cps = _recorded(configs, {1: _cp(10), 2: _cp(10), 3: _cp(20), 4: _cp(20), 5: _cp(30), 7: _cp(40), 8: _cp(50)})
    calls = []
    detect = _detector({"quiet": 10, "busy": 21, "moved": 41, "down": None}, changed={3}, calls=calls)
    out = ic.check_collection(configs, cps, detect, existing_sinks=_sinks(configs), now=NOW)
    # quiet: fast exit. busy: table 3 changed. new: table 6 was never loaded (no detection asked).
    # moved: the version moved but none of our tables changed. down: no CT version, the ingest reports it.
    assert out["work"] == [[SOURCE, "busy"], [SOURCE, "down"], [SOURCE, "new"]]
    assert sorted(calls) == ["busy", "down", "moved", "quiet"]
    assert out["unchanged_databases"] == 2 and out["unchanged_tables"] == 3
    # moved advances its table to 41 (a position move needs its own row); quiet gets one stamp
    assert [(r[0], r[1]) for r in out["rows"]] == [(7, 41)]
    assert [s[1:] for s in out["database_stamps"]] == [("quiet", 10, 2)]   # moved: every row writes itself
    assert out["work_tables"] == 5


def test_forced_databases_skip_the_check_and_go_to_the_ingest():
    configs = [_cfg(1, "quiet", "a"), _cfg(2, "other", "a")]
    calls = []
    out = ic.check_collection(configs, _recorded(configs, {1: _cp(10), 2: _cp(10)}),
                              _detector({"quiet": 10, "other": 10}, calls=calls),
                              existing_sinks=_sinks(configs), force={"quiet"}, now=NOW)
    assert out["work"] == [[SOURCE, "quiet"]] and calls == ["other"]
    assert [s[1] for s in out["database_stamps"]] == ["other"]


def test_a_dropped_sink_sends_its_database_to_the_ingest_not_the_stamp():
    configs = [_cfg(1, "quiet", "a"), _cfg(2, "quiet", "b")]
    calls = []
    present = _sinks(configs) - {ct_engine.sink_lookup_key(configs[0])}     # quiet.a's sink was dropped
    out = ic.check_collection(configs, _recorded(configs, {1: _cp(10), 2: _cp(10)}), _detector({"quiet": 10}, calls=calls),
                              existing_sinks=present, now=NOW)
    assert out["work"] == [[SOURCE, "quiet"]] and calls == []
    assert out["rows"] == [] and out["database_stamps"] == [] and out["unchanged_databases"] == 0


def test_unknown_sinks_send_every_database_with_a_candidate_to_the_ingest():
    configs = [_cfg(1, "quiet", "a")]
    out = ic.check_collection(configs, _recorded(configs, {1: _cp(10)}), _detector({"quiet": 10}),
                              existing_sinks=None, now=NOW)
    assert out["work"] == [[SOURCE, "quiet"]] and out["database_stamps"] == []


def test_the_check_needs_the_existing_sinks_it_is_given():
    configs = [_cfg(1, "quiet", "a")]
    try:
        ic.check_collection(configs, _recorded(configs, {1: _cp(10)}), _detector({"quiet": 10}), now=NOW)
        raise AssertionError("a check without sink information must not stamp anything")
    except TypeError:
        pass


def test_a_run_check_sends_a_database_with_a_dropped_sink_to_the_ingest():
    configs = [_cfg(1, "quiet", "a", "c1")]
    store = _Store(configs, _recorded(configs, {1: _cp(10)}))
    summary = ic.run_check(store, store, ["c1"], _detector({"quiet": 10}), existing_sinks_of=lambda cfgs: set())
    assert summary["per_collection"][0]["work"] == [[SOURCE, "quiet"]]
    assert store.stamped == {"c1": []}


def test_freshness_skips_a_fresh_database_stamp():
    configs = [_cfg(1, "quiet", "a")]
    stamps = {(ss.database_source(configs[0]), "quiet"): {"version": 10, "checked_at": NOW - timedelta(minutes=5)}}
    out = ic.check_collection(configs, _recorded(configs, {1: _cp(10)}), _detector({"quiet": 10}),
                              existing_sinks=_sinks(configs), stamps=stamps, interval_s=3600, now=NOW)
    assert out["work"] == [] and out["rows"] == [] and out["database_stamps"] == []
    assert out["unchanged_databases"] == 1














class _Store:
    """The Lakebase calls the check makes, recorded; one collection's rows per name."""

    def __init__(self, configs, checkpoints, held=()):
        self.configs, self.checkpoints, self.held = configs, checkpoints, set(held)
        self.runs, self.finished, self.advanced, self.stamped, self.unlocked = [], {}, [], {}, []
        self.closed, self.work_ids = [], []
        self.alive = True

    def mixed_sinks(self):
        return set()

    def try_lock_collection(self, collection):
        return collection not in self.held

    def unlock_collection(self, collection):
        self.unlocked.append(collection)

    def start_run(self, collection, job_run_id, cluster_id, parallelism, work_id=None):
        self.runs.append((collection, job_run_id, cluster_id, parallelism))
        self.work_ids.append(work_id)
        return len(self.runs)

    def list_task_configs(self, collection):
        return [c for c in self.configs if c["task_collection"] == collection]

    def get_checkpoints(self, ids):
        return {i: self.checkpoints[i] for i in ids if i in self.checkpoints}

    def get_database_checks(self, collection):
        return {}

    def bulk_advance_unchanged(self, rows):
        self.advanced.extend(rows)

    def stamp_databases(self, collection, stamps, run_id):
        self.stamped[collection] = [s[1] for s in stamps]

    def finish_run(self, run_id, **kw):
        self.finished[run_id] = kw

    def close_abandoned_runs(self, collection, run_id):
        self.closed.append((collection, run_id))
        return (0, 0)

    def is_alive(self):
        return self.alive


def test_the_check_stamps_the_quiet_databases_and_lists_each_collections_work():
    configs = [_cfg(1, "quiet", "a", "c1"), _cfg(2, "quiet", "b", "c1"),
               _cfg(3, "new", "a", "c2"), _cfg(4, "old", "a", "c2")]
    store = _Store(configs, _recorded(configs, {1: _cp(10), 2: _cp(10), 4: _cp(10)}))
    summary = ic.run_check(store, store, ["c1", "c2"], _detector({"quiet": 10, "old": 10}),
                           existing_sinks_of=_sinks, job_run_id="77", cluster_id="serverless", parallelism=4)
    assert [(r["collection"], r["work"]) for r in summary["per_collection"]] == [("c1", []), ("c2", [[SOURCE, "new"]])]
    assert store.stamped == {"c1": ["quiet"], "c2": ["old"]}
    assert sorted(store.unlocked) == ["c1", "c2"]
    assert [r[0] for r in store.runs] == ["c1", "c2"] and {r[1:] for r in store.runs} == {("77", "serverless", 4)}
    detail = json.loads(store.finished[2]["detail"])["check"]
    assert detail["work"] == [[SOURCE, "new"]] and store.finished[2]["status"] == "completed"
    assert summary["collections_with_work"] == 1 and summary["databases_with_work"] == 1 and summary["busy"] == []
    assert summary["unchanged_databases"] == 2 and summary["database_stamps"] == 2
    # The check's own record is marked from the start, so the app tells it from a group run while it is running.
    assert store.work_ids == ["check-77", "check-77"]


def test_the_check_leaves_a_collection_being_swept_to_that_sweep():
    configs = [_cfg(1, "new", "a", "c1")]
    store = _Store(configs, {}, held={"c1"})
    summary = ic.run_check(store, store, ["c1"], _detector({}), existing_sinks_of=_sinks)
    assert summary["busy"] == ["c1"] and summary["per_collection"] == [] and store.runs == [] and store.unlocked == []


def test_the_check_releases_a_collections_lock_when_its_check_fails():
    def broken(members):
        raise RuntimeError("source unreachable")

    configs = [_cfg(1, "quiet", "a", "c1")]
    store = _Store(configs, _recorded(configs, {1: _cp(10)}))
    summary = ic.run_check(store, store, ["c1"], broken, existing_sinks_of=_sinks)
    assert summary["failed"] == [{"collection": "c1", "error": "RuntimeError: source unreachable"}]
    assert summary["per_collection"] == [] and store.unlocked == ["c1"]
    assert store.finished[1]["status"] == "failed" and "source unreachable" in store.finished[1]["detail"]
    assert store.closed == [("c1", 1)]
    assert "c1: RuntimeError: source unreachable" in ic.failure_message(summary["failed"])


def test_one_collection_whose_check_fails_does_not_stop_the_others():
    def detect(members):
        if members[0][0]["src_database"] == "bad":
            raise ValueError("malformed control row")
        return _detector({"quiet": 10})(members)

    configs = [_cfg(1, "bad", "a", "c1"), _cfg(2, "quiet", "a", "c2"), _cfg(3, "new", "a", "c3")]
    store = _Store(configs, _recorded(configs, {1: _cp(10), 2: _cp(10)}))
    summary = ic.run_check(store, store, ["c1", "c2", "c3"], detect, existing_sinks_of=_sinks)
    assert [f["collection"] for f in summary["failed"]] == ["c1"]
    assert [(r["collection"], r["work"]) for r in summary["per_collection"]] == [("c2", []), ("c3", [[SOURCE, "new"]])]
    assert store.stamped["c2"] == ["quiet"] and sorted(store.unlocked) == ["c1", "c2", "c3"]


def test_a_collection_with_work_keeps_its_lock_until_its_run_is_started():
    configs = [_cfg(1, "quiet", "a", "c1"), _cfg(2, "new", "a", "c2")]
    store = _Store(configs, _recorded(configs, {1: _cp(10)}))
    summary = ic.run_check(store, store, ["c1", "c2"], _detector({"quiet": 10}), existing_sinks_of=_sinks,
                           hold_work_locks=True)
    # c2 has work: its lock stays held, so a second check of c2 meanwhile finds it busy and starts nothing.
    assert summary["held"] == ["c2"] and store.unlocked == ["c1"]
    store.held.add("c2")
    again = ic.run_check(store, store, ["c2"], _detector({}), existing_sinks_of=_sinks, hold_work_locks=True)
    assert again["busy"] == ["c2"] and again["per_collection"] == []
    ic.release_locks(store, summary["held"])
    assert store.unlocked == ["c1", "c2"]


def test_the_checks_source_queries_take_their_share_of_the_per_server_cap():
    import threading
    import time as clock

    joined, in_flight, most = [], [0], [0]
    mu = threading.Lock()

    def detect(members):
        with mu:
            in_flight[0] += 1
            most[0] = max(most[0], in_flight[0])
        clock.sleep(0.02)
        with mu:
            in_flight[0] -= 1
        return _detector({c["src_database"]: 10 for c, _ in members})(members)

    # Two sessions read the server (an ingest run and this check): a cap of 4 gives the check 2 at a time.
    cap = ic.SourceCap(4, join=lambda sources: joined.extend(sources) or {s: 11 for s in sources},
                       runs=lambda keys: {11: 2}, source_of=lambda cfg: "connection:sql_a")
    configs = [_cfg(i, f"db{i}", "a", "c1") for i in range(1, 9)]
    store = _Store(configs, _recorded(configs, {i: _cp(10) for i in range(1, 9)}))
    with ThreadPoolExecutor(max_workers=8) as pool:
        summary = ic.run_check(store, store, ["c1"], detect, existing_sinks_of=_sinks, map_fn=pool.map,
                               source_cap=cap)
    assert joined == ["connection:sql_a"] and cap.limiter.limit("connection:sql_a") == 2
    assert most[0] == 2 and summary["unchanged_databases"] == 8


def test_without_a_cap_the_check_leaves_detection_as_it_is():
    cap = ic.SourceCap(0, join=None, runs=None, source_of=None)
    detect = _detector({})
    assert cap.wrap([], detect) is detect


def test_when_the_sessions_cant_be_counted_the_check_takes_the_whole_cap():
    def refuse(keys):
        raise RuntimeError("pg_locks unavailable")

    cap = ic.SourceCap(6, join=lambda sources: {s: 1 for s in sources}, runs=refuse, source_of=lambda cfg: "host:h:1433")
    cap.wrap([_cfg(1, "db1", "a")], _detector({}))
    assert cap.limiter.limit("host:h:1433") == 6


def test_a_check_closes_the_runs_a_dead_sweep_left_open_once_it_holds_the_lock():
    configs = [_cfg(1, "quiet", "a", "c1")]
    store = _Store(configs, _recorded(configs, {1: _cp(10)}))
    summary = ic.run_check(store, store, ["c1"], _detector({"quiet": 10}), existing_sinks_of=_sinks)
    assert store.closed == [("c1", 1)] and summary["per_collection"][0]["run_id"] == 1


def test_a_check_whose_lock_connection_dropped_writes_nothing_and_fails():
    configs = [_cfg(1, "quiet", "a", "c1")]
    store = _Store(configs, _recorded(configs, {1: _cp(10)}))
    store.alive = False                       # the session lock went with the dropped connection
    summary = ic.run_check(store, store, ["c1"], _detector({"quiet": 10}), existing_sinks_of=_sinks)
    assert [f["collection"] for f in summary["failed"]] == ["c1"] and "lock" in summary["failed"][0]["error"]
    assert summary["per_collection"] == [] and store.advanced == [] and store.stamped == {}
    assert store.finished[1]["status"] == "failed" and store.unlocked == ["c1"]


def test_a_failed_unlock_is_reported_and_does_not_hide_the_check_result():
    configs = [_cfg(1, "quiet", "a", "c1")]
    store = _Store(configs, _recorded(configs, {1: _cp(10)}))

    def dead_unlock(collection):
        raise RuntimeError("connection closed")

    store.unlock_collection = dead_unlock
    summary = ic.run_check(store, store, ["c1"], _detector({"quiet": 10}), existing_sinks_of=_sinks)
    assert summary["per_collection"][0]["work"] == [] and store.finished[1]["status"] == "completed"

    def broken(members):
        raise RuntimeError("source unreachable")

    summary = ic.run_check(store, store, ["c1"], broken, existing_sinks_of=_sinks)
    assert "source unreachable" in summary["failed"][0]["error"]


def test_a_failing_close_of_abandoned_runs_does_not_stop_the_check():
    configs = [_cfg(1, "quiet", "a", "c1")]
    store = _Store(configs, _recorded(configs, {1: _cp(10)}))

    def broken_close(collection, run_id):
        raise RuntimeError("telemetry down")

    store.close_abandoned_runs = broken_close
    summary = ic.run_check(store, store, ["c1"], _detector({"quiet": 10}), existing_sinks_of=_sinks)
    assert summary["per_collection"][0]["work"] == [] and store.finished[1]["status"] == "completed"
