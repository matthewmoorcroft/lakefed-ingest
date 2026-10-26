"""The check: which databases of a group of collections need an ingest, and the work list of each."""
import json
from datetime import datetime, timedelta, timezone

import ct_engine
import initial_check as ic
import planner
import state_store as ss

NOW = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)
SOURCE = "conn|0|conn"   # state_store.database_source of _cfg


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
        self.closed = []
        self.alive = True

    def mixed_sinks(self):
        return set()

    def try_lock_collection(self, collection):
        return collection not in self.held

    def unlock_collection(self, collection):
        self.unlocked.append(collection)

    def start_run(self, collection, job_run_id, cluster_id, parallelism):
        self.runs.append((collection, job_run_id, cluster_id, parallelism))
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
    try:
        ic.run_check(store, store, ["c1"], broken, existing_sinks_of=_sinks)
        raise AssertionError("the check should fail")
    except RuntimeError as e:
        assert "source unreachable" in str(e)
    assert store.unlocked == ["c1"]
    assert store.finished[1]["status"] == "failed" and "source unreachable" in store.finished[1]["detail"]
    assert store.closed == [("c1", 1)]


def test_a_check_closes_the_runs_a_dead_sweep_left_open_once_it_holds_the_lock():
    configs = [_cfg(1, "quiet", "a", "c1")]
    store = _Store(configs, _recorded(configs, {1: _cp(10)}))
    summary = ic.run_check(store, store, ["c1"], _detector({"quiet": 10}), existing_sinks_of=_sinks)
    assert store.closed == [("c1", 1)] and summary["per_collection"][0]["run_id"] == 1


def test_a_check_whose_lock_connection_dropped_writes_nothing_and_fails():
    configs = [_cfg(1, "quiet", "a", "c1")]
    store = _Store(configs, _recorded(configs, {1: _cp(10)}))
    store.alive = False                       # the session lock went with the dropped connection
    try:
        ic.run_check(store, store, ["c1"], _detector({"quiet": 10}), existing_sinks_of=_sinks)
        raise AssertionError("a check that lost its lock must fail")
    except RuntimeError as e:
        assert "lock" in str(e)
    assert store.advanced == [] and store.stamped == {}
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

    try:
        ic.run_check(store, store, ["c1"], broken, existing_sinks_of=_sinks)
        raise AssertionError("the check should fail")
    except RuntimeError as e:
        assert "source unreachable" in str(e)


def test_a_failing_close_of_abandoned_runs_does_not_stop_the_check():
    configs = [_cfg(1, "quiet", "a", "c1")]
    store = _Store(configs, _recorded(configs, {1: _cp(10)}))

    def broken_close(collection, run_id):
        raise RuntimeError("telemetry down")

    store.close_abandoned_runs = broken_close
    summary = ic.run_check(store, store, ["c1"], _detector({"quiet": 10}), existing_sinks_of=_sinks)
    assert summary["per_collection"][0]["work"] == [] and store.finished[1]["status"] == "completed"
