"""A job's groups spread again when its number of groups per server changes (main._regroup_plan, the endpoint's
refusals): only servers whose number changes move, few databases move, none is split, and nothing moves while a run
or a write of the job is going. The apply itself runs against Postgres (large_writes_pg_test.py)."""
import pytest

pytest.importorskip("fastapi")
import main  # noqa: E402


def _rows(groups, sizes, server="sql_a", job="job"):
    """Control rows of one server: database i of sizes[i] tables in groups[i] (a group number)."""
    rows, next_id = [], 1
    for i, (group, tables) in enumerate(zip(groups, sizes)):
        for t in range(tables):
            rows.append({"id": next_id, "task_collection": f"{job}_{server}_c{group:02d}", "src_connection": server,
                         "use_remote_query": True, "src_host": None, "src_port": None, "src_database": f"db{i:02d}",
                         "src_table": f"t{t}"})
            next_id += 1
    return rows


def test_more_groups_move_only_what_the_new_groups_take():
    rows = _rows([1] * 6 + [2] * 6, [10] * 12)
    plan = main._regroup_plan("job", rows, 3)
    assert plan["new"] == ["job_sql_a_c03"] and plan["emptied"] == []
    assert plan["summary"]["databases_moving"] == 4 and plan["summary"]["tables_moving"] == 40
    after = {}
    for r in rows:
        group = next((a for i, _, a, _ in plan["moves"] if i == r["id"]), r["task_collection"])
        after.setdefault(r["src_database"], set()).add(group)
    assert all(len(groups) == 1 for groups in after.values())          # a database is never split
    counts = {}
    for groups in after.values():
        counts[next(iter(groups))] = counts.get(next(iter(groups)), 0) + 1
    assert sorted(counts.values()) == [4, 4, 4]


def test_an_unchanged_number_moves_nothing():
    plan = main._regroup_plan("job", _rows([1, 2, 2], [5, 1, 1]), 2)
    assert plan["moves"] == [] and plan["summary"]["unchanged"] is True


def test_blank_takes_the_default_by_tables_and_fewer_groups_empty_some():
    plan = main._regroup_plan("job", _rows([1, 2, 3, 4], [5, 5, 5, 5]), None)       # 20 tables: one group by default
    assert plan["summary"]["servers"][0]["groups_after"] == 1 and len(plan["emptied"]) == 3
    assert {after for _, _, after, _ in plan["moves"]} == {"job_sql_a_c01"}


def test_regrouping_waits_for_a_run_or_a_write_of_the_job(monkeypatch):
    job, members = {"id": 7}, [{"task_collection": "job_sql_a_c01", "removed_at": None}]
    monkeypatch.setattr(main, "_job_active_runs", lambda job, members: [{"run_id": 1}])
    assert "a run of this job is going" in main._regroup_blocker(job, members)
    monkeypatch.setattr(main, "_job_active_runs", lambda job, members: [])
    monkeypatch.setattr(main.db, "query", lambda sql, params=(): [{"status": "running"}])
    assert "still going" in main._regroup_blocker(job, members)
    monkeypatch.setattr(main.db, "query", lambda sql, params=(): [{"status": "succeeded"}])
    assert main._regroup_blocker(job, members) is None
