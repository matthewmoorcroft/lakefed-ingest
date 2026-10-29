"""Deleting an app job's tables (delete_job): which groups go, the run locks, what is deleted and kept, the targets."""
import re

import pytest

import delete_job as dj


class Column:
    def __init__(self, name):
        self.name = name


class FakeCursor:
    """Answers the delete job's statements from a small in-memory state database."""

    def __init__(self, schedules, members, control, locked=(), selection=None):
        self.schedules, self.members, self.control, self.locked = schedules, members, control, set(locked)
        self.selection = selection      # job_selection rows by job id; None: a database below schema version 9
        self.executed, self.description, self.rowcount, self._result = [], [], -1, []

    def _answer(self, columns, rows):
        self.description = [Column(c) for c in columns]
        self._result = [tuple(r[c] for c in columns) for r in rows]

    def execute(self, sql, params=()):
        self.executed.append((sql, params))
        if sql == dj.schedule_sql():
            self._answer(["id", "name", "deleted_at"], [s for s in self.schedules if s["id"] == params[0]])
        elif sql == dj.groups_sql():
            sid = params[0]
            live = {s["id"] for s in self.schedules if s["deleted_at"] is None}
            held = {m["task_collection"] for m in self.members
                    if m["schedule_id"] != sid and m["removed_at"] is None and m["schedule_id"] in live}
            mine = sorted({m["task_collection"] for m in self.members if m["schedule_id"] == sid} - held)
            self._answer(["task_collection"], [{"task_collection": g} for g in mine])
        elif sql == dj.lock_sql():
            self._result = [(params[0] not in self.locked,)]
        elif sql == dj.targets_sql():
            groups = set(params[0])
            key = lambda r: tuple(r[k].lower() for k in ("sink_catalog", "sink_schema", "sink_table"))
            theirs = {key(r) for r in self.control if r["task_collection"] not in groups}
            mine = {}
            for r in self.control:
                if r["task_collection"] in groups:
                    mine[(r["sink_catalog"], r["sink_schema"], r["sink_table"])] = key(r) in theirs
            self._answer(["sink_catalog", "sink_schema", "sink_table", "shared"],
                         [{"sink_catalog": c, "sink_schema": s, "sink_table": t, "shared": shared}
                          for (c, s, t), shared in sorted(mine.items())])
        elif sql == dj.selection_table_sql():
            self._result = [(self.selection is not None,)]
        elif sql == dj.delete_selection_sql():
            self.rowcount = (self.selection or {}).get(params[0], 0)
        elif sql.startswith("delete from"):
            table = re.match(r"delete from lakefed_ingest\.(\w+)", sql).group(1)
            self.rowcount = {"control": sum(r["task_collection"] in params[0] for r in self.control)}.get(table, 2)
        else:
            raise AssertionError(f"unexpected statement: {sql}")

    def fetchall(self):
        return self._result

    def fetchone(self):
        return self._result[0]


def schedule(sid, deleted=True, name=None):
    return {"id": sid, "name": name or f"Job {sid}", "deleted_at": "2026-10-29" if deleted else None}


def member(sid, group, removed=False):
    return {"schedule_id": sid, "task_collection": group, "removed_at": "2026-10-29" if removed else None}


def row(group, table, schema="sales", catalog="main"):
    return {"task_collection": group, "sink_catalog": catalog, "sink_schema": schema, "sink_table": table}


def statements(cur):
    return [sql for sql, _ in cur.executed]


def test_a_deleted_jobs_groups_rows_go_in_order_under_each_groups_run_lock():
    cur = FakeCursor([schedule(7)], [member(7, "g1", removed=True), member(7, "g2", removed=True)],
                     [row("g1", "a"), row("g1", "b"), row("g2", "c")])
    result = dj.delete_rows(cur, 7)
    assert result["groups"] == ["g1", "g2"] and result["deleted"]["control"] == 3
    assert [p for sql, p in cur.executed if sql == dj.lock_sql()] == [("lakefed_ingest:g1",), ("lakefed_ingest:g2",)]
    deletes = [re.match(r"delete from lakefed_ingest\.(\w+)", s).group(1) for s in statements(cur) if s.startswith("delete")]
    assert deletes == ["reseed_queue", "checkpoint", "database_check", "check_work", "rollup_database",
                       "rollup_group", "control"]
    assert all(p == (["g1", "g2"],) for sql, p in cur.executed if sql.startswith("delete"))
    locks_end = max(i for i, s in enumerate(statements(cur)) if s == dj.lock_sql())
    assert locks_end < min(i for i, s in enumerate(statements(cur)) if s.startswith("delete"))


def test_run_history_and_the_change_log_are_kept():
    touched = " ".join(sql for sql, _ in dj.DELETES)
    for kept in ("ingest_run", "ingest_event", "control_change", "schedule"):
        assert f"lakefed_ingest.{kept} " not in touched and not touched.endswith(f"lakefed_ingest.{kept}")
    assert "drop schema" not in dj.drop_sql("`a`.`b`.`c`").lower()


def test_a_group_that_is_loading_refuses_the_lot():
    cur = FakeCursor([schedule(7)], [member(7, "g1", removed=True), member(7, "g2", removed=True)],
                     [row("g1", "a"), row("g2", "c")], locked={"lakefed_ingest:g2"})
    with pytest.raises(dj.RunGoing, match="g2"):
        dj.delete_rows(cur, 7)
    assert not any(s.startswith("delete") for s in statements(cur))


def test_a_job_that_is_not_deleted_or_does_not_exist_is_refused():
    cur = FakeCursor([schedule(7, deleted=False)], [member(7, "g1")], [row("g1", "a")])
    with pytest.raises(ValueError, match="isn't deleted"):
        dj.delete_rows(cur, 7)
    with pytest.raises(ValueError, match="doesn't exist"):
        dj.delete_rows(cur, 8)
    assert not any(s.startswith("delete") for s in statements(cur))


def test_a_group_now_in_another_live_job_stays():
    cur = FakeCursor([schedule(7), schedule(8, deleted=False)],
                     [member(7, "g1", removed=True), member(7, "g2", removed=True), member(8, "g2")],
                     [row("g1", "a"), row("g2", "c")])
    assert dj.delete_rows(cur, 7)["groups"] == ["g1"]


def test_a_group_of_another_deleted_job_or_removed_from_one_still_goes():
    cur = FakeCursor([schedule(7), schedule(8), schedule(9, deleted=False)],
                     [member(7, "g1", removed=True), member(8, "g1", removed=True), member(9, "g1", removed=True)],
                     [row("g1", "a")])
    assert dj.delete_rows(cur, 7)["groups"] == ["g1"]


def test_a_job_with_no_groups_left_deletes_nothing():
    cur = FakeCursor([schedule(7), schedule(8, deleted=False)], [member(7, "g1", removed=True), member(8, "g1")],
                     [row("g1", "a")])
    result = dj.delete_rows(cur, 7)
    assert result == {"job": "Job 7", "groups": [], "deleted": {}, "targets": {"drop": [], "kept": []}}
    assert not any(s.startswith("delete") or s == dj.lock_sql() for s in statements(cur))


def test_targets_another_groups_rows_also_write_are_kept_and_named():
    cur = FakeCursor([schedule(7)], [member(7, "g1", removed=True)],
                     [row("g1", "a"), row("g1", "fan_in"), row("g1", "fan_in"), row("other", "FAN_IN"),
                      row("g1", "my-table", schema="01-sales")])
    targets = dj.delete_rows(cur, 7)["targets"]
    assert targets == {"drop": ["`main`.`01-sales`.`my-table`", "`main`.`sales`.`a`"], "kept": ["`main`.`sales`.`fan_in`"]}
    assert dj.drop_sql(targets["drop"][0]) == "DROP TABLE IF EXISTS `main`.`01-sales`.`my-table`"


def test_the_targets_query_compares_names_without_case_and_reads_every_other_group():
    sql = dj.targets_sql()
    assert "lower(sink_table)" in sql and "not (task_collection = any(%s))" in sql


def test_the_summary_counts_what_went_and_names_what_was_kept_or_failed():
    result = {"job": "Hourly", "groups": ["g1", "g2"], "deleted": {"control": 5, "checkpoint": 4},
              "targets": {"drop": ["`m`.`s`.`a`"], "kept": ["`m`.`s`.`b`"]}}
    assert dj.summary(result, ["`m`.`s`.`a`"]) == {
        "job": "Hourly", "groups": 2, "tables": 5, "deleted": {"control": 5, "checkpoint": 4}, "dropped": 1,
        "kept": 1, "kept_names": ["`m`.`s`.`b`"], "drop_failed": 0, "drop_failed_names": {}}
    many = {f"`m`.`s`.`t{i}`": "denied" for i in range(25)}
    message = dj.failure_message(many)
    assert message.startswith("the job's tables are deleted, but 25 target table(s) could not be dropped")
    assert "`m`.`s`.`t19`: denied" in message and "`m`.`s`.`t20`" not in message and message.endswith("and 5 more (the run's output lists them)")


def test_what_the_job_was_made_from_goes_with_it_where_the_database_keeps_it():
    cur = FakeCursor([schedule(7)], [member(7, "g1", removed=True)], [row("g1", "a")], selection={7: 3, 8: 2})
    result = dj.delete_rows(cur, 7)
    assert result["deleted"]["job_selection"] == 3
    assert [p for sql, p in cur.executed if sql == dj.delete_selection_sql()] == [(7,)]
    assert statements(cur)[-1] == dj.delete_selection_sql()          # in the same transaction, after the groups' rows
    # A job with no groups left still forgets it; a database below version 9 has nothing to forget.
    cur = FakeCursor([schedule(7)], [], [], selection={7: 1})
    assert dj.delete_rows(cur, 7)["deleted"] == {"job_selection": 1}
    cur = FakeCursor([schedule(7)], [member(7, "g1", removed=True)], [row("g1", "a")])
    assert "job_selection" not in dj.delete_rows(cur, 7)["deleted"]
    assert dj.delete_selection_sql() not in statements(cur)
