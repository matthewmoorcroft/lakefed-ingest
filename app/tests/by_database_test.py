"""A big job written one database at a time (by_database.py and main._create_by_database): the groups its tables take,
how its databases are paged and planned, the write's transactions and progress, resume by the same request id, and the
check before any write. The source and Lakebase are fakes; the SQL itself runs against Postgres in large_writes_pg_test.py."""
import contextlib
import copy
import json
import time

import pytest

import by_database as bd
import new_job as nj
import queries as q

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
from api_test import FakeCursor, FakeDB  # noqa: E402
from background_task_test import Tasks  # noqa: E402
from new_job_test import FAN_IN  # noqa: E402

COLUMNS = ["src_connection", "src_database", "src_schema", "src_table", "select_list", "primary_key"]


# ---- the big job's pure parts --------------------------------------------------------------------------------------

def test_groups_are_sized_by_tables_not_four_per_server():
    assert bd.groups_for_tables(22_000 * 240) == 423          # the 5.3 million tables of one server: about 425
    assert bd.groups_for_tables(bd.TABLES_PER_GROUP) == 1
    assert bd.groups_for_tables(bd.TABLES_PER_GROUP + 1) == 2
    assert bd.groups_for_tables(0) == 1


def test_the_summary_names_every_group_of_each_server_before_anything_is_read():
    summary = bd.summary_of("Big job", [{"server": "sql_a", "databases": 22_000, "tables": 22_000 * 240}])
    assert summary["by_database"] is True and summary["checked"] is False
    assert len(summary["groups"]) == 423 and summary["groups"][0] == "big_job_sql_a_c01"
    assert summary["tables"] == 5_280_000 and summary["databases"] == 22_000


def test_databases_are_paged_in_key_order_and_a_short_page_ends_them():
    names = [("sql_a", f"db_{i:04d}") for i in range(1200)]
    asked = []

    def page(after):
        asked.append(after)
        rows = [{"server": s, "src_database": d} for s, d in names if (s, d.lower(), d) > after][: bd.DATABASE_PAGE]
        return rows

    got = list(bd.database_pages(page))
    assert [(r["server"], r["src_database"]) for r in got] == names
    assert asked[0] == ("", "", "") and len(asked) == 3          # 500, 500, then 200: the short page ends it


def test_a_selection_skips_the_databases_its_whole_server_already_covers():
    selection = main.Selection(upload_id="u1", whole_server=True, filter="eng",
                               databases=[{"server": "sql_a", "database": "eng_1"}, {"server": "sql_a", "database": "other"}],
                               tables=[{"server": "sql_a", "database": "eng_2", "tables": ["gen.t1"]},
                                       {"server": "sql_a", "database": "other", "tables": ["gen.t9"]}])
    matched = [("sql_a", "eng_1"), ("sql_a", "eng_2")]
    units = [(u.database, u.every) for u in bd.selection_units(selection, iter(matched))]
    assert units == [("eng_1", True), ("eng_2", True), ("other", True)]   # other is named whole, its table pick is covered


def test_a_unit_keeps_only_the_tables_named_when_it_is_not_a_whole_database():
    selection = main.Selection(upload_id="u1", tables=[{"server": "sql_a", "database": "eng_2", "tables": ["GEN.T1"]}])
    (unit,) = list(bd.selection_units(selection, iter(())))
    rows = [{"src_schema": "gen", "src_table": "t1"}, {"src_schema": "gen", "src_table": "t2"}]
    assert not unit.every and [unit.keeps(r) for r in rows] == [True, False]


def test_databases_are_packed_into_transactions_up_to_the_limit_and_a_database_is_never_split(monkeypatch):
    monkeypatch.setattr(bd, "TABLES_PER_TRANSACTION", 10)
    units = [bd.Unit("sql_a", f"d{i}", True) for i in range(5)]
    chunks = [[unit.database for unit in chunk] for chunk in bd.transactions_of(list(zip(units, [4, 4, 4, 12, 1])))]
    assert chunks == [["d0", "d1"], ["d2"], ["d3"], ["d4"]]     # d3 is bigger than the limit: a transaction of its own


def test_the_check_reads_databases_in_batches_and_each_database_gets_only_its_own_rows():
    units = [bd.Unit("sql_a", "Eng_1", True), bd.Unit("sql_a", "eng_2", False, frozenset({"gen.t2"}))]
    rows = [{"server": "sql_a", "src_database": "eng_1", "src_schema": "gen", "src_table": "t1"},
            {"server": "SQL_A", "src_database": "ENG_2", "src_schema": "gen", "src_table": "t1"},
            {"server": "sql_a", "src_database": "eng_2", "src_schema": "gen", "src_table": "t2"}]
    assert [(unit.database, [row["src_table"] for row in kept]) for unit, kept in bd.rows_by_unit(units, rows)] == [
        ("Eng_1", ["t1"]), ("eng_2", ["t2"])]
    many = [bd.Unit("s", f"d{i}", True) for i in range(bd.DATABASES_PER_READ * 2 + 1)]
    assert [len(batch) for batch in bd.read_batches(many)] == [bd.DATABASES_PER_READ, bd.DATABASES_PER_READ, 1]


def test_a_database_is_planned_into_the_group_the_whole_job_gives_it():
    rows = [{"server": "sql_a", "src_database": "eng_007", "src_schema": "gen", "src_table": f"t{i}", "line": i + 2,
             "fields": {"src_connection": "sql_a", "src_database": "eng_007", "src_schema": "gen", "src_table": f"t{i}",
                        "select_list": "id, a", "primary_key": "id"}} for i in range(3)]
    plan = bd.plan_database("Big job", FAN_IN, rows, COLUMNS, {"sql_a": 423}, set(), set())
    assert plan["errors"] == [] and [line["line"] for line in plan["lines"]] == [2, 3, 4]
    assert plan["groups"] == [nj.group_name("Big job", "sql_a", "eng_007", 423)]


def test_a_group_another_job_uses_is_refused_unless_the_job_owns_it():
    rows = [{"server": "sql_a", "src_database": "eng_007", "src_schema": "gen", "src_table": "t1", "line": 2,
             "fields": {"src_connection": "sql_a", "src_database": "eng_007", "src_schema": "gen", "src_table": "t1",
                        "select_list": "id", "primary_key": "id"}}]
    group = nj.group_name("Big job", "sql_a", "eng_007", 423)
    refused = bd.plan_database("Big job", FAN_IN, rows, COLUMNS, {"sql_a": 423}, {group}, set())
    assert refused["lines"][0]["status"] == "invalid"
    owned = bd.plan_database("Big job", FAN_IN, rows, COLUMNS, {"sql_a": 423}, {group}, {group})
    assert owned["lines"][0]["status"] == "valid"


def test_planning_22000_databases_of_240_tables_takes_about_a_few_minutes_with_fakes(capsys):
    """Planning only (the reads and writes are not timed): a sample of databases is planned and the time for all 22,000
    is worked out from it. The check pass and the write pass each plan every database once."""
    sample = 300
    groups = {"sql_a": bd.groups_for_tables(22_000 * 240)}

    def rows_of(i):
        database = f"eng_{i:05d}"
        return [{"server": "sql_a", "src_database": database, "src_schema": "gen", "src_table": f"t{t:04d}", "line": t + 2,
                 "fields": {"src_connection": "sql_a", "src_database": database, "src_schema": "gen",
                            "src_table": f"t{t:04d}", "select_list": "id, a", "primary_key": "id"}}
                for t in range(240)]

    started = time.perf_counter()
    planned = 0
    for i in range(sample):
        plan = bd.plan_database("Big job", FAN_IN, rows_of(i), COLUMNS, groups, set(), set())
        assert plan["errors"] == []
        planned += len(plan["lines"])
    took = time.perf_counter() - started
    per_database = took / sample
    print(f"planned {planned:,} tables in {sample} databases: {per_database * 1000:.1f} ms a database, "
          f"about {per_database * 22_000 / 60:.1f} minutes for 22,000 databases, one pass")
    assert planned == sample * 240 and per_database < 0.05


# ---- the write, with a fake source and Lakebase --------------------------------------------------------------------

class Source:
    """A whole upload's databases: {(server, database): [table names]}, read as the source queries read them."""
    def __init__(self, databases):
        self.databases = databases

    def tables(self):
        return sum(len(names) for names in self.databases.values())

    def rows(self, server, database):
        names = self.databases.get((server, database), [])
        return [{"server": server, "src_database": database, "src_schema": "gen", "src_table": name, "line": at + 2,
                 "fields": {"src_connection": server, "src_database": database, "src_schema": "gen", "src_table": name,
                            "select_list": "id, a", "primary_key": "id"}} for at, name in enumerate(names)]

    def keys_after(self, after, filter_, limit):
        keys = sorted(((s, d.lower(), d) for s, d in self.databases if filter_.lower() in d.lower() and (s, d.lower(), d) > after))
        return [{"server": s, "src_database": d, "tables": len(self.databases[(s, d)])} for s, _, d in keys[:limit]]


class Store:
    """Lakebase, faked: the control rows, the schedule members, the schedules made, the transactions, a database
    whose write fails (fail_on) as a dropped connection would, and the heartbeats that fired after a commit (beats) and
    the done counts they lowered (lowered)."""
    def __init__(self):
        self.controls, self.sinks, self.members, self.schedules = [], {}, set(), []
        self.transactions, self.fail_on, self.control_statements = 0, None, 0
        self.beats, self.lowered = 0, []


def _beat_after_commit(store, tasks):
    """A heartbeat that fires right after a commit. It holds no progress of its own (a by-database write reports its
    progress inside its transaction, not to the beat), so it writes the zero it started with: it must not take the
    task's done count back down."""
    if 1 not in tasks.rows:
        return
    before = tasks.rows[1]["done"]
    main.db.execute(q.task_progress_sql(), (0, 1))
    store.beats += 1
    if tasks.rows[1]["done"] < before:
        store.lowered.append((before, tasks.rows[1]["done"]))


class ByDatabaseTasks(Tasks):
    def execute(self, sql, params=()):
        if sql == q.task_progress_json_sql():
            self.rows[params[2]].update(progress=json.loads(params[0]), done=params[1])
            return
        super().execute(sql, params)

    def query(self, sql, params=()):
        if sql == q.resume_task_sql():
            row = self.rows.get(params[0])
            if row and row["status"] == "failed" and row["result"] is None and row.get("progress") is not None:
                row.update(status="running", error=None)
                return [{"id": params[0]}]
            return []
        return super().query(sql, params)


def _rows_for_keys(source, keys):
    """The rows of the databases a chunked read names (queries.database_rows_chunk_sql's keys)."""
    wanted = set(keys)
    return [row for (server, database) in source.databases for row in source.rows(server, database)
            if bd.read_key(server, database) in wanted]


def _rows_for_exact(source, servers, databases):
    """The rows of the databases an exact read names (queries.database_rows_exact_sql's server and database lists)."""
    wanted = set(zip(servers, databases))
    return [row for (server, database) in source.databases for row in source.rows(server, database)
            if (server, database) in wanted]


class ByDatabaseCursor(FakeCursor):
    def __init__(self, fake, store, tasks):
        super().__init__(fake)
        self.store, self.tasks = store, tasks

    def execute(self, sql, params=()):
        s = self.store
        self._rows = []
        if sql == q.insert_schedule_sql():
            self.db.statements.append((sql, params))
            sid = 7 + len(s.schedules)
            s.schedules.append(sid)
            self._rows = [(sid,)]
        elif sql.startswith("insert into lakefed_ingest.control ("):
            self.db.statements.append((sql, params))
            s.control_statements += 1
            width = len(q.CONTROL_INSERT_COLUMNS)
            for at in range(0, len(params), width):
                chunk = tuple(params[at:at + width])
                row = dict(zip(q.CONTROL_INSERT_COLUMNS, chunk))
                if row["src_database"] == s.fail_on:
                    raise RuntimeError("the source went away")
                key = q.control_sink_key(chunk)
                if key in s.sinks:
                    continue
                s.sinks[key] = row
                s.controls.append({**row, "sink_key": key})
                self._rows.append((len(s.controls), *key))
        elif sql == q.insert_member_if_absent_sql():
            self.db.statements.append((sql, params))
            key = (params[0], params[1])
            if key not in s.members:
                s.members.add(key)
                self._rows = [(len(s.members),)]
        elif sql == q.insert_members_if_absent_sql():
            self.db.statements.append((sql, params))
            sid, groups = params[0], params[1]
            added = [group for group in groups if (sid, group) not in s.members]
            s.members.update((sid, group) for group in added)
            self._rows = [(group,) for group in added]
        elif sql == q.job_database_sinks_chunk_sql():
            self.db.statements.append((sql, params))
            groups, databases = params
            self._rows = [(row["src_database"].lower(), ".".join(row["sink_key"][:3]), row["sink_key"][3],
                           row["task_collection"]) for row in s.controls
                          if row["task_collection"] in groups and row["src_database"] in databases]
        elif sql == q.task_progress_json_sql():
            self.db.statements.append((sql, params))
            self.tasks.rows[params[2]].update(progress=json.loads(params[0]), done=params[1])
        elif sql == q.task_job_sql():
            self.db.statements.append((sql, params))
            self.tasks.rows[params[1]]["job_id"] = params[0]
        elif sql == q.task_written_sql():
            self.db.statements.append((sql, params))
            row = self.tasks.rows[params[3]]
            row.update(result=json.loads(params[0]), done=params[2], job_id=params[1] or row["job_id"])
        else:
            self._execute(sql, params)
            return

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


SCHEDULE = dict(name="Big job", cadence_minutes=60, timezone="UTC", stagger=True, paused=True, options=None,
                job_variant="default", parameters={}, notify_emails=None)


@pytest.fixture
def big(monkeypatch):
    source = Source({("sql_a", f"eng_{i:03d}"): [f"t{t}" for t in range(5)] for i in range(30)})
    store, tasks, fake = Store(), ByDatabaseTasks(), FakeDB()

    def query(sql, params=()):
        if sql == q.selected_count_sql("upload"):
            return [{"n": source.tables()}]
        if sql == q.selected_servers_sql("upload"):
            servers = {}
            for (server, database), names in source.databases.items():
                entry = servers.setdefault(server, {"server": server, "databases": 0, "tables": 0})
                entry["databases"] += 1
                entry["tables"] += len(names)
            return list(servers.values())
        if sql == q.source_databases_after_sql("upload"):
            return source.keys_after(tuple(params[2:5]), params[1], params[5])
        if sql == q.database_rows_sql("upload"):
            return source.rows(params[1], params[2])
        if sql == q.database_rows_chunk_sql("upload"):
            return _rows_for_keys(source, params[1])
        if sql == q.database_rows_exact_sql():
            return _rows_for_exact(source, params[0], params[1])
        if sql == q.upload_sql():
            return [{"upload_id": "u1", "columns": COLUMNS}]
        if sql == q.collections_sql():
            return [{"task_collection": g} for g in sorted({row["task_collection"] for row in store.controls})]
        if sql == q.schedule_sql():
            return [dict(SCHEDULE, id=params[0])] if params[0] in store.schedules else []
        if sql == q.schedule_members_sql():
            return [{"task_collection": g, "removed_at": None} for sid, g in store.members if sid == params[0][0]]
        return tasks.query(sql, params)

    def fetch(cur, sql, params=()):
        if sql == q.schedule_members_sql():
            return query(sql, params)
        if sql == q.job_database_sinks_sql():
            groups, database = params
            return [{"sink": ".".join(key[:3]), "client_id": key[3]} for key in
                    (row["sink_key"] for row in store.controls
                     if row["task_collection"] in groups and row["src_database"].lower() == database.lower())]
        raise AssertionError(sql)

    @contextlib.contextmanager
    def transaction():
        store.transactions += 1
        saved = copy.deepcopy((store.controls, store.sinks, store.members, store.schedules, tasks.rows))
        try:
            yield ByDatabaseCursor(fake, store, tasks)
        except Exception:
            store.controls, store.sinks, store.members, store.schedules, tasks.rows = saved
            raise
        _beat_after_commit(store, tasks)

    monkeypatch.setattr(main.db, "query", query)
    monkeypatch.setattr(main.db, "fetch", fetch)
    monkeypatch.setattr(main.db, "execute", tasks.execute)
    monkeypatch.setattr(main.db, "transaction", transaction)
    monkeypatch.setattr(main.jobs, "schedule_variants", lambda: {"default": "1", "check": "2"})
    monkeypatch.setattr(main, "_start_background", tasks.started.append)
    monkeypatch.setattr(main, "BACKGROUND_ROWS", 50)
    monkeypatch.setattr(main, "TASK_BEAT_S", 3600)
    synced = []
    monkeypatch.setattr(main, "_sync", lambda sid, user: synced.append(sid) or [])
    c = TestClient(main.app, headers={"X-Forwarded-Email": "op@example.com"})
    c.store, c.tasks, c.synced = store, tasks, synced
    return c


def _create(client, request_id="big-1", dry_run=False):
    return client.post("/api/ingestion-jobs", json={"name": "Big job", "settings": FAN_IN, "dry_run": dry_run,
                                                    "request_id": request_id,
                                                    "source": {"upload_id": "u1", "whole_server": True}})


def test_a_big_create_is_a_task_that_writes_its_databases_in_transactions(big):
    r = _create(big)
    assert r.status_code == 202 and r.json()["task"]["total"] == 150 and big.store.schedules == []
    big.tasks.run()
    done = big.get(f"/api/tasks/{r.json()['task']['id']}").json()
    assert done["status"] == "succeeded" and done["job_id"] == 7 and done["result"]["summary"]["tables"] == 150
    assert len(big.store.controls) == 150 and big.synced == [7]
    assert big.tasks.rows[1]["progress"] == {"stage": "writing", "databases_total": 30, "databases_written": 30,
                                             "tables_written": 150}
    assert big.store.transactions == 1 + 1 + 1       # the job, the 150 tables in one transaction, then the result
    assert {g for _, g in big.store.members} == {row["task_collection"] for row in big.store.controls}


def test_a_whole_server_upload_reads_each_database_by_its_exact_names(big, monkeypatch):
    seen = []
    read = main.db.query

    def recording(sql, params=()):
        seen.append(sql)
        return read(sql, params)

    monkeypatch.setattr(main.db, "query", recording)
    _create(big)
    big.tasks.run()
    assert seen.count(q.database_rows_exact_sql()) == 2        # the check's batch, then the one transaction
    assert q.database_rows_chunk_sql("upload") not in seen


def test_a_dry_run_of_a_big_source_says_it_is_read_by_database_and_not_yet_checked(big):
    r = _create(big, dry_run=True)
    summary = r.json()["summary"]
    assert r.status_code == 200 and summary["by_database"] is True and summary["checked"] is False
    assert summary["tables"] == 150 and big.tasks.rows == {} and big.store.transactions == 0


def test_a_transaction_holds_whole_databases_up_to_the_table_limit(big, monkeypatch):
    monkeypatch.setattr(bd, "TABLES_PER_TRANSACTION", 50)
    _create(big)
    big.tasks.run()
    assert big.get("/api/tasks/1").json()["status"] == "succeeded" and len(big.store.controls) == 150
    assert big.store.transactions == 1 + 3 + 1       # the job, three transactions of ten databases, the result


def test_a_database_that_fails_takes_its_transaction_and_the_same_request_finishes_the_job(big, monkeypatch):
    monkeypatch.setattr(bd, "TABLES_PER_TRANSACTION", 50)     # ten to a transaction: eng_020 starts the third
    big.store.fail_on = "eng_020"
    _create(big)
    big.tasks.run()
    failed = big.get("/api/tasks/1").json()
    assert failed["status"] == "failed" and failed["error"] == "the source went away"
    assert failed["progress"] == {"stage": "writing", "databases_total": 30, "databases_written": 20, "tables_written": 100}
    assert len(big.store.controls) == 100 and big.synced == []    # the third transaction was rolled back
    big.store.fail_on = None
    before = big.store.control_statements
    again = _create(big)
    assert again.status_code == 202 and again.json()["task"]["id"] == 1 and len(big.tasks.started) == 1
    big.tasks.run()
    done = big.get("/api/tasks/1").json()
    assert done["status"] == "succeeded" and done["job_id"] == 7
    assert len(big.store.controls) == 150 and big.store.schedules == [7] and big.synced == [7]
    assert big.store.control_statements - before == 1           # eng_020 to eng_029 only, in one statement


def test_the_same_request_id_after_success_answers_the_task_and_starts_nothing(big):
    _create(big)
    big.tasks.run()
    again = _create(big)
    assert again.status_code == 202 and again.json()["task"]["status"] == "succeeded"
    assert big.tasks.started == [] and big.store.schedules == [7] and len(big.store.controls) == 150


def test_a_line_that_cannot_be_taken_stops_the_job_before_anything_is_written(big, monkeypatch):
    big.store.fail_on = None
    source = Source({("sql_a", f"eng_{i:03d}"): [f"t{t}" for t in range(5)] for i in range(30)})
    source.databases[("sql_a", "eng_010")] = ["", "t1"]           # an empty table name: the line is invalid
    original = main.db.query

    def query(sql, params=()):
        if sql == q.database_rows_sql("upload"):
            return source.rows(params[1], params[2])
        if sql == q.database_rows_chunk_sql("upload"):
            return _rows_for_keys(source, params[1])
        if sql == q.database_rows_exact_sql():
            return _rows_for_exact(source, params[0], params[1])
        if sql == q.source_databases_after_sql("upload"):
            return source.keys_after(tuple(params[2:5]), params[1], params[5])
        return original(sql, params)

    monkeypatch.setattr(main.db, "query", query)
    _create(big)
    big.tasks.run()
    failed = big.get("/api/tasks/1").json()
    assert failed["status"] == "failed" and "eng_010" in failed["error"]
    assert big.store.controls == [] and big.store.schedules == []


def test_a_heartbeat_after_a_commit_never_lowers_a_big_create_progress(big):
    r = _create(big)
    big.tasks.run()
    assert big.get(f"/api/tasks/{r.json()['task']['id']}").json()["status"] == "succeeded"
    assert big.store.beats > 1 and big.store.lowered == []


def test_adding_tables_by_reference_to_a_job_writes_each_database_as_its_own_task(big, monkeypatch):
    own = _job_for_a_big_add(big, monkeypatch)
    dry = big.post("/api/ingestion-jobs/7/tables", json={"source": {"upload_id": "u1", "whole_server": True}})
    assert dry.status_code == 200 and dry.json()["summary"]["by_database"] is True and dry.json()["summary"]["tables"] == 150
    assert big.tasks.rows == {} and big.store.controls == []
    r = _add(big, "add-big-1")
    assert r.status_code == 202 and r.json()["task"]["kind"] == "tables" and r.json()["task"]["total"] == 150
    big.tasks.run()
    done = big.get(f"/api/tasks/{r.json()['task']['id']}").json()
    assert done["status"] == "succeeded", done.get("error")
    assert done["result"]["summary"]["tables"] == 150
    assert len(big.store.controls) == 150 and big.store.schedules == [7]
    assert big.synced == []                                   # its databases join the server's groups: no new job to sync
    groups = {g for sid, g in big.store.members if sid == 7}
    assert own <= groups and groups == own
    assert big.tasks.rows[1]["progress"]["databases_written"] == 30


def test_a_heartbeat_after_a_commit_never_lowers_a_big_add_progress(big, monkeypatch):
    _job_for_a_big_add(big, monkeypatch)
    _add(big, "add-beat")
    big.tasks.run()
    assert big.tasks.rows[1]["status"] == "succeeded" and big.store.beats > 1 and big.store.lowered == []


def _job_for_a_big_add(client, monkeypatch) -> set:
    """Job 7 holds the engagement job's groups (its tables, read as the job's own rows), so the big source's server is
    one the job already has: its databases go into those groups. Returns the groups."""
    from new_job_test import _job_rows
    monkeypatch.setattr(main, "_refuse_job_tables", lambda job, groups, request: None)
    job_rows = _job_rows()
    own = {r["task_collection"] for r in job_rows}
    client.store.schedules.append(7)
    client.store.members.update((7, g) for g in own)
    answer = main.db.query
    monkeypatch.setattr(main.db, "query",
                        lambda sql, params=(): job_rows if sql == q.groups_control_rows_sql() else answer(sql, params))
    return own


def _add(client, request_id: str):
    return client.post("/api/ingestion-jobs/7/tables", json={"source": {"upload_id": "u1", "whole_server": True},
                                                             "dry_run": False, "request_id": request_id})
