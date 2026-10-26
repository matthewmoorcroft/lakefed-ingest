"""The monitoring SQL run against a real Postgres: a throwaway database with the Lakebase schema and a
small fixture (2 databases on one server, a running run with unchanged, loaded, in-flight and failed
tables, a first load with per-table counts and one batched by the run).

Skipped unless LAKEFED_APP_TEST_PG names a Postgres server (a libpq URI to any database there) where
the test may create and drop its own database, for example a local pgserver."""
import json
import os
import sys
import uuid
from pathlib import Path

import pytest
import queries as q

URI = os.environ.get("LAKEFED_APP_TEST_PG")
pytestmark = pytest.mark.skipif(not URI, reason="LAKEFED_APP_TEST_PG not set")
REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def pg():
    psycopg = pytest.importorskip("psycopg")
    sys.path.insert(0, str(REPO / "src" / "lakefed_ingest"))
    import state_store
    name = f"lakefed_app_test_{uuid.uuid4().hex[:8]}"
    admin = psycopg.connect(URI, autocommit=True)
    admin.execute(f"create database {name}")
    conn = psycopg.connect(psycopg.conninfo.make_conninfo(URI, dbname=name), autocommit=True)
    try:
        schema = (REPO / "src/lakefed_ingest/lakebase_schema.sql").read_text()
        for statement in state_store.split_sql_statements(schema):
            conn.execute(statement)
        _fixture(conn)
        yield conn
    finally:
        conn.close()
        admin.execute(f"drop database {name}")
        admin.close()


def _fixture(conn):
    for db in ("db1", "db2"):
        conn.execute(
            "insert into lakefed_ingest.control (task_collection, src_host, src_database, src_schema, src_table, "
            "sink_catalog, sink_schema, sink_table, client_id, primary_key, select_list, source_instance_group, "
            "freshness_sla_minutes, write_mode, created_at) "
            "select 'c1', 'Host-A', %s, 'dbo', 't' || g, 'm', 'l', 't' || g, %s, 'id', 'id', 'srv1', 60, 'append', "
            "now() - interval '3 days' from generate_series(1, 10) g", (db, db))
    ids = {(d, t): i for i, d, t in conn.execute("select id, src_database, src_table from lakefed_ingest.control")}
    # db1: every table loaded; db2: t9 failing, t10 never loaded, t8 late (last success 2 h ago, SLA 60 min).
    conn.execute("insert into lakefed_ingest.checkpoint (control_id, src_database, src_schema, src_table, phase, "
                 "status, last_success_at) select id, src_database, src_schema, src_table, 'incremental', "
                 "case when src_database = 'db2' and src_table = 't9' then 'failed' else 'ok' end, "
                 "case when src_database = 'db2' and src_table = 't8' then now() - interval '2 hours' "
                 "else now() - interval '30 minutes' end "
                 "from lakefed_ingest.control where not (src_database = 'db2' and src_table = 't10')")
    # First load 3 days ago: db1 counted per table (10 x 100 rows), db2 batched (run total 500).
    conn.execute("insert into lakefed_ingest.ingest_run (run_id, task_collection, started_at, finished_at, status, "
                 "total, ok, failed, skipped, detail) values (1, 'c1', now() - interval '3 days', "
                 "now() - interval '3 days', 'completed', 20, 20, 0, 0, %s)",
                 (json.dumps({"batch_rows": 500, "batch_rows_unknown": 0}),))
    conn.execute("insert into lakefed_ingest.ingest_event (run_id, task_collection, control_id, src_database, "
                 "src_schema, src_table, action, status, started_at, finished_at, rows_merged) "
                 "select 1, 'c1', id, src_database, src_schema, src_table, 'seed', 'ok', now() - interval '3 days', "
                 "now() - interval '3 days', case when src_database = 'db1' then 100 end "
                 "from lakefed_ingest.control where src_table <> 't10' or src_database = 'db1'")
    # A run going now: started 5 minutes ago; db1 t1..t6 unchanged (stamped at planning), t7 loaded with 7
    # rows, t8 in flight; db2 t9 failed.
    conn.execute("insert into lakefed_ingest.ingest_run (run_id, task_collection, started_at, status) "
                 "values (2, 'c1', now() - interval '5 minutes', 'running')")
    conn.execute("update lakefed_ingest.checkpoint set last_success_at = now() - interval '4 minutes' "
                 "where src_database = 'db1' and src_table in ('t1','t2','t3','t4','t5','t6','t7')")
    for table, status, rows in (("t7", "ok", 7), ("t8", "running", None)):
        conn.execute("insert into lakefed_ingest.ingest_event (run_id, task_collection, control_id, src_database, "
                     "src_schema, src_table, action, status, started_at, finished_at, rows_merged) "
                     "values (2, 'c1', %s, 'db1', 'dbo', %s, 'increment', %s, now() - interval '2 minutes', "
                     "case when %s = 'ok' then now() - interval '1 minute' end, %s)",
                     (ids[("db1", table)], table, status, status, rows))
    conn.execute("insert into lakefed_ingest.ingest_event (run_id, task_collection, control_id, src_database, "
                 "src_schema, src_table, action, status, started_at, finished_at, error) "
                 "values (2, 'c1', %s, 'db2', 'dbo', 't9', 'increment', 'failed', "
                 "now() - interval '3 minutes', now() - interval '3 minutes', "
                 "'[FAILED_JDBC.CONNECTION] connection reset')", (ids[("db2", "t9")],))


def rows(conn, sql, params=()):
    cur = conn.execute(sql, params)
    cols = [d.name for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def test_every_enabled_table_is_in_exactly_one_state(pg):
    c = rows(pg, q.collections_overview_sql())[0]
    states = (c["state_current"], c["state_late"], c["state_first_load"], c["state_failing"])
    assert states == (17, 1, 1, 1) and sum(states) == c["enabled_tables"] == 20
    assert (c["databases"], c["servers"], c["server"]) == (2, 1, "srv1")
    assert c["rows_24h"] == 7


def test_running_run_progress_counts_tables_stamped_since_it_started(pg):
    r = rows(pg, q.running_runs_sql())[0]
    assert (r["run_id"], r["enabled_tables"], r["tables_checked"]) == (2, 20, 7)
    assert (r["tables_ok"], r["tables_running"], r["tables_failed"]) == (1, 1, 1)
    assert r["last_activity_at"] is not None and r["server"] == "srv1"


def test_servers_roll_up_their_databases(pg):
    s = rows(pg, q.servers_sql())[0]
    assert (s["server"], s["databases"], s["tables"]) == ("srv1", 2, 20)
    assert (s["collections"], s["running_collections"]) == (1, 1)
    assert (s["state_failing"], s["state_first_load"], s["state_late"], s["state_current"]) == (1, 1, 1, 17)


def test_a_collections_databases_show_this_runs_progress_and_their_rows(pg):
    dbs = {d["src_database"]: d for d in rows(pg, q.collection_databases_sql(), ("c1", "c1"))}
    assert (dbs["db1"]["tables"], dbs["db1"]["checked_this_run"], dbs["db1"]["state_current"]) == (10, 7, 10)
    assert (dbs["db2"]["checked_this_run"], dbs["db2"]["state_failing"], dbs["db2"]["state_first_load"]) == (0, 1, 1)
    written = {d["src_database"]: d for d in rows(pg, q.collection_database_rows_sql(), ("c1",))}
    assert (written["db1"]["rows_first_loads"], written["db1"]["first_loads_uncounted"]) == (1000, 0)
    db1 = written["db1"]
    assert (db1["rows_changes_24h"], db1["loaded_this_run"], db1["in_flight"]) == (7, 1, 1)
    assert dbs["db1"]["server"] == "srv1"
    assert (written["db2"]["rows_first_loads"], written["db2"]["first_loads_uncounted"]) == (None, 9)
    assert written["db2"]["failed_this_run"] == 1
    batch = rows(pg, q.collection_batch_runs_sql(), ("c1",))
    assert [(b["run_id"], b["databases"]) for b in batch] == [(1, ["db2"])]
    assert q.batch_rows(batch[0]["detail"]) == (500, 0)


def test_a_runs_databases_and_rows(pg):
    by_db = {d["src_database"]: d for d in rows(pg, q.run_databases_sql(), (2,))}
    assert (by_db["db1"]["loaded"], by_db["db1"]["in_flight"], by_db["db1"]["rows_written"]) == (1, 1, 7)
    assert by_db["db2"]["failed"] == 1
    per_run = {r["run_id"]: r for r in rows(pg, q.run_rows_sql(), ([1, 2],))}
    assert (per_run[1]["rows_written"], per_run[1]["uncounted"]) == (1000, 9)
    assert {r["run_id"] for r in rows(pg, q.run_batch_rows_sql(), (24 * 7,))} == {1}


def test_connection_databases_match_by_name_or_host_case_insensitively(pg):
    found = rows(pg, q.connection_databases_sql(), ("no_such_connection", "host-a"))
    assert [(d["src_database"], d["tables"], d["collections"]) for d in found] == [("db1", 10, "c1"), ("db2", 10, "c1")]


def test_failed_tables_are_one_row_per_table_with_its_failures(pg):
    found = rows(pg, q.failed_tables_sql(), (24, 10, 24))
    assert [(r["src_database"], r["src_table"], r["failures"]) for r in found] == [("db2", "t9", 1)]
    assert found[0]["error"].startswith("[FAILED_JDBC.CONNECTION]") and found[0]["first_at"] == found[0]["last_at"]
    assert rows(pg, q.failed_tables_sql(), (24, 0, 24)) == []
