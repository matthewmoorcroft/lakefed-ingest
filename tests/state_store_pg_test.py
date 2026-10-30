"""StateStore statements run against a real Postgres, where parameters are typed by the server: a throwaway
database with the Lakebase schema.

Skipped unless LAKEFED_TEST_PG names a Postgres server (a libpq URI to any database there) where the test may
create and drop its own database, for example a local Postgres."""
import json
import os
import uuid
from pathlib import Path

import pytest
import state_store as ss

URI = os.environ.get("LAKEFED_TEST_PG")
pytestmark = pytest.mark.skipif(not URI, reason="LAKEFED_TEST_PG not set")
SCHEMA = Path(__file__).resolve().parents[1] / "src" / "lakefed_ingest" / "lakebase_schema.sql"


@pytest.fixture
def conn():
    psycopg = pytest.importorskip("psycopg")
    name = f"lakefed_test_{uuid.uuid4().hex[:8]}"
    admin = psycopg.connect(URI, autocommit=True)
    admin.execute(f"create database {name}")
    db = psycopg.connect(psycopg.conninfo.make_conninfo(URI, dbname=name), autocommit=True)
    try:
        for statement in ss.split_sql_statements(SCHEMA.read_text()):
            db.execute(statement)
        yield db
    finally:
        db.close()
        admin.execute(f"drop database {name}")
        admin.close()


def test_a_run_closes_its_collections_abandoned_runs_and_their_open_events(conn):
    """The run holding the collection's lock closes its other open runs: the statement is prepared by the server,
    so every parameter must have a type Postgres can work out."""
    conn.execute("insert into lakefed_ingest.ingest_run (run_id, task_collection, status) values "
                 "(1, 'c', 'running'), (2, 'c', 'running'), (3, 'other', 'running')")
    conn.execute("insert into lakefed_ingest.ingest_event (run_id, task_collection, control_id, src_table, status, "
                 "error) values (1, 'c', 10, 't10', 'running', null), (1, 'c', 11, 't11', 'retried', 'timed out'), "
                 "(1, 'c', 12, 't12', 'ok', null)")
    store = ss.StateStore(conn)
    assert store.close_abandoned_runs("c", 2) == (1, 2)
    runs = dict(conn.execute("select run_id, status from lakefed_ingest.ingest_run order by run_id").fetchall())
    assert runs == {1: "failed", 2: "running", 3: "running"}
    detail = conn.execute("select detail from lakefed_ingest.ingest_run where run_id = 1").fetchone()[0]
    assert json.loads(detail)["closed_by_run"] == 2
    events = {t: (s, e) for t, s, e in conn.execute(
        "select src_table, status, error from lakefed_ingest.ingest_event order by src_table")}
    assert events["t10"] == ("failed", f"{ss.ABANDONED_RUN_REASON}; closed by run 2")
    assert events["t11"] == ("failed", f"timed out | {ss.ABANDONED_RUN_REASON}; closed by run 2")
    assert events["t12"] == ("ok", None)


@pytest.fixture
def sessions():
    """A throwaway database with the Lakebase schema, and a way to open more sessions on it, as runs are."""
    psycopg = pytest.importorskip("psycopg")
    name = f"lakefed_test_{uuid.uuid4().hex[:8]}"
    admin = psycopg.connect(URI, autocommit=True)
    admin.execute(f"create database {name}")
    target = psycopg.conninfo.make_conninfo(URI, dbname=name)
    opened = []

    def session():
        opened.append(psycopg.connect(target, autocommit=True))
        return opened[-1]
    try:
        for statement in ss.split_sql_statements(SCHEMA.read_text()):
            session().execute(statement)
        yield session
    finally:
        for c in opened:
            c.close()
        admin.execute(f"drop database {name}")
        admin.close()


def test_work_list_runs_share_a_collection_and_anything_needing_it_alone_is_refused(sessions):
    first, second, edit = (ss.StateStore(sessions()) for _ in range(3))
    assert first.try_lock_collection_shared("g1") and second.try_lock_collection_shared("g1")
    assert edit.try_lock_collection("g1") is False                       # a whole run, the check
    xact = sessions()
    assert xact.execute("select pg_try_advisory_xact_lock(hashtextextended(%s, 0))",
                        (ss.collection_lock_key("g1"),)).fetchone()[0] is False      # the app's edits and Delete
    first.unlock_collection_shared("g1")
    second.unlock_collection_shared("g1")
    assert edit.try_lock_collection("g1") is True
    assert first.try_lock_collection_shared("g1") is False               # and the other way round


def test_two_runs_given_one_database_take_it_once_and_the_second_skips_it(sessions):
    first, second = ss.StateStore(sessions()), ss.StateStore(sessions())
    keys = [ss.database_lock_key("g1", "srv", name) for name in ("db1", "db2")]
    assert first.try_lock_databases(keys) == set(keys)
    assert second.try_lock_databases(keys + [ss.database_lock_key("g1", "srv", "db3")]) == \
        {ss.database_lock_key("g1", "srv", "db3")}
    first.unlock_databases(keys)
    assert second.try_lock_databases(keys) == set(keys)


def test_a_run_closes_abandoned_runs_only_when_no_other_run_holds_the_collection(sessions):
    first, second = ss.StateStore(sessions()), ss.StateStore(sessions())
    assert first.try_lock_collection_shared("g1") and second.try_lock_collection_shared("g1")
    assert first.alone_on_collection("g1") is False
    second.unlock_collection_shared("g1")
    assert first.alone_on_collection("g1") is True
    assert second.try_lock_collection_shared("g1") is True               # the exclusive try was let go at once


def test_two_runs_recounting_one_group_at_once_leave_it_exact(sessions):
    """Each run writes its own database's rows and recounts the group; the second recount waits for the first to
    commit, so the group counts both databases."""
    import threading
    setup = sessions()
    setup.execute("insert into lakefed_ingest.control (task_collection, src_host, src_database, src_schema, src_table, "
                  "sink_catalog, sink_schema, sink_table, primary_key, select_list) values "
                  "('g1', 'host-a', 'db1', 'dbo', 't', 'main', 's', 'db1_t', 'id', 'id'), "
                  "('g1', 'host-a', 'db2', 'dbo', 't', 'main', 's', 'db2_t', 'id', 'id')")
    stores = [ss.StateStore(sessions()) for _ in range(2)]
    barrier = threading.Barrier(2)

    def recount(store, database):
        barrier.wait()
        store.rewrite_rollups("g1", [database])
    threads = [threading.Thread(target=recount, args=(s, d)) for s, d in zip(stores, ("db1", "db2"))]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    assert setup.execute("select databases, tables from lakefed_ingest.rollup_group where task_collection = 'g1'"
                         ).fetchone() == (2, 2)


def test_a_work_list_spanning_collections_reads_back_whole_and_per_collection(sessions):
    """A run of a cycle's share: its work list is recorded per collection under one work id, and the run reads it
    whole (its parts) while each part reads its own collection's databases."""
    store = ss.StateStore(sessions())
    store.record_check_work("L:run1", "g1", [("srv", "a"), ("srv", "b")])
    store.record_check_work("L:run1", "g2", [("srv", "c")])
    store.record_check_work("L:run2", "g1", [("srv", "d")])
    assert store.get_check_work_all("L:run1") == {("g1", "srv", "a"), ("g1", "srv", "b"), ("g2", "srv", "c")}
    assert store.get_check_work("L:run1", "g2") == {("srv", "c")}


def test_a_check_and_a_run_of_one_collection_go_at_once_on_different_databases(sessions):
    """The check (shared, groups B) and a work-list run both hold the collection; each has its own databases, and a
    whole-collection run is refused while either goes."""
    check, run, whole = (ss.StateStore(sessions()) for _ in range(3))
    assert check.try_lock_collection_shared("g1") and run.try_lock_collection_shared("g1")
    assert run.try_lock_databases([ss.database_lock_key("g1", "srv", "a")])
    held = check.try_lock_databases([ss.database_lock_key("g1", "srv", n) for n in ("a", "b")])
    assert held == {ss.database_lock_key("g1", "srv", "b")}           # the check leaves the run's database out
    assert whole.try_lock_collection("g1") is False
