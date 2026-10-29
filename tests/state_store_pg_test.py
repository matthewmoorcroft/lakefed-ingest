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
