"""A database's check stamp key, built in SQL (the app's and the engine's ``database_source_sql``) and in
Python, is the same text for every source type and port; and re-applying the schema drops the stamps
the old blank-port key left behind. Run against a real Postgres.

Skipped unless LAKEFED_APP_TEST_PG names a Postgres server (a libpq URI to any database there) where
the test may create and drop its own database."""
import os
import sys
import uuid
from pathlib import Path

import pytest
import queries as q

URI = os.environ.get("LAKEFED_APP_TEST_PG")
pytestmark = pytest.mark.skipif(not URI, reason="LAKEFED_APP_TEST_PG not set")
REPO = Path(__file__).resolve().parents[2]
CASES = [{"src_type": t, "src_host": "Host-A", "src_connection": "conn", "src_port": port, "secret_key": secret}
         for t in (None, "", "sqlserver", "oracle", "postgresql", "mysql", "db2")
         for port in (None, 0, 1433, 1521, 5432, 3306, 3307)
         for secret in (None, "k1")]


@pytest.fixture(scope="module")
def pg():
    psycopg = pytest.importorskip("psycopg")
    name = f"lakefed_app_source_{uuid.uuid4().hex[:8]}"
    admin = psycopg.connect(URI, autocommit=True)
    admin.execute(f"create database {name}")
    conn = psycopg.connect(psycopg.conninfo.make_conninfo(URI, dbname=name), autocommit=True)
    try:
        _apply_schema(conn)
        yield conn
    finally:
        conn.close()
        admin.execute(f"drop database {name}")
        admin.close()


def test_the_sql_stamp_key_is_the_python_one_for_every_source_type_and_port(pg):
    engine = _engine()
    placeholders = ", ".join("(cast(%s as text), cast(%s as text), cast(%s as text), cast(%s as integer), "
                             "cast(%s as text))" for _ in CASES)
    params = [v for c in CASES for v in (c["src_type"], c["src_host"], c["src_connection"], c["src_port"],
                                         c["secret_key"])]
    sql = (f"select {q.database_source_sql('t')} from (values {placeholders}) "
           "as t(src_type, src_host, src_connection, src_port, secret_key)")
    in_sql = [row[0] for row in pg.execute(sql, params).fetchall()]
    assert in_sql == [engine.database_source(c) for c in CASES]
    blank = CASES.index({"src_type": "sqlserver", "src_host": "Host-A", "src_connection": "conn", "src_port": None,
                         "secret_key": "k1"})
    assert in_sql[blank] == "host-a|1433|k1"


def test_reapplying_the_schema_drops_the_stamps_under_the_old_blank_port_key(pg):
    pg.execute("delete from lakefed_ingest.database_check")
    stamps = [("srv|0|k", "db_old"), ("srv|1433|k", "db_new"), ("srv|1433|a|0|b", "db_pipe")]
    for source, database in stamps:
        pg.execute("insert into lakefed_ingest.database_check (task_collection, source, src_database, "
                   "checked_version, tables) values ('c1', %s, %s, 1, 1)", (source, database))
    _apply_schema(pg)
    left = sorted(r[0] for r in pg.execute("select source from lakefed_ingest.database_check").fetchall())
    assert left == ["srv|1433|a|0|b", "srv|1433|k"]


def _engine():
    engine_dir = REPO / "src" / "lakefed_ingest"
    if str(engine_dir) not in sys.path:
        sys.path.insert(0, str(engine_dir))
    import state_store
    return state_store


def _apply_schema(conn):
    state_store = _engine()
    for statement in state_store.split_sql_statements((REPO / "src/lakefed_ingest/lakebase_schema.sql").read_text()):
        conn.execute(statement)
