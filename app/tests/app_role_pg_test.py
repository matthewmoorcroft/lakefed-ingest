"""The app's writes as the app's own Lakebase role can make them: select, insert and update only, as
grant_app_access.ipynb grants, no DELETE. The other Postgres tests run as the tables' owner, who may do anything, so a
statement the deployed app is refused (a DELETE before an insert, say) passes there; here it fails.

Each test drives the app through its API (TestClient), with Lakebase reached as a role holding exactly those grants
(SET ROLE on every connection the app opens) and the workspace stubbed. The asserts read as the owner.

Skipped unless LAKEFED_APP_TEST_PG names a Postgres server (see monitoring_pg_test.py) where the test may create a
role; skipped, saying so, when it can't."""
import itertools
import os
import sys
import uuid
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402

URI = os.environ.get("LAKEFED_APP_TEST_PG")
pytestmark = pytest.mark.skipif(not URI, reason="LAKEFED_APP_TEST_PG not set")
REPO = Path(__file__).resolve().parents[2]
SETTINGS = {"fan_in": True, "sink_catalog": "main", "sink_schema": "eng", "sink_table": "{table}"}
# The grants of grant_app_access.ipynb, on the schema's tables and sequences.
GRANTS = ("grant usage on schema lakefed_ingest to {role}",
          "grant select, insert, update on all tables in schema lakefed_ingest to {role}",
          "grant usage, select on all sequences in schema lakefed_ingest to {role}")
SUMMARY = {"sch": "dbo", "row_count": "12", "ct_enabled": "1", "select_cols": "id, amount", "excluded_cols": None,
           "pk_cols": "id", "pk_unreadable": "0", "pk_first_type": "int", "rowversion_col": None, "identity_col": None,
           "identity_type": None}


class _Warehouse:
    """The SQL warehouse, answering at once: the server's databases, every database open with 2 tables, and each
    database's two tables."""

    def __init__(self, databases):
        self.databases, self.answers, self.ids = databases, {}, itertools.count(1)

    def start(self, statement, parameters, wait_s=None):
        query = " ".join(str(p.get("value") if isinstance(p, dict) else p) for p in parameters) + statement
        if "sys.databases" in query:
            rows = [{"name": name} for name in self.databases]
        elif "COUNT(*) AS tables" in query:
            rows = [{"tables": "2"}]
        else:
            rows = [{**SUMMARY, "tbl": table} for table in ("orders", "lines")]
        statement_id = f"s-{next(self.ids)}"
        self.answers[statement_id] = {"statement_id": statement_id, "state": "SUCCEEDED", "rows": rows, "error": None}
        return self.answers[statement_id]

    def read(self, statement_id):
        return self.answers[statement_id]


@pytest.fixture
def client(monkeypatch):
    psycopg = pytest.importorskip("psycopg")
    sys.path.insert(0, str(REPO / "src" / "lakefed_ingest"))
    import state_store
    name = f"lakefed_role_test_{uuid.uuid4().hex[:8]}"
    role = f"lakefed_app_role_{uuid.uuid4().hex[:8]}"
    admin = psycopg.connect(URI, autocommit=True)
    admin.execute(f"create database {name}")
    target = psycopg.conninfo.make_conninfo(URI, dbname=name)
    owner = psycopg.connect(target, autocommit=True)
    made_role = False
    try:
        schema = (REPO / "src/lakefed_ingest/lakebase_schema.sql").read_text()
        for statement in state_store.split_sql_statements(schema):
            owner.execute(statement)
        try:
            owner.execute(f"create role {role} nologin")
            made_role = True
            owner.execute(f"grant {role} to current_user")
        except psycopg.Error as error:
            pytest.skip(f"can't make a role to test the app's grants with here: {error}".splitlines()[0])
        for grant in GRANTS:
            owner.execute(grant.format(role=role))

        def as_app():
            conn = psycopg.connect(target, autocommit=True)
            conn.execute(f"set role {role}")
            return conn
        monkeypatch.setattr(main.db, "connect", as_app)
        monkeypatch.setattr(main.jobs, "schedule_variants", lambda: {"default": "1", "check": "2"})
        monkeypatch.setattr(main.jobs, "user_job_permission", lambda job_id, user: "CAN_MANAGE")
        monkeypatch.setattr(main, "_sync", lambda sid, user: [])
        monkeypatch.setattr(main, "_refuse_job_tables", lambda job, groups, request: None)
        monkeypatch.setattr(main, "_require_connection", lambda name: None)
        monkeypatch.setattr(main, "_job_active_runs", lambda job, members: [])
        monkeypatch.setattr(main, "_start_background", lambda work: work())
        monkeypatch.setattr(main, "TASK_BEAT_S", 3600)
        fake = _Warehouse([f"eng_{n:03d}" for n in range(1, 4)])
        monkeypatch.setattr(main.warehouse, "configured", lambda: True)
        monkeypatch.setattr(main.warehouse, "start", fake.start)
        monkeypatch.setattr(main.warehouse, "read", fake.read)
        monkeypatch.setattr(main.warehouse, "warehouse_state", lambda: "RUNNING")
        c = TestClient(main.app, headers={"X-Forwarded-Email": "op@example.com"})
        c.role, c.owner = role, owner
        c.sql = lambda statement, params=(): owner.execute(statement, params).fetchall()
        yield c
    finally:
        if made_role:
            owner.execute(f"drop owned by {role}")
        owner.close()
        admin.execute(f"drop database {name} with (force)")
        if made_role:
            admin.execute(f"drop role if exists {role}")
        admin.close()


def _discover(client, databases):
    """Discover on a source as the page does it through the warehouse: the listing (the connection test), the access
    check of the page shown, then the read of the chosen databases until it ends. Returns the read's discovery id."""
    listing = client.post("/api/discoveries/source", json={"src_connection": "sql_a", "kind": "list"})
    assert listing.status_code == 200 and listing.json()["state"] == "SUCCEEDED", listing.text
    listing_id = listing.json()["discovery_id"]
    for _ in range(3):
        access = client.post(f"/api/discoveries/{listing_id}/access?connection=sql_a", json={"first": databases})
        assert access.status_code == 200, access.text
    read = client.post("/api/discoveries/source", json={"src_connection": "sql_a", "kind": "tables",
                                                        "databases": databases})
    assert read.status_code == 200, read.text
    did = read.json()["discovery_id"]
    for _ in range(5):
        step = client.get(f"/api/discoveries/{did}/reads?connection=sql_a")
        assert step.status_code == 200, step.text
        if step.json()["state"] == "SUCCEEDED":
            break
    assert step.json()["failed"] == 0 and step.json()["done"] == len(databases), step.json()
    return did


def test_the_role_really_has_no_delete(client):
    """The harness itself: the app's role is refused a DELETE, so the other tests would see one."""
    conn = main.db.connect()
    try:
        with pytest.raises(Exception, match="permission denied"):
            conn.execute("delete from lakefed_ingest.discovered_table")
    finally:
        conn.close()


def test_discovery_through_the_warehouse_runs_as_the_apps_role(client):
    did = _discover(client, ["eng_001", "eng_002"])
    tables = client.sql("select src_database, src_table from lakefed_ingest.discovered_table where discovery_id = %s "
                        "order by 1, 2", (did,))
    assert tables == [("eng_001", "lines"), ("eng_001", "orders"), ("eng_002", "lines"), ("eng_002", "orders")]


def test_a_job_by_database_then_add_tables_and_a_new_number_of_groups_run_as_the_apps_role(client):
    did = _discover(client, ["eng_001", "eng_002", "eng_003"])
    selection = {"discovery_id": did, "databases": [{"server": "sql_a", "database": d} for d in ("eng_001", "eng_002")]}
    made = client.post("/api/ingestion-jobs", json={"name": "Role", "settings": SETTINGS, "groups_per_server": 2,
                                                    "dry_run": False, "request_id": "role-1", "source": selection})
    assert made.status_code in (200, 202), made.text
    job_id = made.json().get("id") or client.get(f"/api/tasks/{made.json()['task']['id']}").json()["job_id"]
    added = client.post(f"/api/ingestion-jobs/{job_id}/tables", json={
        "source": {"discovery_id": did, "databases": [{"server": "sql_a", "database": "eng_003"}]},
        "dry_run": False, "request_id": "role-2"})
    assert added.status_code in (200, 202), added.text
    regrouped = client.post(f"/api/ingestion-jobs/{job_id}/groups", json={"groups_per_server": 3, "dry_run": False,
                                                                           "request_id": "role-3"})
    assert regrouped.status_code == 200, regrouped.text
    assert client.sql("select count(*), count(distinct src_database) from lakefed_ingest.control") == [(6, 3)]


def _draft(client, databases):
    """The read started as the databases are chosen (a draft), one step done: its discovery id, its reads not ended."""
    listing = client.post("/api/discoveries/source", json={"src_connection": "sql_a", "kind": "list"}).json()
    access = client.post(f"/api/discoveries/{listing['discovery_id']}/access?connection=sql_a",
                         json={"first": databases})
    assert access.status_code == 200, access.text
    read = client.post("/api/discoveries/source", json={"src_connection": "sql_a", "kind": "tables", "draft": True,
                                                        "databases": databases[:1]})
    assert read.status_code == 200, read.text
    did = read.json()["discovery_id"]
    grown = client.post("/api/discoveries/source", json={"src_connection": "sql_a", "kind": "tables", "draft_id": did,
                                                         "databases": databases[1:]})
    assert grown.status_code == 200 and grown.json()["discovery_id"] == did, grown.text
    return did


@pytest.mark.parametrize("by_database", [False, True])
def test_create_from_a_draft_still_reading_waits_for_it_then_takes_it_as_the_apps_role(client, monkeypatch, by_database):
    monkeypatch.setattr(main, "DRAFT_POLL_S", 0)
    if by_database:
        monkeypatch.setattr(main, "BACKGROUND_ROWS", 1)
    did = _draft(client, ["eng_001", "eng_002"])
    assert client.sql("select count(*) from lakefed_ingest.discovery where discovery_id = %s and status = 'running'",
                      (did,))[0][0] > 0, "the draft's read should still be going when Create is pressed"
    selection = {"discovery_id": did, "databases": [{"server": "sql_a", "database": d} for d in ("eng_001", "eng_002")]}
    preview = client.post("/api/ingestion-jobs", json={"name": "Draft", "settings": SETTINGS, "source": selection})
    assert preview.status_code == 200, preview.text
    made = client.post("/api/ingestion-jobs", json={"name": "Draft", "settings": SETTINGS, "dry_run": False,
                                                    "request_id": f"draft-{by_database}", "source": selection})
    assert made.status_code in (200, 202), made.text
    assert client.sql("select count(*), count(distinct src_database) from lakefed_ingest.control") == [(4, 2)]
    marks = client.sql("select distinct listing ? 'draft', (listing ->> 'job_id') is not null "
                       "from lakefed_ingest.discovery where discovery_id = %s", (did,))
    assert marks == [(False, True)], "Create takes the draft: no draft mark left, the job named"


def test_a_discarded_draft_reads_no_more_and_cant_be_created_from_as_the_apps_role(client, monkeypatch):
    cancelled = []
    monkeypatch.setattr(main.warehouse, "cancel", cancelled.append)
    did = _draft(client, ["eng_001", "eng_002"])
    gone = client.post(f"/api/discoveries/{did}/discard")
    assert gone.status_code == 200 and gone.json()["cancelled"] == len(cancelled), gone.text
    step = client.get(f"/api/discoveries/{did}/reads?connection=sql_a").json()
    assert step["state"] in ("SUCCEEDED", "RUNNING")
    assert client.sql("select count(*) from lakefed_ingest.discovery where discovery_id = %s "
                      "and not listing ? 'discarded'", (did,)) == [(0,)]
    made = client.post("/api/ingestion-jobs", json={"name": "Gone", "settings": SETTINGS, "dry_run": False,
                                                    "request_id": "draft-gone", "source": {"discovery_id": did,
                                                    "databases": [{"server": "sql_a", "database": "eng_001"}]}})
    assert made.status_code == 422 and "discarded" in made.text, made.text
    assert client.sql("select count(*) from lakefed_ingest.control") == [(0,)]
    import queries as q
    assert main.db.query(q.drafts_to_purge_sql(), (7200,)) == [{"discovery_id": did}]    # the poller's look, as the app


def test_manages_export_reads_as_the_apps_role(client, monkeypatch):
    did = _discover(client, ["eng_001", "eng_002"])
    selection = {"discovery_id": did, "databases": [{"server": "sql_a", "database": d} for d in ("eng_001", "eng_002")]}
    made = client.post("/api/ingestion-jobs", json={"name": "Role", "settings": SETTINGS, "groups_per_server": 1,
                                                    "dry_run": False, "request_id": "role-export", "source": selection})
    assert made.status_code in (200, 202), made.text
    monkeypatch.setattr(main.jobs, "APP_NAME", "lakefed-ops")
    monkeypatch.setattr(main.jobs, "user_app_permission", lambda user: "CAN_MANAGE")
    monkeypatch.setattr(main.q, "EXPORT_PAGE", 3)
    exported = client.get("/api/manage/export.csv")
    assert exported.status_code == 200, exported.text
    lines = exported.text.splitlines()
    assert lines[0].startswith("ingestion_job,src_connection,src_database,") and len(lines) == 5
    assert {line.split(",")[0] for line in lines[1:]} == {"Role"}


def test_the_schema_upgrade_reads_the_version_and_takes_its_lock_as_the_apps_role(client, monkeypatch):
    import schema_upgrade
    started = []

    def run_now(job_id):
        started.append(job_id)
        return 41
    # The schema is applied here without recording a version, as a database with no schema reads: left alone.
    nothing = schema_upgrade.check(main.db.connect, "16", lambda job_id: [], run_now)
    assert (nothing["state"], nothing["database"], started) == ("missing", 0, [])
    behind = schema_upgrade.SCHEMA_VERSION - 1
    client.sql("insert into lakefed_ingest.schema_version (version) values (%s) returning version", (behind,))
    with main.db.connect() as conn:
        assert schema_upgrade.database_version(conn) == behind
    other = main.db.connect()
    try:
        assert other.execute("select pg_try_advisory_lock(%s, %s)", schema_upgrade.UPGRADE_LOCK).fetchone()[0]
        held = schema_upgrade.check(main.db.connect, "16", lambda job_id: [], run_now)
        assert (held["state"], started) == ("busy", []), held
        other.execute("select pg_advisory_unlock(%s, %s)", schema_upgrade.UPGRADE_LOCK)
        first = schema_upgrade.check(main.db.connect, "16", lambda job_id: [], run_now)
        assert (first["state"], first["database"], first["run_id"], first["error"]) == ("started", behind, 41, None)
        again = schema_upgrade.check(main.db.connect, "16", lambda job_id: [{"run_id": 41, "url": "https://h/run/41"}],
                                     run_now)
        assert (again["state"], started) == ("running", ["16"]), again
        assert other.execute("select pg_try_advisory_lock(%s, %s)", schema_upgrade.UPGRADE_LOCK).fetchone()[0], (
            "the check lets its lock go")
    finally:
        other.close()
    assert client.sql("select action, field, old_value, new_value from lakefed_ingest.control_change "
                      "where action = 'schema_upgrade'") == [("schema_upgrade", "schema_version", str(behind),
                                                              str(schema_upgrade.SCHEMA_VERSION))]
    monkeypatch.setattr(main.jobs, "APP_NAME", "lakefed-ops")
    monkeypatch.setattr(main.jobs, "user_app_permission", lambda user: "CAN_MANAGE")
    monkeypatch.setattr(main.jobs, "app_deployment", lambda: {})
    monkeypatch.setattr(main.jobs, "SETUP_JOB_ID", "16")
    monkeypatch.setattr(main.run_poller, "runs_of", lambda job_id, active_only, limit: [{"run_id": 41, "url": "u"}])
    schema = client.get("/api/manage/version").json()["schema"]
    assert (schema["database"], schema["app"], schema["run"], schema["error"]) == (
        behind, schema_upgrade.SCHEMA_VERSION, {"run_id": 41, "url": "u"}, None)
