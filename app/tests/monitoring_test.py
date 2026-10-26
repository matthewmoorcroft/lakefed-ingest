"""Monitoring views: the overview's progress and state counts, a collection's and a run's databases,
rows written per run, grouped failures and UC connections. SQL shape and Python logic here; the SQL
itself runs against a real Postgres in monitoring_pg_test.py."""
import json

import pytest
import queries as q

MONITORING_SQL = (q.servers_sql(), q.collection_databases_sql(), q.collection_database_rows_sql(),
                  q.run_databases_sql(), q.run_rows_sql(), q.run_batch_rows_sql(), q.collection_batch_runs_sql(),
                  q.connection_databases_sql())


def test_monitoring_statements_are_parameterized_and_schema_qualified():
    for sql in MONITORING_SQL:
        assert "lakefed_ingest." in sql
        assert "{" not in sql            # every f-string placeholder was filled
    assert q.collection_databases_sql().count("%s") == 2
    assert q.collection_database_rows_sql().count("%s") == 1
    assert q.collection_batch_runs_sql().count("%s") == 2
    assert q.failed_tables_sql().count("%s") == 2
    assert q.connection_databases_sql().count("%s") == 2


def test_running_runs_report_progress_over_the_whole_collection():
    sql = q.running_runs_sql()
    for column in ("enabled_tables", "tables_checked", "last_activity_at", "server"):
        assert f"as {column}" in sql
    assert "cp.last_success_at >= r.started_at" in sql


def test_overview_adds_exclusive_states_and_rows_without_changing_existing_columns():
    sql = q.collections_overview_sql()
    for column in ("enabled_tables", "failing_tables", "seeding_tables", "sla_breached_tables", "last_run_status",
                   "state_current", "state_late", "state_first_load", "state_failing", "rows_24h", "databases"):
        assert column in sql
    assert "%s" not in sql


@pytest.mark.parametrize("detail,expected", [
    (json.dumps({"batch_rows": 5, "batch_rows_unknown": 2}), (5, 2)),
    ({"batch_rows": 7}, (7, 0)),
    ("not json", (0, 0)),
    ("[1, 2]", (0, 0)),
    (None, (0, 0)),
])
def test_batch_rows_reads_a_run_detail_safely(detail, expected):
    assert q.batch_rows(detail) == expected


@pytest.mark.parametrize("error,expected", [
    ("[FAILED_JDBC.CONNECTION] Failed to connect to the database: connection reset", "connection"),
    ("Read timed out after 30000 ms", "timeout"),
    ("The SELECT permission was denied on the object 'orders'", "permission"),
    ("Invalid object name 'dbo.orders'.", "missing"),
    ("Something else entirely", "other"),
    (None, "other"),
    # Near misses: a name that contains a cause word is not that cause.
    ("Invalid column name 'network_id'.", "missing"),
    ("Invalid column name 'timeout_ms'.", "missing"),
    ("Invalid column name 'socket_count'.", "missing"),
    ("Invalid column name 'network'.", "missing"),
    ("Invalid column name 'socket'.", "missing"),
    ("Invalid column name 'timeout'.", "missing"),
    ("Invalid object name 'dbo.network'.", "missing"),
    ("Invalid column name 'network status'.", "missing"),
    ('Invalid column name "network".', "missing"),
    ("Invalid object name [dbo].[network].", "missing"),
    ("connection reset while reading [dbo].[orders]", "connection"),
    ("Login timeout expired for 'reader'", "connection"),
    ('The TCP/IP connection to the host sql01, port 1433 has failed. Error: "Connection timed out: no further '
     'information.". Verify the connection properties.', "connection"),
    ("[FAILED_JDBC.CONNECTION] Failed to open the connection", "connection"),
    ("A network-related or instance-specific error occurred while establishing a connection", "connection"),
    ("java.net.SocketException: Connection reset", "connection"),
    ("The TCP/IP connection to the host failed: network is unreachable", "connection"),
])
def test_classify_error(error, expected):
    assert q.classify_error(error)[0] == expected


def test_error_signature_blanks_names_and_numbers_so_tables_group_together():
    a = q.error_signature("Invalid object name 'dbo.t0041'. Line 12")
    b = q.error_signature("Invalid object name 'dbo.t0838'. Line 7")
    assert a == b and "t0041" not in a


def test_group_errors_counts_tables_and_databases_per_cause():
    rows = [dict(finished_at=f"2026-10-23T07:{m:02d}:00+00:00", task_collection=c, src_database=d, src_schema="dbo",
                 src_table=t, error="[FAILED_JDBC.CONNECTION] Failed to connect to the database: connection reset")
            for m, c, d, t in ((51, "c7", "db_083", "t0041"), (46, "c7", "db_083", "t0838"),
                               (40, "c6", "db_073", "t0328"))]
    rows.append(dict(finished_at="2026-10-23T08:00:00+00:00", task_collection="c1", src_database="db_001",
                     src_schema="dbo", src_table="x", error="Invalid object name 'dbo.x'."))
    groups = q.group_errors(rows)
    assert [g["error_class"] for g in groups] == ["connection", "missing"]
    first = groups[0]
    assert first["tables"] == 3 and len(first["databases"]) == 2
    assert first["first_at"] < first["last_at"]
    assert first["databases"][1]["tables"] == ["dbo.t0041", "dbo.t0838"]


def test_group_errors_counts_a_table_failing_on_every_run_once():
    rows = [dict(finished_at=f"2026-10-23T{h:02d}:00:00+00:00", task_collection="c1", control_id=7, src_database="db1",
                 src_schema="dbo", src_table="orders", error="Read timed out after 30000 ms") for h in (9, 8, 7)]
    g = q.group_errors(rows)[0]
    assert (g["tables"], g["failures"]) == (1, 3)
    assert g["databases"][0]["tables"] == ["dbo.orders"]
    assert (g["first_at"], g["last_at"]) == ("2026-10-23T07:00:00+00:00", "2026-10-23T09:00:00+00:00")


def test_error_groups_say_when_the_list_was_cut_short(monkeypatch):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    import main
    many = [dict(finished_at="2026-10-23T09:00:00+00:00", task_collection="c1", control_id=i, src_database="db1",
                 src_schema="dbo", src_table=f"t{i}", error="Read timed out") for i in range(4)]
    monkeypatch.setattr(main, "ERROR_TABLES_LIMIT", 3)
    monkeypatch.setattr(main.db, "query", lambda sql, params=(): many[:params[1]])
    body = TestClient(main.app).get("/api/errors/groups?hours=5").json()
    assert body["truncated"] is True and body["tables_shown"] == 3
    assert sum(g["tables"] for g in body["groups"]) == 3


def test_a_table_failing_for_two_causes_is_in_both_groups():
    base = dict(task_collection="c1", control_id=7, src_database="db1", src_schema="dbo", src_table="orders")
    rows = [dict(base, finished_at="2026-10-23T09:00:00+00:00", error="Invalid column name 'qty'.", failures=1),
            dict(base, finished_at="2026-10-23T08:00:00+00:00", error="connection reset", failures=1)]
    by_class = {g["error_class"]: g for g in q.group_errors(rows)}
    assert set(by_class) == {"missing", "connection"}
    assert by_class["connection"]["tables"] == by_class["missing"]["tables"] == 1
    assert by_class["connection"]["last_at"] == "2026-10-23T08:00:00+00:00"


def test_failure_groups_keep_two_servers_databases_of_one_name_apart():
    rows = [dict(task_collection="c1", server=s, control_id=i, src_database="Sales", src_schema="dbo", src_table=t,
                 finished_at="2026-10-23T09:00:00+00:00", error="connection reset")
            for i, s, t in ((1, "srvX", "orders"), (2, "srvY", "customers"))]
    g = q.group_errors(rows)[0]
    assert sorted((d["server"], d["tables"][0]) for d in g["databases"]) == [("srvX", "dbo.orders"), ("srvY", "dbo.customers")]


# ---- API (needs FastAPI and httpx, like api_test.py) --------------------------------------------

@pytest.fixture
def api(monkeypatch):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    import main
    from fastapi.testclient import TestClient

    answers = {
        q.collections_overview_sql(): [dict(task_collection="c1", rows_24h=100)],
        q.running_runs_sql(): [dict(run_id=9, task_collection="c1", enabled_tables=10, tables_checked=4)],
        q.servers_sql(): [dict(server="s1", databases=2, tables=10)],
        q.run_batch_rows_sql(): [dict(run_id=8, task_collection="c1", detail=json.dumps({"batch_rows": 50}))],
        q.collection_databases_sql(): [dict(server="s1", src_database="db1", tables=5), dict(server="s1", src_database="db2", tables=5)],
        q.collection_database_rows_sql(): [dict(server="s1", src_database="db1", rows_first_loads=1000, first_loads=5,
                                                first_loads_uncounted=0, rows_changes_24h=3, in_flight=1,
                                                failed_this_run=0)],
        q.collection_batch_runs_sql(): [dict(run_id=7, detail=json.dumps({"batch_rows": 400, "batch_rows_unknown": 0}),
                                             databases=["db2"], keys=[q.server_database_key("s1", "db2")])],
        q.run_rows_sql(): [dict(run_id=9, detail=None, rows_written=12, uncounted=1),
                           dict(run_id=8, detail=json.dumps({"batch_rows": 50, "batch_rows_unknown": 0}),
                                rows_written=None, uncounted=3)],
        q.recent_errors_sql(): [dict(finished_at="2026-10-23T07:40:00+00:00", task_collection="c1", src_database="db1",
                                     src_schema="dbo", src_table="t", error="connection reset")],
        q.failed_tables_sql(): [dict(finished_at="2026-10-23T07:40:00+00:00", task_collection="c1", control_id=1, src_database="db1",
                                     src_schema="dbo", src_table="t", error="connection reset")],
        q.connection_databases_sql(): [dict(src_database="db1", collections="c1, c2", tables=4,
                                            newest_success_at=None)],
    }

    def query(sql, params=()):
        if sql.startswith("select run_id, task_collection, status, started_at"):      # runs_sql
            return [dict(run_id=9, task_collection="c1", status="running"), dict(run_id=8, task_collection="c1",
                                                                                 status="completed")]
        return answers.get(sql, [])

    class Connection:
        def __init__(self, name, kind, host):
            self.name, self.connection_type, self.options = name, kind, {"host": host, "port": "1433"}
            self.owner, self.comment = "owner", None

    class Workspace:
        class connections:
            @staticmethod
            def list():
                return [Connection("sql_b", "SQLSERVER", "h2"), Connection("pg_a", "POSTGRESQL", "h1")]

    monkeypatch.setattr(main.db, "query", query)
    monkeypatch.setattr(main.db, "workspace", lambda: Workspace())
    monkeypatch.setattr(main, "MONITOR_CACHE_S", 0)
    return TestClient(main.app)


def test_overview_adds_batched_rows_servers_and_progress(api):
    data = api.get("/api/overview").json()
    assert data["collections"][0]["rows_24h"] == 150
    assert data["running"][0]["tables_checked"] == 4 and data["servers"][0]["server"] == "s1"


def test_collection_databases_merge_rows_and_place_a_batch_on_its_one_database(api):
    data = api.get("/api/collections/c1/databases").json()
    by_name = {d["src_database"]: d for d in data["databases"]}
    assert by_name["db1"]["rows_first_loads"] == 1000 and by_name["db1"]["in_flight"] == 1
    assert by_name["db2"]["rows_first_loads"] is None and by_name["db2"]["batches"] == [{"run_id": 7, "rows": 400}]
    assert data["running"]["run_id"] == 9


def test_runs_report_rows_written_with_batches_and_say_when_partial(api):
    runs = {r["run_id"]: r for r in api.get("/api/runs").json()}
    assert runs[9]["rows_written"] == 12 and runs[9]["rows_partial"] is True
    assert runs[8]["rows_written"] == 50 and runs[8]["rows_partial"] is False


def test_error_groups_endpoint(api):
    body = api.get("/api/errors/groups?hours=24").json()
    groups = body["groups"]
    assert groups[0]["error_class"] == "connection" and groups[0]["tables"] == 1
    assert (body["tables_shown"], body["truncated"]) == (1, False)


def test_connections_list_sorted_with_tracked_counts_and_discoverable_flag(api):
    data = api.get("/api/connections").json()
    assert [c["name"] for c in data["connections"]] == ["pg_a", "sql_b"]
    sql_b = data["connections"][1]
    assert sql_b["discoverable"] is True and data["connections"][0]["discoverable"] is False
    assert sql_b["tracked"] == {"databases": 1, "tables": 4, "collections": 2}
    assert api.get("/api/connections/sql_b/databases").json()[0]["src_database"] == "db1"
    assert api.get("/api/connections/nope/databases").status_code == 404


def test_connections_listing_error_is_returned_not_raised(api, monkeypatch):
    import main

    def refuse():
        raise PermissionError("no USE CONNECTION")
    monkeypatch.setattr(main, "_connections", refuse)
    data = api.get("/api/connections").json()
    assert data["connections"] == [] and "USE CONNECTION" in data["error"]
