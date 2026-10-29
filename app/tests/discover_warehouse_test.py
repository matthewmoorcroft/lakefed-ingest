"""Test connection and the database listing through the SQL warehouse (POST /api/discoveries/source with a warehouse
set, then GET /api/discoveries/{id}/warehouse/{statement}), with the warehouse client and the database faked."""
import json

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
import queries as q  # noqa: E402
import warehouse  # noqa: E402


class _Warehouse:
    """The warehouse client: each statement answers the states given for it in turn, the last one repeating."""
    ENDED = warehouse.ENDED

    def __init__(self, answers, state="RUNNING"):
        self.answers, self.state, self.sent, self.read_ids = list(answers), state, [], []

    def configured(self):
        return True

    def remote_query_statement(self, connection, database, query):
        return warehouse.remote_query_statement(connection, database, query)

    def start(self, statement, parameters):
        self.sent.append((statement, parameters))
        return self._next()

    def read(self, statement_id):
        self.read_ids.append(statement_id)
        return self._next()

    def warehouse_state(self):
        return self.state

    def _next(self):
        return self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]


def _answer(state, rows=None, error=None):
    return {"statement_id": "s1", "state": state, "rows": rows, "error": error}


@pytest.fixture
def client(monkeypatch):
    written = []
    monkeypatch.setattr(main, "connections", lambda: {"connections": [{"name": "sql_a"}], "error": None})
    monkeypatch.setattr(main.jobs, "run_now", lambda job_id, params: pytest.fail("the discovery job was started"))
    monkeypatch.setattr(main.db, "execute", lambda sql, params=(): written.append((sql, params)))
    c = TestClient(main.app, headers={"X-Forwarded-Email": "op@example.com"})
    c.written = written

    def use(answers, state="RUNNING"):
        fake = _Warehouse(answers, state)
        monkeypatch.setattr(main, "warehouse", fake)
        return fake
    c.use = use
    return c


def _rows_written(client):
    """The discovery rows written, as (database, status, error) per row, from every insert."""
    out = []
    for sql, params in client.written:
        assert sql.startswith(f"insert into {q.SCHEMA}.discovery ")
        width = len(q.DISCOVERY_ROW_COLUMNS)
        for at in range(0, len(params), width):
            row = dict(zip(q.DISCOVERY_ROW_COLUMNS, params[at:at + width]))
            out.append(row)
    return out


def test_test_connection_lists_every_database_through_the_warehouse_in_one_statement_and_saves_them(client):
    fake = client.use([_answer("SUCCEEDED", rows=[{"name": "eng_002"}, {"name": "eng_001"}])])
    body = client.post("/api/discoveries/source", json={"src_connection": "sql_a", "kind": "list"}).json()
    assert body["via"] == "warehouse" and body["state"] == "SUCCEEDED" and body["databases"] == 2
    statement, parameters = fake.sent[0]
    assert statement == "SELECT * FROM remote_query(:conn, database => :db, query => :q)"
    values = {p["name"]: p["value"] for p in parameters}
    assert values["conn"] == "sql_a" and values["db"] == "master"
    assert values["q"] == main.source_catalog.databases_sql("")
    rows = _rows_written(client)
    assert [(r["src_database"], r["status"]) for r in rows] == [("eng_001", "listed"), ("eng_002", "listed")]
    assert {r["discovery_id"] for r in rows} == {body["discovery_id"]}
    assert rows[0]["src_connection"] == "sql_a" and rows[0]["requested_by"] == "op@example.com"
    assert json.loads(rows[0]["listing"]) == {"via": "warehouse"}


def test_a_listing_still_going_answers_its_statement_and_whether_the_warehouse_is_starting(client):
    client.use([_answer("PENDING")], state="STARTING")
    body = client.post("/api/discoveries/source", json={"src_connection": "sql_a", "kind": "list"}).json()
    assert body == {"discovery_id": body["discovery_id"], "via": "warehouse", "statement_id": "s1", "state": "PENDING",
                    "warehouse": "STARTING"}
    assert client.written == []


def test_the_page_asks_again_until_the_listing_ends_then_it_is_saved_once(client):
    fake = client.use([_answer("PENDING"), _answer("RUNNING"), _answer("SUCCEEDED", rows=[{"name": "eng_001"}])])
    started = client.post("/api/discoveries/source", json={"src_connection": "sql_a", "kind": "list"}).json()
    path = f"/api/discoveries/{started['discovery_id']}/warehouse/s1?connection=sql_a"
    assert client.get(path).json()["state"] == "RUNNING"
    done = client.get(path).json()
    assert done["state"] == "SUCCEEDED" and done["databases"] == 1
    assert fake.read_ids == ["s1", "s1"] and len(_rows_written(client)) == 1
    assert "on conflict (discovery_id, src_database) do nothing" in client.written[0][0]


def test_a_failed_listing_says_why_and_records_it_on_master(client):
    client.use([_answer("FAILED", error="[REMOTE_QUERY] Login failed for user 'x'.")])
    body = client.post("/api/discoveries/source", json={"src_connection": "sql_a", "kind": "list"}).json()
    assert body["state"] == "FAILED" and body["error"] == "[REMOTE_QUERY] Login failed for user 'x'."
    rows = _rows_written(client)
    assert [(r["src_database"], r["status"], r["error"]) for r in rows] == \
        [("master", "failed", "[REMOTE_QUERY] Login failed for user 'x'.")]


def test_a_server_with_no_user_database_lists_none_and_says_so(client):
    client.use([_answer("SUCCEEDED", rows=[])])
    body = client.post("/api/discoveries/source", json={"src_connection": "sql_a", "kind": "list"}).json()
    assert body["state"] == "SUCCEEDED" and body["databases"] == 0 and client.written == []


def test_asking_after_a_listing_refuses_a_connection_the_app_cant_see(client):
    client.use([_answer("RUNNING")])
    answer = client.get("/api/discoveries/d1/warehouse/s1?connection=sql_other")
    assert answer.status_code == 404 and "USE CONNECTION" in answer.json()["detail"]


def test_listing_through_azure_still_runs_the_discovery_job(client, monkeypatch):
    client.use([_answer("SUCCEEDED", rows=[])])
    started = []
    monkeypatch.setattr(main.jobs, "DISCOVER_JOB_ID", "555")
    monkeypatch.setattr(main, "_refuse_run_of", lambda job_id, request: None)
    monkeypatch.setattr(main.jobs, "run_now", lambda job_id, params: started.append(params) or 42)
    body = client.post("/api/discoveries/source", json={
        "src_connection": "sql_a", "kind": "list", "azure_credential": "reader",
        "azure_resource_id": "/subscriptions/1/resourceGroups/rg/providers/Microsoft.Sql/servers/srv"}).json()
    assert body["run_id"] == 42 and started


def test_with_no_warehouse_set_the_listing_runs_the_discovery_job_as_before(client, monkeypatch):
    fake = client.use([_answer("SUCCEEDED", rows=[])])
    monkeypatch.setattr(fake, "configured", lambda: False)
    started = []
    monkeypatch.setattr(main.jobs, "DISCOVER_JOB_ID", "555")
    monkeypatch.setattr(main, "_refuse_run_of", lambda job_id, request: None)
    monkeypatch.setattr(main.jobs, "run_now", lambda job_id, params: started.append(params) or 42)
    assert client.post("/api/discoveries/source", json={"src_connection": "sql_a", "kind": "list"}).json()["run_id"] == 42
    assert fake.sent == []
