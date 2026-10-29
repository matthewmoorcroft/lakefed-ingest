"""Every ingestion group belongs to an ingestion job: a write that would put tables into a group no job holds is
refused before anything is written (main._refuse_groups_outside_jobs), and a group with tables can't leave its job
(schedules_api_test.py). New ingestion job makes its groups with the job, and Add tables uses the job's own groups."""
import contextlib

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi import HTTPException  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
import queries as q  # noqa: E402
from api_test import DISCOVERED, FakeCursor, FakeDB  # noqa: E402

JOBS = [{"id": 1, "name": "Engagements"}]
MEMBERS = [{"schedule_id": 1, "task_collection": "eng_c01", "removed_at": None},
           {"schedule_id": 1, "task_collection": "eng_c02", "removed_at": "2026-10-29T10:00:00+00:00"}]


def _jobs(sql, params=()):
    if sql == q.schedules_sql():
        return [dict(j) for j in JOBS]
    if sql == q.schedule_members_sql():
        return [dict(m) for m in MEMBERS]
    return DISCOVERED if "discovered_table" in sql else []


@pytest.fixture
def client(monkeypatch):
    fake = FakeDB()
    monkeypatch.setattr(main.db, "query", _jobs)
    monkeypatch.setattr(main.db, "transaction", contextlib.contextmanager(lambda: (yield FakeCursor(fake))))
    c = TestClient(main.app, headers={"X-Forwarded-Email": "op@example.com"})
    c.db = fake
    return c


def test_a_group_a_job_holds_takes_tables(monkeypatch):
    monkeypatch.setattr(main.db, "query", _jobs)
    main._refuse_groups_outside_jobs(["eng_c01", None, ""])


def test_a_group_in_no_job_or_only_removed_from_one_is_refused(monkeypatch):
    monkeypatch.setattr(main.db, "query", _jobs)
    with pytest.raises(HTTPException) as refused:
        main._refuse_groups_outside_jobs(["eng_c01", "eng_c02", "loose"])
    assert refused.value.status_code == 422
    assert "eng_c02, loose is in none" in refused.value.detail and "New ingestion job" in refused.value.detail


def test_add_table_into_a_group_in_no_job_writes_nothing(client):
    row = {"task_collection": "loose", "src_type": "sqlserver", "src_host": "h", "secret_key": "k", "src_database": "db1",
           "src_schema": "dbo", "src_table": "orders", "select_list": "id", "primary_key": "id", "load_type": "ct",
           "sink_catalog": "main", "sink_schema": "bronze", "sink_table": "orders"}
    r = client.post("/api/tables", json=row)
    assert r.status_code == 422 and "is in none" in r.text
    assert client.db.of("insert into lakefed_ingest.control (") == []


def test_register_and_import_into_a_group_in_no_job_write_nothing(client):
    r = client.post("/api/discoveries/d1/register", json={"task_collection": "loose"})
    assert r.status_code == 422 and "is in none" in r.text
    csv = "src_host,secret_key,src_database,src_schema,src_table,select_list,primary_key\nh,k,db1,dbo,orders,id,id\n"
    r = client.post("/api/import", json={"task_collection": "loose", "csv": csv, "dry_run": False})
    assert r.status_code == 422 and "is in none" in r.text
    assert client.db.of("insert into lakefed_ingest.control (") == []
