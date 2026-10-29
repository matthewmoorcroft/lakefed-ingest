"""The operations app's steps at scale: creating an ingestion job by reference to a discovery, and uploading a CSV of
tables. They call the app's own planning (new_job.plan, read_upload) and its write helpers (main._insert_controls,
main._log_lines, main._keep_selection, ...) with the recording cursor from lakebase_fake. no_connections() is in force for
all of it, so a step that tried to reach a database would raise.

What is not measured here: the reads the route does from Lakebase (the selection's rows, the discovered rows) and the
request's own JSON handling. Those come back as rows built in Python, and the report says so.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import paths  # noqa: F401  (sys.path)
import lakebase_fake
import main as app
import new_job as nj
import queries as q
import server_catalog
import simulation as sim
import steps

CREATE_SETTINGS = {"sink_catalog": "lab", "sink_schema": "scale", "sink_table": "{database}_{table}"}
SCALE_USER = "scale@example.com"
SCALE_JOB = "scale-job"
SCALE_BATCH = "scale-batch"
WHOLE_SERVER_PART = ("server", sim.connection_spec()["src_connection"], None, None, None)


@dataclass
class CreateRun:
    """One create by reference: the planned lines, the groups, and what the writes sent."""
    tables: int
    groups: int
    csv_bytes: int
    errors: List[str]
    csv_s: float
    plan_s: float
    write_s: float
    summary: dict
    connection: lakebase_fake.RecordingConnection


@dataclass
class UploadRun:
    """One upload of a CSV of tables: what read_upload made of it, and what the upload loop sent."""
    lines: int
    errors: List[str]
    csv_bytes: int
    read_s: float
    write_s: float
    connection: Optional[lakebase_fake.RecordingConnection]


def _discovered_fields(spec: dict, database: str, table: dict) -> dict:
    """A discovered table as the discovery selection reads it into a CSV line (DISCOVERY_COLUMNS)."""
    return {"src_connection": spec["src_connection"], "src_host": None, "src_port": None, "secret_key": None,
            "src_database": database, "src_schema": table["src_schema"], "src_table": table["src_table"],
            "select_list": table.get("select_list"), "primary_key": table.get("primary_key"),
            "cursor_column": table.get("cursor_column"), "load_type": table.get("load_type")}


def discovered_rows(catalog: server_catalog.ServerCatalog, limit: Optional[int] = None) -> List[dict]:
    """The discovered tables of a server as rows of DISCOVERY_COLUMNS (what a selection by discovery reads)."""
    spec = sim.connection_spec()
    rows: List[dict] = []
    steps.discover(catalog, limit=limit,
                   on_found=lambda database, found: rows.extend(_discovered_fields(spec, database, t) for t in found))
    return rows


def _control_ids(params: tuple) -> List[tuple]:
    """insert_controls_sql returns (id, sink key) for each row written: ids are numbered in the order given."""
    width = len(q.CONTROL_INSERT_COLUMNS)
    return [(number, *q.control_sink_key(params[at:at + width]))
            for number, at in enumerate(range(0, len(params), width), start=1)]


def _answers(sql: str, params: tuple) -> List[tuple]:
    """What the reads and returning clauses of the create path give back: every collection is free, every control row
    is written, the schedule gets id 1, and no sink or database collides."""
    if "pg_try_advisory_xact_lock" in sql:
        return [(True,)]
    if sql.startswith(f"insert into {q.SCHEMA}.control ("):
        return _control_ids(params)
    if sql.startswith(f"insert into {q.SCHEMA}.schedule ("):
        return [(1,)]
    return []


def _write_job(cur, plan: dict, selected: dict, name: str, user: str, batch: str) -> None:
    """The writes create_ingestion_job makes in its transaction, in its order, minus the schedule check (which reads the
    Databricks jobs) and the cron offsets (each member starts at 0)."""
    rows = [line["row"] for line in plan["lines"]]
    app._lock_collections(cur, plan["groups"])
    cur.execute(q.insert_schedule_sql(), (None,) * (len(q.SCHEDULE_FIELDS) + 1))
    schedule_id = cur.fetchone()[0]
    app._insert_controls(cur, rows, user, batch)
    app._refuse_mixed_sinks(cur, rows)
    app._refuse_split_databases(cur, rows)
    for group in plan["groups"]:
        cur.execute(q.insert_member_sql(), (schedule_id, group, 0))
    app._log_lines(cur, [(user, "schedule_create", None, group, "schedule", None, name, None, batch)
                         for group in plan["groups"]])
    app._keep_selection(cur, schedule_id, selected, user)
    app._touch(cur, user, [schedule_id])


def create_by_reference(catalog: server_catalog.ServerCatalog, limit: Optional[int] = None,
                        groups_per_server: int = nj.DEFAULT_GROUPS_PER_SERVER) -> CreateRun:
    """Create a job whose tables are a whole server's discovery: the CSV the selection makes, the plan, and the writes.
    Runs under no_connections()."""
    with lakebase_fake.no_connections():
        rows = discovered_rows(catalog, limit=limit)
        started = time.perf_counter()
        text = nj.csv_of(list(nj.DISCOVERY_COLUMNS), rows)
        csv_s = time.perf_counter() - started
        started = time.perf_counter()
        plan = nj.plan(SCALE_JOB, text, CREATE_SETTINGS, groups_per_server, taken_groups=set())
        plan_s = time.perf_counter() - started
        summary = nj.summary(plan)
        connection = lakebase_fake.RecordingConnection(answers=_answers)
        selected = {"parts": [WHOLE_SERVER_PART], "kind": "discovery", "source_id": "scale-discovery"}
        started = time.perf_counter()
        if not plan["errors"]:
            with connection.transaction() as cur:
                _write_job(cur, plan, selected, SCALE_JOB, SCALE_USER, SCALE_BATCH)
        write_s = time.perf_counter() - started
    return CreateRun(tables=len(plan["lines"]), groups=len(plan["groups"]), csv_bytes=len(text.encode()),
                     errors=list(plan["errors"]), csv_s=csv_s, plan_s=plan_s, write_s=write_s, summary=summary,
                     connection=connection)


def upload_by_reference(catalog: server_catalog.ServerCatalog, limit: Optional[int] = None) -> UploadRun:
    """Upload the discovered tables of a server as a CSV, as upload_tables does: read it, then write the header and one
    line per table, q.CONTROL_BATCH lines to a statement. Runs under no_connections()."""
    with lakebase_fake.no_connections():
        rows = discovered_rows(catalog, limit=limit)
        text = nj.csv_of(list(nj.DISCOVERY_COLUMNS), rows)
        return _upload_text(text, lines_hint=len(rows))


def upload_text(text: str) -> UploadRun:
    """read_upload and the upload loop on a CSV text, as upload_tables runs them. Runs under no_connections()."""
    with lakebase_fake.no_connections():
        return _upload_text(text, lines_hint=None)


MONITORING_PATHS = ("/api/overview", "/api/collections", "/api/errors", "/api/ingestion-jobs", "/api/ingestion-jobs/1",
                    "/api/ingestion-jobs/1/live", "/api/runs", "/api/tables", "/api/changes", "/api/schedules",
                    "/api/timeline", "/api/discoveries")


def monitoring_shapes(paths: Tuple[str, ...] = MONITORING_PATHS) -> List[dict]:
    """The Lakebase statements each monitoring page issues, with the app's own routes through TestClient and the database
    stubbed to record and answer with no rows. This gives the shape of each page (which statements, how many); the rows
    a statement returns at scale are not in it. Runs under no_connections()."""
    from unittest import mock

    from fastapi.testclient import TestClient

    client = TestClient(app.app, headers={"X-Forwarded-Email": SCALE_USER})
    shapes = []
    with lakebase_fake.no_connections():
        for path in paths:
            recorded = lakebase_fake.RecordingConnection()
            with mock.patch.object(app.db, "query", lambda sql, params=(): (recorded.record(sql, 0), [])[1]), \
                    mock.patch.object(app.db, "execute", lambda sql, params=(): recorded.record(sql, 0)):
                response = client.get(path)
            shapes.append({"path": path, "status": response.status_code, "statements": recorded.total_statements(),
                           "kinds": dict(recorded.statements)})
    return shapes


def _upload_text(text: str, lines_hint: Optional[int]) -> UploadRun:
    started = time.perf_counter()
    read = nj.read_upload(text)
    read_s = time.perf_counter() - started
    if read["errors"]:
        return UploadRun(lines=lines_hint or 0, errors=list(read["errors"]), csv_bytes=len(text.encode()), read_s=read_s,
                         write_s=0.0, connection=None)
    upload_id, lines = "scale-upload", read["lines"]
    ok = sum(line["status"] == "ok" for line in lines)
    connection = lakebase_fake.RecordingConnection()
    started = time.perf_counter()
    with connection.transaction() as cur:
        cur.execute(q.insert_upload_sql(), (None,) * 7)
        for at in range(0, len(lines), q.CONTROL_BATCH):
            chunk = lines[at:at + q.CONTROL_BATCH]
            cur.execute(q.insert_upload_lines_sql(len(chunk)), tuple(
                v for line in chunk for v in (upload_id, line["line"], line["server"], line["src_database"],
                                              line["src_schema"], line["src_table"], line["load_type"],
                                              json.dumps(line["fields"]), line["status"], line["errors"])))
    write_s = time.perf_counter() - started
    return UploadRun(lines=len(lines), errors=[], csv_bytes=len(text.encode()), read_s=read_s, write_s=write_s,
                     connection=connection)


def _repeated_csv(line_count: int) -> str:
    """A CSV of line_count copies of one table: enough lines to reach a limit, and cheap to build."""
    sample = _discovered_fields(sim.connection_spec(), "lfc_eng_00000", {"src_schema": "dbo", "src_table": "t_00000"})
    return nj.csv_of(list(nj.DISCOVERY_COLUMNS), [sample] * line_count)


def create_refusal(line_count: int) -> Tuple[List[str], float]:
    """nj.plan on a CSV of line_count lines: its refusal, and how long the planning took to give it."""
    text = _repeated_csv(line_count)
    started = time.perf_counter()
    plan = nj.plan(SCALE_JOB, text, CREATE_SETTINGS, taken_groups=set())
    return list(plan["errors"]), time.perf_counter() - started


def upload_refusal(line_count: int) -> Tuple[List[str], float]:
    """read_upload on a CSV of line_count lines: its refusal, and how long reading took to give it."""
    text = _repeated_csv(line_count)
    started = time.perf_counter()
    read = nj.read_upload(text)
    return list(read["errors"]), time.perf_counter() - started


def sample_line_bytes(line_count: int = 1000) -> float:
    """The size of one CSV line of a discovered table in bytes, header excluded: the body of an upload is this times its
    lines (JSON escaping adds a little more, which is left out)."""
    text = _repeated_csv(line_count)
    header_bytes = text.index("\n") + 1
    return (len(text.encode()) - header_bytes) / line_count
