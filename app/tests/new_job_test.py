"""New ingestion job: the plan (new_job.py) from a CSV of where each table is and the job's settings, and its endpoint
(POST /api/ingestion-jobs) with a fake database. The SQL itself runs against a real Postgres in new_job_pg_test.py."""
import contextlib
import json

import pytest

import new_job as nj

CSV = ("src_connection,src_database,src_schema,src_table,select_list,primary_key\n"
       "sql_a,eng_001,gen,t0001,\"id, amount\",id\n"
       "sql_a,eng_002,gen,t0001,\"id, amount\",id\n"
       "sql_a,eng_002,gen,t0002,\"id, qty\",id\n")
FAN_IN = {"fan_in": True, "sink_catalog": "main", "sink_schema": "engagements", "sink_table": "{table}",
          "identity": {"engagement": "{database}"}}


def test_each_line_becomes_a_row_with_the_jobs_settings_and_a_group_per_server():
    plan = nj.plan("Engagement extraction", CSV, FAN_IN, groups_per_server=4)
    assert plan["errors"] == [] and [line["status"] for line in plan["lines"]] == ["valid"] * 3
    rows = [line["row"] for line in plan["lines"]]
    assert {r["sink_schema"] + "." + r["sink_table"] for r in rows} == {"engagements.t0001", "engagements.t0002"}
    assert [(r["client_id"], json.loads(r["client_attrs"])) for r in rows[:2]] == [
        ("eng_001", {"engagement": "eng_001"}), ("eng_002", {"engagement": "eng_002"})]
    assert all(r["use_remote_query"] and r["load_type"] == "ct" and r["write_mode"] == "merge" for r in rows)
    # One database's tables are always in one group, named after the job and its server.
    assert rows[1]["task_collection"] == rows[2]["task_collection"]
    assert all(g.startswith("engagement_extraction_sql_a_c0") for g in plan["groups"])


def test_a_mistyped_setting_is_refused_and_the_words_for_yes_and_no_are_read():
    assert nj.plan("Job", CSV, {**FAN_IN, "sink_shema": "x"}, 4)["errors"] == ["unknown setting sink_shema"]
    assert nj.plan("Job", CSV, {**FAN_IN, "fan_in": "sometimes"}, 4)["errors"] == ["fan_in: 'sometimes' is not true or false"]
    off = nj.plan("Job", CSV, {**FAN_IN, "fan_in": "false", "sink_table": "{database}_{table}"}, 4)
    assert off["errors"] == [] and off["settings"]["fan_in"] is False
    assert all(line["row"]["client_id"] is None for line in off["lines"])
    assert nj.plan("Job", CSV, {**FAN_IN, "lowercase": "no"}, 4)["settings"]["lowercase"] is False


def _server(databases: int, tables: int) -> str:
    return "src_connection,src_database,src_schema,src_table,select_list,primary_key\n" + "".join(
        f"sql_lab,eng_{d:03d},gen,t{t:04d},\"id, v\",id\n" for d in range(1, databases + 1) for t in range(1, tables + 1))


def test_a_whole_server_of_100000_tables_fits_one_job():
    plan = nj.plan("Big job", _server(100, 1000), {"fan_in": True, "sink_catalog": "main", "sink_schema": "s"}, 8, set())
    assert plan["errors"] == [] and len(plan["lines"]) == 100_000 and len(plan["groups"]) == 8


def test_more_lines_than_a_job_takes_at_once_are_refused(monkeypatch):
    monkeypatch.setattr(nj, "MAX_LINES", 10)
    plan = nj.plan("Big job", _server(1, 11), {"sink_catalog": "main", "sink_schema": "s"}, 4, set())
    assert plan["errors"] == ["11 lines: at most 10 in one import"]


def test_import_csv_keeps_its_own_cap_of_5000_lines():
    import config_import as ci
    read = ci.plan_import(_server(1, 5001), "g", {"src_type": "sqlserver", "load_type": "ct", "use_remote_query": True,
                                                  "sink_catalog": "c", "sink_schema": "s", "sink_table": "{table}"}, [])
    assert read["errors"] == ["5,001 lines: at most 5,000 in one import"]


def test_the_same_database_always_joins_the_same_group():
    first = nj.plan("Job", CSV, FAN_IN, 4)["lines"][0]["group"]
    assert first == nj.group_name("Job", "sql_a", "eng_001", 4) == nj.plan("Job", CSV, FAN_IN, 4)["lines"][0]["group"]


def test_without_fan_in_two_databases_cannot_write_one_table():
    lines = nj.plan("Job", CSV, {**FAN_IN, "fan_in": False}, 4)["lines"]
    assert lines[1]["status"] == "invalid" and "turn fan-in on" in lines[1]["errors"][0]
    named = nj.plan("Job", CSV, {**FAN_IN, "fan_in": False, "sink_table": "{database}_{table}"}, 4)["lines"]
    assert [line["status"] for line in named] == ["valid"] * 3


def test_write_modes_follow_each_tables_load_type():
    plan = nj.plan("Job", CSV, {**FAN_IN, "load_type": "full", "write_mode_full": "append"}, 4)
    assert {line["row"]["write_mode"] for line in plan["lines"]} == {"append"}
    plan = nj.plan("Job", CSV, {**FAN_IN, "write_mode_changes": "append"}, 4)
    assert {line["row"]["write_mode"] for line in plan["lines"]} == {"append"}
    plan = nj.plan("Job", CSV, {**FAN_IN, "load_type": "cursor", "write_mode_changes": "append"}, 4)
    assert {line["row"]["write_mode"] for line in plan["lines"]} == {"merge"}     # cursor tables always merge


@pytest.mark.parametrize("given,error", [
    ({**FAN_IN, "sink_catalog": ""}, "set the target catalog"),
    ({**FAN_IN, "sink_table": "{database}_{table}"}, "{database}"),
    ({**FAN_IN, "load_type": "cdc"}, "load type must be one of"),
    ({**FAN_IN, "identity": {"9bad": "{database}"}}, "identity column 9bad"),
])
def test_settings_the_job_cannot_take_are_refused_before_any_line(given, error):
    plan = nj.plan("Job", CSV, given, 4)
    assert plan["lines"] == [] and any(error in e for e in plan["errors"])


def test_a_line_naming_both_a_connection_and_a_host_or_neither_is_invalid():
    text = ("src_connection,src_host,src_database,src_schema,src_table,select_list,primary_key\n"
            "sql_a,h1,eng_003,gen,t0001,id,id\n"
            ",,eng_004,gen,t0001,id,id\n")
    both, neither = nj.plan("Job", text, FAN_IN, 4)["lines"]
    assert both["status"] == "invalid" and "not both" in " ".join(both["errors"])
    assert neither["status"] == "invalid" and "src_host is required" in neither["errors"]


def test_a_group_already_in_use_refuses_the_line():
    taken = {nj.group_name("Job", "sql_a", "eng_001", 4)}
    line = nj.plan("Job", CSV, FAN_IN, 4, taken)["lines"][0]
    assert line["status"] == "invalid" and "already in use" in line["errors"][-1]


def test_the_summary_counts_tables_databases_and_groups_per_server():
    summary = nj.summary(nj.plan("Job", CSV, FAN_IN, 4))
    assert summary["tables"] == 3 and summary["invalid"] == []
    assert [(s["server"], s["databases"], s["tables"], s["read_through"]) for s in summary["servers"]] == [
        ("sql_a", 2, 3, "UC connection")]
    assert summary["servers"][0]["load_types"] == {"ct": 3}


# ---- the endpoint (needs FastAPI and httpx) -----------------------------------------------------

@pytest.fixture
def client(monkeypatch):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient
    import main
    import queries as q
    from api_test import FakeCursor, FakeDB

    class Cursor(FakeCursor):
        def execute(self, sql, params=()):
            super().execute(sql, params)
            if sql == q.insert_schedule_sql():
                self._rows = [(7,)]

    fake = FakeDB()
    monkeypatch.setattr(main.db, "query", lambda sql, params=(): [{"task_collection": "other"}]
                        if sql == q.collections_sql() else [])
    monkeypatch.setattr(main.db, "transaction", contextlib.contextmanager(lambda: (yield Cursor(fake))))
    monkeypatch.setattr(main.jobs, "schedule_variants", lambda: {"default": "1", "check": "2"})
    synced = []
    monkeypatch.setattr(main, "_sync", lambda sid, user: synced.append(sid) or [])
    c = TestClient(main.app, headers={"X-Forwarded-Email": "op@example.com"})
    c.db, c.synced, c.q = fake, synced, q
    return c


def _create(client, dry_run=True, **extra):
    return client.post("/api/ingestion-jobs", json={"name": "Engagement extraction", "csv": CSV, "settings": FAN_IN,
                                                    "dry_run": dry_run, **extra})


def test_a_preview_writes_nothing(client):
    body = _create(client).json()
    assert body["created"] is False and body["summary"]["tables"] == 3
    assert client.db.statements == [] and client.synced == []


def test_create_writes_the_job_its_tables_and_groups_in_one_go_then_syncs(client):
    body = _create(client, dry_run=False).json()
    assert body["created"] is True and body["id"] == 7 and client.synced == [7]
    schedule = client.db.of("insert into lakefed_ingest.schedule (")
    assert len(schedule) == 1 and schedule[0][0] == "Engagement extraction" and schedule[0][6] is True   # paused
    inserted = [dict(zip(client.q.CONTROL_INSERT_COLUMNS, p)) for p in client.db.of("insert into lakefed_ingest.control (")]
    assert [r["src_database"] for r in inserted] == ["eng_001", "eng_002", "eng_002"]
    members = client.db.of("insert into lakefed_ingest.schedule_member")
    assert sorted(m[1] for m in members) == body["summary"]["groups"] and all(m[0] == 7 for m in members)
    assert client.db.of("select pg_try_advisory_xact_lock")


def test_create_is_refused_while_a_line_is_invalid(client):
    body = client.post("/api/ingestion-jobs", json={"name": "Job", "csv": CSV, "settings": {**FAN_IN, "fan_in": False},
                                                     "dry_run": False}).json()
    assert body["created"] is False and body["summary"]["invalid"]
    assert client.db.statements == []


def test_a_target_another_table_writes_refuses_the_whole_job(client):
    taken = CSV.replace("t0002", "lines")          # the fake database says the sink of a table named lines is taken
    r = client.post("/api/ingestion-jobs", json={"name": "Job", "csv": taken, "settings": FAN_IN, "dry_run": False})
    assert r.status_code == 409 and "already writes" in r.json()["detail"] and client.synced == []


def test_settings_or_a_schedule_it_cannot_take_are_422(client):
    r = _create(client, settings={**FAN_IN, "sink_catalog": ""})
    assert r.status_code == 422 and "set the target catalog" in r.json()["detail"]["errors"]
    r = _create(client, run={"cadence_minutes": 7})
    assert r.status_code == 422 and any("cadence" in e for e in r.json()["detail"]["errors"])


# ---- adding a CSV of tables to an existing job --------------------------------------------------

def _job_rows():
    """The job made from CSV above, as its control rows read back."""
    rows = [dict(line["row"], id=i + 1, task_enabled=True) for i, line in enumerate(nj.plan("Engagement extraction", CSV, FAN_IN, 4)["lines"])]
    return rows


def test_tables_added_to_a_job_take_its_settings_and_its_groups():
    more = ("src_connection,src_database,src_schema,src_table,select_list,primary_key\n"
            "sql_a,eng_002,gen,t0003,\"id, x\",id\n"            # a database the job has: its group
            "sql_a,eng_009,gen,t0001,\"id, amount\",id\n"       # a new database on the job's server: one of its groups
            "sql_b,ops_01,gen,t0001,\"id, amount\",id\n"        # a new server: new groups
            "sql_a,eng_001,gen,t0001,\"id, amount\",id\n")      # a table the job already writes: left as it is
    rows = _job_rows()
    plan = nj.plan_for_job("Engagement extraction", more, "ct", rows, 4)
    assert plan["errors"] == []
    by_table = {line["table"]: line for line in plan["lines"]}
    assert by_table["eng_002.gen.t0003"]["group"] == next(r["task_collection"] for r in rows if r["src_database"] == "eng_002")
    assert by_table["eng_009.gen.t0001"]["group"] in {r["task_collection"] for r in rows}
    assert by_table["ops_01.gen.t0001"]["group"].startswith("engagement_extraction_sql_b_c0")
    assert by_table["eng_001.gen.t0001"]["status"] == "in_job"
    assert plan["new_groups"] == [by_table["ops_01.gen.t0001"]["group"]]
    new = by_table["eng_009.gen.t0001"]["row"]
    assert (new["sink_schema"], new["sink_table"], new["client_id"], json.loads(new["client_attrs"])) == (
        "engagements", "t0001", "eng_009", {"engagement": "eng_009"})
    summary = nj.summary(plan)
    assert (summary["tables"], summary["in_job"]) == (3, 1)


def test_a_job_whose_tables_differ_on_a_setting_must_set_it_first():
    rows = _job_rows()
    rows[0]["sink_schema"] = "elsewhere"
    plan = nj.plan_for_job("Engagement extraction", CSV, "ct", rows, 4)
    assert plan["lines"] == [] and "target schema" in plan["errors"][0].replace("sink", "target").replace("_", " ")


@pytest.fixture
def job_client(monkeypatch):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient
    import main
    import queries as q
    from api_test import FakeCursor, FakeDB

    rows = _job_rows()
    schedule = dict(id=7, name="Engagement extraction", cadence_minutes=60, timezone="UTC", stagger=True, paused=True,
                    options=None, job_variant="default", parameters={}, notify_emails=None)
    members = [dict(id=i, schedule_id=7, task_collection=g, offset_minutes=0, job_id=None, removed_at=None, delete_pending=False)
               for i, g in enumerate(sorted({r["task_collection"] for r in rows}))]

    def answer(sql, params=()):
        if sql == q.schedule_sql():
            return [dict(schedule)]
        if sql == q.schedule_members_sql():
            return [dict(m) for m in members]
        if sql.startswith(q.groups_control_rows_sql().split(" for update")[0][:60]):
            return [dict(r) for r in rows]
        if sql == q.collections_sql():
            return [{"task_collection": m["task_collection"]} for m in members]
        return []

    fake = FakeDB()
    monkeypatch.setattr(main.db, "query", answer)
    monkeypatch.setattr(main.db, "fetch", lambda cur, sql, params=(): answer(sql, params))
    monkeypatch.setattr(main.db, "transaction", contextlib.contextmanager(lambda: (yield FakeCursor(fake))))
    monkeypatch.setattr(main, "_refuse_job", lambda job, request, action: None)
    synced = []
    monkeypatch.setattr(main, "_sync", lambda sid, user: synced.append(sid) or [])
    c = TestClient(main.app, headers={"X-Forwarded-Email": "op@example.com"})
    c.db, c.synced, c.q = fake, synced, q
    return c


MORE = ("src_connection,src_database,src_schema,src_table,select_list,primary_key\n"
        "sql_a,eng_009,gen,t0001,\"id, amount\",id\n"
        "sql_b,ops_01,gen,t0001,\"id, amount\",id\n")


def test_adding_tables_previews_then_writes_them_and_the_new_groups(job_client):
    body = job_client.post("/api/ingestion-jobs/7/tables", json={"csv": MORE}).json()
    assert body["applied"] is False and body["summary"]["tables"] == 2 and job_client.db.statements == []
    body = job_client.post("/api/ingestion-jobs/7/tables", json={"csv": MORE, "dry_run": False}).json()
    assert body["applied"] is True
    inserted = [dict(zip(job_client.q.CONTROL_INSERT_COLUMNS, p)) for p in job_client.db.of("insert into lakefed_ingest.control (")]
    assert [r["src_database"] for r in inserted] == ["eng_009", "ops_01"]
    members = job_client.db.of("insert into lakefed_ingest.schedule_member")
    assert [m[1] for m in members] == body["summary"]["new_groups"] and job_client.synced == [7]


def test_a_line_may_give_its_own_load_type_as_discovery_recommends_it():
    text = ("src_connection,src_database,src_schema,src_table,select_list,primary_key,cursor_column,load_type\n"
            "sql_a,eng_001,gen,t0001,\"id, rv\",id,rv,cursor\n"
            "sql_a,eng_001,gen,t0002,\"id, a\",id,,full\n"
            "sql_a,eng_001,gen,t0003,\"id, a\",id,,\n"
            "sql_a,eng_001,gen,t0004,\"id, a\",id,,cdc\n")
    lines = nj.plan("Job", text, {**FAN_IN, "write_mode_full": "append"}, 4)["lines"]
    assert [(line["row"]["load_type"], line["row"]["write_mode"]) for line in lines[:3]] == [
        ("cursor", "merge"), ("full", "append"), ("ct", "merge")]
    assert lines[0]["row"]["cursor_column"] == "rv" and lines[3]["status"] == "invalid"
    assert "load_type must be one of" in " ".join(lines[3]["errors"])
