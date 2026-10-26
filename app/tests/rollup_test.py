"""The roll-ups of per-database rows (rollup.py) into groups, servers and connections."""
import rollup


def state(server, group, database, tables=0, current=0, late=0, first=0, failing=0, no_sla=0, connection=None,
          host=None, newest=None, oldest=None):
    return dict(server=server, task_collection=group, src_database=database, tables=tables, state_current=current,
                state_late=late, state_first_load=first, state_failing=failing, no_sla_tables=no_sla,
                src_connection=connection, src_host=host, newest_success_at=newest, oldest_success_at=oldest)


STATES = [state("srvX", "c1", "Sales", tables=3, current=3, newest="2026-10-26T10:00"),
          state("srvY", "c1", "Sales", tables=2, current=1, late=1, newest="2026-10-26T11:00"),
          state("srvY", "c1", "Archive", tables=0),
          state("srvY", "c2", "Hr", tables=4, first=2, failing=2, no_sla=4, connection="hr_conn")]


def test_two_servers_same_named_databases_are_two_and_disabled_only_ones_none():
    g = rollup.groups(STATES)["c1"]
    assert (g["databases"], g["servers"], g["server"]) == (2, 2, "srvX")
    assert (g["enabled_tables"], g["state_current"], g["state_late"]) == (5, 4, 1)
    assert g["newest_success_at"] == "2026-10-26T11:00"


def test_state_sums_equal_the_inputs():
    groups = rollup.groups(STATES)
    for k in rollup.STATES + ("no_sla_tables",):
        assert sum(g[k] for g in groups.values()) == sum(s[k] for s in STATES)


def test_a_group_whose_tables_are_all_disabled_is_still_listed():
    g = rollup.groups([state("srvZ", "c9", "Old", tables=0)])["c9"]
    assert (g["enabled_tables"], g["databases"], g["server"]) == (0, 0, None)


def test_servers_roll_up_databases_groups_and_running_groups():
    by = {s["server"]: s for s in rollup.servers(STATES, running_groups={"c2"})}
    assert (by["srvY"]["databases"], by["srvY"]["tables"], by["srvY"]["collections"]) == (2, 6, 2)
    assert (by["srvY"]["running_collections"], by["srvX"]["running_collections"]) == (1, 0)
    assert [s["server"] for s in rollup.servers(STATES)] == ["srvX", "srvY"]


def test_a_connection_matches_by_name_or_host_case_insensitively():
    rows = STATES + [state("srvW", "c3", "Ops", tables=1, host="sql-w.example.com")]
    assert [d["src_database"] for d in rollup.connection_databases(rows, "hr_conn", None)] == ["Hr"]
    assert [d["src_database"] for d in rollup.connection_databases(rows, "other", "SQL-W.example.com")] == ["Ops"]
    assert rollup.tracked(rows, "hr_conn", "") == {"databases": 1, "tables": 4, "collections": 1}
    assert rollup.connection_databases(rows, "nothing", "") == []


SCHEDULES = [dict(id=1, name="East hourly", cadence_minutes=60, timezone="UTC", paused=False),
             dict(id=2, name="Nightly", cadence_minutes=1440, timezone="UTC", paused=True)]
MEMBERS = [dict(schedule_id=1, task_collection="c1", offset_minutes=0, removed_at=None),
           dict(schedule_id=2, task_collection="c1", offset_minutes=0, removed_at=None),
           dict(schedule_id=2, task_collection="c2", offset_minutes=30, removed_at="2026-10-01T00:00:00+00:00"),
           dict(schedule_id=2, task_collection="c3", offset_minutes=10, removed_at=None)]


def test_a_group_in_two_jobs_adds_to_both_and_one_in_no_job_is_not_in_any_job():
    states = STATES + [state("srvZ", "c3", "Ops", tables=5, current=5)]
    jobs, not_in_job = rollup.jobs(states, SCHEDULES, MEMBERS)
    by_name = {j["name"]: j for j in jobs}
    assert (by_name["East hourly"]["enabled_tables"], by_name["East hourly"]["databases"]) == (5, 2)
    assert (by_name["Nightly"]["enabled_tables"], by_name["Nightly"]["databases"], by_name["Nightly"]["servers"]) == (10, 3, 3)
    assert by_name["Nightly"]["groups"] == ["c1", "c3"] and by_name["Nightly"]["state_late"] == 1
    assert not_in_job["groups"] == ["c2"] and (not_in_job["enabled_tables"], not_in_job["state_failing"]) == (4, 2)
    assert rollup.jobs(STATES, SCHEDULES, MEMBERS + [dict(schedule_id=1, task_collection="c2", offset_minutes=30,
                                                          removed_at=None)])[1] is None
