"""SQL for the operations app — pure builders over the Lakebase schema (docs/lakebase_schema.md).

Every statement is parameterized (psycopg %s); nothing user-supplied is formatted into SQL. Kept
free of web-framework and database imports so it is unit-tested in app/tests/queries_test.py.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

SCHEMA = "lakefed_ingest"
# The default port of each source type (the engine's source_adapters); a test keeps them equal.
DEFAULT_PORTS = {"sqlserver": 1433, "oracle": 1521, "postgresql": 5432, "mysql": 3306}


# ---- freshness SLA ------------------------------------------------------------------------------

def database_source_sql(alias: str) -> str:
    """The server, port and credential a control row reads its database through: the key of its
    database's check stamp (the engine's ``state_store.database_source_sql``; a test keeps them equal).
    A blank port reads the source type's default port, as ``DEFAULT_PORTS`` lists it, except on a
    connection-backed row (no host), whose port is the connection's and stays 0."""
    whens = " ".join(f"when '{src_type}' then {port}" for src_type, port in DEFAULT_PORTS.items())
    default_port = f"(case coalesce(nullif(lower(trim({alias}.src_type)), ''), 'sqlserver') {whens} else 0 end)"
    connection_backed = f"(nullif({alias}.src_host, '') is null and nullif({alias}.src_connection, '') is not null)"
    return (f"lower(coalesce(nullif({alias}.src_host, ''), nullif({alias}.src_connection, ''), '')) || '|' || "
            f"coalesce(nullif({alias}.src_port, 0), "
            f"case when {connection_backed} then 0 else {default_port} end) || '|' || "
            f"coalesce(nullif({alias}.secret_key, ''), nullif({alias}.src_connection, ''), '')")


# A wholly unchanged database gets one check stamp (database_check) instead of a last_success_at on each
# table. The stamp vouches for a ct table in the incremental phase at or past its version; for those, the
# last success is the later of the two (the engine's planner.stamp_vouches). Aliases: t = control,
# cp = checkpoint, dc = database_check (DATABASE_CHECK_JOIN).
DATABASE_CHECK_JOIN = (f"left join {SCHEMA}.database_check dc on dc.task_collection = t.task_collection "
                       f"and dc.src_database = t.src_database and dc.source = {database_source_sql('t')}")
LAST_SUCCESS = ("greatest(cp.last_success_at, case when cp.strategy = 'ct' and cp.phase = 'incremental' "
                "and cp.position ~ '^[0-9]+$' then case when cp.position::bigint >= dc.checked_version "
                "then dc.checked_at end end)")

# An enabled table with control.freshness_sla_minutes breaches its SLA when its last success (or, if
# it has never succeeded, its control row's creation) is older than the SLA. Tables without an SLA
# are never flagged. Aliases as above.
SLA_BREACHED = ("(t.task_enabled and t.freshness_sla_minutes is not null "
                f"and coalesce({LAST_SUCCESS}, t.created_at) < now() - make_interval(mins => t.freshness_sla_minutes))")


# The server a table's row reaches, as the engine's per-source query cap counts it (ct_engine.source_key): its UC
# connection when it reads through one (or names one and no host), else its host and port (the source type's default
# port when none is set, or 0, as the engine reads it). The instance-group label does not decide it: two labels on one
# server are one server. Alias t = control.


def endpoint_sql(alias: str) -> str:
    """SQL: a row's host and effective port, "host:port" (rollup.endpoint in Python), for a table with src_host,
    src_port and src_type under `alias`."""
    default = (f"case lower(coalesce({alias}.src_type, 'sqlserver')) when 'oracle' then '1521' "
               f"when 'postgresql' then '5432' when 'mysql' then '3306' else '1433' end")
    return f"lower({alias}.src_host) || ':' || coalesce(nullif({alias}.src_port, 0)::text, {default})"


SERVER = (f"(case when t.src_connection is not null and (t.use_remote_query or t.src_host is null) "
          f"then lower(t.src_connection) when t.src_host is not null then {endpoint_sql('t')} else '' end)")
# One source database: its server (SERVER) and its name, so two servers' databases of the same name stay
# two databases. Alias t = control.
DATABASE = f"({SERVER}, coalesce(t.src_database, ''))"

# Each enabled table in exactly one state, the first that applies: failing, waiting for a first load,
# late (SLA breached), else current. Aliases: t = control, cp = checkpoint.
_FAILING = "cp.status = 'failed'"
_FIRST_LOAD = "(cp.control_id is null or cp.phase = 'seeding')"
STATE_COUNTS = (
    f"count(*) filter (where t.task_enabled and {_FAILING}) as state_failing, "
    f"count(*) filter (where t.task_enabled and not coalesce({_FAILING}, false) "
    f"and {_FIRST_LOAD}) as state_first_load, "
    f"count(*) filter (where t.task_enabled and not coalesce({_FAILING}, false) and not {_FIRST_LOAD} "
    f"and {SLA_BREACHED}) as state_late, "
    f"count(*) filter (where t.task_enabled and not coalesce({_FAILING}, false) and not {_FIRST_LOAD} "
    f"and not {SLA_BREACHED}) as state_current")


# ---- overview -----------------------------------------------------------------------------------

def database_states_sql() -> str:
    """One row per (server, connection, host, ingestion group, database, instance-group label): enabled tables by
    state (STATE_COUNTS), tables without a freshness target, the oldest and newest success of its enabled tables,
    and the newest of any table (what a UC connection's databases show). The one scan
    the monitoring views share: rollup.py sums it into groups, servers and connections. The label rides along so
    Sources can say where it differs from the server."""
    return f"""
select {SERVER} as server, t.src_connection, lower(t.src_host) as src_host, t.task_collection,
       coalesce(t.src_database, '') as src_database, t.source_instance_group as instance_label,
       count(*) filter (where t.task_enabled) as tables,
       count(*) filter (where t.task_enabled and t.freshness_sla_minutes is null) as no_sla_tables,
       {STATE_COUNTS},
       min({LAST_SUCCESS}) filter (where t.task_enabled) as oldest_success_at,
       max({LAST_SUCCESS}) filter (where t.task_enabled) as newest_success_at,
       max({LAST_SUCCESS}) as newest_any_success_at
from {SCHEMA}.control t
left join {SCHEMA}.checkpoint cp on cp.control_id = t.id
{DATABASE_CHECK_JOIN}
group by 1, 2, 3, 4, 5, 6"""


def not_a_check(alias: str = "") -> str:
    """SQL: the ingest_run row is a group run, not the check job's own record of a check (schedules.is_check):
    work_id "check-<job run>", or a detail holding the check's summary for records written before that mark."""
    a = f"{alias}." if alias else ""
    return (f"left(coalesce({a}work_id, ''), 6) <> 'check-' "
            f"and left(coalesce({a}detail, ''), 9) <> '{{\"check\":'")


def last_runs_sql() -> str:
    """Each ingestion group's latest run: the groups with tables or in a job, each read newest first through
    ingest_run_collection_idx, so it costs one index step per group rather than a scan of every run ever made."""
    return f"""
select r.* from (select task_collection from {SCHEMA}.control
                 union select task_collection from {SCHEMA}.schedule_member) g
cross join lateral (
    select task_collection, run_id, status, started_at, finished_at, total, ok, failed, skipped, detail
    from {SCHEMA}.ingest_run
    where task_collection = g.task_collection and {not_a_check()}
    order by started_at desc limit 1
) r"""


def running_progress_sql() -> str:
    """Every running group run's progress in one pass: its group's enabled tables (a work-list run's
    planned tables: its total, recorded at the start) and how many it has checked (stamped since it
    started, on the database's clock like started_at, a database check stamp included), its table events so far
    (unchanged tables are stamped in bulk without events), the loads that finished in the last 5 minutes
    (live speed and time left), and its group's 7-day median and p95 run time. A run another run closed
    never recorded its totals (its finish is the closing time), so it is left out of the median and p95.
    Skipped events are reseeds queued: done for this run, though not stamped as checked."""
    return f"""
with r as (
    select run_id, task_collection, job_run_id, cluster_id, parallelism, started_at, total
    from {SCHEMA}.ingest_run where status = 'running' and {not_a_check()}
),
tables as (
    select r.run_id,
           count(*) filter (where t.task_enabled) as enabled_tables,
           count(*) filter (where t.task_enabled and {LAST_SUCCESS} >= r.started_at) as tables_checked
    from r join {SCHEMA}.control t on t.task_collection = r.task_collection
    left join {SCHEMA}.checkpoint cp on cp.control_id = t.id
    {DATABASE_CHECK_JOIN}
    group by r.run_id
),
events as (
    select e.run_id,
           count(*) filter (where e.status = 'ok')      as tables_ok,
           count(*) filter (where e.status = 'failed')  as tables_failed,
           count(*) filter (where e.status = 'running') as tables_running,
           count(*) filter (where e.status = 'skipped') as tables_skipped,
           min(e.started_at)                            as work_started_at,
           count(*) filter (where e.status = 'ok' and e.finished_at > now() - interval '5 minutes') as ok_last_5m,
           sum(e.rows_merged) filter (where e.status = 'ok' and e.finished_at > now() - interval '5 minutes') as rows_last_5m
    from {SCHEMA}.ingest_event e join r on r.run_id = e.run_id
    group by e.run_id
)
select r.run_id, r.task_collection, r.job_run_id, r.cluster_id, r.parallelism, r.started_at,
       round(extract(epoch from (now() - r.started_at)))::int as elapsed_s,
       coalesce(r.total, tb.enabled_tables, 0) as enabled_tables, coalesce(tb.tables_checked, 0) as tables_checked,
       coalesce(ev.tables_ok, 0) as tables_ok, coalesce(ev.tables_failed, 0) as tables_failed,
       coalesce(ev.tables_running, 0) as tables_running, coalesce(ev.tables_skipped, 0) as tables_skipped,
       ev.work_started_at,
       coalesce(ev.ok_last_5m, 0) as ok_last_5m, ev.rows_last_5m, p.p50_s, p.p95_s
from r
left join tables tb on tb.run_id = r.run_id
left join events ev on ev.run_id = r.run_id
left join lateral (
    select round(percentile_cont(0.5) within group (order by extract(epoch from (x.finished_at - x.started_at)))::numeric) as p50_s,
           round(percentile_cont(0.95) within group (order by extract(epoch from (x.finished_at - x.started_at)))::numeric) as p95_s
    from {SCHEMA}.ingest_run x
    where x.task_collection = r.task_collection and x.finished_at is not null and x.status <> 'skipped'
      and {not_a_check("x")}
      and x.total is not null and x.started_at > now() - interval '7 days') p on true
order by r.started_at desc"""


# Loads that recorded no row count and that no batched append's total covers: a batch's members record none, since
# the batch's commit reports one total for them all (on the run's detail, batch_rows), and their pool is batch<size>.
UNCOUNTED_OUTSIDE_BATCHES = ("count(*) filter (where e.status = 'ok' and e.rows_merged is null "
                             "and coalesce(e.pool, '') not like 'batch%%')")


def run_work_sql() -> str:
    """Per group run: its work span (first table start to last table finish), tables loaded and failed,
    and rows written (uncounted: loads with no count outside a batched append). Param: array of run ids."""
    return (f"select e.run_id, min(e.started_at) as work_started_at, max(e.finished_at) as work_finished_at, "
            f"count(*) filter (where e.status = 'ok') as loaded, "
            f"count(*) filter (where e.status = 'failed') as failed_tables, "
            f"sum(e.rows_merged) as rows_written, "
            f"{UNCOUNTED_OUTSIDE_BATCHES} as uncounted "
            f"from {SCHEMA}.ingest_event e where e.run_id = any(%s) group by e.run_id")


def collections_overview_sql() -> str:
    """One row per task_collection: table counts, the latest run, and current health. The state_* counts
    put each enabled table in exactly one state (STATE_COUNTS) for the state bar; rows_24h is the rows
    its table loads wrote in the last 24 hours (batched appends record theirs on the run, so they are
    added from run_batch_rows_sql)."""
    return f"""
with tables as (
    select t.task_collection,
           count(*) filter (where t.task_enabled)                                   as enabled_tables,
           count(*)                                                                 as all_tables,
           count(*) filter (where t.task_enabled and cp.status = 'failed')          as failing_tables,
           count(*) filter (where t.task_enabled and (cp.control_id is null or cp.phase = 'seeding')) as seeding_tables,
           count(*) filter (where t.task_enabled and t.freshness_sla_minutes is null) as no_sla_tables,
           count(*) filter (where {SLA_BREACHED})                                   as sla_breached_tables,
           max({LAST_SUCCESS})                                                      as last_success_at,
           count(distinct {DATABASE}) filter (where t.task_enabled)               as databases,
           count(distinct {SERVER}) filter (where t.task_enabled)                  as servers,
           min({SERVER}) filter (where t.task_enabled)                             as server,
           {STATE_COUNTS}
    from {SCHEMA}.control t
    left join {SCHEMA}.checkpoint cp on cp.control_id = t.id
    {DATABASE_CHECK_JOIN}
    group by t.task_collection
),
last_run as (
    select distinct on (task_collection) task_collection, run_id, status, started_at, finished_at,
           total, ok, failed, skipped, reseeded
    from {SCHEMA}.ingest_run
    order by task_collection, started_at desc
)
select tb.task_collection, tb.enabled_tables, tb.all_tables, tb.failing_tables, tb.seeding_tables,
       tb.sla_breached_tables, tb.no_sla_tables, tb.last_success_at,
       r.run_id as last_run_id, r.status as last_run_status, r.started_at as last_run_started,
       r.finished_at as last_run_finished, r.ok as last_run_ok, r.failed as last_run_failed,
       tb.databases, tb.servers, tb.server, tb.state_current, tb.state_late, tb.state_first_load, tb.state_failing,
       w.rows_24h
from tables tb
left join last_run r on r.task_collection = tb.task_collection
left join lateral (select sum(e.rows_merged) as rows_24h from {SCHEMA}.ingest_event e
                   where e.task_collection = tb.task_collection
                     and e.finished_at > now() - interval '24 hours') w on true
order by tb.task_collection"""


def running_runs_sql() -> str:
    """Runs still marked running (newest first), with progress: tables finished so far (unchanged tables
    are bulk-advanced without events, so these count tables that needed work) and elapsed time against
    the collection's typical run (median of its last 20 finished runs).

    tables_checked out of enabled_tables is the run's progress: the engine stamps
    checkpoint.last_success_at on every unchanged table once change detection ends (or one database_check
    stamp for a wholly unchanged database, LAST_SUCCESS), and on every loaded table as it finishes, both on
    the database's clock like started_at. enabled_tables is the tables the run planned (ingest_run.total
    while it runs; fewer than the collection's for a run on a check's work list), else the collection's
    enabled tables. last_activity_at is when a table of the run last started or finished, the sign that the
    run is alive."""
    return f"""
select r.run_id, r.task_collection, r.job_run_id, r.started_at, r.parallelism,
       round(extract(epoch from (now() - r.started_at)))::int as elapsed_s,
       (select count(*) from {SCHEMA}.ingest_event e where e.run_id = r.run_id and e.status = 'ok')      as tables_ok,
       (select count(*) from {SCHEMA}.ingest_event e where e.run_id = r.run_id and e.status = 'failed')  as tables_failed,
       (select count(*) from {SCHEMA}.ingest_event e where e.run_id = r.run_id and e.status = 'running') as tables_running,
       (select round(percentile_cont(0.5) within group (order by extract(epoch from (x.finished_at - x.started_at)))::numeric)
          from (select started_at, finished_at from {SCHEMA}.ingest_run x
                where x.task_collection = r.task_collection and x.finished_at is not null and x.status <> 'skipped'
                order by x.started_at desc limit 20) x)                                                as typical_s,
       coalesce(r.total, (select count(*) from {SCHEMA}.control t
         where t.task_collection = r.task_collection and t.task_enabled))                     as enabled_tables,
       (select count(*) from {SCHEMA}.control t join {SCHEMA}.checkpoint cp on cp.control_id = t.id
         {DATABASE_CHECK_JOIN}
         where t.task_collection = r.task_collection and t.task_enabled
           and {LAST_SUCCESS} >= r.started_at)                                               as tables_checked,
       (select max(coalesce(e.finished_at, e.started_at)) from {SCHEMA}.ingest_event e
         where e.run_id = r.run_id)                                                          as last_activity_at,
       (select min({SERVER}) from {SCHEMA}.control t where t.task_collection = r.task_collection)      as server
from {SCHEMA}.ingest_run r
where r.status = 'running'
order by r.started_at desc"""


# ---- runs / events ------------------------------------------------------------------------------

_RUN_COLUMNS = ("run_id, task_collection, status, started_at, finished_at, "
                "round(extract(epoch from (coalesce(finished_at, now()) - started_at))::numeric, 0) as duration_s, "
                "total, ok, failed, skipped, reseeded, job_run_id, cluster_id, parallelism, detail, work_id")


def runs_sql(collection: Optional[str], limit: int = 100) -> Tuple[str, tuple]:
    """Recent group runs, optionally for one collection. Returns (sql, params)."""
    where, params = f"where {not_a_check()} ", ()
    if collection:
        where, params = f"where task_collection = %s and {not_a_check()} ", (collection,)
    sql = f"select {_RUN_COLUMNS} from {SCHEMA}.ingest_run {where}order by started_at desc limit %s"
    return sql, params + (int(limit),)


def run_sql() -> str:
    """One run, with the same columns as runs_sql. Param: run_id."""
    return f"select {_RUN_COLUMNS} from {SCHEMA}.ingest_run where run_id = %s"


# The most table events the group run page lists.
RUN_EVENTS_LIMIT = 500


def run_events_sql() -> str:
    """Per-table events of one run, failures first, at most RUN_EVENTS_LIMIT. Param: run_id."""
    return (f"select control_id, src_database, src_schema, src_table, sink_fqn, action, stage, status, "
            f"duration_ms, rows_merged, position_from, position_to, error "
            f"from {SCHEMA}.ingest_event where run_id = %s "
            f"order by (status = 'failed') desc, duration_ms desc nulls last limit {RUN_EVENTS_LIMIT}")


def in_flight_sql() -> str:
    """The tables a group run is loading now, longest first. Params: run_id, limit."""
    return (f"select e.src_database, e.src_schema, e.src_table, e.action, e.stage, e.started_at, "
            f"round(extract(epoch from (now() - e.started_at)))::int as running_s, e.retry_count "
            f"from {SCHEMA}.ingest_event e where e.run_id = %s and e.status = 'running' "
            f"order by e.started_at limit %s")


def in_flight_count_sql() -> str:
    """How many tables a group run is loading now. Param: run_id."""
    return f"select count(*) as tables from {SCHEMA}.ingest_event where run_id = %s and status = 'running'"


def recent_errors_sql() -> str:
    """Failed table events in the last N hours, newest first. Params: hours, limit."""
    return (f"select e.finished_at, e.task_collection, e.run_id, e.control_id, e.src_database, "
            f"e.src_schema, e.src_table, e.stage, e.error "
            f"from {SCHEMA}.ingest_event e "
            f"where e.status = 'failed' and e.started_at > now() - make_interval(hours => %s) "
            f"order by e.started_at desc limit %s")


def servers_sql() -> str:
    """One row per source server (SERVER) with its databases' enabled tables by state, how many
    collections it spans and how many of them are running now."""
    return f"""
select {SERVER} as server,
       count(distinct t.src_database) filter (where t.task_enabled) as databases,
       count(*) filter (where t.task_enabled)                       as tables,
       count(distinct t.task_collection)                            as collections,
       {STATE_COUNTS},
       max({LAST_SUCCESS}) filter (where t.task_enabled)            as newest_success_at,
       min({LAST_SUCCESS}) filter (where t.task_enabled)            as oldest_success_at,
       count(distinct t.task_collection) filter (where exists (
           select 1 from {SCHEMA}.ingest_run r where r.task_collection = t.task_collection
           and r.status = 'running'))                               as running_collections
from {SCHEMA}.control t
left join {SCHEMA}.checkpoint cp on cp.control_id = t.id
{DATABASE_CHECK_JOIN}
group by 1
order by 1"""


def collection_databases_sql() -> str:
    """One row per source database of a collection: its enabled tables by state, its oldest and newest
    success, and, while a run of the collection is going, how many of its tables that run has checked
    (stamped since it started). Bounded to one collection, so it needs no roll-up table.
    Params: task_collection, task_collection."""
    return f"""
with run as (
    select started_at from {SCHEMA}.ingest_run
    where task_collection = %s and status = 'running' order by started_at desc limit 1
)
select {SERVER} as server,
       coalesce(t.src_database, '') as src_database,
       count(*) filter (where t.task_enabled) as tables,
       {STATE_COUNTS},
       count(*) filter (where t.task_enabled
                        and {LAST_SUCCESS} >= (select started_at from run)) as checked_this_run,
       min({LAST_SUCCESS}) filter (where t.task_enabled) as oldest_success_at,
       max({LAST_SUCCESS}) filter (where t.task_enabled) as newest_success_at
from {SCHEMA}.control t
left join {SCHEMA}.checkpoint cp on cp.control_id = t.id
{DATABASE_CHECK_JOIN}
where t.task_collection = %s
group by 1, 2
order by 2, 1"""


def collection_database_rows_sql() -> str:
    """Rows written per source database (server and name) of a collection: by first loads (seed, reseed
    or full), each table's latest successful one only, which is the source's row count at that load, so
    a reseed or a full reload replaces a table's count instead of adding to it; and by changes in the
    last 24 hours. With the first loads that recorded no count (batched appends record one total on the
    run), and the running run's tables loaded, in flight and failed. Events of a deleted control row
    count under a blank server. Params: task_collection."""
    return f"""
with ev as (
    select {SERVER} as server,
           coalesce(e.src_database, '') as src_database,
           e.id, e.run_id, e.control_id, e.action, e.status, e.finished_at, e.rows_merged
    from {SCHEMA}.ingest_event e
    left join {SCHEMA}.control t on t.id = e.control_id
    where e.task_collection = %s
      -- Only the events read below, not the group's whole history: first loads (any age, through
      -- ingest_event_first_load_idx), the last 24 hours, and the running run's.
      and ((e.status = 'ok' and e.action in ('seed', 'reseed', 'full'))
           or e.finished_at > now() - interval '24 hours' or e.status = 'running'
           or exists (select 1 from {SCHEMA}.ingest_run r where r.run_id = e.run_id and r.status = 'running'))
),
latest_first as (
    select distinct on (control_id) server, src_database, rows_merged
    from ev where status = 'ok' and action in ('seed', 'reseed', 'full')
    order by control_id, finished_at desc nulls last, id desc
),
first_loads as (
    select server, src_database, sum(rows_merged) as rows_first_loads, count(*) as first_loads,
           count(*) filter (where rows_merged is null) as first_loads_uncounted
    from latest_first group by 1, 2
),
activity as (
    select ev.server, ev.src_database,
           sum(ev.rows_merged) filter (where ev.status = 'ok' and ev.action = 'increment'
                                       and ev.finished_at > now() - interval '24 hours') as rows_changes_24h,
           count(*) filter (where ev.status = 'ok' and r.status = 'running')             as loaded_this_run,
           count(*) filter (where ev.status = 'running' and r.status = 'running')        as in_flight,
           count(*) filter (where ev.status = 'failed' and r.status = 'running')         as failed_this_run
    from ev join {SCHEMA}.ingest_run r on r.run_id = ev.run_id
    group by 1, 2
)
select a.server, a.src_database, f.rows_first_loads, coalesce(f.first_loads, 0) as first_loads,
       coalesce(f.first_loads_uncounted, 0) as first_loads_uncounted, a.rows_changes_24h,
       a.loaded_this_run, a.in_flight, a.failed_this_run
from activity a
left join first_loads f on f.server = a.server and f.src_database = a.src_database"""


def run_databases_sql() -> str:
    """One row per source database (server and name) of a run: tables loaded, in flight and failed, and
    rows written (uncounted: loads with no per-table count, such as members of a batched append). Events
    of a deleted control row count under a blank server. Param: run_id."""
    return f"""
select {SERVER} as server,
       coalesce(e.src_database, '') as src_database,
       count(*) filter (where e.status = 'ok')      as loaded,
       count(*) filter (where e.status = 'running') as in_flight,
       count(*) filter (where e.status = 'failed')  as failed,
       sum(e.rows_merged)                           as rows_written,
       count(*) filter (where e.status = 'ok' and e.rows_merged is null) as uncounted
from {SCHEMA}.ingest_event e
left join {SCHEMA}.control t on t.id = e.control_id
where e.run_id = %s
group by 1, 2
order by 2, 1"""


def group_runs_sql() -> str:
    """The runs of some ingestion groups that started in a window, oldest first: the group runs placed into
    job runs (schedules.job_runs). Params: groups, window start, window end."""
    return (f"select {_RUN_COLUMNS} from {SCHEMA}.ingest_run "
            f"where task_collection = any(%s) and started_at >= %s and started_at < %s order by started_at")


def first_started_sql() -> str:
    """The first start per ingestion group among the records of one check job run ("check-<job run>") or one
    launch's group runs ("<launch_id>:<collection>"): on a check schedule they all belong to the job run its
    members' first one started in. Params: key, key + ":%"."""
    return (f"select task_collection, min(started_at) as started_at from {SCHEMA}.ingest_run "
            f"where work_id = %s::text or work_id like %s::text group by task_collection")


def last_ran_group_runs_sql() -> str:
    """Per membership of each job, its group's newest finished group run (not skipped, not running) while it was
    the member, with its schedule_id: where a job's last job run is found when none of its recent triggers ran.
    Within one member, start order is slot order, so the newest slot among these is the job's newest that ran.
    Each member's is one step down ingest_run_collection_idx, not a pass over its whole history. Param: schedule ids."""
    return f"""
select m.schedule_id, r.*
from {SCHEMA}.schedule_member m
cross join lateral (
    select {_RUN_COLUMNS} from {SCHEMA}.ingest_run
    where task_collection = m.task_collection and status not in ('skipped', 'running')
      and (m.added_at is null or started_at >= m.added_at) and (m.removed_at is null or started_at < m.removed_at)
    order by started_at desc limit 1
) r
where m.schedule_id = any(%s)"""


def group_runs_in_windows_sql() -> str:
    """The runs of each (ingestion group, window start, window end) given, in one read: the job runs of several
    jobs, each around its own slot. Params: groups, starts, ends (arrays of one length)."""
    return (f"select {_RUN_COLUMNS} from {SCHEMA}.ingest_run r where exists ("
            f"select 1 from unnest(%s::text[], %s::timestamptz[], %s::timestamptz[]) as w(task_collection, starts, ends) "
            f"where w.task_collection = r.task_collection and r.started_at >= w.starts and r.started_at < w.ends) "
            f"order by started_at")    # exists: a run in two windows (overlapping, or a group in two jobs) comes once


def group_runs_before_sql() -> str:
    """The newest group runs of some ingestion groups that started before a time, newest first. Params: groups,
    before, limit."""
    return (f"select {_RUN_COLUMNS} from {SCHEMA}.ingest_run "
            f"where task_collection = any(%s) and started_at < %s and {not_a_check()} "
            f"order by started_at desc limit %s")


def job_run_databases_sql() -> str:
    """run_databases_sql over the group runs of one job run, with each database's ingestion group: failing
    first, then in flight, then by name. Param: run ids."""
    return f"""
select e.task_collection, {SERVER} as server, coalesce(e.src_database, '') as src_database,
       count(*) filter (where e.status = 'ok')      as loaded,
       count(*) filter (where e.status = 'running') as in_flight,
       count(*) filter (where e.status = 'failed')  as failed,
       sum(e.rows_merged)                           as rows_written,
       count(*) filter (where e.status = 'ok' and e.rows_merged is null) as uncounted
from {SCHEMA}.ingest_event e
left join {SCHEMA}.control t on t.id = e.control_id
where e.run_id = any(%s)
group by 1, 2, 3
order by (count(*) filter (where e.status = 'failed')) desc, (count(*) filter (where e.status = 'running')) desc, 3, 2, 1"""


def run_failures_sql() -> str:
    """The failed table events of some group runs, newest first, with each table's server: what
    group_errors groups by cause. Param: run ids."""
    return (f"select e.finished_at, e.task_collection, e.run_id, e.control_id, {SERVER} as server, e.src_database, "
            f"e.src_schema, e.src_table, e.stage, e.error from {SCHEMA}.ingest_event e "
            f"left join {SCHEMA}.control t on t.id = e.control_id "
            f"where e.run_id = any(%s) and e.status = 'failed' order by e.finished_at desc")


def not_ingested_sql() -> str:
    """Per discovered (host and port, database), from its latest successful discovery: its endpoint ("host:port",
    as SERVER spells a JDBC server), tables found, how many have no control row yet (matched as
    discovered_tables_sql's registered_id is, in any group), and the largest of those."""
    return f"""
with latest as (
    select distinct on ({endpoint_sql('d')}, lower(d.src_database)) d.discovery_id, d.src_host, d.src_database,
           d.finished_at, {endpoint_sql('d')} as endpoint
    from {SCHEMA}.discovery d where d.status = 'ok'
    order by {endpoint_sql('d')}, lower(d.src_database), d.finished_at desc
),
found as (
    select l.endpoint, l.src_host, l.src_database, l.finished_at, dt.src_schema, dt.src_table, dt.size_mb,
           exists (select 1 from {SCHEMA}.control c where c.src_host is not null and {endpoint_sql('c')} = l.endpoint
                   and lower(c.src_database) = lower(dt.src_database) and lower(c.src_schema) = lower(dt.src_schema)
                   and lower(c.src_table) = lower(dt.src_table)) as registered
    from latest l
    join {SCHEMA}.discovered_table dt on dt.discovery_id = l.discovery_id and dt.src_database = l.src_database
)
select endpoint, src_host, src_database, finished_at as discovered_at, count(*) as discovered,
       count(*) filter (where not registered) as not_registered,
       (array_agg(src_schema || '.' || src_table order by size_mb desc nulls last)
          filter (where not registered))[1] as largest_not_registered
from found
group by 1, 2, 3, 4
order by 1, 3"""


def _database_rows() -> str:
    return (f"select t.* from {SCHEMA}.control t where t.task_collection = %s and {SERVER} = %s "
            f"and coalesce(t.src_database, '') = %s")


def database_tables_sql() -> str:
    """One database's tables in one ingestion group, with their state now and their part in a group run
    (its latest event for the table, and whether it is still to go): failing first, then in flight, then to
    go, then by name. A table's last success counts its database's check stamp (LAST_SUCCESS). Params: group,
    server, database, run_id (or None), schema.table pattern, limit, offset."""
    return f"""
with t as ({_database_rows()}),
r as (select run_id, started_at from {SCHEMA}.ingest_run where run_id = %s)
select t.id, t.src_schema, t.src_table, t.load_type, t.task_enabled, cp.phase, cp.status, {LAST_SUCCESS} as last_success_at,
       round(extract(epoch from (now() - {LAST_SUCCESS})) / 60)::int as lag_min,
       t.freshness_sla_minutes as sla_min, {SLA_BREACHED} as sla_breached,
       case when not t.task_enabled then 'disabled' when {_FAILING} then 'failing' when {_FIRST_LOAD} then 'first load'
            when {SLA_BREACHED} then 'late' else 'current' end as state,
       e.status as run_status, e.stage as run_stage, e.started_at as run_started_at, e.rows_merged as run_rows,
       round(extract(epoch from (now() - e.started_at)))::int as run_running_s,
       (r.run_id is not null and e.status is null and t.task_enabled
        and ({LAST_SUCCESS} is null or {LAST_SUCCESS} < r.started_at)) as run_to_go
from t
left join {SCHEMA}.checkpoint cp on cp.control_id = t.id
{DATABASE_CHECK_JOIN}
left join r on true
left join lateral (select ev.status, ev.stage, ev.started_at, ev.rows_merged from {SCHEMA}.ingest_event ev
                   where ev.run_id = r.run_id and ev.control_id = t.id order by ev.started_at desc limit 1) e on true
where (t.src_schema || '.' || t.src_table) ilike %s
order by coalesce({_FAILING}, false) desc, (e.status = 'running') is true desc,
         (r.run_id is not null and e.status is null and ({LAST_SUCCESS} is null or {LAST_SUCCESS} < r.started_at)) desc,
         t.src_schema, t.src_table
limit %s offset %s"""


def database_tables_count_sql() -> str:
    """How many tables database_tables_sql pages through. Params: group, server, database, schema.table pattern."""
    return (f"with t as ({_database_rows()}) select count(*) as tables from t "
            f"where (t.src_schema || '.' || t.src_table) ilike %s")


def rows_written_since_sql() -> str:
    """The rows these groups' table loads wrote in the last N hours, and how many loads recorded no count outside a
    batched append (batched appends record theirs on the run: run_batch_rows_sql). Params: groups, hours."""
    return (f"select coalesce(sum(e.rows_merged), 0) as rows_written, "
            f"{UNCOUNTED_OUTSIDE_BATCHES} as uncounted "
            f"from {SCHEMA}.ingest_event e where e.task_collection = any(%s) "
            f"and e.finished_at > now() - make_interval(hours => %s)")


def run_batch_rows_sql() -> str:
    """Runs with a detail that started in the last N hours, for their batched appends' row totals.
    Param: hours."""
    return (f"select run_id, task_collection, detail from {SCHEMA}.ingest_run "
            f"where detail is not null and started_at > now() - make_interval(hours => %s)")


def server_database_key(server, database) -> str:
    """A source database (server and name) as one text, as SERVER_DATABASE_KEY spells it in SQL."""
    return f"{server or ''}\x1f{database or ''}"


# server_database_key in SQL. Alias t = control, e = ingest_event.
SERVER_DATABASE_KEY = f"{SERVER} || chr(31) || coalesce(e.src_database, '')"


def collection_batch_runs_sql() -> str:
    """A collection's runs with a detail whose loads with no count were all their tables' latest first
    loads (seed, reseed or full), and the databases of those loads: such a run's batched append total is
    rows in first loads of those databases. A run that also batched changes is left out, because its one
    total can't be split between first loads and changes, and so is an older batch a later first load
    replaced. ``databases`` names them; ``keys`` are their server_database_key (server and name), for
    matching. Params: task_collection, task_collection."""
    return (f"select r.run_id, r.detail, array_remove(array_agg(distinct e.src_database), null) as databases, "
            f"array_agg(distinct {SERVER_DATABASE_KEY}) as keys "
            f"from {SCHEMA}.ingest_run r join {SCHEMA}.ingest_event e on e.run_id = r.run_id "
            f"and e.status = 'ok' and e.rows_merged is null "
            f"left join {SCHEMA}.control t on t.id = e.control_id "
            f"left join (select distinct on (control_id) control_id, id from {SCHEMA}.ingest_event "
            f"           where task_collection = %s and status = 'ok' and action in ('seed', 'reseed', 'full') "
            f"           order by control_id, finished_at desc nulls last, id desc) latest on latest.control_id = e.control_id "
            f"where r.task_collection = %s and r.detail is not null group by r.run_id, r.detail "
            f"having bool_and(e.action in ('seed', 'reseed', 'full') and e.id = latest.id)")


def batch_rows(detail) -> tuple:
    """(rows, rows of loads with no count) from a run's detail: batched appends record one total per
    batch commit (batch_rows) and how many members had none (batch_rows_unknown). (0, 0) without."""
    try:
        d = json.loads(detail) if isinstance(detail, str) else (detail or {})
    except ValueError:
        return 0, 0
    if not isinstance(d, dict):
        return 0, 0
    return int(d.get("batch_rows") or 0), int(d.get("batch_rows_unknown") or 0)


def _count(row: dict, key: str) -> int:
    return int(row.get(key) or 0)


def _detail(run: dict) -> dict:
    """A run's detail as a dict: it is text that may not be JSON, so parse it here rather than cast it in SQL."""
    detail = run.get("detail")
    try:
        d = json.loads(detail) if isinstance(detail, str) else (detail or {})
    except ValueError:
        return {}
    return d if isinstance(d, dict) else {}


def open_runs_sql() -> str:
    """Every run still recorded as running, group runs and checks, with the Databricks run that ran it: what the app
    reconciles with Databricks (main._close_ended_runs)."""
    return f"select run_id, task_collection, job_run_id, started_at from {SCHEMA}.ingest_run where status = 'running'"


def close_ended_run_sql() -> str:
    """Close one run still 'running' whose Databricks run has ended, and its events still running (or waiting for a
    retry that never came), as failed: what the engine's close_abandoned_runs does when the group's next run starts,
    done by the app when it finds the Databricks run over, so a run cancelled in Databricks doesn't stay running.
    Params: detail, run_id, error (appended to a retried attempt's), error. Returns (runs closed, events closed)."""
    return (
        f"with dead as (update {SCHEMA}.ingest_run set status = 'failed', finished_at = now(), detail = %s "
        "where run_id = %s and status = 'running' returning run_id), "
        f"events as (update {SCHEMA}.ingest_event e set status = 'failed', finished_at = coalesce(e.finished_at, now()), "
        "duration_ms = coalesce(e.duration_ms, (extract(epoch from (now() - e.started_at)) * 1000)::bigint), "
        "error = case when e.status = 'retried' then concat(e.error, ' | ', %s) else %s end "
        "from dead where e.run_id = dead.run_id and (e.status = 'running' or ("
        f"e.status = 'retried' and not exists (select 1 from {SCHEMA}.ingest_event later "
        "where later.run_id = e.run_id and later.control_id = e.control_id and later.id > e.id))) "
        "returning e.id) "
        "select (select count(*) from dead), (select count(*) from events)"
    )


def run_outcome(run: dict, progress: Optional[dict] = None) -> dict:
    """What a group run's engine record means, as the pages show it: a label, a tone and an optional note.
    The engine records a run that loaded with some table failures as failed, and a run another run closed
    (it stopped without finishing) as failed with closed_by_run in its detail. A running run is detecting
    changes until its first table is checked or loaded, then loading changed tables."""
    status, failed = run.get("status"), _count(run, "failed")
    if status == "completed" and not failed:
        return {"label": "Succeeded", "tone": "ok", "note": None}
    if status == "skipped":
        return {"label": "Skipped: already running", "tone": "neutral", "note": None}
    if status == "running":
        if progress is None:
            return {"label": "Running", "tone": "run", "note": None}
        started = any(_count(progress, k) for k in ("tables_ok", "tables_failed", "tables_running", "tables_checked"))
        return {"label": "Loading changed tables" if started else "Detecting changes", "tone": "run", "note": None}
    closed_by = _detail(run).get("closed_by_run")
    if closed_by is not None:
        return {"label": "Ended without finishing", "tone": "bad", "note": f"closed by run {closed_by}"}
    if _detail(run).get("closed_by_app"):
        return {"label": "Ended without finishing", "tone": "bad", "note": _detail(run).get("reason")}
    if status in ("failed", "completed") and failed:
        return {"label": "Succeeded with failures", "tone": "warn", "note": f"{failed:,} {'table' if failed == 1 else 'tables'} failed"}
    return {"label": "Failed", "tone": "bad", "note": None}


def with_outcome(run: dict, progress: Optional[dict] = None) -> dict:
    """The run with its outcome (run_outcome); a run another run closed has no duration or speed, since
    its finish (and its last events') is the closing time, not when its work stopped."""
    run["outcome"] = run_outcome(run, progress)
    run["ended_without_finishing"] = run["outcome"]["label"] == "Ended without finishing"
    if run["ended_without_finishing"]:
        run["duration_s"] = None
    return run


def _when(value):
    """A timestamp as a datetime: lakebase.query sends them as ISO text."""
    return datetime.fromisoformat(value) if isinstance(value, str) else value


def loaded_tables(run: dict) -> int:
    """Tables a group run loaded: a finished run's ok less its unchanged tables and reseeds queued (both
    count as ok), else its ok events."""
    if run.get("ok") is not None:
        return max(_count(run, "ok") - _count(run, "skipped") - _count(run, "reseeded"), 0)
    return _count(run, "loaded")


def work_speed(run: dict) -> Optional[float]:
    """Tables loaded a minute over the run's work span (first table start to last finish), which leaves
    out planning and change detection. A finished run's ok counts its unchanged tables (cursor tables whose
    watermark didn't move record ok events too) and its reseeds queued, so loaded is ok less both; a run
    with no totals yet counts its ok events. None before any table loaded."""
    start, end, loaded = _when(run.get("work_started_at")), _when(run.get("work_finished_at")), loaded_tables(run)
    if not (start and end and loaded):
        return None
    minutes = max((end - start).total_seconds(), 1) / 60
    return round(loaded / minutes, 1)


# A finished job run's outcome is the worst of its group runs', in this order.
JOB_RUN_ORDER = ("Ended without finishing", "Failed", "Succeeded with failures", "Succeeded")


def job_run_outcome(members: List[dict]) -> dict:
    """A job run's outcome from its members (schedules.job_runs): running while any group run (extra runs
    included) is running or any member is waiting for or starting its trigger, or being checked; otherwise the worst
    of its group runs' outcomes, extra runs included, with a member its check found unchanged as completed and one
    whose check failed as failed, or "Not run" when none ran. The note counts the members that did not run, as
    "17 of 18 group runs, 1 skipped"."""
    runs = [m["run"] for m in members if m.get("run")]
    ran = [r for r in runs if r.get("status") != "skipped"]
    extras = [r for m in members for r in m.get("extra") or [] if r.get("status") != "skipped"]
    skipped = sum(1 for m in members if m["state"] == "skipped") + (len(runs) - len(ran))
    no_run = sum(1 for m in members if m["state"] == "no run")
    starting = sum(1 for m in members if m["state"] == "starting")
    unchanged = sum(1 for m in members if m["state"] == "unchanged")
    check_failed = sum(1 for m in members if m["state"] == "check failed")
    note = None
    if len(ran) < len(members):
        note = ", ".join([f"{len(ran)} of {len(members)} group runs"] + ([f"{skipped} skipped"] if skipped else [])
                         + ([f"{starting} starting"] if starting else [])
                         + ([f"{unchanged} unchanged"] if unchanged else [])
                         + ([f"{check_failed} check failed"] if check_failed else [])
                         + ([f"{no_run} not run"] if no_run else []))
    if (any(m["state"] in ("waiting", "starting", "checking") for m in members)
            or any(r.get("status") == "running" for r in runs + extras)):
        return {"label": "Running", "tone": "run", "note": note}
    if not ran and not extras and not unchanged and not check_failed:
        return {"label": "Not run", "tone": "neutral", "note": note}
    # A member its check found unchanged completed with nothing to load; one whose check failed failed.
    labels = [run_outcome(r)["label"] for r in ran + extras] + ["Succeeded"] * unchanged + ["Failed"] * check_failed
    worst = min(labels, key=lambda label: JOB_RUN_ORDER.index(label) if label in JOB_RUN_ORDER else len(JOB_RUN_ORDER))
    tone = {"Succeeded": "ok", "Succeeded with failures": "warn"}.get(worst, "bad")
    return {"label": worst, "tone": tone, "note": note}


def job_run_expected_end(members: List[dict], progress_by_run: dict, p50_by_group: dict, now):
    """When a running job run should end: the latest of each running group run's time left (time_left), each
    waiting member's start plus its group's typical run, and for a member starting, its typical run from now.
    None when there is nothing to go on."""
    ends = []
    for m in members:
        for extra in m.get("extra") or []:
            if extra.get("status") == "running":
                left = time_left(progress_by_run.get(extra.get("run_id")) or {}, now)
                if left and not (left["basis"] == "typical" and left["seconds"] == 0):
                    ends.append(now + timedelta(seconds=left["seconds"]))
        run = m.get("run")
        if run and run.get("status") == "running":
            left = time_left(progress_by_run.get(run.get("run_id")) or {}, now)
            if left and not (left["basis"] == "typical" and left["seconds"] == 0):     # past its typical run: no estimate
                ends.append(now + timedelta(seconds=left["seconds"]))
        elif m["state"] == "waiting" and p50_by_group.get(m["task_collection"]) is not None:
            ends.append(_when(m["fire_at"]) + timedelta(seconds=int(p50_by_group[m["task_collection"]])))
        elif m["state"] == "starting" and p50_by_group.get(m["task_collection"]) is not None:
            ends.append(now + timedelta(seconds=int(p50_by_group[m["task_collection"]])))
    return max(ends) if ends else None


# Live speed and a rate-based time left wait for this much loading, so the first few loads don't set the pace.
LIVE_SPEED_AFTER_S = 180
LIVE_WINDOW_S = 300


def _live_window_minutes(progress: dict, now) -> Optional[float]:
    start = _when(progress.get("work_started_at"))
    if not start:
        return None
    loading_s = (now - start).total_seconds()
    if loading_s < LIVE_SPEED_AFTER_S:
        return None
    return min(loading_s, LIVE_WINDOW_S) / 60


def live_speed(progress: dict, now) -> Optional[dict]:
    """A running group run's tables and rows a minute over its last 5 minutes of loading (fewer, when it
    has loaded for less). None in its first 3 minutes of loading."""
    minutes = _live_window_minutes(progress, now)
    if minutes is None:
        return None
    return {"tables_per_min": round(_count(progress, "ok_last_5m") / minutes, 1),
            "rows_per_min": round(_count(progress, "rows_last_5m") / minutes, 1)}


def time_left(progress: dict, now) -> Optional[dict]:
    """About how long a running group run has left. While loading (3 minutes on): the tables to go at the
    last 5 minutes' rate. Before that, or with no recent loads: its group's typical (p50) run time less the
    time so far. None with neither."""
    minutes = _live_window_minutes(progress, now)
    rate = _count(progress, "ok_last_5m") / minutes if minutes else 0
    if rate:
        to_go = max(_count(progress, "enabled_tables") - _count(progress, "tables_checked")
                    - _count(progress, "tables_running") - _count(progress, "tables_failed")
                    - _count(progress, "tables_skipped"), 0)
        return {"seconds": round(to_go / rate * 60), "basis": "rate"}
    if progress.get("p50_s") is None:
        return None
    return {"seconds": max(int(progress["p50_s"]) - _count(progress, "elapsed_s"), 0), "basis": "typical"}


# Failure causes, by the error text, most specific first. Connection and timeout failures usually clear
# on the next run; the others need a change at the source or in config.
ERROR_CLASSES = (
    ("connection", "Could not connect to the source database",
     ("failed_jdbc.connection", "connection reset", "connection refused", "could not connect", "network",
      "login timeout", "socket", "tcp/ip connection to the host")),
    ("timeout", "Timed out", ("timeout", "timed out")),
    ("permission", "Permission denied at the source or sink",
     ("permission", "denied", "not authorized", "login failed")),
    ("change_tracking", "Change Tracking is off at the source", ("change tracking is not enabled",)),
    ("missing", "A table, column or object is missing", ("invalid object name", "does not exist", "not found",
                                                          "invalid column")),
    ("schema", "The schema does not match", ("schema", "mismatch", "cannot cast", "data type")),
)


# Each cause phrase matches only as a whole word or phrase, so a name that contains one (a column called
# network_id or timeout_ms) is not read as that cause.
_ERROR_PATTERNS = tuple((name, label, re.compile("|".join(rf"(?<![a-z0-9_]){re.escape(n)}(?![a-z0-9_])" for n in needles)))
                        for name, label, needles in ERROR_CLASSES)


# Quoted names in an error: anything in single quotes, one word in double quotes, or a bracketed name
# that is not an upper-case error tag.
_QUOTED_NAMES = re.compile(r"'[^'\n]{1,256}'|\"[\w.$#@-]{1,128}\"|\[(?![A-Z0-9_.]+\])[^\]\n]{1,256}\]")


def classify_error(error: Optional[str]) -> Tuple[str, str]:
    """(class, label) of an error message; ('other', 'Other errors') when nothing matches. Names the
    message quotes (a column or table such as 'network', "network" or [network]) are left out, so a name
    is never read as a cause; a double-quoted phrase (a driver's quoted cause) and an upper-case error
    tag such as [FAILED_JDBC.CONNECTION] are kept."""
    text = _QUOTED_NAMES.sub("''", error or "").lower()
    for name, label, pattern in _ERROR_PATTERNS:
        if pattern.search(text):
            return name, label
    return "other", "Other errors"


def error_signature(error: Optional[str]) -> str:
    """The error's first line with numbers, quoted names and hex ids blanked, so the same cause on
    different tables groups together."""
    first = (error or "").strip().splitlines()[0] if (error or "").strip() else ""
    first = re.sub(r"'[^']*'|\"[^\"]*\"", "'…'", first)
    first = re.sub(r"0x[0-9a-fA-F]+|\b\d+\b", "#", first)
    return first[:240]


def failed_tables_sql() -> str:
    """The tables that failed in the last N hours, newest failure first, at most `limit` tables: one row
    per table and error message, with how many times the table failed with it, its first and last time,
    and the table's server (SERVER). A table that failed for two causes has a row for each.
    Params: hours, limit (tables)."""
    return f"""
with recent as (
    select control_id, error, task_collection, src_database, src_schema, src_table, started_at, finished_at
    from {SCHEMA}.ingest_event
    where status = 'failed' and started_at > now() - make_interval(hours => %s)
),
tables as (
    select control_id, max(started_at) as table_last_started_at from recent
    group by control_id order by table_last_started_at desc limit %s
),
causes as (
    select r.control_id, r.error, count(*) as failures, min(r.finished_at) as first_at, max(r.finished_at) as last_at,
           (array_agg(r.task_collection order by r.started_at desc))[1] as task_collection,
           (array_agg(r.src_database order by r.started_at desc))[1] as src_database,
           (array_agg(r.src_schema order by r.started_at desc))[1] as src_schema,
           (array_agg(r.src_table order by r.started_at desc))[1] as src_table,
           max(tb.table_last_started_at) as table_last_started_at
    from recent r join tables tb on tb.control_id = r.control_id
    group by r.control_id, r.error
)
select c.control_id, c.error, c.failures, c.first_at, c.last_at, c.last_at as finished_at, c.task_collection,
       c.src_database, c.src_schema, c.src_table, {SERVER} as server
from causes c
left join {SCHEMA}.control t on t.id = c.control_id
order by c.table_last_started_at desc, c.control_id, c.last_at desc"""


def group_errors(rows: List[dict]) -> List[dict]:
    """Failed tables grouped by cause (class and signature), most tables first: tables (each counted
    once per cause, however often it failed), failures, databases (server and name, with collection),
    first and last time, and one raw example message. Takes failed_tables_sql's rows (one per table, with its failures and times) or
    plain failed events (one per failure), and counts a table once either way."""
    groups: dict = {}
    for r in rows:
        name, label = classify_error(r.get("error"))
        g = groups.setdefault((name, error_signature(r.get("error"))), {
            "error_class": name, "label": label, "signature": error_signature(r.get("error")),
            "example": (r.get("error") or "")[:2000], "tables": 0, "failures": 0, "databases": {}, "seen": set(),
            "first_at": None, "last_at": None})
        g["failures"] += int(r.get("failures") or 1)
        table = r.get("control_id") or (r.get("task_collection"), r.get("src_database"), r.get("src_schema"),
                                        r.get("src_table"))
        if table not in g["seen"]:
            g["seen"].add(table)
            g["tables"] += 1
            db = g["databases"].setdefault((r.get("task_collection"), r.get("server"), r.get("src_database")), {
                "task_collection": r.get("task_collection"), "server": r.get("server"),
                "src_database": r.get("src_database"), "tables": []})
            db["tables"].append(f"{r.get('src_schema')}.{r.get('src_table')}")
        for at in (r.get("first_at") or r.get("finished_at"), r.get("last_at") or r.get("finished_at")):
            if at:
                g["first_at"] = min(g["first_at"] or at, at)
                g["last_at"] = max(g["last_at"] or at, at)
    out = []
    for g in sorted(groups.values(), key=lambda g: -g["tables"]):
        del g["seen"]
        g["databases"] = sorted(g["databases"].values(),
                                key=lambda d: (str(d["task_collection"]), str(d["src_database"]), str(d["server"])))
        out.append(g)
    return out


# ---- UC connections (bulk import) ---------------------------------------------------------------

def connection_databases_sql() -> str:
    """The databases lakefed tracks on one source: by UC connection name or host, each with its
    collections, enabled tables and newest success. Params: connection name, host."""
    return (f"select t.src_database, string_agg(distinct t.task_collection, ', ') as collections, "
            f"count(*) filter (where t.task_enabled) as tables, max({LAST_SUCCESS}) as newest_success_at "
            f"from {SCHEMA}.control t left join {SCHEMA}.checkpoint cp on cp.control_id = t.id "
            f"{DATABASE_CHECK_JOIN} "
            f"where t.src_connection = %s or lower(t.src_host) = lower(%s) "
            f"group by 1 order by 1")


# ---- tables (control + checkpoint) --------------------------------------------------------------

TABLE_FILTERS = ("all", "failing", "sla_breached", "no_sla", "seeding", "disabled")


# The order Tables pages in, and the cursor it pages after: one value per column, the row's id last.
TABLES_ORDER = "t.task_collection, coalesce(t.src_database, ''), t.src_schema, t.src_table, t.id"


def tables_sql(collection: Optional[str], search: Optional[str], status_filter: str,
               limit: int, database: Optional[str] = None, after: Optional[list] = None) -> Tuple[str, tuple]:
    """Control rows joined with their checkpoint, a page of `limit` in TABLES_ORDER after the cursor `after`
    (the last row's group, database, schema, table and id). `database` filters on one database exactly.
    Returns (sql, params)."""
    if status_filter not in TABLE_FILTERS:
        raise ValueError(f"status_filter must be one of {TABLE_FILTERS}")
    conds, params = [], []
    if collection:
        conds.append("t.task_collection = %s")
        params.append(collection)
    if database is not None:
        conds.append("coalesce(t.src_database, '') = %s")
        params.append(database)
    if search:
        conds.append("(t.src_database || '.' || t.src_schema || '.' || t.src_table || ' ' || "
                     "t.sink_catalog || '.' || t.sink_schema || '.' || t.sink_table || ' ' || "
                     "coalesce(t.client_id, '')) ilike %s")
        params.append(f"%{search}%")
    if status_filter == "failing":
        conds.append("cp.status = 'failed'")
    elif status_filter == "sla_breached":
        conds.append(SLA_BREACHED)
    elif status_filter == "no_sla":
        conds.append("t.task_enabled and t.freshness_sla_minutes is null")
    elif status_filter == "seeding":
        conds.append("(cp.control_id is null or cp.phase = 'seeding')")
    elif status_filter == "disabled":
        conds.append("not t.task_enabled")
    if after:
        conds.append(f"({TABLES_ORDER}) > (%s, %s, %s, %s, %s)")
        params.extend(after)
    where = ("where " + " and ".join(conds) + " ") if conds else ""
    sql = (f"select t.id, t.task_collection, t.task_enabled, t.src_type, t.load_type, "
           f"t.src_database, t.src_schema, t.src_table, "
           f"t.sink_catalog || '.' || t.sink_schema || '.' || t.sink_table as sink, t.client_id, "
           f"cp.phase, cp.status, {LAST_SUCCESS} as last_success_at, "
           f"round(extract(epoch from (now() - {LAST_SUCCESS})) / 60)::int as lag_min, "
           f"t.freshness_sla_minutes as sla_min, {SLA_BREACHED} as sla_breached, "
           f"cp.position, cp.detail "
           f"from {SCHEMA}.control t left join {SCHEMA}.checkpoint cp on cp.control_id = t.id "
           f"{DATABASE_CHECK_JOIN} "
           f"{where}order by {TABLES_ORDER} limit %s")
    return sql, tuple(params) + (int(limit),)


def collections_sql() -> str:
    return f"select distinct task_collection from {SCHEMA}.control order by 1"


def set_enabled_sql() -> str:
    """Params: enabled (bool), control ids (list)."""
    return f"update {SCHEMA}.control set task_enabled = %s where id = any(%s)"


# A reseed flip also clears the status (a requested reseed isn't a failing table, and the seed budget
# defers failed seeds) and any pending append: the job settles a pending append before it looks at the
# phase, so a left-over one would move the table back to incremental and undo the reseed. The job's
# own reseed (state_store.upsert_checkpoint_sql) clears them the same way.
_RESEED_SET = "phase = 'seeding', status = 'ok', pending_position = null, pending_run_id = null, updated_at = now()"


def force_reseed_sql() -> str:
    """Flip checkpoints back to seeding (the next run re-copies those tables). Param: control ids."""
    return (f"update {SCHEMA}.checkpoint set {_RESEED_SET}, detail = 'reseed requested from the app' "
            f"where control_id = any(%s)")


def log_reseed_sql() -> str:
    """Record manual reseeds in the reseed log. Params: reason, control ids."""
    return (f"insert into {SCHEMA}.reseed_queue (control_id, src_database, src_schema, src_table, reason) "
            f"select id, src_database, src_schema, src_table, %s from {SCHEMA}.control where id = any(%s)")


# ---- add a control row --------------------------------------------------------------------------

LOAD_TYPES = ("ct", "cursor", "full")
SRC_TYPES = ("sqlserver", "oracle", "postgresql", "mysql")
CONTROL_INSERT_COLUMNS = (
    "task_collection", "src_type", "src_host", "src_port", "secret_key", "src_database", "src_schema",
    "src_table", "select_list", "primary_key", "load_type", "cursor_column", "sink_catalog",
    "sink_schema", "sink_table", "client_id", "load_partitioned", "partition_col", "partition_size_mb",
    "freshness_sla_minutes", "src_connection", "use_remote_query", "write_mode", "client_attrs", "jdbc_options",
)
WRITE_MODES = ("merge", "append")
_ATTR_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")     # ct_engine.client_attrs


def _truthy(v) -> bool:
    return v if isinstance(v, bool) else str(v or "").strip().lower() in ("true", "1", "yes", "y")


TRUE_WORDS, FALSE_WORDS = ("true", "1", "yes", "y", "on"), ("false", "0", "no", "n", "off")


def parse_bool(v) -> bool:
    """A yes or no setting as given: a boolean, or one of the words for one ("false" is false). Anything else raises
    ValueError, so a mistyped value is refused rather than read as true."""
    if isinstance(v, bool):
        return v
    if isinstance(v, int) and v in (0, 1):
        return bool(v)
    word = str(v).strip().lower() if v is not None else ""
    if word in TRUE_WORDS:
        return True
    if word in FALSE_WORDS:
        return False
    raise ValueError(f"{v!r} is not true or false")


def _json_object(v):
    """A JSON object given as text or a dict -> the dict; None when blank; raises ValueError otherwise."""
    if v is None or (isinstance(v, str) and not v.strip()):
        return None
    obj = json.loads(v) if isinstance(v, str) else v
    if not isinstance(obj, dict):
        raise ValueError("not a JSON object")
    return obj


def client_attrs_errors(attrs, select_list) -> List[str]:
    """The ingestion job's client_attrs rules (ct_engine.client_attrs), plus no clash with select_list."""
    try:
        obj = _json_object(attrs)
    except ValueError:
        return ["client_attrs must be a JSON object {column: value}"]
    errs, seen = [], set()
    selected = {c.strip().lower() for c in str(select_list or "").split(",") if c.strip()}
    for name, value in (obj or {}).items():
        if not _ATTR_NAME.match(str(name)) or str(name).startswith("__"):
            errs.append(f"client_attrs column '{name}': use letters, digits and _ (1-128 characters, not starting with __)")
        elif name.lower() in seen:
            errs.append(f"client_attrs column '{name}' is given twice")
        elif name.lower() in selected:
            errs.append(f"client_attrs column '{name}' is also in select_list")
        if isinstance(value, (dict, list)):
            errs.append(f"client_attrs column '{name}': the value must be a string, number, boolean or null")
        seen.add(str(name).lower())
    return errs


def validate_control_row(row: dict) -> List[str]:
    """Form-level checks with clear messages (the database CHECK constraints are the backstop)."""
    errs = []
    remote = _truthy(row.get("use_remote_query"))
    # A UC-connection read needs the connection and no host or secret; a JDBC read needs both.
    source = ("src_connection",) if remote else ("src_host", "secret_key")
    for col in ("task_collection",) + source + ("src_database", "src_schema", "src_table",
                                                "select_list", "sink_catalog", "sink_schema", "sink_table"):
        if col == "src_host" and str(row.get("src_connection") or "").strip():
            continue                     # JDBC falls back to the connection's host
        if not str(row.get(col) or "").strip():
            errs.append(f"{col} is required" + (" when use_remote_query is set" if col == "src_connection" else ""))
    if row.get("src_type") not in SRC_TYPES:
        errs.append(f"src_type must be one of {', '.join(SRC_TYPES)}")
    lt = row.get("load_type")
    if lt not in LOAD_TYPES:
        errs.append(f"load_type must be one of {', '.join(LOAD_TYPES)}")
    if lt in ("ct", "cursor") and not str(row.get("primary_key") or "").strip():
        errs.append(f"primary_key is required for load_type {lt}")
    if lt == "cursor" and not str(row.get("cursor_column") or "").strip():
        errs.append("cursor_column is required for load_type cursor")
    if lt in ("ct", "cursor") and row.get("src_type") != "sqlserver":
        errs.append(f"load_type {lt} is only supported for src_type sqlserver")
    if "*" in str(row.get("select_list") or ""):
        errs.append("select_list must list explicit columns (* isn't supported)")
    if row.get("load_partitioned") and (not row.get("partition_col") or not row.get("partition_size_mb")):
        errs.append("partitioned loads need partition_col and partition_size_mb")
    sla = row.get("freshness_sla_minutes")
    if sla not in (None, ""):
        try:
            ok = int(sla) > 0 and float(sla) == int(sla)
        except (TypeError, ValueError):
            ok = False
        if not ok:
            errs.append("freshness_sla_minutes must be a whole number of minutes greater than 0 (blank = no SLA)")
    cid = row.get("client_id")
    if cid not in (None, "") and not re.match(r"^[A-Za-z0-9_.:-]{1,128}$", str(cid)):
        errs.append("client_id may only use letters, digits and _ . : - (1-128 characters)")
    if "__client_id__" in {c.strip().lower() for c in str(row.get("select_list") or "").split(",")}:
        errs.append("select_list must not include __client_id__ (consolidated sinks add it)")
    opts = row.get("jdbc_options")
    if opts not in (None, ""):
        try:
            ok = isinstance(json.loads(opts) if isinstance(opts, str) else opts, dict)
        except ValueError:
            ok = False
        if not ok:
            errs.append("jdbc_options must be a JSON object")
    mode = str(row.get("write_mode") or "merge").strip().lower()
    if mode not in WRITE_MODES:
        errs.append("write_mode must be merge or append")
    elif mode == "append" and lt not in ("ct", "full"):
        errs.append(f"write_mode append supports load_type ct and full, not {lt}")
    errs += client_attrs_errors(row.get("client_attrs"), row.get("select_list"))
    return errs


def insert_control_sql() -> str:
    cols = ", ".join(CONTROL_INSERT_COLUMNS)
    marks = ", ".join(["%s"] * len(CONTROL_INSERT_COLUMNS))
    return f"insert into {SCHEMA}.control ({cols}) values ({marks}) returning id"


def control_params(row: dict) -> tuple:
    """Insert params in CONTROL_INSERT_COLUMNS order; blank strings become NULL (the NOT NULL columns
    write_mode, use_remote_query and load_partitioned get their defaults when omitted; JSON objects are
    stored as text)."""
    def clean(c, v):
        if c in ("use_remote_query", "load_partitioned"):
            return _truthy(v)
        if c in ("client_attrs", "jdbc_options") and isinstance(v, dict):
            return json.dumps(v)
        if isinstance(v, str):
            v = v.strip()
            v = v or None
        if c == "write_mode":
            return (v or "merge").lower()
        return v
    return tuple(clean(c, row.get(c)) for c in CONTROL_INSERT_COLUMNS)


# ---- bulk import (table discovery) --------------------------------------------------------------

DEFAULT_PARTITION_THRESHOLD_MB = 1024   # tables at least this large get a partitioned seed / full load
DEFAULT_PARTITION_SIZE_MB = 512


def discoveries_sql() -> str:
    """Recent discoveries, one row each. Param: limit."""
    return (f"select discovery_id, min(started_at) as started_at, max(finished_at) as finished_at, "
            f"min(src_host) as src_host, count(*) as databases, "
            f"count(*) filter (where status = 'ok') as ok, count(*) filter (where status = 'failed') as failed, "
            f"count(*) filter (where status = 'running') as running, coalesce(sum(tables), 0) as tables, "
            f"min(requested_by) as requested_by "
            f"from {SCHEMA}.discovery group by discovery_id order by min(started_at) desc limit %s")


def discovery_databases_sql() -> str:
    """Per-database status of one discovery. Param: discovery_id."""
    return (f"select src_database, status, tables, error, started_at, finished_at "
            f"from {SCHEMA}.discovery where discovery_id = %s order by src_database")


def discovered_tables_sql() -> str:
    """Discovered tables of one discovery, with the source connection and any existing control row
    for the same source table (``registered_id``). Param: discovery_id."""
    return (f"select dt.*, d.src_type, d.src_connection, d.src_host, d.src_port, d.secret_key, "
            f"(select min(c.id) from {SCHEMA}.control c where lower(c.src_host) = lower(d.src_host) "
            f"   and lower(c.src_database) = lower(dt.src_database) and lower(c.src_schema) = lower(dt.src_schema) "
            f"   and lower(c.src_table) = lower(dt.src_table)) as registered_id "
            f"from {SCHEMA}.discovered_table dt join {SCHEMA}.discovery d "
            f"  on d.discovery_id = dt.discovery_id and d.src_database = dt.src_database "
            f"where dt.discovery_id = %s order by dt.src_database, dt.src_schema, dt.src_table")


def parse_databases(text: Optional[str]) -> List[str]:
    """Database names from a comma- / newline-separated list (order kept, duplicates dropped)."""
    seen, out = set(), []
    for part in str(text or "").replace("\n", ",").split(","):
        name = part.strip()
        if name and name not in seen:
            seen.add(name)
            out.append(name)
    return out


def identifier(text: str) -> str:
    """A safe, lower-case Delta identifier part (letters, digits, underscores)."""
    import re
    out = re.sub(r"[^a-z0-9_]+", "_", str(text).lower()).strip("_")
    return out or "t"


def render(template: Optional[str], database: str, schema: str, table: str, as_identifier: bool = True):
    """Fill {database} / {schema} / {table} in a template; blank template -> None."""
    if not str(template or "").strip():
        return None
    f = identifier if as_identifier else (lambda v: v)
    return (str(template).replace("{database}", f(database)).replace("{schema}", f(schema))
            .replace("{table}", f(table)).strip())


def recommended_partitioning(size_mb, partition_candidate, threshold_mb: float, partition_size_mb: int):
    """(load_partitioned, partition_col, partition_size_mb) for a discovered table."""
    if partition_candidate and float(size_mb or 0) >= float(threshold_mb):
        return True, partition_candidate, int(partition_size_mb)
    return False, None, None


def review_rows(discovered: List[dict], shared: dict) -> List[dict]:
    """The review grid: one editable row per discovered table, from the recommendations plus the
    shared fields (sink templates, client_id template, partition threshold). Registered tables and
    tables with no readable columns start excluded."""
    rows = []
    for t in discovered:
        db, sch, tbl = t["src_database"], t["src_schema"], t["src_table"]
        part, pcol, psize = recommended_partitioning(t.get("size_mb"), t.get("partition_candidate"),
                                                     shared.get("partition_threshold_mb", DEFAULT_PARTITION_THRESHOLD_MB),
                                                     shared.get("partition_size_mb", DEFAULT_PARTITION_SIZE_MB))
        registered = t.get("registered_id")
        rows.append({
            "include": bool(shared.get("include_default", True)) and not registered and bool(t.get("select_list")),
            "src_database": db, "src_schema": sch, "src_table": tbl,
            "rows": t.get("row_count"), "size_mb": float(t.get("size_mb") or 0),
            "ct_enabled": bool(t.get("ct_enabled")),
            "load_type": t.get("load_type"), "primary_key": t.get("primary_key"),
            "cursor_column": t.get("cursor_column"),
            "load_partitioned": part, "partition_col": pcol, "partition_size_mb": psize,
            "sink_catalog": render(shared.get("sink_catalog"), db, sch, tbl),
            "sink_schema": render(shared.get("sink_schema"), db, sch, tbl),
            "sink_table": render(shared.get("sink_table"), db, sch, tbl),
            "client_id": render(shared.get("client_id"), db, sch, tbl, as_identifier=False),
            "freshness_sla_minutes": shared.get("freshness_sla_minutes") or None,
            "registered_id": registered, "notes": t.get("notes"),
            # carried through, not shown for editing
            "_select_list": t.get("select_list"), "_src_type": t.get("src_type"), "_src_host": t.get("src_host"),
            "_src_port": t.get("src_port"), "_secret_key": t.get("secret_key"),
        })
    return rows


def control_row_from_review(row: dict, task_collection: str) -> dict:
    """A control row (for validate_control_row / control_params) from an edited review row."""
    return {
        "task_collection": task_collection, "src_type": row["_src_type"], "src_host": row["_src_host"],
        "src_port": row["_src_port"], "secret_key": row["_secret_key"], "src_database": row["src_database"],
        "src_schema": row["src_schema"], "src_table": row["src_table"], "select_list": row["_select_list"],
        "primary_key": row.get("primary_key"), "load_type": row.get("load_type"),
        "cursor_column": row.get("cursor_column") if row.get("load_type") == "cursor" else None,
        "sink_catalog": row.get("sink_catalog"), "sink_schema": row.get("sink_schema"),
        "sink_table": row.get("sink_table"), "client_id": row.get("client_id"),
        "load_partitioned": bool(row.get("load_partitioned")),
        "partition_col": row.get("partition_col") if row.get("load_partitioned") else None,
        "partition_size_mb": row.get("partition_size_mb") if row.get("load_partitioned") else None,
        "freshness_sla_minutes": row.get("freshness_sla_minutes"),
    }


def insert_control_if_absent_sql() -> str:
    """Like insert_control_sql, but a row whose sink (or sink + client_id) is already taken is skipped
    (the unique index) instead of failing the whole import."""
    return insert_control_sql().replace(" returning id", " on conflict do nothing returning id")


# Control rows written by one statement: 25 columns each, so 1,000 rows is 25,000 parameters, well under Postgres'
# 65,535. A large job's tables go in a few dozen statements instead of one round trip per table.
CONTROL_BATCH = 1000


def insert_controls_sql(n: int) -> str:
    """insert_control_if_absent_sql for n rows in one statement: a row whose sink (or sink + client_id) is taken
    is skipped. It returns each written row's id with its sink key (control_sink_key), so the caller can tell
    which rows were skipped. Params: control_params of each row in turn, flattened."""
    row = "(" + ", ".join(["%s"] * len(CONTROL_INSERT_COLUMNS)) + ")"
    return (f"insert into {SCHEMA}.control ({', '.join(CONTROL_INSERT_COLUMNS)}) values {', '.join([row] * n)} "
            f"on conflict do nothing returning id, lower(sink_catalog), lower(sink_schema), lower(sink_table), "
            f"coalesce(client_id, '')")


def control_sink_key(params: tuple) -> tuple:
    """A row's sink key as the unique index control_sink_client_uq reads it, from its control_params."""
    at = {c: params[CONTROL_INSERT_COLUMNS.index(c)] for c in ("sink_catalog", "sink_schema", "sink_table", "client_id")}
    return (str(at["sink_catalog"] or "").lower(), str(at["sink_schema"] or "").lower(),
            str(at["sink_table"] or "").lower(), at["client_id"] or "")


def log_changes_sql(n: int) -> str:
    """log_change_sql for n change log lines in one statement. Params: each line's 9 values in turn, flattened."""
    one = "(" + ", ".join(["%s"] * 9) + ")"
    return log_change_sql().split(" values ")[0] + " values " + ", ".join([one] * n)


# ---- background writes (app_task) -----------------------------------------------------------------

APP_TASK_FIELDS = ("id", "kind", "request_id", "job_id", "status", "done", "total", "result", "error", "created_by",
                   "created_at", "heartbeat_at", "finished_at")


def insert_task_sql() -> str:
    """Record a background write, or find the one a request id already made (the same request sent again).
    Params: kind, request_id, job_id, total, result (JSON text), created_by. Returns the task's id and whether this
    call made it."""
    return (f"insert into {SCHEMA}.app_task (kind, request_id, job_id, total, result, created_by) "
            f"values (%s, %s, %s, %s, %s::jsonb, %s) on conflict (request_id) do update set request_id = excluded.request_id "
            f"returning id, (xmax = 0) as made")


def task_sql() -> str:
    """One task. Param: id."""
    return f"select {', '.join(APP_TASK_FIELDS)} from {SCHEMA}.app_task where id = %s"


def task_of_request_sql() -> str:
    """The task a request id made, if any. Param: request_id."""
    return f"select id from {SCHEMA}.app_task where request_id = %s"


def job_tasks_sql() -> str:
    """A job's tasks of the last day, newest first. Param: job_id."""
    return (f"select {', '.join(APP_TASK_FIELDS)} from {SCHEMA}.app_task where job_id = %s "
            f"and created_at > now() - interval '1 day' order by created_at desc limit 10")


def task_progress_sql() -> str:
    """A running task's progress and heartbeat. Params: done, id."""
    return f"update {SCHEMA}.app_task set done = %s, heartbeat_at = now() where id = %s and status = 'running'"


def finish_task_sql() -> str:
    """A task's end. Params: status ('succeeded' or 'failed'), job_id (or null to keep it), result (JSON text, or null
    to keep it), error, id. Only a running task is finished, so a late finish can't undo a stale one's verdict."""
    return (f"update {SCHEMA}.app_task set status = %s, job_id = coalesce(%s, job_id), "
            f"result = coalesce(%s::jsonb, result), error = %s, finished_at = now() "
            f"where id = %s and status = 'running'")


def task_written_sql() -> str:
    """Inside the write's own transaction, its last statement: what it wrote, so the task's result commits with the
    tables or not at all. Params: result (JSON text), job_id (or null to keep it), done, id."""
    return (f"update {SCHEMA}.app_task set result = %s::jsonb, job_id = coalesce(%s, job_id), done = %s, "
            f"heartbeat_at = now() where id = %s")


def stale_tasks_sql() -> str:
    """End the running tasks whose heartbeat stopped (the app restarted while writing): one with a result had
    committed its tables and only missed what follows (failed_note is kept as its error), the rest were rolled back
    with the app (failed, with rolled_back as the error). Params: rolled_back, failed_note, seconds, id (or null),
    job_id (or null)."""
    return (f"update {SCHEMA}.app_task set status = case when result is null then 'failed' else 'succeeded' end, "
            f"error = case when result is null then %s else %s end, finished_at = now() "
            f"where status = 'running' and heartbeat_at < now() - make_interval(secs => %s) "
            f"and (id = %s or job_id = %s)")


EDITABLE_REVIEW_FIELDS = ("include", "load_type", "primary_key", "cursor_column", "load_partitioned",
                          "partition_col", "partition_size_mb", "sink_catalog", "sink_schema", "sink_table",
                          "client_id", "freshness_sla_minutes")


def review_key(row: dict) -> tuple:
    return (row["src_database"], row["src_schema"], row["src_table"])


def merge_review_edits(review: List[dict], edits: List[dict]) -> List[dict]:
    """Overlay a reviewer's edits on the server-computed review rows, by source table. Only
    EDITABLE_REVIEW_FIELDS are taken from the client; connection, secret and select_list always come
    from the stored discovery. Tables without an edit keep their computed values."""
    by_key = {review_key(e): e for e in edits}
    out = []
    for r in review:
        e = by_key.get(review_key(r))
        out.append({**r, **{f: e[f] for f in EDITABLE_REVIEW_FIELDS if e is not None and f in e}})
    return out


def public_review_row(row: dict) -> dict:
    """A review row without the server-only fields (connection, secret key, select_list)."""
    return {k: v for k, v in row.items() if not k.startswith("_")}


# ---- editing control rows -----------------------------------------------------------------------
#
# Integrity rules (mirrored by the ingestion planner's identity check, which also catches SQL edits):
# changing where a ct / cursor table reads from, where it writes to, or its merge key makes the
# existing checkpoint meaningless, so the edit flips the table to seeding in the same transaction.
# Edits are refused while either affected collection is running (the run's advisory lock).

CONTROL_EDIT_COLUMNS = (
    "task_collection", "task_enabled", "tier", "priority", "src_type", "src_host", "src_port", "secret_key",
    "src_database", "src_schema", "src_table", "select_list", "primary_key", "load_type", "cursor_column",
    "sink_catalog", "sink_schema", "sink_table", "client_id", "load_partitioned", "partition_col",
    "partition_size_mb", "jdbc_options", "freshness_sla_minutes", "src_connection", "use_remote_query",
    "write_mode", "client_attrs",
)
BULK_EDIT_FIELDS = ("task_collection", "task_enabled", "tier", "priority", "freshness_sla_minutes", "load_partitioned",
                    "partition_col", "partition_size_mb", "jdbc_options", "sink_catalog", "sink_schema",
                    "src_connection", "use_remote_query")
RESEED_FIELDS = {
    "src_type": "source_changed", "src_host": "source_changed", "src_port": "source_changed",
    "src_database": "source_changed", "src_schema": "source_changed", "src_table": "source_changed",
    "sink_catalog": "sink_changed", "sink_schema": "sink_changed", "sink_table": "sink_changed",
    "client_id": "sink_changed", "primary_key": "primary_key_changed",
}
INT_FIELDS = {"src_port", "priority", "partition_size_mb", "freshness_sla_minutes"}
BOOL_FIELDS = {"task_enabled", "load_partitioned", "use_remote_query"}
JSON_FIELDS = {"jdbc_options", "client_attrs"}
CASE_INSENSITIVE = {"src_host", "src_database", "src_schema", "src_table", "sink_catalog", "sink_schema",
                    "sink_table", "primary_key", "src_connection", "write_mode"}
SINK_FIELDS = ("sink_catalog", "sink_schema", "sink_table")


def normalize_value(field: str, value):
    """Form / JSON value -> the stored value (blank strings -> NULL, numbers and booleans typed; a blank
    write_mode is merge and a blank use_remote_query false, the columns' defaults; JSON objects as text)."""
    if field == "write_mode":
        return str(value or "").strip().lower() or "merge"
    if field == "use_remote_query":
        return _truthy(value)
    if field in JSON_FIELDS and isinstance(value, dict):
        return json.dumps(value)
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None
    if value is None:
        return None
    if field in INT_FIELDS:
        return int(value)
    if field in BOOL_FIELDS:
        return value if isinstance(value, bool) else str(value).lower() in ("true", "1", "yes", "y")
    return value


def _same(field: str, old, new, row: dict) -> bool:
    if field in CASE_INSENSITIVE:
        norm = lambda v: ",".join(p.strip() for p in str(v).lower().split(",")) if v is not None else None
        return norm(old) == norm(new)
    if field == "src_port":
        default = DEFAULT_PORTS.get(str(row.get("src_type") or "sqlserver"))
        return (old or default) == (new or default)
    if field in JSON_FIELDS:            # compare the objects, not their spelling
        try:
            return _json_object(old) == _json_object(new)
        except ValueError:
            return old == new
    if field == "use_remote_query":
        return bool(old) == bool(new)
    return old == new


def plan_edit(rows: List[dict], changes: dict, bulk: bool) -> List[dict]:
    """What an edit will do to each control row: {id, changes: {field: [old, new]}, reseed (reason
    or None), notes, errors}. Pure; the API applies it only when no row has errors."""
    allowed = BULK_EDIT_FIELDS if bulk else CONTROL_EDIT_COLUMNS
    bad = sorted(set(changes) - set(allowed))
    plans = []
    for row in rows:
        p = {"id": row["id"], "table": f"{row['src_database']}.{row['src_schema']}.{row['src_table']}",
             "changes": {}, "reseed": None, "notes": [], "errors": []}
        if bad:
            p["errors"].append(("cannot be changed in bulk: " if bulk else "unknown fields: ") + ", ".join(bad))
            plans.append(p)
            continue
        try:
            new_values = {f: normalize_value(f, v) for f, v in changes.items()}
        except (TypeError, ValueError) as e:
            p["errors"].append(f"invalid value: {e}")
            plans.append(p)
            continue
        merged = dict(row)
        for f, v in new_values.items():
            if not _same(f, row.get(f), v, row):
                p["changes"][f] = [row.get(f), v]
                merged[f] = v
        p["errors"] += validate_control_row(merged)
        lt = merged.get("load_type")
        incremental = lt in ("ct", "cursor")
        reasons = [RESEED_FIELDS[f] for f in p["changes"] if f in RESEED_FIELDS]
        if "src_connection" in p["changes"] and not merged.get("src_host"):
            reasons.append("source_changed")     # the connection is the source's host (state_store.source_identity)
        if "write_mode" in p["changes"] and not any(f in p["changes"] for f in SINK_FIELDS):
            p["errors"].append("write_mode can't change on the same sink (an append sink is a change log, a merge "
                               "sink the current rows): point the row at a new sink in the same edit")
        if incremental and reasons:
            p["reseed"] = reasons[0]
        if "load_type" in p["changes"] or ("cursor_column" in p["changes"] and lt == "cursor"):
            p["notes"].append("the load strategy changes: the next run reseeds the table")
        if "select_list" in p["changes"]:
            old_cols = {c.strip().lower() for c in str(row.get("select_list") or "").split(",") if c.strip()}
            new_cols = {c.strip().lower() for c in str(merged.get("select_list") or "").split(",") if c.strip()}
            if new_cols - old_cols:
                p["notes"].append(f"new columns {', '.join(sorted(new_cols - old_cols))}: the next run reseeds to backfill them")
            if old_cols - new_cols:
                p["notes"].append(f"removed columns {', '.join(sorted(old_cols - new_cols))} stay in the sink and stop updating")
        if any(f in p["changes"] for f in ("sink_catalog", "sink_schema", "sink_table")):
            old_sink = ".".join(str(row.get(k)) for k in ("sink_catalog", "sink_schema", "sink_table"))
            p["notes"].append(f"the old sink {old_sink} is left in place; drop it once the new one is verified")
        if "client_id" in p["changes"]:
            p["notes"].append("the rows already written under the old client_id stay in the sink; delete them separately")
        if "use_remote_query" in p["changes"]:
            p["notes"].append("Spark JDBC and remote_query map source types differently: keep each sink's rows on "
                              "one of the two (a new sink is safest)")
        if "client_attrs" in p["changes"]:
            p["notes"].append("rows already written keep their client_attrs values; columns added now are NULL on them")
        if "task_collection" in p["changes"]:
            p["notes"].append(f"moves to ingestion group {merged['task_collection']}: that group's runs and schedule apply")
        if p["reseed"]:
            p["notes"].insert(0, f"reseed required ({p['reseed']}): the table is copied again on its next run")
        plans.append(p)
    return plans


def control_rows_for_update_sql() -> str:
    cols = ", ".join(("id",) + CONTROL_EDIT_COLUMNS)
    return f"select {cols} from {SCHEMA}.control where id = any(%s) order by id for update"


def control_groups_sql() -> str:
    """The groups of some control rows, read without locking them (for the permission check before the locks).
    Param: ids (list)."""
    return f"select distinct task_collection from {SCHEMA}.control where id = any(%s)"


def control_row_sql() -> str:
    cols = ", ".join(("id",) + CONTROL_EDIT_COLUMNS)
    return f"select {cols} from {SCHEMA}.control where id = %s"


def collection_control_rows_sql() -> str:
    """Every control row of one ingestion group, enabled or not, oldest first: what its settings are read from
    (config_import.group_settings) and what its export writes. Param: task_collection."""
    cols = ", ".join(("id",) + CONTROL_EDIT_COLUMNS)
    return f"select {cols} from {SCHEMA}.control where task_collection = %s order by id"


def groups_control_rows_sql(for_update: bool = False) -> str:
    """Every control row of the given ingestion groups, enabled or not, oldest first: an ingestion job's rows, which
    its settings are read from (job_settings.read). Locked for update when an edit applies. Param: list of groups."""
    cols = ", ".join(("id",) + CONTROL_EDIT_COLUMNS)
    return (f"select {cols} from {SCHEMA}.control where task_collection = any(%s) order by id"
            + (" for update" if for_update else ""))


# An ingestion job's tables as a tree: its servers, a server's databases, a database's tables. Each level is
# searched and paged in SQL, so a job of millions of tables never comes back whole. Aliases t, cp as STATE_COUNTS.
_JOB_ROWS = (f"from {SCHEMA}.control t left join {SCHEMA}.checkpoint cp on cp.control_id = t.id {DATABASE_CHECK_JOIN} "
             "where t.task_collection = any(%s)")


def groups_row_count_sql() -> str:
    """How many control rows the given groups have. Param: list of groups."""
    return f"select count(*) as n from {SCHEMA}.control where task_collection = any(%s)"


def job_servers_sql() -> str:
    """Params: groups, search pattern (ilike), limit, offset. Each row carries the level's total."""
    return f"""
select {SERVER} as server, min(t.src_connection) as src_connection, min(lower(t.src_host)) as src_host,
       count(distinct coalesce(t.src_database, '')) as databases, count(*) as tables,
       count(*) filter (where t.task_enabled) as enabled_tables, {STATE_COUNTS}, count(*) over () as total
{_JOB_ROWS} and {SERVER} ilike %s
group by 1 order by 1 limit %s offset %s"""


def job_databases_sql() -> str:
    """Params: groups (for the job's table count), groups, server, search pattern, limit, offset. ``missing`` is
    how many of the job's tables the database lacks."""
    return f"""
with job_tables as (
    select count(distinct (lower(src_schema), lower(src_table))) as n from {SCHEMA}.control where task_collection = any(%s)
)
select coalesce(t.src_database, '') as database, min(t.client_id) as client_id, count(*) as tables,
       count(*) filter (where t.task_enabled) as enabled_tables, {STATE_COUNTS},
       (select n from job_tables) - count(*) as missing, count(*) over () as total
{_JOB_ROWS} and {SERVER} = %s and coalesce(t.src_database, '') ilike %s
group by 1 order by 1 limit %s offset %s"""


def job_tables_sql() -> str:
    """One database's tables of a job, each with its last success counting its database's check stamp
    (LAST_SUCCESS), as the rest of the app shows it. Params: groups, server, database, search pattern, limit, offset."""
    return f"""
select t.id, t.src_schema as schema, t.src_table as "table", t.load_type, t.primary_key, t.select_list,
       t.sink_catalog || '.' || t.sink_schema || '.' || t.sink_table as target, t.task_enabled as enabled,
       t.task_collection, cp.status, cp.phase, max({LAST_SUCCESS}) as last_success_at, {STATE_COUNTS},
       count(*) over () as total
{_JOB_ROWS} and {SERVER} = %s and coalesce(t.src_database, '') = %s
  and (t.src_schema || '.' || t.src_table) ilike %s
group by t.id, cp.control_id order by lower(t.src_schema), lower(t.src_table) limit %s offset %s"""


_TARGET = "t.sink_catalog || '.' || t.sink_schema || '.' || t.sink_table"
# Rows the run records say each table wrote over the last 7 days: its ok events' merged or appended rows.
_WRITTEN_7D = (f"(select e.control_id, sum(e.rows_merged) as rows_written, max(e.finished_at) as last_written "
               f"from {SCHEMA}.ingest_event e where e.task_collection = any(%s) and e.status = 'ok' "
               f"and e.finished_at >= now() - interval '7 days' group by e.control_id)")
# A job's rows (alias t) with their checkpoints, database stamps and rows written ("written"), before the WHERE.
_JOB_ROWS_WRITTEN = (f"from {SCHEMA}.control t left join {SCHEMA}.checkpoint cp on cp.control_id = t.id "
                     f"{DATABASE_CHECK_JOIN} left join written w on w.control_id = t.id")


def job_targets_sql() -> str:
    """A job's target tables, one row each: how many of its source tables write it (and how many outside the job),
    from how many databases, their states now, and the rows written over 7 days by the run records; fan_in_total
    counts the matching targets written from more than one source table. Exact row counts would need a SQL warehouse,
    which the app avoids. Params: groups, groups, groups, search pattern, limit, offset."""
    return f"""
with written as {_WRITTEN_7D},
outside as (
  select o.sink_catalog || '.' || o.sink_schema || '.' || o.sink_table as target, count(*) as n
  from {SCHEMA}.control o where not (o.task_collection = any(%s)) group by 1)
select {_TARGET} as target, t.sink_catalog as catalog, t.sink_schema as schema, t.sink_table as "table",
       count(*) as sources, count(distinct ({SERVER}, coalesce(t.src_database, ''))) as databases,
       count(*) filter (where t.client_id is not null) as with_client_id,
       coalesce(max(x.n), 0) as outside_sources, {STATE_COUNTS},
       sum(w.rows_written) as rows_written_7d, max(w.last_written) as last_written, count(*) over () as total,
       sum(case when count(*) + coalesce(max(x.n), 0) > 1 then 1 else 0 end) over () as fan_in_total
{_JOB_ROWS_WRITTEN}
left join outside x on x.target = {_TARGET}
where t.task_collection = any(%s) and {_TARGET} ilike %s
group by t.sink_catalog, t.sink_schema, t.sink_table
order by lower({_TARGET}) limit %s offset %s"""


def job_target_sources_sql() -> str:
    """The source tables of a job that write one target, with each one's state and its rows written over 7 days.
    Params: groups, groups, catalog, schema, table, limit, offset."""
    return f"""
with written as {_WRITTEN_7D}
select t.id, {SERVER} as server, t.src_database as database, t.src_schema as schema, t.src_table as "table",
       t.client_id, t.task_collection, t.task_enabled as enabled, cp.status, cp.phase,
       max({LAST_SUCCESS}) as last_success_at, {STATE_COUNTS}, max(w.rows_written) as rows_written_7d, max(w.last_written) as last_written,
       count(*) over () as total
{_JOB_ROWS_WRITTEN}
where t.task_collection = any(%s) and t.sink_catalog = %s and t.sink_schema = %s and t.sink_table = %s
group by t.id, cp.control_id
order by lower({SERVER}), lower(coalesce(t.src_database, '')), lower(t.src_schema), lower(t.src_table)
limit %s offset %s"""


def job_row_ids_sql() -> str:
    """The ids of a job's rows on one server, optionally one database and one table (schema and table; blank =
    all). Params: groups, server ('' = every server), database, schema, table."""
    return f"""
select t.id from {SCHEMA}.control t where t.task_collection = any(%s)
  and (%s = '' or {SERVER} = %s) and (%s = '' or coalesce(t.src_database, '') = %s)
  and (%s = '' or lower(t.src_schema) = lower(%s)) and (%s = '' or lower(t.src_table) = lower(%s))
order by t.id"""


def update_control_sql(fields: List[str]) -> str:
    """UPDATE of whitelisted columns only (field names come from CONTROL_EDIT_COLUMNS)."""
    assert fields and all(f in CONTROL_EDIT_COLUMNS for f in fields), fields
    return f"update {SCHEMA}.control set " + ", ".join(f"{f} = %s" for f in fields) + " where id = %s"


# The kept target tables the Delete dialog names (the rest are counted).
DELETE_KEPT_LISTED = 20


def collection_lock_sql() -> str:
    """Take a collection's run lock for this transaction (the same key the ingestion job holds for a
    whole run). False = a run of that collection is in progress. Param: lock key."""
    return "select pg_try_advisory_xact_lock(hashtextextended(%s, 0))"


def collection_lock_key(collection: str) -> str:
    return "lakefed_ingest:" + str(collection)   # state_store.LOCK_NAMESPACE + collection


def flip_to_seeding_sql() -> str:
    """Params: detail, control ids."""
    return f"update {SCHEMA}.checkpoint set {_RESEED_SET}, detail = %s where control_id = any(%s)"


def mixed_sinks_for_sql() -> str:
    """Sinks among the given (lower-cased catalog.schema.table) list claimed by a dedicated row and by
    other rows. Param: list of sink names."""
    return (f"select lower(sink_catalog || '.' || sink_schema || '.' || sink_table) as sink from {SCHEMA}.control "
            f"where lower(sink_catalog || '.' || sink_schema || '.' || sink_table) = any(%s) group by 1 "
            f"having count(*) filter (where client_id is null) > 0 and count(*) > 1")


def database_source(row: dict) -> str:
    """``database_source_sql`` of a control-row dict (the engine's ``state_store.database_source``)."""
    host = str(row.get("src_host") or row.get("src_connection") or "").lower()
    credential = row.get("secret_key") or row.get("src_connection") or ""
    port = int(row.get("src_port") or 0)
    if not port and (row.get("src_host") or not row.get("src_connection")):
        port = DEFAULT_PORTS.get((str(row.get("src_type") or "").strip(" ").lower()) or "sqlserver", 0)
    return f"{host}|{port}|{credential}"


def database_key(row: dict) -> str:
    """One source database (its server identity and name) as one text, the key ``split_databases_sql`` takes."""
    return f"{database_source(row)}\x1f{row.get('src_database') or ''}"


def split_databases_sql() -> str:
    """Databases among the given keys (``database_key``) whose control rows are in more than one
    collection. A database's tables share one collection: its run takes the collection's lock and the
    check job reads the database once for the collection. Param: list of keys. Returns key, database,
    collections. The database names (each key's last part) narrow the rows through control_database_idx."""
    key = f"{database_source_sql('t')} || chr(31) || coalesce(t.src_database, '')"
    return (f"with k as (select unnest(%s::text[]) as key) "
            f"select {key} as key, min(t.src_database) as src_database, "
            f"array_agg(distinct t.task_collection order by t.task_collection) as collections "
            f"from {SCHEMA}.control t where t.src_database in (select split_part(key, chr(31), -1) from k) "
            f"and {key} in (select key from k) group by 1 having count(distinct t.task_collection) > 1")


def log_change_sql() -> str:
    """Params: changed_by, action, control_id, task_collection, field, old_value, new_value,
    reseed_reason, batch_id."""
    return (f"insert into {SCHEMA}.control_change (changed_by, action, control_id, task_collection, field, "
            f"old_value, new_value, reseed_reason, batch_id) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)")


def changes_sql(control_id: Optional[int], limit: int) -> Tuple[str, tuple]:
    where, params = "", ()
    if control_id is not None:
        where, params = "where control_id = %s ", (int(control_id),)
    return (f"select changed_at, changed_by, action, control_id, task_collection, field, old_value, new_value, "
            f"reseed_reason, batch_id from {SCHEMA}.control_change {where}order by changed_at desc, id desc limit %s",
            params + (int(limit),))


def checkpoint_for_sql() -> str:
    """One table's checkpoint, its last success counting its database's check stamp (LAST_SUCCESS).
    Param: control_id."""
    return (f"select cp.phase, cp.status, cp.position, cp.strategy, cp.detail, {LAST_SUCCESS} as last_success_at, "
            f"cp.updated_at, cp.source_identity, cp.sink_identity, cp.merge_key from {SCHEMA}.checkpoint cp "
            f"join {SCHEMA}.control t on t.id = cp.control_id {DATABASE_CHECK_JOIN} where cp.control_id = %s")


# ---- schedules ----------------------------------------------------------------------------------

SCHEDULE_FIELDS = ("name", "cadence_minutes", "timezone", "parameters", "job_variant", "stagger", "paused",
                   "notify_emails", "options")
JSON_SCHEDULE_FIELDS = ("parameters", "options")


def schedules_sql() -> str:
    """Active schedules, by name."""
    return (f"select id, {', '.join(SCHEDULE_FIELDS)}, created_by, created_at, updated_at, deleted_at "
            f"from {SCHEMA}.schedule where deleted_at is null order by lower(name)")


def set_schedule_option_sql() -> str:
    """Set one key of a schedule's options to a number. Params: key (as a one-element text array), value, id."""
    return (f"update {SCHEMA}.schedule set options = jsonb_set(coalesce(options, '{{}}'::jsonb), %s::text[], "
            f"to_jsonb(%s::bigint)) where id = %s")


def clear_schedule_option_sql() -> str:
    """Remove one key from a schedule's options. Params: key, id."""
    return f"update {SCHEMA}.schedule set options = options - %s where id = %s"


def claim_rediscovery_sql() -> str:
    """Claim a job's re-discovery: its record becomes running, unless a run holds it (a running record younger than
    the stale limit). Returns the id when claimed. Params: the running record as JSON, id, stale seconds."""
    return (f"update {SCHEMA}.schedule set options = jsonb_set(coalesce(options, '{{}}'::jsonb), '{{rediscovery}}', "
            f"%s::jsonb) where id = %s and deleted_at is null and (options->'rediscovery' is null "
            f"or options->'rediscovery'->>'status' is distinct from 'running' "
            f"or coalesce((options->'rediscovery'->>'started_at')::timestamptz, 'epoch'::timestamptz) "
            f"< now() - make_interval(secs => %s)) returning id")


def set_rediscovery_sql() -> str:
    """Write a job's re-discovery record (options.rediscovery). Params: the record as JSON, id."""
    return (f"update {SCHEMA}.schedule set options = jsonb_set(coalesce(options, '{{}}'::jsonb), '{{rediscovery}}', "
            f"%s::jsonb) where id = %s")


def selection_discoveries_sql() -> str:
    """The discovery rows a selection was made from (one per database read): the server's connection or host and port,
    its secrets, its listing and when each was read. Param: list of discovery ids."""
    return (f"select lower(coalesce(src_connection, src_host)) as server, src_type, src_connection, src_host, src_port, "
            f"secret_key, src_database, listing, finished_at from {SCHEMA}.discovery where discovery_id = any(%s)")


def discovery_statuses_sql() -> str:
    """Each database a discovery read, with its status and error (the master row when a server couldn't be listed).
    Param: list of discovery ids."""
    return (f"select lower(coalesce(src_connection, src_host)) as server, src_database, status, error "
            f"from {SCHEMA}.discovery where discovery_id = any(%s)")


def schedule_sql() -> str:
    """One schedule (deleted or not). Param: id."""
    return (f"select id, {', '.join(SCHEDULE_FIELDS)}, created_by, created_at, updated_at, deleted_at "
            f"from {SCHEMA}.schedule where id = %s")


def schedule_members_sql() -> str:
    """Members of the given schedules, active and removed. ``delete_pending``: removed, and its job not
    deleted yet (no successful sync since the removal). Param: list of schedule ids."""
    return (f"select id, schedule_id, task_collection, offset_minutes, job_id, synced_at, sync_error, added_at, removed_at, "
            f"(removed_at is not null and job_id is not null and (synced_at is null or synced_at < removed_at)) "
            f"as delete_pending "
            f"from {SCHEMA}.schedule_member where schedule_id = any(%s) order by schedule_id, task_collection")


# Syncs of one schedule are serialized: a second one waits, then sees the job ids the first recorded
# (two concurrent syncs would each create a job for an unsynced member). Two int keys, a class of its
# own: separate from the collection run locks (one key) and the job's other two-key classes.
SCHEDULE_LOCK_CLASS = 0x6C667364


def schedule_lock_sql() -> str:
    """Wait for, and take, a schedule's sync lock for this transaction. Params: class, schedule key."""
    return "select pg_advisory_xact_lock(%s::int, %s::int)"


def schedule_lock_key(schedule_id: int) -> int:
    return int(schedule_id) & 0x7FFFFFFF


def insert_schedule_sql() -> str:
    cols = SCHEDULE_FIELDS + ("created_by",)
    vals = ", ".join("%s::jsonb" if c in JSON_SCHEDULE_FIELDS else "%s" for c in cols)
    return f"insert into {SCHEMA}.schedule ({', '.join(cols)}) values ({vals}) returning id"


_STAMP = (f"update {SCHEMA}.schedule set updated_at = now(), "
          f"options = jsonb_set(coalesce(options, '{{}}'::jsonb), '{{updated_by}}', to_jsonb(%s::text)) ")


def touch_jobs_sql() -> str:
    """Record who last changed these ingestion jobs, and when (options.updated_by, updated_at). Params: user, ids."""
    return _STAMP + "where id = any(%s) and deleted_at is null"


def touch_group_jobs_sql() -> str:
    """The same for every ingestion job holding one of these groups, when their tables change. Params: user, groups."""
    return (_STAMP + f"where deleted_at is null and id in (select schedule_id from {SCHEMA}.schedule_member "
            "where task_collection = any(%s) and removed_at is null)")


def update_schedule_sql(fields: List[str]) -> str:
    bad = [f for f in fields if f not in SCHEDULE_FIELDS]
    if bad or not fields:
        raise ValueError(f"not schedule fields: {bad}")
    sets = ", ".join(f"{f} = %s::jsonb" if f in JSON_SCHEDULE_FIELDS else f"{f} = %s" for f in fields)
    return f"update {SCHEMA}.schedule set {sets}, updated_at = now() where id = %s and deleted_at is null"


def delete_schedule_sql() -> str:
    """Soft delete (the app has no DELETE privilege). Param: id."""
    return f"update {SCHEMA}.schedule set deleted_at = now(), updated_at = now() where id = %s and deleted_at is null"


def insert_member_sql() -> str:
    return (f"insert into {SCHEMA}.schedule_member (schedule_id, task_collection, offset_minutes) "
            f"values (%s, %s, %s) returning id")


def update_member_offset_sql() -> str:
    return f"update {SCHEMA}.schedule_member set offset_minutes = %s where id = %s"


def remove_members_sql() -> str:
    """Soft-remove members (their jobs are deleted by the next sync). Param: list of member ids."""
    return f"update {SCHEMA}.schedule_member set removed_at = now() where id = any(%s) and removed_at is null"


def set_schedule_flag_sql() -> str:
    """Set one key of a schedule's options to true or false. Params: key (as a one-element text array), value, id."""
    return (f"update {SCHEMA}.schedule set options = jsonb_set(coalesce(options, '{{}}'::jsonb), %s::text[], "
            f"to_jsonb(%s::boolean)) where id = %s")


def delete_groups_sql() -> str:
    """The groups a job's Delete removes: every member it had, removed or not, that no other live job holds (the
    delete job's delete_job.groups_sql, read the same way). Params: schedule id, schedule id."""
    return (f"select distinct m.task_collection from {SCHEMA}.schedule_member m where m.schedule_id = %s "
            f"and not exists (select 1 from {SCHEMA}.schedule_member o join {SCHEMA}.schedule s on s.id = o.schedule_id "
            f"where o.task_collection = m.task_collection and o.removed_at is null and s.deleted_at is null "
            f"and o.schedule_id <> %s) order by 1")


def delete_targets_sql() -> str:
    """The groups' tables and target tables, for the Delete dialog: how many tables (control rows), how many target
    tables only they write, and the target tables rows of any other group also write (kept; names compared without
    case, as delete_job.targets_sql does), at most ``DELETE_KEPT_LISTED`` of them by name. Params: groups, groups,
    groups."""
    return (f"with mine as (select distinct sink_catalog, sink_schema, sink_table from {SCHEMA}.control "
            f"where task_collection = any(%s)), "
            f"theirs as (select distinct lower(sink_catalog) c, lower(sink_schema) s, lower(sink_table) t "
            f"from {SCHEMA}.control where not (task_collection = any(%s))), "
            f"marked as (select m.*, exists (select 1 from theirs where c = lower(m.sink_catalog) "
            f"and s = lower(m.sink_schema) and t = lower(m.sink_table)) as shared from mine m) "
            f"select (select count(*) from {SCHEMA}.control where task_collection = any(%s)) as tables, "
            f"(select count(*) from marked where not shared) as targets, "
            f"(select count(*) from marked where shared) as kept, "
            f"(select coalesce(json_agg(k order by k), '[]'::json) from (select sink_catalog || '.' || sink_schema "
            f"|| '.' || sink_table as k from marked where shared order by 1 limit {DELETE_KEPT_LISTED}) named) "
            f"as kept_names")


def deletions_sql() -> str:
    """The latest deleted jobs whose Delete started the delete job (``options.delete_drop_targets`` set), newest
    first, for the Ingestion jobs page. Param: how many."""
    return (f"select id, name, options, deleted_at from {SCHEMA}.schedule where deleted_at is not null "
            f"and options ? 'delete_drop_targets' order by deleted_at desc limit %s")


def member_synced_sql() -> str:
    """Record a successful sync. Params: job_id, member id."""
    return f"update {SCHEMA}.schedule_member set job_id = %s, sync_error = null, synced_at = now() where id = %s"


def member_sync_failed_sql() -> str:
    """Record a failed sync, keeping synced_at (a removed member's job delete stays pending and is retried).
    Params: sync_error, member id."""
    return f"update {SCHEMA}.schedule_member set sync_error = %s where id = %s"


def collection_run_stats_sql() -> str:
    """Per collection: the latest run, run-time percentiles over 7 days, runs and failures in 24 hours,
    and the strictest freshness SLA among its enabled tables. Param: list of collections (4 times)."""
    return f"""
with r as (
    select task_collection, status, started_at, finished_at,
           extract(epoch from (finished_at - started_at)) as dur_s
    from {SCHEMA}.ingest_run
    where task_collection = any(%s) and started_at > now() - interval '7 days' and {not_a_check()}
),
agg as (
    select task_collection,
           round(percentile_cont(0.95) within group (order by dur_s) filter (where finished_at is not null and status <> 'skipped')::numeric) as p95_s,
           round(percentile_cont(0.5) within group (order by dur_s) filter (where finished_at is not null and status <> 'skipped')::numeric) as p50_s,
           count(*) filter (where started_at > now() - interval '24 hours')                      as runs_24h,
           count(*) filter (where started_at > now() - interval '24 hours' and status = 'failed') as failed_24h,
           count(*) filter (where started_at > now() - interval '24 hours' and status = 'skipped') as lock_skipped_24h
    from r group by task_collection
),
last as (
    select distinct on (task_collection) task_collection, run_id as last_run_id, status as last_status,
           started_at as last_started, finished_at as last_finished,
           round(extract(epoch from (coalesce(finished_at, now()) - started_at)))::int as last_duration_s
    from {SCHEMA}.ingest_run where task_collection = any(%s) and {not_a_check()}
    order by task_collection, started_at desc
),
sla as (
    select task_collection, min(freshness_sla_minutes) as min_sla_minutes, count(*) as enabled_tables
    from {SCHEMA}.control where task_enabled and task_collection = any(%s) group by task_collection
)
select c.task_collection, l.last_run_id, l.last_status, l.last_started, l.last_finished, l.last_duration_s,
       a.p50_s, a.p95_s, coalesce(a.runs_24h, 0) as runs_24h, coalesce(a.failed_24h, 0) as failed_24h,
       coalesce(a.lock_skipped_24h, 0) as lock_skipped_24h, s.min_sla_minutes, coalesce(s.enabled_tables, 0) as enabled_tables
from unnest(%s::text[]) as c(task_collection)
left join last l on l.task_collection = c.task_collection
left join agg a on a.task_collection = c.task_collection
left join sla s on s.task_collection = c.task_collection
order by c.task_collection"""


def check_work_found_sql() -> str:
    """What some checks found to load: per group, the databases with changes each check listed for its run.
    Param: work ids (list)."""
    return (f"select task_collection, count(*) as databases from {SCHEMA}.check_work where work_id = any(%s) "
            f"group by task_collection")


def database_checks_since_sql() -> str:
    """How many databases of some groups a check found unchanged and stamped since a time. Params: groups (list),
    since (timestamptz)."""
    return (f"select count(*) as databases from {SCHEMA}.database_check where task_collection = any(%s) "
            f"and checked_at >= %s")


# ---- uploads and selections by reference ----------------------------------------------------------------------------
# New and Add tables never hold a whole server's tables in the browser: an uploaded CSV is kept in table_upload_line,
# a discovery's tables are in discovered_table, and the pages show their servers and databases, page their tables, and
# name what to take (a selection) instead of sending the tables back.

UPLOAD_LINE_COLUMNS = ("upload_id", "line", "server", "src_database", "src_schema", "src_table", "load_type", "fields",
                       "status", "errors")
JOB_SELECTION_COLUMNS = ("job_id", "scope", "server", "src_database", "tables", "filter", "source_kind", "source_id",
                         "created_by")


def insert_upload_sql() -> str:
    """An upload's header row. Params: upload_id, file_name, uploaded_by, columns, lines, tables, invalid."""
    return (f"insert into {SCHEMA}.table_upload (upload_id, file_name, uploaded_by, columns, lines, tables, invalid) "
            f"values (%s, %s, %s, %s, %s, %s, %s)")


def insert_upload_lines_sql(n: int) -> str:
    """n lines of an upload in one statement. Params: each line's UPLOAD_LINE_COLUMNS in turn (fields as JSON text)."""
    one = "(%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)"
    return f"insert into {SCHEMA}.table_upload_line ({', '.join(UPLOAD_LINE_COLUMNS)}) values {', '.join([one] * n)}"


def upload_sql() -> str:
    """An upload's header row. Param: upload_id."""
    return (f"select upload_id, file_name, uploaded_by, uploaded_at, columns, lines, tables, invalid "
            f"from {SCHEMA}.table_upload where upload_id = %s")


def upload_invalid_sql() -> str:
    """An upload's first invalid lines, for the page to say why. Params: upload_id, limit."""
    return (f"select line, concat_ws('.', src_database, src_schema, src_table) as table, errors "
            f"from {SCHEMA}.table_upload_line where upload_id = %s and status = 'invalid' order by line limit %s")


def source_rows_sql(kind: str) -> str:
    """The tables of an upload (its ok lines; param upload_id) or of discoveries (param: list of discovery ids; a
    table found by more than one, its latest finding), each with its server, database, schema, table, load type, and
    what the CSV a plan reads takes from it: for an upload its line and fields, for a discovery its settings. The
    subquery the views and the selection read from."""
    if kind == "upload":
        return (f"select server, src_database, src_schema, src_table, load_type, null::text as cursor_column, "
                f"fields->>'primary_key' as primary_key, null::bigint as row_count, null::text as notes, line, fields "
                f"from {SCHEMA}.table_upload_line where upload_id = %s and status = 'ok'")
    return (f"select distinct on (lower(coalesce(d.src_connection, d.src_host)), lower(t.src_database), "
            f"lower(t.src_schema), lower(t.src_table)) "
            f"lower(coalesce(d.src_connection, d.src_host)) as server, t.src_database, t.src_schema, t.src_table, "
            f"t.load_type, t.cursor_column, t.primary_key, t.row_count, t.notes, 0 as line, "
            f"jsonb_build_object('src_connection', d.src_connection, "
            f"'src_host', case when d.src_connection is null then d.src_host end, "
            f"'src_port', case when d.src_connection is null then d.src_port::text end, "
            f"'secret_key', case when d.src_connection is null then d.secret_key end, "
            f"'src_database', t.src_database, 'src_schema', t.src_schema, 'src_table', t.src_table, "
            f"'select_list', t.select_list, 'primary_key', t.primary_key, 'cursor_column', t.cursor_column, "
            f"'load_type', t.load_type) as fields "
            f"from {SCHEMA}.discovered_table t join {SCHEMA}.discovery d "
            f"on d.discovery_id = t.discovery_id and d.src_database = t.src_database "
            f"where t.discovery_id = any(%s) "
            f"order by lower(coalesce(d.src_connection, d.src_host)), lower(t.src_database), lower(t.src_schema), "
            f"lower(t.src_table), t.discovered_at desc")


_LOAD_TYPE_COUNTS = ("count(*) filter (where load_type = 'ct') as ct, "
                     "count(*) filter (where load_type = 'cursor') as cursor, "
                     "count(*) filter (where load_type = 'full') as full, "
                     "count(*) filter (where coalesce(load_type, '') not in ('ct', 'cursor', 'full')) as unset")
_MATCHES = "position(lower(%s) in lower(r.src_database)) > 0"


def source_servers_sql(kind: str) -> str:
    """A source's servers: each one's databases, tables and tables by load type, over the databases matching the
    filter. Params: the source's (see source_rows_sql), filter."""
    return (f"select r.server, count(distinct lower(r.src_database)) as databases, count(*) as tables, "
            f"{_LOAD_TYPE_COUNTS} from ({source_rows_sql(kind)}) r where {_MATCHES} group by r.server order by r.server")


def source_databases_sql(kind: str) -> str:
    """One page of a source's databases matching the filter, by server and name: each one's tables and tables by load
    type. Params: the source's, filter, limit, offset."""
    return (f"select r.server, r.src_database, count(*) as tables, {_LOAD_TYPE_COUNTS} "
            f"from ({source_rows_sql(kind)}) r where {_MATCHES} group by r.server, r.src_database "
            f"order by r.server, lower(r.src_database) limit %s offset %s")


def source_tables_sql(kind: str) -> str:
    """One page of a database's tables in a source, by schema and name, those matching the search (in schema.table).
    Params: the source's, server, database, search, limit, offset. Each row carries the page's total as total."""
    return (f"select r.src_schema, r.src_table, r.load_type, r.cursor_column, r.primary_key, r.row_count, r.notes, "
            f"count(*) over () as total from ({source_rows_sql(kind)}) r "
            f"where lower(r.server) = lower(%s) and r.src_database = %s "
            f"and position(lower(%s) in lower(r.src_schema || '.' || r.src_table)) > 0 "
            f"order by lower(r.src_schema), lower(r.src_table) limit %s offset %s")


def selection_key(server: str, database: str, table: str = "") -> str:
    """A database (or a table of it, schema.table) as selected_rows_sql matches it: lower case, chr(31) between."""
    return "\x1f".join(str(part or "").lower() for part in ((server, database, table) if table else (server, database)))


_SELECTED = ("(%s and position(lower(%s) in lower(r.src_database)) > 0) "
             "or lower(r.server) || chr(31) || lower(r.src_database) = any(%s) "
             "or lower(r.server) || chr(31) || lower(r.src_database) || chr(31) || lower(r.src_schema || '.' || r.src_table) "
             "= any(%s)")


def selected_rows_sql(kind: str, count: bool = False) -> str:
    """The tables a selection takes from a source: with whole_server, every table of a database matching the filter;
    the whole databases named; the tables named (selection_key). Params: the source's, whole_server, filter, database
    keys, table keys. In the order a plan reads them; count gives only how many."""
    if count:
        return f"select count(*) as n from ({source_rows_sql(kind)}) r where {_SELECTED}"
    return (f"select r.server, r.src_database, r.src_schema, r.src_table, r.line, r.fields "
            f"from ({source_rows_sql(kind)}) r where {_SELECTED} "
            f"order by r.server, lower(r.src_database), r.line, lower(r.src_schema), lower(r.src_table)")


def insert_job_selection_sql(n: int) -> str:
    """n parts of what a job was made from, in one statement. Params: each part's JOB_SELECTION_COLUMNS in turn."""
    one = "(" + ", ".join(["%s"] * len(JOB_SELECTION_COLUMNS)) + ")"
    return f"insert into {SCHEMA}.job_selection ({', '.join(JOB_SELECTION_COLUMNS)}) values {', '.join([one] * n)}"


def job_selection_sql() -> str:
    """What a job was made from, part by part, oldest first. Param: job_id."""
    return (f"select id, {', '.join(JOB_SELECTION_COLUMNS)}, created_at from {SCHEMA}.job_selection "
            f"where job_id = %s order by created_at, id")
