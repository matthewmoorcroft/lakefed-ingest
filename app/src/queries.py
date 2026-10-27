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


# ---- freshness SLA ------------------------------------------------------------------------------

# An enabled table with control.freshness_sla_minutes breaches its SLA when its last success (or, if
# it has never succeeded, its control row's creation) is older than the SLA. Tables without an SLA
# are never flagged. Aliases: t = control, cp = checkpoint.
SLA_BREACHED = ("(t.task_enabled and t.freshness_sla_minutes is not null "
                "and coalesce(cp.last_success_at, t.created_at) < now() - make_interval(mins => t.freshness_sla_minutes))")


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
       min(cp.last_success_at) filter (where t.task_enabled) as oldest_success_at,
       max(cp.last_success_at) filter (where t.task_enabled) as newest_success_at,
       max(cp.last_success_at) as newest_any_success_at
from {SCHEMA}.control t
left join {SCHEMA}.checkpoint cp on cp.control_id = t.id
group by 1, 2, 3, 4, 5, 6"""


def last_runs_sql() -> str:
    """Each ingestion group's latest run."""
    return f"""
select distinct on (task_collection) task_collection, run_id, status, started_at, finished_at,
       total, ok, failed, skipped, detail
from {SCHEMA}.ingest_run
order by task_collection, started_at desc"""


def running_progress_sql() -> str:
    """Every running group run's progress in one pass: its group's enabled tables and how many it has
    checked (stamped since it started, on the database's clock like started_at), its table events so far
    (unchanged tables are stamped in bulk without events), the loads that finished in the last 5 minutes
    (live speed and time left), and its group's 7-day median and p95 run time. A run another run closed
    never recorded its totals (its finish is the closing time), so it is left out of the median and p95.
    Skipped events are reseeds queued: done for this run, though not stamped as checked."""
    return f"""
with r as (
    select run_id, task_collection, job_run_id, cluster_id, parallelism, started_at
    from {SCHEMA}.ingest_run where status = 'running'
),
tables as (
    select r.run_id,
           count(*) filter (where t.task_enabled) as enabled_tables,
           count(*) filter (where t.task_enabled and cp.last_success_at >= r.started_at) as tables_checked
    from r join {SCHEMA}.control t on t.task_collection = r.task_collection
    left join {SCHEMA}.checkpoint cp on cp.control_id = t.id
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
       coalesce(tb.enabled_tables, 0) as enabled_tables, coalesce(tb.tables_checked, 0) as tables_checked,
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
      and x.total is not null and x.started_at > now() - interval '7 days') p on true
order by r.started_at desc"""


def group_runs_sql() -> str:
    """The runs of some ingestion groups that started in a window, oldest first (the group runs the app
    places into job runs). Params: groups, window start, window end."""
    return (f"select run_id, task_collection, status, started_at, finished_at, total, ok, failed, skipped, "
            f"reseeded, job_run_id, cluster_id, parallelism, detail "
            f"from {SCHEMA}.ingest_run where task_collection = any(%s) and started_at >= %s and started_at < %s "
            f"order by started_at")


def run_work_sql() -> str:
    """Per group run: its work span (first table start to last table finish), tables loaded and failed,
    and rows written (uncounted: loads with no count). Param: array of run ids."""
    return (f"select e.run_id, min(e.started_at) as work_started_at, max(e.finished_at) as work_finished_at, "
            f"count(*) filter (where e.status = 'ok') as loaded, "
            f"count(*) filter (where e.status = 'failed') as failed_tables, "
            f"sum(e.rows_merged) as rows_written, "
            f"count(*) filter (where e.status = 'ok' and e.rows_merged is null) as uncounted "
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
           max(cp.last_success_at)                                                  as last_success_at,
           count(distinct {DATABASE}) filter (where t.task_enabled)               as databases,
           count(distinct {SERVER}) filter (where t.task_enabled)                  as servers,
           min({SERVER}) filter (where t.task_enabled)                             as server,
           {STATE_COUNTS}
    from {SCHEMA}.control t
    left join {SCHEMA}.checkpoint cp on cp.control_id = t.id
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

    tables_checked out of enabled_tables is the run's progress over the whole collection: the engine
    stamps checkpoint.last_success_at on every unchanged table once change detection ends, and on every
    loaded table as it finishes, both on the database's clock like started_at. last_activity_at is when
    a table of the run last started or finished, the sign that the run is alive."""
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
       (select count(*) from {SCHEMA}.control t
         where t.task_collection = r.task_collection and t.task_enabled)                              as enabled_tables,
       (select count(*) from {SCHEMA}.control t join {SCHEMA}.checkpoint cp on cp.control_id = t.id
         where t.task_collection = r.task_collection and t.task_enabled
           and cp.last_success_at >= r.started_at)                                           as tables_checked,
       (select max(coalesce(e.finished_at, e.started_at)) from {SCHEMA}.ingest_event e
         where e.run_id = r.run_id)                                                          as last_activity_at,
       (select min({SERVER}) from {SCHEMA}.control t where t.task_collection = r.task_collection)      as server
from {SCHEMA}.ingest_run r
where r.status = 'running'
order by r.started_at desc"""


# ---- runs / events ------------------------------------------------------------------------------

_RUN_COLUMNS = ("run_id, task_collection, status, started_at, finished_at, "
                "round(extract(epoch from (coalesce(finished_at, now()) - started_at))::numeric, 0) as duration_s, "
                "total, ok, failed, skipped, reseeded, job_run_id, cluster_id, parallelism, detail")


def runs_sql(collection: Optional[str], limit: int = 100) -> Tuple[str, tuple]:
    """Recent runs, optionally for one collection. Returns (sql, params)."""
    where, params = "", ()
    if collection:
        where, params = "where task_collection = %s ", (collection,)
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
       max(cp.last_success_at) filter (where t.task_enabled)        as newest_success_at,
       min(cp.last_success_at) filter (where t.task_enabled)        as oldest_success_at,
       count(distinct t.task_collection) filter (where exists (
           select 1 from {SCHEMA}.ingest_run r where r.task_collection = t.task_collection
           and r.status = 'running'))                               as running_collections
from {SCHEMA}.control t
left join {SCHEMA}.checkpoint cp on cp.control_id = t.id
group by 1
order by 1"""


def server_health(row: dict) -> str:
    """A server's one-word health from its enabled tables by state: "healthy" only when every table is
    current; otherwise the worst state any table is in (failing, then late, then first load), and
    "unknown" when the counts don't add up."""
    count = lambda k: int(row.get(k) or 0)
    if not count("tables"):
        return "no tables"
    for state, label in (("state_failing", "failing"), ("state_late", "late"), ("state_first_load", "first load")):
        if count(state):
            return label
    return "healthy" if count("state_current") == count("tables") else "unknown"


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
                        and cp.last_success_at >= (select started_at from run)) as checked_this_run,
       min(cp.last_success_at) filter (where t.task_enabled) as oldest_success_at,
       max(cp.last_success_at) filter (where t.task_enabled) as newest_success_at
from {SCHEMA}.control t
left join {SCHEMA}.checkpoint cp on cp.control_id = t.id
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


def last_ran_group_runs_sql() -> str:
    """Per membership of each job, its group's newest finished group run (not skipped, not running) while it was
    the member, with its schedule_id: where a job's last job run is found when none of its recent triggers ran.
    Within one member, start order is slot order, so the newest slot among these is the job's newest that ran.
    Param: schedule ids."""
    return f"""
select schedule_id, {_RUN_COLUMNS} from (
    select m.schedule_id, r.*,
           row_number() over (partition by m.id order by r.started_at desc) as newest
    from {SCHEMA}.ingest_run r
    join {SCHEMA}.schedule_member m on m.task_collection = r.task_collection
    where m.schedule_id = any(%s) and r.status not in ('skipped', 'running')
      and (m.added_at is null or r.started_at >= m.added_at) and (m.removed_at is null or r.started_at < m.removed_at)
) x
where newest = 1"""


def group_runs_in_windows_sql() -> str:
    """The runs of each (ingestion group, window start, window end) given, in one read: the job runs of several
    jobs, each around its own slot. Params: groups, starts, ends (arrays of one length)."""
    return (f"select {_RUN_COLUMNS} from {SCHEMA}.ingest_run r where exists ("
            f"select 1 from unnest(%s::text[], %s::timestamptz[], %s::timestamptz[]) as w(task_collection, starts, ends) "
            f"where w.task_collection = r.task_collection and r.started_at >= w.starts and r.started_at < w.ends) "
            f"order by started_at")    # exists: a run in two windows (overlapping, or a group in two jobs) comes once


def group_runs_before_sql() -> str:
    """The newest runs of some ingestion groups that started before a time, newest first. Params: groups,
    before, limit."""
    return (f"select {_RUN_COLUMNS} from {SCHEMA}.ingest_run "
            f"where task_collection = any(%s) and started_at < %s order by started_at desc limit %s")


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
    go, then by name. Params: group, server, database, run_id (or None), schema.table pattern, limit, offset."""
    return f"""
with t as ({_database_rows()}),
r as (select run_id, started_at from {SCHEMA}.ingest_run where run_id = %s)
select t.id, t.src_schema, t.src_table, t.load_type, t.task_enabled, cp.phase, cp.status, cp.last_success_at,
       round(extract(epoch from (now() - cp.last_success_at)) / 60)::int as lag_min,
       t.freshness_sla_minutes as sla_min, {SLA_BREACHED} as sla_breached,
       case when not t.task_enabled then 'disabled' when {_FAILING} then 'failing' when {_FIRST_LOAD} then 'first load'
            when {SLA_BREACHED} then 'late' else 'current' end as state,
       e.status as run_status, e.stage as run_stage, e.started_at as run_started_at, e.rows_merged as run_rows,
       round(extract(epoch from (now() - e.started_at)))::int as run_running_s,
       (r.run_id is not null and e.status is null and t.task_enabled
        and (cp.last_success_at is null or cp.last_success_at < r.started_at)) as run_to_go
from t
left join {SCHEMA}.checkpoint cp on cp.control_id = t.id
left join r on true
left join lateral (select ev.status, ev.stage, ev.started_at, ev.rows_merged from {SCHEMA}.ingest_event ev
                   where ev.run_id = r.run_id and ev.control_id = t.id order by ev.started_at desc limit 1) e on true
where (t.src_schema || '.' || t.src_table) ilike %s
order by coalesce({_FAILING}, false) desc, (e.status = 'running') is true desc,
         (r.run_id is not null and e.status is null and (cp.last_success_at is null or cp.last_success_at < r.started_at)) desc,
         t.src_schema, t.src_table
limit %s offset %s"""


def database_tables_count_sql() -> str:
    """How many tables database_tables_sql pages through. Params: group, server, database, schema.table pattern."""
    return (f"with t as ({_database_rows()}) select count(*) as tables from t "
            f"where (t.src_schema || '.' || t.src_table) ilike %s")


def run_rows_sql() -> str:
    """Per run: rows its table loads wrote, how many loads recorded no count, and its detail (which
    holds a batched append's total). Param: array of run ids."""
    return (f"select r.run_id, r.detail, "
            f"(select sum(e.rows_merged) from {SCHEMA}.ingest_event e where e.run_id = r.run_id) as rows_written, "
            f"(select count(*) from {SCHEMA}.ingest_event e where e.run_id = r.run_id and e.status = 'ok' "
            f"and e.rows_merged is null) as uncounted "
            f"from {SCHEMA}.ingest_run r where r.run_id = any(%s)")


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


def run_outcome(run: dict, progress: Optional[dict] = None) -> dict:
    """What a group run's engine record means, as the pages show it: a label, a tone and an optional note.
    The engine records a run that loaded with some table failures as failed, and a run another run closed
    (it stopped without finishing) as failed with closed_by_run in its detail. A running run is detecting
    changes until its first table is checked or loaded, then loading changed tables."""
    status, failed = run.get("status"), _count(run, "failed")
    if status == "completed" and not failed:
        return {"label": "Completed", "tone": "ok", "note": None}
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
    if status in ("failed", "completed") and failed:
        return {"label": "Completed with failures", "tone": "warn", "note": f"{failed:,} {'table' if failed == 1 else 'tables'} failed"}
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
JOB_RUN_ORDER = ("Ended without finishing", "Failed", "Completed with failures", "Completed")


def job_run_outcome(members: List[dict]) -> dict:
    """A job run's outcome from its members (schedules.job_runs): running while any group run (extra runs
    included) is running or any member is waiting for or starting its trigger; otherwise the worst of its group runs'
    outcomes, extra runs included, or "Not run" when none ran. The note counts the members that did not run, as "17 of 18 group runs, 1 skipped"."""
    runs = [m["run"] for m in members if m.get("run")]
    ran = [r for r in runs if r.get("status") != "skipped"]
    extras = [r for m in members for r in m.get("extra") or [] if r.get("status") != "skipped"]
    skipped = sum(1 for m in members if m["state"] == "skipped") + (len(runs) - len(ran))
    no_run = sum(1 for m in members if m["state"] == "no run")
    starting = sum(1 for m in members if m["state"] == "starting")
    note = None
    if len(ran) < len(members):
        note = ", ".join([f"{len(ran)} of {len(members)} group runs"] + ([f"{skipped} skipped"] if skipped else [])
                         + ([f"{starting} starting"] if starting else []) + ([f"{no_run} no run"] if no_run else []))
    if (any(m["state"] in ("waiting", "starting") for m in members)
            or any(r.get("status") == "running" for r in runs + extras)):
        return {"label": "Running", "tone": "run", "note": note}
    if not ran and not extras:
        return {"label": "Not run", "tone": "neutral", "note": note}
    worst = min((run_outcome(r)["label"] for r in ran + extras), key=lambda label: JOB_RUN_ORDER.index(label)
                if label in JOB_RUN_ORDER else len(JOB_RUN_ORDER))
    tone = {"Completed": "ok", "Completed with failures": "warn"}.get(worst, "bad")
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
            f"count(*) filter (where t.task_enabled) as tables, max(cp.last_success_at) as newest_success_at "
            f"from {SCHEMA}.control t left join {SCHEMA}.checkpoint cp on cp.control_id = t.id "
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
           f"cp.phase, cp.status, cp.last_success_at, "
           f"round(extract(epoch from (now() - cp.last_success_at)) / 60)::int as lag_min, "
           f"t.freshness_sla_minutes as sla_min, {SLA_BREACHED} as sla_breached, "
           f"cp.position, cp.detail "
           f"from {SCHEMA}.control t left join {SCHEMA}.checkpoint cp on cp.control_id = t.id "
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
    return (f"select dt.*, d.src_type, d.src_host, d.src_port, d.secret_key, "
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
DEFAULT_PORTS = {"sqlserver": 1433, "oracle": 1521, "postgresql": 5432, "mysql": 3306}
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


def control_row_sql() -> str:
    cols = ", ".join(("id",) + CONTROL_EDIT_COLUMNS)
    return f"select {cols} from {SCHEMA}.control where id = %s"


def update_control_sql(fields: List[str]) -> str:
    """UPDATE of whitelisted columns only (field names come from CONTROL_EDIT_COLUMNS)."""
    assert fields and all(f in CONTROL_EDIT_COLUMNS for f in fields), fields
    return f"update {SCHEMA}.control set " + ", ".join(f"{f} = %s" for f in fields) + " where id = %s"


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
    """One table's checkpoint. Param: control_id."""
    return (f"select phase, status, position, strategy, detail, last_success_at, updated_at, source_identity, "
            f"sink_identity, merge_key from {SCHEMA}.checkpoint where control_id = %s")


# ---- schedules ----------------------------------------------------------------------------------

SCHEDULE_FIELDS = ("name", "cadence_minutes", "timezone", "parameters", "job_variant", "stagger", "paused",
                   "notify_emails")


def schedules_sql() -> str:
    """Active schedules, by name."""
    return (f"select id, {', '.join(SCHEDULE_FIELDS)}, created_by, created_at, updated_at, deleted_at "
            f"from {SCHEMA}.schedule where deleted_at is null order by lower(name)")


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
    vals = ", ".join("%s::jsonb" if c == "parameters" else "%s" for c in cols)
    return f"insert into {SCHEMA}.schedule ({', '.join(cols)}) values ({vals}) returning id"


def update_schedule_sql(fields: List[str]) -> str:
    bad = [f for f in fields if f not in SCHEDULE_FIELDS]
    if bad or not fields:
        raise ValueError(f"not schedule fields: {bad}")
    sets = ", ".join(f"{f} = %s::jsonb" if f == "parameters" else f"{f} = %s" for f in fields)
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
    where task_collection = any(%s) and started_at > now() - interval '7 days'
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
    from {SCHEMA}.ingest_run where task_collection = any(%s)
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


def timeline_runs_sql() -> str:
    """Runs overlapping a window. Params: window end, window start."""
    return (f"select run_id, task_collection, status, started_at, finished_at, job_run_id, ok, failed, skipped "
            f"from {SCHEMA}.ingest_run "
            f"where started_at < %s and coalesce(finished_at, now()) > %s order by task_collection, started_at")
