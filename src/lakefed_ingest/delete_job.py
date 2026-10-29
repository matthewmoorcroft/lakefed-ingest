"""Deleting an operations-app job's tables (run by delete_job.ipynb, job lakefed_ingest_delete_job).

The app has no DELETE privilege on the Lakebase tables, so when a job is deleted in the app its groups' tables would
stay behind, as groups in no job. This job runs as the deployer (the state database's owner) and removes them:

- **Which groups:** the deleted job's members that no other live job holds (a group is in one job, but a removed
  member could have joined another job since).
- **What goes, in one transaction under each group's run lock:** the groups' control rows and what is keyed by them
  (checkpoint, reseed_queue) or by the group (database_check, check_work). A run of any of the groups refuses the
  lot: nothing is deleted while one is loading.
- **What stays:** run history (ingest_run, ingest_event) and the change log (control_change), so the job's runs and
  changes can still be read after it is gone; their rows keep the group and control id of rows that no longer exist.
- **Target tables, when asked:** after the commit, the tables only these groups write; one another job's rows also
  write is kept and named. A schema is never dropped.

Pure (no Spark, no Lakebase): the notebook passes a cursor and runs the drops; tested with fakes.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from ct_engine import sink_fqn
from state_store import collection_lock_key

SCHEMA = "lakefed_ingest"


def schedule_sql() -> str:
    """The job, deleted or not. Param: schedule id."""
    return f"select id, name, deleted_at from {SCHEMA}.schedule where id = %s"


def groups_sql() -> str:
    """The job's groups no other live job holds: every member it had, removed or not, that isn't an active member of
    another job. Params: schedule id, schedule id."""
    return (f"select distinct m.task_collection from {SCHEMA}.schedule_member m where m.schedule_id = %s "
            f"and not exists (select 1 from {SCHEMA}.schedule_member o join {SCHEMA}.schedule s on s.id = o.schedule_id "
            f"where o.task_collection = m.task_collection and o.removed_at is null and s.deleted_at is null "
            f"and o.schedule_id <> %s) order by 1")


def lock_sql() -> str:
    """A group's run lock for this transaction, on the key a run holds (state_store.collection_lock_key); false: a
    run of it is loading. Param: lock key."""
    return "select pg_try_advisory_xact_lock(hashtextextended(%s, 0))"


def targets_sql() -> str:
    """Each target table the groups' rows write, and whether rows of any other group write it too (names compared
    without case, as Unity Catalog and control_sink_client_uq do). Params: groups, groups."""
    return (f"with mine as (select distinct sink_catalog, sink_schema, sink_table from {SCHEMA}.control "
            f"where task_collection = any(%s)), "
            f"theirs as (select distinct lower(sink_catalog) c, lower(sink_schema) s, lower(sink_table) t "
            f"from {SCHEMA}.control where not (task_collection = any(%s))) "
            f"select m.sink_catalog, m.sink_schema, m.sink_table, "
            f"exists (select 1 from theirs where c = lower(m.sink_catalog) and s = lower(m.sink_schema) "
            f"and t = lower(m.sink_table)) as shared from mine m order by 1, 2, 3")


# Keyed by the groups' control rows first, then by the group, then the rows themselves: no foreign keys hold them
# together, so the order only keeps a failure part way from leaving checkpoints of rows already gone (the
# transaction undoes it all anyway). Each takes the groups as its one parameter.
DELETES = (
    ("reseed_queue", f"delete from {SCHEMA}.reseed_queue where control_id in "
                     f"(select id from {SCHEMA}.control where task_collection = any(%s))"),
    ("checkpoint", f"delete from {SCHEMA}.checkpoint where control_id in "
                   f"(select id from {SCHEMA}.control where task_collection = any(%s))"),
    ("database_check", f"delete from {SCHEMA}.database_check where task_collection = any(%s)"),
    ("check_work", f"delete from {SCHEMA}.check_work where task_collection = any(%s)"),
    ("control", f"delete from {SCHEMA}.control where task_collection = any(%s)"),
)


def drop_sql(table: str) -> str:
    """Drops one target table, named as plan_targets names it. Never a schema."""
    return f"DROP TABLE IF EXISTS {table}"


def plan_targets(rows: List[dict]) -> Dict[str, List[str]]:
    """{"drop": tables only these groups write, "kept": tables other groups' rows write too}, as the quoted names
    the ingestion job writes (ct_engine.sink_fqn), each once."""
    drop = list(dict.fromkeys(sink_fqn(r) for r in rows if not r["shared"]))
    kept = list(dict.fromkeys(sink_fqn(r) for r in rows if r["shared"]))
    return {"drop": drop, "kept": kept}


class RunGoing(RuntimeError):
    """A run of one of the job's groups is loading: nothing was deleted."""


def _rows(cur) -> List[dict]:
    columns = [d.name if hasattr(d, "name") else d[0] for d in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def delete_rows(cur, schedule_id: int) -> dict:
    """Inside the caller's transaction: check the job is deleted, take each group's run lock, read what its tables
    write, then delete the groups' rows. Returns {"job", "groups", "deleted": {table: rows}, "targets": plan}.
    Raises ValueError for a job that isn't deleted (or doesn't exist) and RunGoing while a group is loading."""
    cur.execute(schedule_sql(), (int(schedule_id),))
    found = _rows(cur)
    if not found:
        raise ValueError(f"ingestion job {schedule_id} doesn't exist")
    job = found[0]
    if job["deleted_at"] is None:
        raise ValueError(f"ingestion job {job['name']} ({schedule_id}) isn't deleted: delete it in the app first")
    cur.execute(groups_sql(), (int(schedule_id), int(schedule_id)))
    groups = [r["task_collection"] for r in _rows(cur)]
    if not groups:
        return {"job": job["name"], "groups": [], "deleted": {}, "targets": {"drop": [], "kept": []}}
    for group in groups:
        cur.execute(lock_sql(), (collection_lock_key(group),))
        if not cur.fetchone()[0]:
            raise RunGoing(f"a run of group {group} is loading: nothing was deleted; run the delete again once it ends")
    cur.execute(targets_sql(), (groups, groups))
    targets = plan_targets(_rows(cur))
    deleted = {}
    for table, sql in DELETES:
        cur.execute(sql, (groups,))
        deleted[table] = cur.rowcount
    return {"job": job["name"], "groups": groups, "deleted": deleted, "targets": targets}


# Names a summary or a failure lists; the rest are counted, so a job of many thousands of tables stays readable.
LISTED = 20


def summary(result: dict, dropped: List[str], failed: Optional[Dict[str, str]] = None) -> dict:
    """What the run did, for its exit value: counts, with the kept and failed target tables named up to LISTED."""
    failed = failed or {}
    return {"job": result["job"], "groups": len(result["groups"]), "tables": result["deleted"].get("control", 0),
            "deleted": result["deleted"], "dropped": len(dropped), "kept": len(result["targets"]["kept"]),
            "kept_names": result["targets"]["kept"][:LISTED], "drop_failed": len(failed),
            "drop_failed_names": dict(list(failed.items())[:LISTED])}


def failure_message(failed: Dict[str, str]) -> str:
    """Why the run fails after its rows are deleted: the target tables it couldn't drop, which a second run won't
    find (the rows that listed them are gone), so they are named to be dropped by hand."""
    named = "; ".join(f"{table}: {error}" for table, error in list(failed.items())[:LISTED])
    more = f"; and {len(failed) - LISTED} more (the run's output lists them)" if len(failed) > LISTED else ""
    return f"the job's tables are deleted, but {len(failed)} target table(s) could not be dropped (drop them by hand): {named}{more}"
