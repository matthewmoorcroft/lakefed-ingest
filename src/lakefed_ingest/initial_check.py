"""The check: which source databases of a group of collections need an ingest.

``check_launch.ipynb`` runs this first, on serverless, over one group of collections (a schedule's
members, or every collection with enabled rows). For each database it asks the same questions a
sweep's planning asks (``planner``): has any table never been loaded, is any append pending or reseed
due, and did the change tracking version move past the checkpoints (then, did any of our tables
change). A database with nothing to do gets its check stamp (``planner.unchanged_writes``) and no
ingest. The others are their collection's work list: the check job records it (``check_work``) and
starts that collection's run for those databases only (``launcher.py``). A quiet cycle starts no
ingest cluster.

``check_collection`` is pure (no Spark, no Lakebase): the notebook passes the source queries in, as
``detect_one``. ``run_check`` takes the Lakebase calls as objects, so it is tested with fakes.
"""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, List, Optional

import planner


def check_collection(configs: List[dict], checkpoints: Dict[int, dict], detect_one: Callable, *,
                     existing_sinks: Optional[set], stamps=None, interval_s: float = 0, mixed_sinks=frozenset(),
                     reseed_on_new_columns: bool = True, snapshot_isolation: bool = False, force=frozenset(),
                     now=None, map_fn=map) -> dict:
    """Check one collection's databases.

    ``existing_sinks`` is the set of sink tables that exist (``ct_engine.existing_sinks``). It is
    required, so a check can't run without knowing whether a sink was dropped (a dropped sink would
    otherwise be stamped as unchanged). None means the sinks couldn't be listed: then nothing is
    vouched for, and every database with a table to check goes to the ingest, which checks them itself.

    ``detect_one(members) -> planner.detect_group output`` checks one detection group (one source
    database); ``map_fn`` runs it over the groups (the notebook passes a parallel map). ``force``
    names databases to send to the ingest without asking them anything.

    A database needs an ingest when one of its rows is routed to work before detection (seed,
    pending append, reseed, a full or cursor table, an unsupported row, which the ingest reports),
    when its detection planned anything (changed tables, reseeds, fallbacks) or failed (the ingest
    records the failure), or when it is forced. Detection runs only for the others. The rest are
    unchanged: ``rows`` and ``database_stamps`` are their bookkeeping, for the caller to write while
    it holds the collection's lock.

    Returns {work: sorted [source, src_database] pairs, rows, database_stamps, unchanged_databases,
    unchanged_tables, work_tables, detected, queries, fast_exits}.
    """
    plans, candidates, legacy = planner.route_tables(configs, checkpoints, mixed_sinks=mixed_sinks,
                                                     reseed_on_new_columns=reseed_on_new_columns,
                                                     snapshot_isolation=snapshot_isolation,
                                                     existing_sinks=existing_sinks)
    forced = {str(name) for name in force}
    elsewhere = (planner.database_keys(p["cfg"] for p in plans) | planner.database_keys(c for c, _ in legacy)
                 | planner.database_keys(c for c in configs if c.get("src_database") in forced))
    if existing_sinks is None:
        elsewhere |= planner.database_keys(c for c, _ in candidates)
    to_detect = [members for members in planner.group_by_source(candidates).values()
                 if not (planner.database_keys(c for c, _ in members) & elsewhere)]
    outputs = list(map_fn(detect_one, to_detect))
    unchanged_members, unchanged_outputs = [], []
    for members, out in zip(to_detect, outputs):
        if out["plans"] or out["failed"] or out["cur"] is None:
            elsewhere |= planner.database_keys(c for c, _ in members)
        else:
            unchanged_members.append(members)
            unchanged_outputs.append(out)
    rows, database_stamps = planner.unchanged_writes(unchanged_members, unchanged_outputs, checkpoints, interval_s,
                                                     stamps=stamps, now=now)
    return {"work": sorted(list(key) for key in elsewhere), "rows": rows, "database_stamps": database_stamps,
            "unchanged_databases": len(unchanged_members),
            "unchanged_tables": sum(len(m) for m in unchanged_members),
            "work_tables": sum(1 for c in configs if planner.database_key(c) in elsewhere),
            "detected": len(to_detect), "queries": sum(o["queries"] for o in outputs),
            "fast_exits": sum(int(o["fast_exit"]) for o in outputs)}


def run_check(store, lock_store, collections: List[str], detect_one: Callable, *,
              existing_sinks_of: Callable, job_run_id: str = "", cluster_id: str = "", parallelism: int = 32,
              interval_s: float = 0, reseed_on_new_columns: bool = True, snapshot_isolation: bool = False,
              force=frozenset(), map_fn=map, collection_workers: int = 8) -> dict:
    """Check every collection of the group and write the unchanged databases' stamps.

    ``existing_sinks_of(configs) -> set or None`` lists the sink tables of a collection's rows (the
    notebook passes ``ct_engine.existing_sinks``); see ``check_collection``.

    Each collection is checked under its lock (``lock_store``'s session holds it), with an
    ``ingest_run`` row whose ``detail.check`` records what was found; a collection whose lock another
    sweep holds is left to that sweep and listed as busy. ``map_fn`` runs ``detect_one`` over each
    collection's databases (pass a shared thread pool's map to check databases of every collection at
    once); up to ``collection_workers`` collections are checked at a time. A failed check marks its run
    failed, releases the collection's lock and fails the whole check, so nothing ingests on a half-known state.

    Returns the summary; ``per_collection`` holds each checked collection's ``work``.
    """
    mixed_sinks = store.mixed_sinks()

    def check_one(collection):
        started = time.monotonic()
        if not lock_store.try_lock_collection(collection):
            return None
        run_id = None
        try:
            run_id = store.start_run(collection, job_run_id, cluster_id, parallelism)
            _close_abandoned_runs(store, collection, run_id)
            configs = store.list_task_configs(collection)
            checkpoints = store.get_checkpoints([c["id"] for c in configs])
            result = check_collection(configs, checkpoints, detect_one, existing_sinks=existing_sinks_of(configs),
                                      stamps=store.get_database_checks(collection), interval_s=interval_s,
                                      mixed_sinks=mixed_sinks,
                                      reseed_on_new_columns=reseed_on_new_columns,
                                      snapshot_isolation=snapshot_isolation, force=force, map_fn=map_fn)
            if not lock_store.is_alive():
                raise RuntimeError("the collection's lock connection dropped during the check, so the lock is gone "
                                   "and nothing was written")
            writes_started = time.monotonic()
            store.bulk_advance_unchanged(result["rows"])
            store.stamp_databases(collection, result["database_stamps"], run_id)
            check = {k: result[k] for k in ("work", "unchanged_databases", "unchanged_tables", "work_tables",
                                            "detected", "queries", "fast_exits")}
            check.update(table_rows=len(result["rows"]), database_stamps=len(result["database_stamps"]),
                         write_ms=int((time.monotonic() - writes_started) * 1000),
                         check_s=round(time.monotonic() - started, 1))
            unchanged = result["unchanged_tables"]
            store.finish_run(run_id, total=unchanged, ok=unchanged, failed=0, skipped=unchanged, status="completed",
                             detail=json.dumps({"check": check}))
            return {"collection": collection, "run_id": run_id, **check}
        except BaseException as error:
            if run_id is not None:
                _finish_failed(store, run_id, error)
            raise
        finally:
            _release_lock(lock_store, collection)

    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=max(1, min(len(collections), collection_workers)),
                            thread_name_prefix="collection") as pool:
        checked = list(pool.map(check_one, collections))
    results = [r for r in checked if r is not None]
    return {"collections": len(collections), "busy": [c for c, r in zip(collections, checked) if r is None],
            "check_s": round(time.monotonic() - started, 1),
            "collections_with_work": sum(1 for r in results if r["work"]),
            "databases_with_work": sum(len(r["work"]) for r in results),
            "unchanged_databases": sum(r["unchanged_databases"] for r in results),
            "unchanged_tables": sum(r["unchanged_tables"] for r in results),
            "table_rows": sum(r["table_rows"] for r in results),
            "database_stamps": sum(r["database_stamps"] for r in results),
            "per_collection": results}


def _close_abandoned_runs(store, collection: str, run_id) -> None:
    """Close the collection's runs a dead sweep or check left open. The check holds the collection's lock
    here, as the sweep does before it starts. Telemetry only: a failure never stops the check."""
    try:
        store.close_abandoned_runs(collection, run_id)
    except Exception as e:
        print(f"[telemetry] could not close earlier unfinished runs of {collection}: {str(e)[:200]}")


def _release_lock(lock_store, collection: str) -> None:
    """Release the collection's lock. Best effort: a dropped lock connection has already let go of it, and
    a failure here must not replace the outcome of the check."""
    try:
        lock_store.unlock_collection(collection)
    except Exception as e:
        print(f"[telemetry] could not release the lock of {collection}: {str(e)[:200]}")


def _finish_failed(store, run_id, error: BaseException) -> None:
    """Mark the check's run failed, with what stopped it. Best effort: the error that stopped the check is
    the one that goes on."""
    detail = json.dumps({"check": {"error": f"{type(error).__name__}: {str(error)[:400]}"}})
    try:
        store.finish_run(run_id, total=0, ok=0, failed=0, status="failed", detail=detail)
    except Exception as e:
        print(f"[telemetry] could not mark check run {run_id} failed: {str(e)[:200]}")
