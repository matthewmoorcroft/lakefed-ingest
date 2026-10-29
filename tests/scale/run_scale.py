"""Runs the scale steps and prints the tables docs/scale.md reports. Nothing connects to a database or a workspace.

    python tests/scale/run_scale.py                    # 40 databases (the default run's size)
    LAKEFED_SCALE=1 python tests/scale/run_scale.py    # 417 databases (100,080 tables) and 22,000 (5,280,000 tables)

Discovery runs in full at each size. Create and upload run in full where the app's planning accepts the size (up to
its 120,000-line cap); above the cap they run as refusal probes, and the planning time for the full size is extrapolated
from the probe and marked as such. Server times are simulated from the latency given (see simulation.Latency).
"""
from __future__ import annotations

import os
import sys
import time
from typing import List, Optional

import paths  # noqa: F401  (sys.path)
import app_steps as apps
import new_job as nj
import server_catalog
import simulation as sim
import steps

LATENCIES_S = (0.25, 0.5, 0.9, 1.5, 2.0)
WORKER_COUNTS = (steps.NOTEBOOK_WORKERS, steps.DESIGN_WORKERS)
TABLES_PER_DATABASE = server_catalog.DEFAULT_TABLES_PER_DATABASE
DEFAULT_SIZES = (40,)
SCALE_SIZES = (417, 22_000)


def sizes_from_environment() -> tuple:
    return SCALE_SIZES if os.environ.get("LAKEFED_SCALE") == "1" else DEFAULT_SIZES


def peak_memory_mb() -> float:
    """The process's peak resident memory so far, in megabytes (macOS reports bytes, Linux kilobytes)."""
    import resource
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024


def discovery_result(databases: int) -> dict:
    """Discovery of a server of that many databases, with its Lakebase writes: calls, statements, rows and the simulated
    wall time at each latency and worker count."""
    catalog = server_catalog.ServerCatalog(databases=databases)
    run, connection = steps.discover_writes(catalog)
    wall = {(latency, workers): run.wall_s(sim.Latency(latency), workers)
            for latency in LATENCIES_S for workers in WORKER_COUNTS}
    return {"databases": databases, "tables": run.tables, "discovered": run.discovered,
            "refused": len(run.refused), "retried": len(run.retried), "calls": run.calls(),
            "calls_by_kind": dict(run.calls_by_kind), "statements": connection.total_statements(),
            "rows": connection.total_rows(), "python_s": run.cpu_s, "peak_mb": peak_memory_mb(), "wall": wall,
            "retry_waits_s": run.retry_waits_s}


def create_result(databases: int) -> dict:
    """Create by reference over a server of that many databases: the full create when the planning takes the size, or
    the refusal probe at the cap with the planning time extrapolated from it."""
    catalog = server_catalog.ServerCatalog(databases=databases)
    tables = databases * TABLES_PER_DATABASE
    if tables <= nj.MAX_LINES:
        run = apps.create_by_reference(catalog)
        return {"databases": databases, "tables": run.tables, "measured": True, "errors": run.errors,
                "groups": run.groups, "csv_bytes": run.csv_bytes, "csv_s": run.csv_s, "plan_s": run.plan_s,
                "write_s": run.write_s, "statements": run.connection.total_statements(),
                "rows": run.connection.total_rows(), "peak_mb": peak_memory_mb()}
    errors, probe_s = apps.create_refusal(nj.MAX_LINES + 1)
    return {"databases": databases, "tables": tables, "measured": False, "errors": errors,
            "probe_lines": nj.MAX_LINES + 1, "probe_s": probe_s,
            "extrapolated_plan_s": probe_s * tables / (nj.MAX_LINES + 1)}


def upload_result(databases: int) -> dict:
    """An upload of a server's tables as one CSV: the full upload when read_upload takes the size, or its refusal probe
    at the cap, with the body size the route would see."""
    catalog = server_catalog.ServerCatalog(databases=databases)
    tables = databases * TABLES_PER_DATABASE
    if tables <= nj.MAX_LINES:
        run = apps.upload_by_reference(catalog)
        return {"databases": databases, "tables": tables, "measured": True, "errors": run.errors, "lines": run.lines,
                "csv_bytes": run.csv_bytes, "read_s": run.read_s, "write_s": run.write_s,
                "statements": run.connection.total_statements() if run.connection else 0,
                "rows": run.connection.total_rows() if run.connection else 0}
    errors, probe_s = apps.upload_refusal(nj.MAX_LINES + 1)
    line_bytes = apps.sample_line_bytes()
    return {"databases": databases, "tables": tables, "measured": False, "errors": errors,
            "probe_lines": nj.MAX_LINES + 1, "probe_s": probe_s, "line_bytes": line_bytes,
            "one_body_bytes": line_bytes * tables, "uploads_at_cap": -(-tables // nj.MAX_LINES),
            "capped_body_bytes": line_bytes * nj.MAX_LINES, "body_limit_bytes": apps.app.MAX_UPLOAD_BYTES}


def _seconds(value: float) -> str:
    return f"{value:,.1f}"


def print_discovery(results: List[dict]) -> None:
    print("## Discovery (discover_tables.ipynb, with its Lakebase writes)")
    print()
    print("| Databases | Tables | Refused | Retried | Calls | Statements | Rows | Python s | "
          "Peak MB (process so far) |")
    print("|---|---|---|---|---|---|---|---|---|")
    for r in results:
        print(f"| {r['databases']:,} | {r['tables']:,} | {r['refused']} | {r['retried']} | {r['calls']:,} | "
              f"{r['statements']:,} | {r['rows']:,} | {_seconds(r['python_s'])} | {r['peak_mb']:,.0f} |")
    print()
    print("Simulated wall time (s), discovery's listing plus the databases on a pool, by per-call latency:")
    print()
    print("| Databases | Workers | " + " | ".join(f"{l} s" for l in LATENCIES_S) + " |")
    print("|---|---|" + "---|" * len(LATENCIES_S))
    for r in results:
        for workers in WORKER_COUNTS:
            cells = " | ".join(_seconds(r["wall"][(l, workers)]) for l in LATENCIES_S)
            print(f"| {r['databases']:,} | {workers} | {cells} |")
    print()


def print_create(results: List[dict]) -> None:
    print("## Create by reference (new_job.plan and the writes of create_ingestion_job)")
    print()
    for r in results:
        if r["measured"]:
            print(f"- {r['databases']:,} databases, {r['tables']:,} tables: planned in {_seconds(r['plan_s'])} s "
                  f"(CSV {_seconds(r['csv_s'])} s, writes {_seconds(r['write_s'])} s), "
                  f"{r['statements']:,} statements, {r['rows']:,} rows, {r['groups']} groups, "
                  f"{r['csv_bytes']:,} CSV bytes, errors {r['errors'] or 'none'}, peak {r['peak_mb']:,.0f} MB")
        else:
            print(f"- {r['databases']:,} databases, {r['tables']:,} tables: refused at the cap "
                  f"({r['probe_lines']:,} lines probe, {_seconds(r['probe_s'])} s): {r['errors']}. "
                  f"Full planning time extrapolated from the probe: about {_seconds(r['extrapolated_plan_s'])} s "
                  f"(not run: the planning holds every row before it refuses)")
    print()


def print_upload(results: List[dict]) -> None:
    print("## Upload of a CSV of tables (read_upload and the line writes of upload_tables)")
    print()
    for r in results:
        if r["measured"]:
            print(f"- {r['databases']:,} databases, {r['lines']:,} lines: read in {_seconds(r['read_s'])} s, "
                  f"written in {_seconds(r['write_s'])} s, {r['statements']:,} statements, {r['rows']:,} rows, "
                  f"{r['csv_bytes']:,} CSV bytes")
        else:
            one_mb = r["one_body_bytes"] / 1e6
            cap_mb = r["capped_body_bytes"] / 1e6
            print(f"- {r['databases']:,} databases, {r['tables']:,} tables: refused at the cap "
                  f"({r['probe_lines']:,} lines probe, {_seconds(r['probe_s'])} s): {r['errors']}. "
                  f"At about {r['line_bytes']:.0f} bytes a line, one upload would be about {one_mb:,.0f} MB against "
                  f"the {r['body_limit_bytes']:,} byte body limit; at the cap, {r['uploads_at_cap']} uploads of "
                  f"about {cap_mb:,.1f} MB each.")
    print()


def print_monitoring(shapes: List[dict]) -> None:
    print("## Monitoring pages (the statements each page issues; empty answers, so the shape and not the row count)")
    print()
    print("| Page | Status | Statements | Statement kinds |")
    print("|---|---|---|---|")
    for shape in shapes:
        kinds = ", ".join(f"{kind} x{count}" for kind, count in sorted(shape["kinds"].items())) or "none"
        print(f"| {shape['path']} | {shape['status']} | {shape['statements']} | {kinds} |")
    print()


def main(argv: Optional[List[str]] = None) -> None:
    sizes = tuple(int(a) for a in (argv or []) if a.isdigit()) or sizes_from_environment()
    started = time.perf_counter()
    print(f"Sizes (databases): {', '.join(f'{s:,}' for s in sizes)}")
    print()
    print_discovery([discovery_result(s) for s in sizes])
    print_create([create_result(s) for s in sizes])
    print_upload([upload_result(s) for s in sizes])
    print_monitoring(apps.monitoring_shapes())
    print(f"Total Python time: {time.perf_counter() - started:,.0f} s")


if __name__ == "__main__":
    main(sys.argv[1:])
