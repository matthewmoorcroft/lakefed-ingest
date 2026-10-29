# Scale simulation

The scale harness (`tests/scale/`) runs the discover job, the operations app's create-by-reference and upload
paths, and the monitoring pages' queries against a simulated 22,000-database SQL Server catalog
(`tests/synthetic/server_catalog.py`, 240 tables each, 5,280,000 tables). Nothing connects to a database or a workspace:
discovery reads go to a simulated server at the discovery boundary, and Lakebase statements go to recording fakes.

This is a plain Python run on a laptop. It shows the code's call counts, statement counts and row counts, its
Python time on this machine, and the wall time a given per-call latency would give. It does not measure Lakebase or
SQL Server latency. The numbers that need the real stable-app test are listed at the end.

## Running it

    python tests/scale/run_scale.py                  # 40 databases, the size the default test run uses
    python tests/scale/run_scale.py 417              # 417 databases, 100,080 tables (99,360 discovered)
    python tests/scale/run_scale.py 22000            # 22,000 databases, 5,280,000 tables
    LAKEFED_SCALE=1 python tests/scale/run_scale.py  # both sizes in one process (peak memory is then the larger run's)

Run one size per process for clean peak memory. The default `pytest` run stays small; `LAKEFED_SCALE=1 pytest
tests/scale` adds the 417-database and 22,000-database tests.

## Results

Measured by `run_scale.py`, one process per size. "Python s" is the time the code took on this laptop. "Wall" is
simulated from each database's catalog calls and the per-call latency (see the assumptions below).

### Discovery

| Measure | 417 databases (100,080 tables) | 22,000 databases (5,280,000 tables) |
|---|---|---|
| Tables discovered (refused / retried) | 99,360 (3 / 2) | 5,279,280 (3 / 2) |
| Catalog calls | 1,248 | 65,997 |
| Lakebase statements | 100,608 | 5,345,277 |
| Lakebase rows | 100,608 | 5,345,277 |
| Python time | 6.4 s | 349.5 s |
| Peak memory (process) | 138 MB | 140 MB |

Simulated wall time, in seconds, by per-call latency:

| Databases | Workers | 0.25 s | 0.5 s | 0.9 s | 1.5 s | 2.0 s |
|---|---|---|---|---|---|---|
| 417 | 8 (notebook today) | 45.2 | 84.5 | 148.5 | 243.0 | 320.5 |
| 417 | 32 (design) | 24.5 | 26.5 | 38.7 | 64.5 | 86.0 |
| 22,000 | 8 (notebook today) | 2,068.8 | 4,131.5 | 7,432.2 | 12,382.5 | 16,508.0 |
| 22,000 | 32 (design) | 517.8 | 1,034.0 | 1,859.4 | 3,097.5 | 4,130.0 |

At 0.9 s a call the 22,000-database discovery takes 31 minutes at 32 workers and 2.1 hours at the notebook's 8.

### Create by reference

| Measure | 417 databases (99,360 tables) | 22,000 databases (5,280,000 tables) |
|---|---|---|
| Result | planned: 4 groups, 214 statements, 198,738 rows, no errors | refused at the 120,000-line cap |
| CSV size | 15.2 MB | not built (refused before the writes) |
| Planning time | 5.8 s (CSV 0.3 s, writes 1.0 s) | 0.5 s to the refusal at 120,001 lines; full planning about 21.6 s (extrapolated) |
| Peak memory | 653 MB | about 35 GB (extrapolated linearly; not measured) |

### Upload

| Measure | 417 databases (99,360 lines) | 22,000 databases (5,280,000 lines) |
|---|---|---|
| Result | read and written | refused at the 120,000-line cap |
| Read / write time | 0.5 s / 0.2 s | 0.4 s to the refusal at 120,001 lines |
| Statements | 101 (1 header, 100 inserts of up to 1,000 lines) | 121 per 120,000-line upload (computed) |
| Rows | 99,361 (the header, then one row per line) | not written |
| Body size | 15.2 MB (153 bytes a line) | 44 uploads of about 6.4 MB (53 bytes a line, the probe's rate) to about 18.4 MB (the 417 rate) |

The 5,280,000-line file would be 280 MB to 808 MB as one body, over the 64 MiB body limit. The 120,000-line cap splits it
into 44 uploads that are each under the limit, so the cap binds before the body limit at either line size.

### Monitoring pages (statement shapes)

These come from the app's own routes through `TestClient`, with the database stubbed to record statements and answer
with no rows. So the shape is what each page asks for, not how many rows it returns at scale. Pages that 404 or
return early against empty data show fewer statements than they issue for real.

| Page | Status | Statements | Statement kinds |
|---|---|---|---|
| /api/overview | 200 | 4 | select control, select ingest_run, select schedule, with ingest_run |
| /api/collections | 200 | 1 | select control |
| /api/errors | 200 | 1 | select ingest_event |
| /api/ingestion-jobs | 200 | 0 | none (the list reads nothing against empty data) |
| /api/ingestion-jobs/1 | 404 | 0 | none (no job to read) |
| /api/ingestion-jobs/1/live | 404 | 1 | select schedule |
| /api/runs | 200 | 1 | select ingest_run |
| /api/tables | 200 | 1 | select control |
| /api/changes | 200 | 1 | select control_change |
| /api/schedules | 200 | 1 | select schedule |
| /api/timeline | 200 | 0 | none |
| /api/discoveries | 200 | 1 | select discovery |

The kinds are the statement verb and table, with the `lakefed_ingest.` schema left off here.

## What each number assumes

- **Catalog calls.** One listing call, then three catalog queries (tables, columns, pk) per database that is read. The
  totals fit one call for each refused database and one extra call for each of the two retried databases:
  1 + 3 × 414 + 3 + 2 = 1,248 at 417, and 1 + 3 × 21,997 + 3 + 2 = 65,997 at 22,000. The split is inferred from the totals.
- **Lakebase statements.** Per database that is read: a start insert, a delete, one insert per table (the notebook's
  `executemany`, which psycopg sends as one statement per row), and a finish update. A refused database gets only its
  start and finish. At 417: 414 × 3 + 3 × 2 + 99,360 = 100,608. The sequence is the notebook's, mirrored in
  `steps.DiscoveryWriter`, and a test pins its column list to the notebook's.
- **Lakebase rows.** The fake counts one row per statement for these statements (each carries one row), so rows equal
  statements in the discovery rows.
- **Discovery Python time and peak memory.** Measured on this machine, including the simulated server's work and the
  writes' row construction. They exclude the network and the database.
- **Wall time.** The listing and each database's catalog calls (calls x per-call latency, plus retry waits) are laid out
  on a pool in listing order, as a `ThreadPoolExecutor` takes them. The per-call latency is applied afterwards, so one
  run gives every latency in the table. The 8 workers match the notebook (`min(8, databases)`); 32 is the design's cap.
- **Per-call latency 0.25 to 2.0 s.** The design's 25 to 40 minutes at 32 workers for 22,000 databases implies
  0.7 to 1.2 s a call. The 0.9 s line is the middle of that range and gives 31 minutes. This is an assumption, not a
  measurement of the catalog's round trip.
- **Retry waits.** A paused database's waits come from the discovery's retry backoff, with the jitter fixed at 0.5. They
  are booked as waits, not scaled with latency, so they form a fixed floor in the wall time.
- **Lakebase time.** Statements are counted, not timed. No number above includes a Lakebase latency, so the create,
  upload and monitoring rows say nothing about Lakebase time.
- **Create by reference.** `new_job.plan` and the writes of `create_ingestion_job` (controls in thousands, the change
  log, group members, the schedule, the selection, the touch), run through the app's own helpers with a recording
  cursor. The schedule's cron validation (which reads the Databricks jobs) and the read of the selection from Lakebase
  are not run; the selection is built in Python from the discovered rows.
- **The 120,000-line cap.** `config_import.plan_import` builds the full list of parsed lines before it compares the
  count, so an oversize create holds every row before it refuses. That is why the 22,000-database create is not run in
  full: its planning memory is extrapolated. `read_upload` stops at line 120,001.
- **Upload body.** The probe's lines are about 53 bytes; the 417 run's are about 153 bytes (15,196,485 bytes over 99,360
  lines). JSON escaping and the request's own overhead are left out.
- **Peak memory.** Peak resident memory of the process (macOS `ru_maxrss`, cumulative for the process). The
  22,000-database create figure is extrapolated linearly from the 99,360-table run and is not measured.

## Reruns after other pieces merge

- **Create by database.** Changes what the create plans (groups per database, not per server) and the writes it makes.
  Rerun the create rows and the statement counts; the 5,280,000-table create's refusal and its extrapolation move with
  the planner.
- **Rollups.** Add writes and reads per run and per collection. Rerun the write counts and the monitoring pages' shapes.
- **Run poller.** Changes how often and by which statements a run's progress is read. Rerun the monitoring shapes; the
  poll's own statement count per interval is new.
- **Re-discovery.** Writes a new discovery per run and follows `job_selection`. Rerun the discovery rows, and the
  selection's rows at the create.

## Numbers only the real stable-app test can give

- Lakebase statement and round-trip time, for the discovery writes (100,608 at 417 databases; 5,345,277 at 22,000), the
  create's writes, the upload's lines and each monitoring page.
- The real per-call latency of the SQL Server catalog queries, and whether the source's concurrency limit holds at 32.
- The Azure Resource Manager listing time at 22,000 databases (pages of 100).
- Peak memory of the create and the selection read at 5,280,000 tables, on the app's real compute.
- Monitoring pages' latency with 22,000 databases and 5,280,000 control rows in Lakebase (the state scan's cost).
- The upload route's real 413 behaviour through the Apps proxy, and the real request size with JSON encoding.
