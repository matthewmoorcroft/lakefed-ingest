# lakefed_ingest operations app (optional)

A Databricks App for operating the default version of lakefed_ingest. It's a separate, optional
bundle: deploy it only if your workspace allows Databricks Apps. The ingestion bundle at the repo
root doesn't depend on it, and everything the app does can also be done with SQL and the Jobs UI
(see [../docs/monitoring.md](../docs/monitoring.md) and [../docs/lakebase_schema.md](../docs/lakebase_schema.md)).

| Page | What it does |
|---|---|
| **Overview** | Ingestion jobs (a schedule and its ingestion groups) with their tables by state, late databases (a database with a late table; its database page lists the tables), last job run and Status (running, failing, late, paused); groups in no job; job runs going now (progress over all the job's tables, speed, expected end); jobs needing attention, with their failures grouped by cause |
| **Ingestion jobs** | Every ingestion job, filtered (running, failing, late, paused) and searched; one job's details with its job runs (one per trigger: outcome, took, speed, parallelism, clusters) and its groups; one job run with its group runs, databases and failures |
| **Runs** | Job runs of every ingestion job, running first; group runs of groups in no job. A group run's page shows its outcome, speed, time left, tables in flight, databases and table events |
| **Sources** | Each source instance: how it is read (UC connection or JDBC host), its ingestion jobs, its tables by state, and tables discovered and not registered yet; UC connections with nothing registered |
| **Timeline** | Runs per collection over a window (6 hours to 7 days): planned triggers, runs colored by outcome, and triggers Databricks skipped because the previous run was still going |
| **Schedules** | Run collections at a cadence (see [Schedules](#schedules)): create, edit, pause, resume and delete schedules; each member's next run, wrapper job, last run, p95 run time, skipped triggers, and warnings for overruns and SLAs a schedule can't meet |
| **Tables** | Browse control rows with their checkpoint (phase, status, freshness lag, SLA). Filter by collection, text, or failing / SLA breached / no SLA / seeding / disabled. Select one row to **edit** it (every field, plus its change history), or several to **bulk edit** (e.g. move to another collection, or switch to reads through a UC connection), **enable**, **disable** or **force a reseed**. Changing `write_mode` needs a new sink in the same edit (an append sink and a merge sink can't share a table) |
| **Databricks jobs** | Under Schedules and jobs: the Databricks ingestion job's active and recent runs (with the ingestion job that started each); **cancel** a run |
| **Manage** | Only for those with Can Manage on the app itself (see [Security model](#security-model)): the app's version (the commit it was deployed from, and its active deployment) and state database; **Export as CSV** of one, several or every ingestion job's enabled tables, in the columns Create from one CSV reads, with an `ingestion_job` column naming each line's job (streamed a page at a time, so a 200,000-table export works; one job's lines import as a new ingestion job, and an import refuses a file of several jobs); and the Databricks jobs the app runs, each linked |
| **Changes** | The change log: every add, register, edit, enable / disable and reseed made in the app, with who, what and when |
| **Add table** | A form that adds one control row, checked before insert (the database constraints are the backstop): a JDBC source (host and secret) or a UC connection (`use_remote_query`), `write_mode`, `client_attrs` and `jdbc_options` |
| **Bulk import** | Discover a server's tables (the `lakefed_ingest_discover` job, catalog metadata only), review the recommended settings in an editable grid, set shared fields once (collection, sink catalog / schema / table templates, `client_id` template, freshness SLA), and register the selection. Tables at or above a size threshold (default 1 GB) get a partitioned seed; tables already registered start unticked; duplicates are skipped |
| **Tables CSV** (New ingestion job and Add tables) | Discover on a source stays the first choice; the CSV is the fallback. For a CSV, choose the UC connection first, then upload a file with no connection column: `src_database` (required), `src_schema` and `src_table` (both, or neither), then optional `select_list`, `primary_key`, `cursor_column`, `client_id` and `load_type`. A line with a database alone takes every table of that database: its tables are found by name through the connection when the job is created (the task shows discovering, then writing), and a table line in that database is taken whether or not it is picked, and wins over what discovery finds for it. A `src_connection` column is accepted only when every row names the chosen connection; otherwise the file is refused with the lines named. The preview counts the databases, the tables and the lines that can't be taken, with why; a database that can't be opened is named and blocks Create. The page offers a template. The file is kept in Lakebase once uploaded, and there is no separate databases CSV |
| **Import CSV** | Import a CSV of tables into one ingestion group: each line says where a table is (`src_connection`, or `src_host` with `secret_key`; `src_database`, `src_schema`, `src_table`; `select_list`, `primary_key`, `cursor_column`; `client_id` and `attr:<column>` for a fan-in), and the group's settings say how to ingest it (source type, load type, write mode, fan-in, sink name templates, freshness target, read options). A group's settings are the values its rows share, shown on its page with an Export CSV of the same form; a new group, or a setting its rows differ on, is set at import. Every line is previewed and checked as Add table checks a row; Apply writes only when none is invalid, skips tables already registered, and logs each row as an import. At most 5,000 lines per import |

Every write asks for confirmation. The app itself never deletes rows (deleted schedules are marked deleted); it
deletes only the wrapper jobs it created for schedules, and a job's Delete hands its tables to the delete job (below).
To remove tables for good or prune old history,
see [Removing tables and old history](../docs/lakebase_schema.md#removing-tables-and-old-history).

## Schedules

A Databricks job schedule can't pass parameters, so the Databricks ingestion job's own schedule can only run its
default collection. Schedules in the app fill that gap; the app shows each schedule as an ingestion job:

- **A schedule** has a cadence (every 5 minutes to daily; each cadence divides a day), a time zone,
  Databricks job parameters (any of the job's tuning parameters, such as `parallelism`, `table_retries` or
  `max_queries_per_source`; the rest keep the job's defaults), optional failure e-mails, and one or more member
  collections. A collection can be in more than one schedule.
- **One wrapper job per member.** The app creates a small Databricks job for each (schedule,
  collection): a cron schedule and one `run_job` task that starts the Databricks ingestion job with
  `task_collection` set. Wrapper jobs need no compute of their own, and every run still happens in the
  one bundle-deployed Databricks ingestion job, so a redeploy upgrades every schedule. They're named
  `lakefed_ingest schedule: <schedule> / <collection>` and tagged `lakefed_ingest_schedule`,
  `lakefed_ingest_collection` and `lakefed_ingest_app` (the app's Lakebase `<instance>/<database>`, so
  apps of other deployments in the workspace leave them alone). Skipped triggers send no failure e-mail.
- **Overlap.** Wrapper jobs allow one run at a time and don't queue: if a collection's run is still going
  at its next trigger, Databricks skips that trigger (the Timeline marks it), and the next run catches
  up. Separate jobs per collection mean one slow collection never delays the others. Manual and
  app-started runs are still guarded by the collection's run lock.
- **Staggering** (on by default) spreads members' start times over the cadence (e.g. :00 and :07 for two
  collections every 15 minutes), so cluster starts and source queries don't all land at once.
- **Sync and drift.** Every create, edit, pause, resume and delete syncs the wrapper jobs right away and is
  logged on the Changes page. "Check jobs" reports jobs edited or deleted outside the app; "Sync jobs"
  recreates missing ones and undoes outside edits. A sync reconciles by tag: it adopts a member's job
  whose id was never recorded and deletes the schedule's jobs no member owns; a failed delete is retried
  by the next sync. Syncs of one schedule take turns (a Lakebase advisory lock).
- **Warnings** per member: p95 run time (7 days) close to or over the cadence (triggers will be skipped),
  skipped triggers in the last 24 hours, and "can't meet SLA" when the cadence plus the p95 run time is
  longer than the strictest `freshness_sla_minutes` among the collection's tables.

Permissions: the app's service principal owns the wrapper jobs it creates (creating jobs needs no extra
entitlement, and `run_job` tasks need no compute) and already has `CAN_MANAGE` on the ingestion job.
The group in `SCHEDULE_JOB_VIEW_GROUP` (default `users`; blank for owner and admins only) gets
`CAN_VIEW` on them. For more ingestion jobs (e.g. a larger cluster), add an app `job` resource and an
`INGEST_JOB_ID_<NAME>` environment variable; schedules can then pick that variant.

Limits: each running collection counts as one task (2,000 per workspace) plus one parent task while its
wrapper runs (750 per workspace), all collections are runs of the one ingestion job (its
`max_concurrent_runs`, default 100, up to 1,000), and each member is a saved job (12,000 per workspace).

**Check first** (the `check` variant, offered when `CHECK_JOB_ID` is set): one wrapper job for the whole
schedule instead of one per member. At the cadence it starts the check job, `lakefed_ingest_check`, with the
members as `task_collections`; the check asks each member's source databases whether anything changed,
stamps the unchanged ones, and starts the ingestion job only for members with work, each for its databases
with work, with run-now. A quiet cycle starts no ingest cluster. Members are not staggered (one check takes
them all), the schedule is one saved job and one parent task while the check runs, and switching a schedule
to or from `check` replaces its wrapper jobs.

**Who and when.** Each ingestion job shows who created it and when (`created_by`, `created_at`), and who last changed
it and when: an edit, pause or resume of the job, or any change to its groups' tables (`options.updated_by`,
`updated_at`). The job page shows them under its title; the Ingestion jobs list has them as Created and Last updated,
sortable by time. A job never changed since it was created shows its creator as Last updated by, as the Jobs list does; jobs last changed before the app recorded it show "unknown" for who.

**Its own cluster.** The job Edit view's Compute section sets the job's ingestion cluster, like a
Databricks job cluster: driver type, worker type and 0 to 8 workers, defaulting to the ingestion job's own. A run of
the ingestion job can't change its cluster, so a job with its own cluster runs its own copy of the ingestion job:
- on Save, the app starts `lakefed_ingest_cluster_copies`, which runs as the deployer and makes or updates the copy
  (the same tasks and grants, only those cluster settings changed), and records its id in the schedule's options;
- the app then syncs the job's wrappers to start the copy (on `check`, the check job is given the copy as
  `ingest_job_id`);
- back at the defaults, the job runs the ingestion job again and the copy is deleted;
- Sync jobs copies the ingestion job again, so a redeploy reaches the copies.

Changing the cluster needs Can Manage on the job's Databricks job, like any change.

**Deleting a job.** A group always belongs to a job, so a job's Delete leaves no groups behind. The app has no
`DELETE` privilege on the Lakebase tables, so the deletion is finished by the `lakefed_ingest_delete_job` job,
which runs as the deployer:
- the dialog (from `GET /api/ingestion-jobs/{id}/delete-preview`) says how many groups and tables go, the groups
  another job also runs (they stay), and has the tick box **Also delete the target tables**, with how many target
  tables only this job writes and the names of those another job's rows also write (kept);
- Delete is refused while a run of the job is going (Lakebase or the Jobs API sees one), and when the groups have
  tables but no delete job is configured;
- otherwise the app marks the job deleted and its groups removed, syncs (its wrapper jobs and any cluster copy go),
  and starts the delete job with `schedule_id` and `drop_targets`, recording the run in the schedule's options;
- the delete job deletes the groups' control, checkpoint, reseed_queue, database_check and check_work rows in one
  transaction under their run locks, and keeps run history and the change log. Ticked, it then drops the target
  tables only those groups write (`DROP TABLE IF EXISTS`, never a schema);
- the job stays listed on Ingestion jobs as **Deleting** until its delete job has finished and its groups are gone
  (the app reads the delete job's run, and marks the job `delete_finished` on the list read that sees both); the job
  page is read only meanwhile. A delete that failed, or whose delete job never started, shows **Delete failed** with
  the reason and **Run again** on the job page (with the same tick box);
- **Deleted jobs** on Ingestion jobs is history only: the finished deletes.

The same job purges New job's drafts (below) with `purge_drafts`: the app's poller starts it at most once an hour,
and only when a draft is due.

**Run now and Cancel run.** Neither shows an end state before Databricks confirms it. **Run now** reads **Starting…**
until the poller records a run going, and **Cancel run** reads **Cancelling…** until every run of the job has ended.
Both read the `databricks_run` table the poller keeps, with no timer on the page. After 15 minutes without a start, or
10 minutes without a cancel being confirmed, the page says Databricks has not confirmed it and keeps Run now off.

**The other actions wait the same way.** Add databases, Add tables and Create show done only once their tables are written and the
job's Databricks jobs are synced; a wrapper job that did not sync is named. Edit's Save shows saved only once the
wrapper jobs are synced and any cluster copies run has ended; a copies run that failed says the cluster was not applied.

## Discovery through the SQL warehouse

Discover on a source runs on a small serverless SQL warehouse the app bundle makes (`lakefed_ingest discovery`), not on
the discovery job's cluster, so each step answers in seconds rather than waiting minutes for a cluster. Every statement
is plain SQL sent to SQL Server through the Unity Catalog connection (`remote_query`); the app can't open the
connection itself (Unity Catalog lends its login only to Databricks compute), so the warehouse does it, as the app's
service principal.

| Step | What runs on the warehouse |
|---|---|
| The connection test (Next on the connection step) | one statement in master listing the server's online user databases (`source_catalog.DATABASES_SQL`); the filter works on the next step. A failed test shows its codes beside Next (Databricks' error class, SQL Server's error number, the SQLSTATE: `source_catalog.error_codes`), the source's message and what to do; Next moves on only once it passes |
| The database list | one statement per database (`warehouse_reads.ACCESS_SQL`): opening it is the check, and it counts its tables. Only the page shown is checked (another page or filter checks its own), so a server of 22,000 databases is never checked as a whole. A database the login can't open is greyed out, can't be ticked, and says why on hover; ticking the whole server leaves out the ones found so and says how many, and one on a page not looked at fails in the read with the server's reason, named before anything is added |
| Reading the chosen tables | one statement per database (`source_catalog.TABLE_SUMMARY_SQL`, one row per user table), saved to `discovered_table` with the same recommendations as the discovery job |

At most 16 statements of one discovery are going at once (`warehouse_reads.IN_FLIGHT`). Each request the page makes
does one step: it reads the statements going, saves the ones that ended and starts more. Where each database stands
is kept on its `discovery` row (`listing`: its statement, its claim, and a check's answer), and a request claims the
databases it starts, so two pages or app instances never start the same one twice.

The step's top line counts the choice: databases chosen of those listed and their tables (counting while some aren't
checked yet); the same numbers end the step's last line and the job's Chosen summary.

**New job reads the tables as they are chosen (drafts).** Ticking a whole database, or the whole server under its
filter, starts the read of its tables at once, while the rest of the job is filled in. The read is a draft: its rows
are the read's own `discovery` and `discovered_table` rows, marked `draft` in `listing`, and nothing of the job
(control, checkpoint, schedule, rollups) is written before Create.
- **Add the tables chosen** doesn't wait: the reads go on from New job's page, whose preview counts what is read so
  far and says how far the read is (asked every 2 seconds while a database is left).
- **Create** is enabled throughout. Pressed early, it waits for the rest of the read in a background task (its
  progress shows "reading the chosen databases' tables first"), then writes. A database that couldn't be read stops it
  by name before anything is written; one the whole server's read couldn't open is left out and counted.
- Create's transaction takes the draft: the mark comes off and the job is named on its rows.
- **Cancel**, leaving the page, or choosing the tables again before Create discards the draft
  (`POST /api/discoveries/{id}/discard`, by `sendBeacon` as the page closes): nothing more is read and its statements
  still going are cancelled. A draft nobody has asked about for two hours (`DRAFT_TTL_S`) counts as discarded.
- The app has no DELETE, so the delete job purges discarded drafts (`purge_drafts`; see Deleting a job). Nothing is
  kept for a job that wasn't created, once the next purge has run.
- Edit's Add tables reads the same way but still waits at Add, as before.

**The warehouse.** Serverless, PRO, set by three bundle variables: `discovery_warehouse_size` (default `2X-Small`),
`discovery_warehouse_max_clusters` (default 2: it scales out by clusters when statements queue, not by size) and
`discovery_warehouse_auto_stop_mins` (default 5). A serverless warehouse starts in a few seconds. 2X-Small is 4 DBUs an
hour per cluster while it runs: at a list price of about 0.70 USD per serverless SQL DBU, about 2.80 USD an hour, about
5.60 USD with both clusters; it stops after 5 idle minutes, so it costs only while someone chooses tables. Your
contract's price may differ. The app gets `CAN_USE` on it as an app resource (`WAREHOUSE_ID`).

**At a whole server's size** (estimates): a check takes about a second, and only the page shown (100 databases) is
checked, about 7 seconds at 16 at once whatever the server's size. A read of 1,000 tables is about 2 to 5 seconds per
database, about 1 to 2 hours for 22,000 at 16 at once; in New job it starts at the choice, so Create waits only for
what is left when it is pressed. Without a warehouse set (`WAREHOUSE_ID` empty), Discover runs the discovery job as before.

**What stays on the discovery job:** listing an Azure SQL server's databases through Azure (it takes a service
credential only Databricks compute holds; the tables are then read through the warehouse), and sources read by host
and password (no connection to send through). The job stays in the bundle for those.

## Re-discovery

A job made from a discovery can add new databases and tables by itself. On its Edit page, under When it runs,
**Add new databases and tables** is off by default, or daily at an hour in UTC. It's stored as the schedule option
`rediscover_hour`, and the job page shows the hour and the last run.

Once a day at that hour, the scheduler in the app process (a thread; `REDISCOVERY_TICK_S`, default 300 seconds, 0
turns it off) reads the job's selection again and adds what the rules allow. A server reached through a connection is
read through the discovery warehouse, as Add tables reads it (its databases listed again for a whole-server part,
then each database's tables); one listed through Azure, or read by host and password, runs the discovery job:

| The job was chosen as | Re-discovery adds |
|---|---|
| a whole server, with its filter | each new database matching the filter, with all its tables. New tables of a database the job already has are not added |
| a whole database | its new tables |
| specific tables | nothing |

What it adds goes through the same path as Add tables: the run locks, the checks, the job's own settings and the
usual group sizes. The change log records each added table under the user `re-discovery`. Tables the source no
longer has are kept and listed as vanished. A database that can't be opened fails alone and is listed under errors.

Other rules: a paused job is skipped; a run that meets a group already running is deferred to the next day; a job
made from an uploaded CSV has nothing to read again. Each run claims its job in SQL first, so two app instances
don't run the same job twice. The scheduler runs only while the app runs: a day missed while it was down runs on
the first tick after the hour.

**Run it now** on the job page starts the same run (it is refused while one is going). While it runs the page reads
pending from the discovery run in the poller, then **adding tables**, then done with what was found and added, or failed
with the reason. Run it now stays off until the run has ended; a discovery run past its wait is not confirmed, not done.

A whole-server part reads its servers again from `discovery.listing` (how the discovery listed databases: the LIKE
pattern, the Azure resource and the host secret template, never a secret). A discovery made before schema version
12 has no listing, so discover that server again from Add tables. A host whose databases take different secrets, with
no template recorded, can't have a whole-server part read again (discover it from Add tables); its whole-database
parts are read once per secret.

The job page's block shows the last run: its status (running, done, deferred or failed), when it started and
finished, what it found and added, what was skipped, the new databases, the vanished tables, and any errors.

## Guard rails for edits

Data integrity comes first, so edits go through the server's rules, not the browser's:

- **Preview first.** An edit shows, per table, what changes, whether the table reseeds and why,
  warnings, and validation errors. Apply is only offered for a clean preview of exactly that edit.
- **All or nothing.** An edit is one transaction: if any table fails validation, or a sink would
  collide with another control row, nothing changes.
- **Reseed when the meaning changes.** For `ct` and `cursor` tables, changing the source (host,
  port, database, schema, table, engine), the sink (catalog, schema, table, `client_id`) or the
  primary key flips the table to seeding in the same transaction. Without that, the next run would
  merge only new changes into the new place or match on the wrong key. Cosmetic changes (case, an
  explicit default port) don't reseed. Moving a table to another collection, credentials, priority
  and partition settings don't reseed.
- **Warnings for what an edit leaves behind.** For example, the old sink table after a sink change,
  rows of the old `client_id` in a shared sink, and removed `select_list` columns that stay in the sink.
- **Not while running.** Edits, enable / disable and reseeds are refused while an affected collection
  is running (they take that collection's run lock), so they can't race the run's own checkpoint writes.
- **Bulk edits are limited** to fields that are safe across many tables: collection, enabled, tier,
  priority, freshness SLA, partition settings, `jdbc_options`, sink catalog and schema. Source, key and sink table
  name changes are made one table at a time.
- **Change log.** Every change is recorded in `lakefed_ingest.control_change` with the signed-in
  user, in the same transaction.

The ingestion job backs this up for changes made outside the app (for example with SQL): each
checkpoint records the source, sink and key it was written for, and a table that now points
elsewhere, or whose sink table no longer exists, is reseeded instead of silently leaving a gap.

## How it's built

| Part | Where | What |
|---|---|---|
| Backend | `src/main.py` (FastAPI) | JSON API under `/api` (OpenAPI docs at `/api/docs`); serves the built frontend for every other path |
| Data access | `src/lakebase.py`, `src/jobs.py` | Lakebase (psycopg, SDK-minted credential) and the Jobs API, as the app's service principal |
| Run poller | `src/run_poller.py` | One poller (every 15 seconds, one instance at a time through an advisory lock) writes the `databricks_run` table; the pages read that table, and it closes the ingest runs whose Databricks run has ended. `RUN_POLLER=0` turns it off on an instance. Until the table exists, the app reads the Jobs API as before |
| SQL and rules | `src/queries.py` | Every statement (parameterized), form validation, bulk-import review logic |
| Frontend | `src/frontend/` (React, TypeScript, Vite) | Pages under `src/frontend/src/pages/`; React Router, TanStack Query, AG Grid Community for the grids |

Bulk registration is validated and merged on the server: the browser sends only the editable fields
(include, load type, keys, partitioning, sink, client_id). Connection details, secret keys and select
lists always come from the stored discovery.

Databricks Apps builds the frontend at deploy time (`npm install`, then `npm run build` into
`src/static/`), so build output isn't committed. `package-lock.json` pins every dependency.

## Security model

The app runs as its own service principal (created with the app):

- **Jobs:** `CAN_MANAGE` on the ingestion, check, setup and discovery jobs, through app resources, so it can
  read their access lists. This is a trade-off for the per-user checks below: starting runs needs only
  `CAN_MANAGE_RUN`, but the Permissions API returns a job's access list only to those who manage it, so the
  service principal could also edit or delete these jobs. The app never edits or deletes them. The cluster copies
  and delete jobs need only `CAN_MANAGE_RUN`: the app starts them and reads their runs.
- **Lakebase:** granted by the `lakefed_ingest_app_grants` job:
  - `CAN_USE` on the instance (to mint its own short-lived credential);
  - a Postgres role;
  - `SELECT`, `INSERT` and `UPDATE` on the `lakefed_ingest` tables, with no `DELETE`.
- **Source connections:** `USE CONNECTION` on each Unity Catalog connection named in the bundle's
  `source_connections` variable (comma-separated), granted by the same job as whoever runs it, who needs to own
  or manage those connections. The app lists connections as its own identity, so Discover shows only those it
  holds `USE CONNECTION` on; the connections aren't the bundle's, so they are named, not created. A connection
  the job can't grant on is reported in its output and the others are still granted; Discover's screen names
  the grant for any connection it can't see.
- **Discovery warehouse:** `CAN_USE` on the bundle's `lakefed_ingest discovery` SQL warehouse, through an app
  resource. The statements run as the service principal, which holds `USE CONNECTION` on the connections above.
- **Azure SQL listing (optional):** for a login that can't open master, Discover's connection test can have the
  discovery job list an Azure SQL server's (or Managed Instance's) databases through Azure Resource Manager instead.
  It takes a Unity Catalog service credential (Catalog Explorer, External data, Credentials) on an access connector
  whose managed identity has Reader on the server, `ACCESS` on that credential for whoever runs the discovery job,
  and the server's resource id, both typed under Azure SQL on the connection step. The app never calls Azure;
  each listed database is then read through the connection, and one the login can't open fails on its own.
- **Who can act:** every action on an existing ingestion job follows the signed-in user's own Databricks
  permission on the job its runs execute as (the check job on a check schedule, else the ingestion job), as the
  Jobs UI does:
  - running it (Run now), cancelling its run (Cancel run), or a new job that isn't paused: `CAN_MANAGE_RUN`;
  - changing it (Edit and Save, Pause, Resume, Sync jobs), its groups' tables (edit, enable, disable, reseed,
    Add table, Import CSV, Bulk import's register), or deleting it: `CAN_MANAGE` or owner;
  - a change to tables needs `CAN_MANAGE` on every ingestion job that runs one of their groups, since a group
    can be in more than one job and its tables change for all of them;
  - workspace admins may do all of these.

  Who the user is comes from the `X-Forwarded-Email` header the Databricks Apps proxy adds. Without it the app
  refuses every write and every action (403), so an unknown caller never passes a check. A user's groups are
  read from SCIM (their direct groups), and the groups those are in are followed up to 5 levels and 200 groups,
  so a grant to `data-eng` counts for a member of `team-x` inside it. A lookup that fails or passes those caps
  keeps the groups found so far: it can refuse something Databricks would allow, never the reverse.

  Manage and its endpoints (`/api/manage/...`, and starting the setup job) need `CAN_MANAGE` on the app itself (its
  Databricks permission; workspace admins have it). The service principal reads the app's access list through the
  Permissions API: where it isn't allowed that read, only workspace admins see Manage, and the refusal says why.

  Running the setup or discovery job, or cancelling a run, needs `CAN_MANAGE_RUN` on that job. Groups in no
  ingestion job, and new paused jobs, are open to everyone with `CAN_USE` on the app. The server checks every
  call, and the pages show a refused button disabled with the reason. Permissions are read again after 20
  seconds.

The app has no Lakebase `database` resource. Some workspaces refuse to attach one even for the
instance's manager ("User needs MANAGE permission on the resource"), and the grant job works
everywhere.

## Deploy

Prerequisites: the ingestion bundle is deployed and its setup job has run (Lakebase schema exists).

1. Add a target to `app/databricks.yml` for your workspace (the committed `dev` / `prod` hosts are
   placeholders). Set the variables that differ from the defaults:
   - `lakebase_instance`, `lakebase_database`: the ingestion bundle's values.
   - `ingest_job_id`, `setup_job_id`, `discover_job_id`, `check_job_id`, `cluster_copies_job_id`, `delete_job_id`:
     looked up by job name, so deploy the ingestion bundle first. In development mode the ingestion
     bundle prefixes job names (`[dev your_name] lakefed_ingest`), so set the ids or the prefixed
     names for dev targets.
   - `app_name`: 30 characters max.
   - `app_version`: what Manage shows as the version, by default the git commit deployed from. It is blank when the
     bundle is deployed from a folder that isn't a git checkout: pass `--var app_version=<commit>` there.
2. Deploy and start the app, then grant it Lakebase access. Run these from `app/`, as the user who
   manages the Lakebase instance and owns the tables (whoever ran the ingestion setup):

   ```bash
   cd app
   databricks bundle deploy -t <target>
   databricks bundle run lakefed_ingest_app -t <target>          # deploys the code and starts the app
   databricks bundle run lakefed_ingest_app_grants -t <target>   # Lakebase access, and USE CONNECTION on source_connections
   ```
3. Grant `CAN_USE` on the app to your operators, then open the URL printed by `bundle run`.

Re-run the grant job if the app is recreated (new service principal). Default privileges cover
tables the same owner adds later, so schema upgrades don't need it.

The grant job defaults to serverless jobs compute (it only calls the workspace APIs and Lakebase). If
the workspace doesn't have serverless jobs, run it on an existing all-purpose cluster in your target
(it's a one-time job, so a running cluster beats waiting for a job cluster to start):

```yaml
targets:
  <target>:
    resources:
      jobs:
        lakefed_ingest_app_grants:
          tasks:
            - task_key: grant_app_access
              environment_key: ""                 # clears the serverless environment
              existing_cluster_id: <cluster-id>   # an all-purpose cluster you can attach to
              libraries:                          # installed on that cluster
                - pypi:
                    package: "psycopg[binary]"
                - pypi:
                    package: "databricks-sdk==0.145.0"
```

## Develop locally

```bash
cd app/src
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt pytest httpx
npm install
export DATABRICKS_CONFIG_PROFILE=<profile> LAKEBASE_INSTANCE=<instance> LAKEBASE_DATABASE=<db> \
       INGEST_JOB_ID=<id> SETUP_JOB_ID=<id> DISCOVER_JOB_ID=<id>
.venv/bin/python main.py          # API on :8000 (serves the built frontend from static/)
npm run dev                       # frontend with hot reload on :5173, proxying /api to :8000
npm run check                     # type-check + production build
```

Locally the app connects as you. Tests:
- `pytest app/tests` covers the SQL and review logic, and runs as part of the repo's `pytest`;
- the API tests (`tests/api_test.py`) also run when FastAPI and httpx are installed, as in the venv above.

## Not in this version

- Per-user authorization: everyone with `CAN_USE` acts as the app's service principal.
- Discovery for sources other than SQL Server: add their control rows with Add table or SQL.

## License

See [Help and License](../README.md#help) in the repo README, including the licenses of the
third-party libraries this app uses.
