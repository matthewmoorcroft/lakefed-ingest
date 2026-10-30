import { type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api, type Row } from "../api";
import {
  ErrorBox, FillBox, JobStatus, Name, Notice, PageHead, Pager, ProgressBar, RunStatus, Section, Stamp, StateBar,
  fmtClock, fmtDuration, fmtInt, fmtShort, fmtSlot, fmtSpeed, fmtTime, jobRunName, jobRunPath, plural, progressFrom, progressText
} from "../components";

interface Overview {
  jobs: Row[]; jobs_total: number; offset: number; limit: number; not_in_job: Row | null;
  running_job_runs: Row[]; running_total: number; running_not_in_job: Row[];
  attention: Row[]; attention_total: number; totals: Row; read_at: string; cache_s: number;
  next_triggers: Row[]; paused_jobs: number; rows_24h: { rows: number; partial: boolean };
}

const jobPath = (id: unknown) => `/ingestion-jobs/${encodeURIComponent(String(id))}`;

/** A job's last job run that ran, however long ago: its outcome, when, failed tables and how long it took. A
 * trigger that did not run is never shown here; the job's own page lists every trigger. */
function LastJobRun({ job }: { job: Row }) {
  const last = job.last_job_run;
  if (!last) return <span className="muted">Never run</span>;
  return (
    <>
      <Link to={jobRunPath(job.id, last.slot)}><RunStatus status={null} outcome={last.outcome} /></Link>
      <span className="sub">{fmtSlot(last.slot)}{last.took_s != null && `, took ${fmtDuration(last.took_s)}`}
        {Number(last.failed_tables) > 0 && `, ${fmtInt(last.failed_tables)} failed`}</span>
    </>
  );
}

/** Every ingestion job, failing first, with its tables by state and its Status; "Not in any job" last. */
/** The Ingestion jobs list's sort: a time column and its direction, or none for the list's own order. */
export interface JobSort { by: "" | "created_at" | "updated_at"; descending: boolean }

export function JobsTable({ jobs, notInJob, total, offset, limit, onPage, link, fill, sort, onSort }: {
  jobs: Row[]; notInJob?: Row | null; total: number; offset: number; limit: number; onPage: (offset: number) => void;
  link?: boolean; fill?: boolean; sort?: JobSort; onSort?: (sort: JobSort) => void }) {
  // As the Databricks Jobs list: who created each job, and its last change, sortable by time.
  const audit = !!onSort;
  const sortHead = (by: JobSort["by"], label: string) => {
    const on = sort?.by === by;
    return <th><button className="th-sort" aria-sort={on ? (sort!.descending ? "descending" : "ascending") : "none"}
      onClick={() => onSort!({ by, descending: on ? !sort!.descending : true })}>{label}{on ? (sort!.descending ? " ↓" : " ↑") : ""}</button></th>;
  };
  // The user name before the @, the full address on hover, so both columns fit as the Jobs list's Created by does.
  const who = (by: unknown, at: unknown) => <>{by ? <span title={String(by)}>{String(by).split("@")[0]}</span> : <span className="muted">unknown</span>}
    {at ? <span className="sub" title={fmtTime(at)}>{fmtShort(at)}</span> : null}</>;
  const table = (
      <div className="table-wrap"><table className={audit ? "data fit" : "data"}>
        <thead><tr><th>Ingestion job</th><th className="num">Groups</th>{!audit && <th className="num">Databases</th>}
          <th className="num">Tables</th><th>State now</th><th className="num">Failing</th><th className="num">Late databases</th>
          <th>Last job run</th><th>Status</th>
          {audit && <>{sortHead("created_at", "Created")}{sortHead("updated_at", "Last updated")}</>}</tr></thead>
        <tbody>
          {jobs.map((j) => (
            <tr key={j.id ?? "none"}>
              <td className={audit ? "name capped" : "name"}>{j.id != null ? <Link to={jobPath(j.id)}><b><Name>{j.name}</Name></b></Link> : <b><Name>{j.name}</Name></b>}
                <span className="sub">{j.id != null ? j.description : `${fmtInt(j.groups?.length)} ${plural(j.groups?.length, "group")}`}</span></td>
              <td className="num">{fmtInt(j.groups?.length)}</td>{!audit && <td className="num">{fmtInt(j.databases)}</td>}
              <td className="num">{fmtInt(j.enabled_tables)}</td><td><StateBar row={j} /></td>
              <td className="num" style={{ color: Number(j.state_failing) ? "var(--bad)" : undefined }}>{fmtInt(j.state_failing)}</td>
              <td className="num">{fmtInt(j.late_databases)}</td>
              <td>{j.id != null ? <LastJobRun job={j} /> : <Link to="/runs?job=none">its group runs</Link>}</td>
              <td><JobStatus status={j.status} /></td>
              {audit && <><td>{who(j.created_by, j.created_at)}</td><td>{who(j.updated_by, j.updated_at)}</td></>}
            </tr>
          ))}
          {notInJob && (
            <tr className="total">
              <td className="name"><Link to="/ingestion-jobs/not-in-job"><b>Not in any job</b></Link><span className="sub">{notInJob.groups.join(", ")}</span></td>
              <td className="num">{fmtInt(notInJob.groups.length)}</td>{!audit && <td className="num">{fmtInt(notInJob.databases)}</td>}
              <td className="num">{fmtInt(notInJob.enabled_tables)}</td><td><StateBar row={notInJob} /></td>
              <td className="num" style={{ color: Number(notInJob.state_failing) ? "var(--bad)" : undefined }}>{fmtInt(notInJob.state_failing)}</td>
              <td className="num">{fmtInt(notInJob.late_databases)}</td>
              <td><Link to="/runs?job=none">its group runs</Link></td><td><span className="muted">run by hand or by the Databricks job</span></td>
              {audit && <><td></td><td></td></>}
            </tr>
          )}
          {!jobs.length && !notInJob && <tr><td colSpan={audit ? 10 : 9} className="muted">No ingestion jobs here.</td></tr>}
        </tbody>
      </table></div>
  );
  const pager = <Pager offset={offset} limit={limit} total={total} noun="ingestion job" onPage={onPage} />;
  if (fill) return <><FillBox min={240} reserve={44}>{table}</FillBox>{pager}</>;
  return (
    <>
      {table}
      {pager}
      {link && <p className="muted small"><Link to="/ingestion-jobs">All ingestion jobs</Link>, with filters.</p>}
    </>
  );
}

/** The job runs going now, longest running first: group runs, one bar over all the job's tables, speed now,
 * expected end, and running for against typical. Groups in no job list their group runs apart. */
export function RunningNow({ runs, total, notInJob }: { runs: Row[]; total: number; notInJob: Row[] }) {
  return (
    <Section title={`Running now: ${fmtInt(total)} ${plural(total, "job run")}`}
      note="Each bar covers all of the job's tables, its groups that have not started yet included."
      actions={<Link to="/runs">All runs</Link>}>
      {!runs.length && !notInJob.length && <p className="muted">Nothing is running.</p>}
      {runs.map((r) => {
        const p = progressFrom(r.progress);
        return (
          <div key={`${r.job_id}-${r.slot}`} className="run-row">
            <div><div className="title"><Link to={jobRunPath(r.job_id, r.slot)}>{jobRunName(r.job_name, r.slot)}</Link></div>
              <div className="kv"><span>{fmtInt(r.finished_group_runs)} finished</span><span>{fmtInt(r.running_group_runs)} running</span>
                <span>{fmtInt(r.waiting)} waiting</span></div></div>
            <div><div className="count">{progressText(p)}</div><ProgressBar p={p} /></div>
            <div className="kv"><span>Speed now <b>{r.speed_now ? `${fmtSpeed(r.speed_now.tables_per_min)} tables/min` : "not yet"}</b></span>
              <span>Ends about <b>{r.expected_end ? fmtClock(r.expected_end) : "unknown"}</b></span>
              <span>{r.running_s != null ? <>Running for <b>{fmtDuration(r.running_s)}</b></> : "No group run has started yet"}
                {r.typical_s != null && <> (typical {fmtDuration(r.typical_s)})</>}</span></div>
          </div>
        );
      })}
      {runs.length < total && <p className="muted small">{fmtInt(runs.length)} of {fmtInt(total)} running; <Link to="/runs">All runs</Link>.</p>}
      {notInJob.map((r) => (
        <div key={r.run_id} className="run-row">
          <div><div className="title"><Link to={`/runs/${r.run_id}`}>{r.task_collection}, group run {fmtInt(r.run_id)}</Link></div>
            <div className="kv"><span>Not in any job</span></div></div>
          <div><div className="count">{progressText(progressFrom({ total: r.enabled_tables, loaded: r.tables_ok,
            unchanged: Math.max(0, Number(r.tables_checked || 0) - Number(r.tables_ok || 0)), in_flight: r.tables_running, failed: r.tables_failed }))}</div></div>
          <div className="kv"><span>Running for <b>{fmtDuration(r.elapsed_s)}</b></span></div>
        </div>
      ))}
    </Section>
  );
}

/** Jobs with tables failing now, or whose last job run ended without finishing, worst first, with their causes.
 * Late data is not listed here: it is a count on the job, with its reason. */
export function NeedsAttention({ jobs, total }: { jobs: Row[]; total: number }) {
  return (
    <Section title={`Needs attention: ${fmtInt(total)} ${plural(total, "job")}`}
      note="Jobs with tables failing now, or whose last job run ended without finishing. Late data is counted on each job instead."
      actions={total > jobs.length ? <Link to="/ingestion-jobs?filter=failing">All {fmtInt(total)}</Link> : undefined}>
      {!jobs.length && <p className="muted">No job has failing tables.</p>}
      {jobs.map((j) => (
        <div key={j.id} className="attention-row">
          <div className="title"><Link to={jobPath(j.id)}>{j.name}</Link>{" "}
            <span className="muted">{fmtInt(j.state_failing)} {plural(j.state_failing, "table")} failing
              {Number(j.failed_in_a_row) > 1 && `, in each of the last ${fmtInt(j.failed_in_a_row)} job runs`}</span></div>
          {(j.causes ?? []).map((c: Row) => (
            <div key={c.signature ?? c.label} className="kv"><span><b>{c.label}</b>: {fmtInt(c.tables)} {plural(c.tables, "table")}</span>
              {c.first_at && <span>first {fmtClock(c.first_at)}</span>}</div>
          ))}
          {j.last_job_run && j.last_job_run.outcome.label === "Ended without finishing" && <div className="kv">
            <span>Its last job run ended without finishing: <Link to={jobRunPath(j.id, j.last_job_run.slot)}>{jobRunName(j.name, j.last_job_run.slot)}</Link></span></div>}
        </div>
      ))}
    </Section>
  );
}

/** Every enabled table by state, each state linking to the tables in it, as the Jobs UI's run summary counts. */
function Freshness({ totals }: { totals: Row }) {
  const states: [string, string, string, string][] = [
    ["state_loaded", "seg-loaded", "Loaded", "/tables"],
    ["state_first_load", "seg-waiting", "Waiting for a first load", "/tables?status=seeding"],
    ["state_failing", "seg-failing", "Failing", "/tables?status=failing"]];
  return (
    <Section title="Freshness" note="Every enabled table of every job, by its state now. A database is late when one of its tables is past its freshness target; its database page lists those tables.">
      <StateBar row={totals} full />
      <div className="key" style={{ marginTop: 8 }}>
        {states.map(([key, cls, label, to]) => (
          <span key={key}><i className={cls} /><Link to={to}>{label}</Link> <b>{fmtInt(totals[key])}</b></span>
        ))}
        <span><i className="seg-late" /><Link to="/ingestion-jobs?filter=late">Late databases</Link> <b>{fmtInt(totals.late_databases)}</b></span>
      </div>
    </Section>
  );
}

/** The next scheduled triggers across every job, soonest first; paused jobs have none. */
function NextScheduled({ triggers, paused }: { triggers: Row[]; paused: number }) {
  return (
    <Section title="Next scheduled" actions={<Link to="/schedules-and-jobs">Schedules and jobs</Link>}>
      {!triggers.length && <p className="muted">No trigger is scheduled{paused ? `: ${fmtInt(paused)} ${paused === 1 ? "job is" : "jobs are"} paused` : ""}.</p>}
      {triggers.map((t) => (
        <div key={t.id} className="attention-row">
          <div className="title"><Link to={jobPath(t.id)}>{t.name}</Link> <span className="muted">{fmtTime(t.next_start)}</span></div>
          <div className="kv"><span>{t.description}</span></div>
        </div>
      ))}
      {triggers.length > 0 && paused > 0 && <p className="muted small">{fmtInt(paused)} paused {plural(paused, "job")} not shown.</p>}
    </Section>
  );
}

/** Each job's last job run that ran, however long ago, newest first, each opening its job run, as the Jobs list
 * shows each job's last run. A trigger that did not run is never shown here; the job's own page lists every trigger. */
function RecentRuns({ jobs }: { jobs: Row[] }) {
  const last = jobs.filter((j) => j.last_job_run).sort((a, b) => String(b.last_job_run.slot).localeCompare(String(a.last_job_run.slot)))
    .slice(0, 6);
  return (
    <Section title="Recent runs" note="Each job's last job run." actions={<Link to="/runs">All runs</Link>}>
      {!last.length && <p className="muted">No job run has finished yet.</p>}
      {last.map((j) => {
        const r = j.last_job_run;
        return (
          <div key={j.id} className="attention-row">
            <div className="title"><Link to={jobRunPath(j.id, r.slot)}><RunStatus status={null} outcome={r.outcome} /></Link>{" "}
              <Link to={jobPath(j.id)}>{j.name}</Link></div>
            <div className="kv"><span>{fmtSlot(r.slot)}</span>{r.took_s != null && <span>took {fmtDuration(r.took_s)}</span>}
              {Number(r.failed_tables) > 0 && <span>{fmtInt(r.failed_tables)} {plural(r.failed_tables, "table")} failed</span>}</div>
          </div>
        );
      })}
    </Section>
  );
}

/** A status summary, as the Databricks home and Jobs pages open: totals, what runs now, what needs a look, how fresh
 * the tables are, what runs next and what ran last, each item linking to its job or run. The jobs themselves are
 * listed on Ingestion jobs. */
export default function OverviewPage() {
  const ov = useQuery({ queryKey: ["overview"], queryFn: () => api.get<Overview>("/overview"), refetchInterval: 15_000 });
  const d = ov.data;
  const t = d?.totals ?? {};
  const enabled = Number(t.enabled_tables || 0);
  const pct = enabled ? ((100 * Number(t.state_loaded || 0)) / enabled).toFixed(1) : "0.0";
  const jobs = Number(t.jobs || 0);
  const unscheduled = d?.not_in_job?.groups?.length ?? 0;
  const tiles: [string, string, ReactNode, ReactNode][] = d ? [
    ["Ingestion jobs", "/ingestion-jobs", <>{fmtInt(jobs)}<small>{plural(jobs, "job")}</small></>,
      <>{fmtInt(t.running_jobs)} running · {fmtInt(d.paused_jobs)} paused{unscheduled ? ` · ${fmtInt(unscheduled)} ${plural(unscheduled, "group")} in no job` : ""}</>],
    ["Databases", "/sources", <>{fmtInt(t.databases)}<small>{plural(t.databases, "database")}</small></>,
      <>on {fmtInt(t.servers)} {plural(t.servers, "server")}</>],
    ["Tables", "/tables", <>{fmtInt(enabled)}<small>enabled</small></>, <>{pct}% loaded</>],
    ["Rows written, 24 hours", "/runs", <>{d.rows_24h.partial ? "at least " : ""}{fmtInt(d.rows_24h.rows)}<small>rows</small></>,
      <>by every job's runs</>]] : [];
  return (
    <>
      <PageHead title="Overview"
        lead={d && (enabled ? <>{pct}% of <strong>{fmtInt(enabled)}</strong> enabled {plural(enabled, "table")} {enabled === 1 ? "is" : "are"} loaded,
          in <strong>{fmtInt(jobs)}</strong> {plural(jobs, "ingestion job")}.</> : "No enabled tables yet.")}
        right={<Stamp at={d?.read_at} every={30} error={ov.error} />} />
      <ErrorBox error={ov.error} />
      {d && !enabled && <Notice tone="info">No tables are set up yet. <Link to="/ingestion-jobs/new">New ingestion job</Link> makes a job with its tables.</Notice>}
      {/* Only once freshness targets are in use: until then no table can be late, and there is nothing to warn about. */}
      {Number(t.no_sla_tables) > 0 && Number(t.no_sla_tables) < enabled && (
        <Notice tone="warn">{fmtInt(t.no_sla_tables)} enabled {plural(t.no_sla_tables, "table")} {Number(t.no_sla_tables) === 1 ? "has" : "have"} no
          freshness target, so {Number(t.no_sla_tables) === 1 ? "it is" : "they are"} never flagged as late. To set one: Tables, select the
          tables, Bulk edit, then Freshness target.</Notice>
      )}
      {/* Until the first read, the tiles show a dash, never zeros that look like real counts. */}
      <div className="metrics">
        {d ? tiles.map(([label, to, value, sub]) => (
          <Link key={label} to={to} className="metric metric-link"><div className="metric-label">{label}</div>
            <div className="metric-value">{value}</div><div className="metric-sub">{sub}</div></Link>
        )) : ["Ingestion jobs", "Databases", "Tables", "Rows written, 24 hours"].map((label) => (
          <div key={label} className="metric"><div className="metric-label">{label}</div><div className="metric-value muted">–</div></div>))}
      </div>
      {d && <RunningNow runs={d.running_job_runs} total={d.running_total} notInJob={d.running_not_in_job} />}
      {d && <div className="split">
        <div>
          <NeedsAttention jobs={d.attention} total={d.attention_total} />
          <Freshness totals={t} />
        </div>
        <div>
          <NextScheduled triggers={d.next_triggers} paused={d.paused_jobs} />
          <RecentRuns jobs={d.jobs} />
        </div>
      </div>}
    </>
  );
}
