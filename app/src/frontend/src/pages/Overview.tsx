import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api, qs, type Row } from "../api";
import {
  ErrorBox, FillBox, JobStatus, Name, Notice, PageHead, Pager, ProgressBar, RunStatus, Section, Stamp, StateBar, StateKey,
  fmtClock, fmtDuration, fmtInt, fmtSpeed, jobRunName, jobRunPath, plural, progressFrom, progressText
} from "../components";

interface Overview {
  jobs: Row[]; jobs_total: number; offset: number; limit: number; not_in_job: Row | null;
  running_job_runs: Row[]; running_total: number; running_not_in_job: Row[];
  attention: Row[]; attention_total: number; totals: Row; read_at: string; cache_s: number;
}

const jobPath = (id: unknown) => `/ingestion-jobs/${encodeURIComponent(String(id))}`;

/** A job's last finished job run: its outcome, failed tables and how long it took. */
function LastJobRun({ job }: { job: Row }) {
  const last = job.last_job_run;
  if (!last) return <span className="muted">none yet</span>;
  return (
    <>
      <Link to={jobRunPath(job.id, last.slot)}><RunStatus status={null} outcome={last.outcome} /></Link>
      <span className="sub">{fmtClock(last.slot)}{last.took_s != null && `, took ${fmtDuration(last.took_s)}`}
        {Number(last.failed_tables) > 0 && `, ${fmtInt(last.failed_tables)} failed`}</span>
    </>
  );
}

/** Every ingestion job, failing first, with its tables by state and its Status; "Not in any job" last. */
export function JobsTable({ jobs, notInJob, total, offset, limit, onPage, link, fill }: {
  jobs: Row[]; notInJob?: Row | null; total: number; offset: number; limit: number; onPage: (offset: number) => void;
  link?: boolean; fill?: boolean }) {
  const table = (
      <div className="table-wrap"><table className="data">
        <thead><tr><th>Ingestion job</th><th className="num">Groups</th><th className="num">Databases</th>
          <th className="num">Tables</th><th>State now</th><th className="num">Failing</th><th className="num">Late</th>
          <th>Last job run</th><th>Status</th></tr></thead>
        <tbody>
          {jobs.map((j) => (
            <tr key={j.id ?? "none"}>
              <td className="name">{j.id != null ? <Link to={jobPath(j.id)}><b><Name>{j.name}</Name></b></Link> : <b><Name>{j.name}</Name></b>}
                <span className="sub">{j.id != null ? j.description : `${fmtInt(j.groups?.length)} ${plural(j.groups?.length, "group")}`}</span></td>
              <td className="num">{fmtInt(j.groups?.length)}</td><td className="num">{fmtInt(j.databases)}</td>
              <td className="num">{fmtInt(j.enabled_tables)}</td><td><StateBar row={j} /></td>
              <td className="num" style={{ color: Number(j.state_failing) ? "var(--bad)" : undefined }}>{fmtInt(j.state_failing)}</td>
              <td className="num">{fmtInt(j.state_late)}</td>
              <td>{j.id != null ? <LastJobRun job={j} /> : <Link to="/runs?job=none">its group runs</Link>}</td>
              <td><JobStatus status={j.status} /></td>
            </tr>
          ))}
          {notInJob && (
            <tr className="total">
              <td className="name"><b>Not in any job</b><span className="sub">{notInJob.groups.join(", ")}</span></td>
              <td className="num">{fmtInt(notInJob.groups.length)}</td><td className="num">{fmtInt(notInJob.databases)}</td>
              <td className="num">{fmtInt(notInJob.enabled_tables)}</td><td><StateBar row={notInJob} /></td>
              <td className="num" style={{ color: Number(notInJob.state_failing) ? "var(--bad)" : undefined }}>{fmtInt(notInJob.state_failing)}</td>
              <td className="num">{fmtInt(notInJob.state_late)}</td>
              <td><Link to="/runs?job=none">its group runs</Link></td><td><span className="muted">run by hand or by the Databricks job</span></td>
            </tr>
          )}
          {!jobs.length && !notInJob && <tr><td colSpan={9} className="muted">No ingestion jobs here.</td></tr>}
        </tbody>
      </table></div>
  );
  const pager = <Pager offset={offset} limit={limit} total={total} noun="ingestion job" onPage={onPage} />;
  if (fill) return <>{pager}<FillBox min={240}>{table}</FillBox></>;
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
      {!jobs.length && <Notice tone="ok">No job has failing tables.</Notice>}
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

export default function OverviewPage() {
  const [offset, setOffset] = useState(0);
  const ov = useQuery({ queryKey: ["overview", offset], queryFn: () => api.get<Overview>(`/overview${qs({ offset })}`),
                        refetchInterval: 30_000 });
  const d = ov.data;
  const t = d?.totals ?? {};
  const enabled = Number(t.enabled_tables || 0);
  const pct = enabled ? ((100 * Number(t.state_current || 0)) / enabled).toFixed(1) : "0.0";
  const jobs = Number(t.jobs || 0);
  const title = !d ? "Overview"
    : `${fmtInt(jobs)} ${plural(jobs, "ingestion job")}: ${fmtInt(t.running_jobs)} running, ${fmtInt(t.failing_jobs)} with failing tables, `
      + `${fmtInt(t.late_jobs)} with late data`;
  const unscheduled = d?.not_in_job?.groups?.length ?? 0;
  return (
    <>
      <PageHead crumb={<span className="eyebrow">Overview · Is everything healthy now?</span>} title={title}
        lead={d && (enabled ? <>{pct}% of <strong>{fmtInt(enabled)}</strong> enabled {plural(enabled, "table")} {enabled === 1 ? "is" : "are"} current,
          in <strong>{fmtInt(t.groups)}</strong> {plural(t.groups, "ingestion group")} and <strong>{fmtInt(t.databases)}</strong>{" "}
          {plural(t.databases, "database")} on <strong>{fmtInt(t.servers)}</strong> {plural(t.servers, "server")}
          {unscheduled > 0 && <>; {fmtInt(unscheduled)} {plural(unscheduled, "group")} in no job</>}.</> : "No enabled tables yet.")}
        right={<Stamp at={d?.read_at} every={30} error={ov.error} />} />
      <ErrorBox error={ov.error} />
      {d && !enabled && <Notice tone="info">No tables are set up yet. Add them with <Link to="/bulk">Bulk import</Link> or <Link to="/add">Add table</Link>.</Notice>}
      {/* Only once freshness targets are in use: until then no table can be late, and there is nothing to warn about. */}
      {Number(t.no_sla_tables) > 0 && Number(t.no_sla_tables) < enabled && (
        <Notice tone="warn">{fmtInt(t.no_sla_tables)} enabled {plural(t.no_sla_tables, "table")} {Number(t.no_sla_tables) === 1 ? "has" : "have"} no
          freshness target, so {Number(t.no_sla_tables) === 1 ? "it is" : "they are"} never flagged as late. To set one: Tables, select the
          tables, Bulk edit, then Freshness target.</Notice>
      )}
      <div className="metrics">
        <div className="metric"><div className="metric-label">Ingestion jobs</div>
          <div className="metric-value">{fmtInt(jobs)}<small>{plural(jobs, "job")}</small></div>
          <div className="metric-sub">{unscheduled ? `and ${fmtInt(unscheduled)} ${plural(unscheduled, "group")} in no job` : "every group is in one"}</div></div>
        <div className="metric run"><div className="metric-label">Running now</div>
          <div className="metric-value">{fmtInt(d?.running_total)}<small>{plural(d?.running_total, "job run")}</small></div>
          <div className="metric-sub">{d?.running_not_in_job.length ? `and ${fmtInt(d.running_not_in_job.length)} group runs in no job` : "now"}</div></div>
        <div className={`metric ${Number(t.failing_jobs) ? "bad" : ""}`}><div className="metric-label">Failing</div>
          <div className="metric-value">{fmtInt(t.failing_jobs)}<small>{plural(t.failing_jobs, "job")}</small></div>
          <div className="metric-sub">{fmtInt(t.state_failing)} {plural(t.state_failing, "table")} failing now</div></div>
        <div className={`metric ${Number(t.late_jobs) ? "warn" : ""}`}><div className="metric-label">Late data</div>
          <div className="metric-value">{fmtInt(t.late_jobs)}<small>{plural(t.late_jobs, "job")}</small></div>
          <div className="metric-sub">{fmtInt(t.state_late)} {plural(t.state_late, "table")} past their freshness target</div></div>
      </div>
      <Section title="Ingestion jobs" note="Each job is a schedule and its ingestion groups. Failing first, then late, then by name."
        actions={<StateKey />}>
        {d && <JobsTable jobs={d.jobs} notInJob={offset + d.limit >= d.jobs_total ? d.not_in_job : null} total={d.jobs_total}
          offset={offset} limit={d.limit} onPage={setOffset} link />}
      </Section>
      {d && <RunningNow runs={d.running_job_runs} total={d.running_total} notInJob={d.running_not_in_job} />}
      {d && <NeedsAttention jobs={d.attention} total={d.attention_total} />}
    </>
  );
}
