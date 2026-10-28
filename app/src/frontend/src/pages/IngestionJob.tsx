import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate, useParams } from "react-router-dom";
import { api, qs, type Row } from "../api";
import { Refused } from "../permissions";
import {
  Badge, ErrorBox, Menu, Name, Notice, PageHead, RunStatus, Section, StateBar, StateKey, fmtClock, fmtDuration, fmtInt, fmtSlot, fmtSpeed,
  fmtTime, jobRunPath, plural, useConfirm, type ConfirmRequest
} from "../components";

const PAGE = 25;

/** A job run's rows written: "at least" when some loads recorded no count. */
export const rowsText = (r: Row) => (r.rows_partial ? `at least ${fmtInt(r.rows_written)}` : fmtInt(r.rows_written));

/** A trigger that did not run because its job is paused: the Databricks Runs list shows no row for it. */
export const pausedNotRun = (r: Row) => r.outcome?.label === "Not run" && !!r.outcome?.paused;

/** The runs as table rows, each stretch of a paused job's triggers that did not run folded into one quiet line. */
function foldPaused(runs: Row[]): (Row | { folded: Row[] })[] {
  const out: (Row | { folded: Row[] })[] = [];
  for (const r of runs) {
    const last = out[out.length - 1] as { folded?: Row[] } | undefined;
    if (pausedNotRun(r) && last?.folded && last.folded[0].job_id === r.job_id) last.folded.push(r);
    else out.push(pausedNotRun(r) ? { folded: [r] } : r);
  }
  return out;
}

/** One row per job run: outcome, group runs, took, tables loaded, speed, rows written, parallelism, clusters, failed. */
export function JobRunsTable({ runs, showJob }: { runs: Row[]; showJob?: boolean }) {
  const columns = showJob ? 11 : 10;
  return (
    <div className="table-wrap"><table className="data dense">
      <thead><tr><th>Job run</th>{showJob && <th>Ingestion job</th>}<th>Outcome</th><th className="num">Group runs</th>
        <th className="num">Took</th><th className="num">Tables loaded</th><th className="num">Tables/min</th>
        <th className="num">Rows written</th><th className="num">Parallelism</th><th className="num">Clusters</th>
        <th className="num">Failed</th></tr></thead>
      <tbody>
        {foldPaused(runs).map((r) => "folded" in r ? (
          <tr key={`paused-${r.folded[0].job_id}-${r.folded[0].slot}`}>
            <td colSpan={columns} className="muted">Paused: {fmtInt(r.folded.length)} {plural(r.folded.length, "trigger")} did not run,{" "}
              {r.folded.length > 1 ? `${fmtSlot(r.folded[r.folded.length - 1].slot)} to ${fmtSlot(r.folded[0].slot)}` : fmtSlot(r.folded[0].slot)}
              {showJob && <>{" "}(<Link to={`/ingestion-jobs/${r.folded[0].job_id}`}>{r.folded[0].job_name}</Link>)</>}</td>
          </tr>
        ) : (
          <tr key={`${r.job_id}-${r.slot}`}>
            <td className="name"><Link to={jobRunPath(r.job_id, r.slot)}>{new Date(r.slot).toLocaleString([], { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" })}</Link></td>
            {showJob && <td className="name"><Link to={`/ingestion-jobs/${r.job_id}`}><Name>{r.job_name}</Name></Link></td>}
            <td><RunStatus status={null} outcome={r.outcome} />{r.outcome?.note && <span className="sub">{r.outcome.note}</span>}</td>
            <td className="num">{fmtInt(r.ran)} of {fmtInt(r.members)}{Number(r.extra) > 0 && <span className="sub">{fmtInt(r.extra)} extra</span>}</td>
            <td className="num">{r.took_s != null ? fmtDuration(r.took_s) : r.running_s != null ? `${fmtDuration(r.running_s)} so far` : ""}</td>
            <td className="num">{fmtInt(r.loaded)}</td>
            <td className="num">{r.speed_tables_per_min != null ? fmtSpeed(r.speed_tables_per_min) : ""}</td>
            <td className="num">{rowsText(r)}</td>
            <td className="num">{(r.parallelism ?? []).join(", ")}</td><td className="num">{fmtInt(r.clusters)}</td>
            <td className="num" style={{ color: Number(r.failed_tables) ? "var(--bad)" : undefined }}>{fmtInt(r.failed_tables)}</td>
          </tr>
        ))}
        {!runs.length && <tr><td colSpan={columns} className="muted">No job runs in this window.</td></tr>}
      </tbody>
    </table></div>
  );
}

export default function IngestionJob() {
  const { id = "" } = useParams();
  const navigate = useNavigate();
  const [pages, setPages] = useState<string[]>([]);
  const before = pages[pages.length - 1];
  const qc = useQueryClient();
  const confirm = useConfirm();
  const runNow = useMutation({
    mutationFn: () => api.post<Row>(`/ingestion-jobs/${encodeURIComponent(id)}/run`, {}),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["ingestion-job", id] }),
  });
  const remove = useMutation({
    mutationFn: () => api.post<Row>(`/schedules/${encodeURIComponent(id)}/delete`, {}),
    onSuccess: () => { qc.invalidateQueries(); navigate("/ingestion-jobs"); },
  });
  const job = useQuery({ queryKey: ["ingestion-job", id], queryFn: () => api.get<Row>(`/ingestion-jobs/${encodeURIComponent(id)}`),
                         refetchInterval: 60_000 });
  const runs = useQuery({
    queryKey: ["ingestion-job-runs", id, before],
    queryFn: () => api.get<Row>(`/ingestion-jobs/${encodeURIComponent(id)}/runs${qs({ before, limit: PAGE })}`),
    refetchInterval: before ? false : 30_000,
  });
  const j = job.data;
  const last = j?.last_job_run;
  return (
    <>
      <PageHead crumb={<span className="eyebrow"><Link to="/overview">Overview</Link> · <Link to="/ingestion-jobs">Ingestion jobs</Link></span>}
        title={j ? <>{j.name} {j.paused && <Badge value="Paused" />}</> : "Ingestion job"}
        lead={j && <>{j.description}{j.stagger_to ? `, its groups starting up to ${j.stagger_to} minutes after that` : ""};{" "}
          <strong>{fmtInt(j.groups?.length)}</strong> {plural(j.groups?.length, "ingestion group")}, <strong>{fmtInt(j.databases)}</strong>{" "}
          {plural(j.databases, "database")}, <strong>{fmtInt(j.enabled_tables)}</strong> {plural(j.enabled_tables, "table")} on{" "}
          <strong>{fmtInt(j.servers)}</strong> {plural(j.servers, "server")}
          {j.parameters?.parallelism && <>, parallelism <strong>{j.parameters.parallelism}</strong></>}
          {j.job_variant && j.job_variant !== "default" && <>, Databricks job variant <strong>{j.job_variant}</strong></>}
.</>}
        right={j && <span className="card-actions">
          {/* As a Databricks job page: Delete in the more-actions menu, Edit beside Run now, Run now the primary action. */}
          <Menu label="⋮" title="More actions">
            <button className="danger-text" disabled={!!j.delete_blocked || remove.isPending} title={j.delete_blocked ?? undefined}
              onClick={() => confirm.ask(deleteJobRequest(j.name, j.groups?.length, j.enabled_tables)).then((ok) => ok && remove.mutate())}>{remove.isPending ? "Deleting…" : "Delete job"}</button>
          </Menu>
          <button disabled={!!j.change_blocked} title={j.change_blocked ?? undefined}
            onClick={() => navigate(`/ingestion-jobs/${id}/edit`)}>Edit</button>
          <button className="primary" disabled={!!j.run_now?.blocked || runNow.isPending} title={j.run_now?.blocked ?? undefined}
            onClick={() => confirm.ask(runNowRequest(j)).then((ok) => ok && runNow.mutate())}>{runNow.isPending ? "Starting…" : "Run now"}</button></span>} />
      {j && <p className="muted small">Created by {j.created_by ?? "unknown"}{j.created_at ? `, ${fmtTime(j.created_at)}` : ""} ·
        Last updated by {j.updated_by ?? "unknown"}{j.updated_at ? `, ${fmtTime(j.updated_at)}` : ""}</p>}
      {confirm.dialog}
      <Refused action="Run now" refusal={j?.run_now?.blocked} />
      <Refused action="Edit and Delete" refusal={j?.change_blocked} />
      <ErrorBox error={remove.error} />
      <ErrorBox error={runNow.error} />
      {runNow.isSuccess && <Notice tone="ok">Started: {fmtInt(runNow.data.started.length)} {plural(runNow.data.started.length, "Databricks job run")}. The runs show here as they start.</Notice>}
      <ErrorBox error={job.error} />
      {j && (
        <div className="metrics">
          <div className="metric"><div className="metric-label">Tables now</div><StateBar row={j} />
            <div className="metric-sub">{fmtInt(j.state_current)} current, {fmtInt(j.state_late)} late, {fmtInt(j.state_first_load)} waiting, {fmtInt(j.state_failing)} failing</div></div>
          <div className="metric"><div className="metric-label">Last job run</div>
            <div className="metric-value small-value">{last ? <Link to={jobRunPath(j.id, last.slot)}><RunStatus status={null} outcome={last.outcome} /></Link> : "Never run"}</div>
            <div className="metric-sub">{last ? `${fmtSlot(last.slot)}${last.took_s != null ? `, took ${fmtDuration(last.took_s)}` : ""}` : ""}</div></div>
          <div className="metric"><div className="metric-label">Next trigger</div>
            <div className="metric-value small-value">{j.paused ? "Paused" : j.next_start ? fmtSlot(j.next_start) : "–"}</div>
            <div className="metric-sub">{j.paused ? "no trigger fires until it is resumed" : "its first group starts then"}</div></div>
          <div className="metric"><div className="metric-label">Speed</div>
            <div className="metric-value">{j.median_speed_tables_per_min != null ? fmtSpeed(j.median_speed_tables_per_min) : "–"}<small>tables/min</small></div>
            <div className="metric-sub">{Number(j.speed_job_runs) ? `median of the last ${fmtInt(j.speed_job_runs)} job ${plural(j.speed_job_runs, "run")} that loaded tables`
              : "no job run loaded tables in 7 days"}</div></div>
          <div className="metric"><div className="metric-label">Job run time</div>
            <div className="metric-value">{j.p95_took_s != null ? fmtDuration(j.p95_took_s) : "–"}</div>
            <div className="metric-sub">p95 over 7 days</div></div>
        </div>
      )}
      <Section title="Schedule" note="Every trigger, newest first: the ones that ran, are waiting or starting, were skipped, or did not run. Group runs the check job started together are one job run; other group runs are placed by the job's current schedule, and one started by hand inside a slot is an extra run."
        actions={<span className="pager-buttons">
          <button className="ghost" disabled={!pages.length} onClick={() => setPages(pages.slice(0, -1))}>Newer</button>
          <button className="ghost" disabled={!runs.data?.next_before} onClick={() => setPages([...pages, runs.data!.next_before])}>Older</button>
        </span>}>
        <ErrorBox error={runs.error} />
        {runs.data && <JobRunsTable runs={runs.data.job_runs} />}
      </Section>
      {j && (
        <Section title={`Ingestion groups: ${fmtInt(j.groups_detail.length)}`} actions={<StateKey />}>
          <div className="table-wrap"><table className="data">
            <thead><tr><th>Ingestion group</th><th className="num">Starts at</th><th className="num">Databases</th><th className="num">Tables</th>
              <th>State now</th><th className="num">Failing</th><th>Last group run</th><th className="num">p95 run time</th><th>Wrapper job</th></tr></thead>
            <tbody>
              {j.groups_detail.map((g: Row) => (
                <tr key={g.task_collection}>
                  <td className="name"><Link to={`/collections/${encodeURIComponent(g.task_collection)}`}><b>{g.task_collection}</b></Link></td>
                  <td className="num">{g.starts ?? `:${String(g.offset_minutes % 60).padStart(2, "0")}`}</td>
                  <td className="num">{fmtInt(g.databases)}</td><td className="num">{fmtInt(g.enabled_tables)}</td><td><StateBar row={g} /></td>
                  <td className="num" style={{ color: Number(g.state_failing) ? "var(--bad)" : undefined }}>{fmtInt(g.state_failing)}</td>
                  <td>{g.last_run ? <><Link to={`/runs/${g.last_run.run_id}`}><RunStatus status={g.last_run.status} outcome={g.last_run.outcome} /></Link>
                    <span className="sub">group run {fmtInt(g.last_run.run_id)} at {fmtClock(g.last_run.started_at)}</span></> : <span className="muted">never run</span>}</td>
                  <td className="num">{g.p95_s != null ? fmtDuration(g.p95_s) : ""}</td>
                  <td>{g.job_url ? <a href={g.job_url} target="_blank" rel="noreferrer">open</a> : <span className="muted">not synced</span>}</td>
                </tr>
              ))}
            </tbody>
          </table></div>
        </Section>
      )}
    </>
  );
}

// What Run now starts, for its dialog: one run of every group, each on its own cluster, which costs money.
function runNowRequest(j: Row): ConfirmRequest {
  const groups = Number(j.run_now?.groups ?? 0);
  const starts = j.run_now?.check
    ? `the check job over its ${fmtInt(groups)} ${plural(groups, "group")}, which starts an ingest run, each on its own job cluster, for every group with changes (up to ${fmtInt(groups)} ${plural(groups, "cluster")})`
    : `an ingest run for each of its ${fmtInt(groups)} ${plural(groups, "group")}, each on its own job cluster (${fmtInt(groups)} ${plural(groups, "cluster")})`;
  return { title: `Run ${j.name} now?`, action: "Run now",
    body: <><p>This starts {starts}, over its {fmtInt(j.enabled_tables)} {plural(j.enabled_tables, "table")}.</p>
      <p>Clusters and the sources' load cost money while they run.</p></> };
}

/** What deleting a job does, for its dialog (the job page and the Schedules card): the job goes, everything it
 * loaded stays. */
export function deleteJobRequest(name: unknown, groups: unknown, tables: unknown): ConfirmRequest {
  return { title: `Delete ${name}?`, action: "Delete", danger: true,
    body: <><p>Its schedule and its Databricks jobs are deleted.</p>
      <p>Its {fmtInt(groups)} {plural(groups, "group")} move to Not in any job, and their {fmtInt(tables)} {plural(tables, "table")},
        target tables, checkpoints and run history all stay.</p></> };
}
