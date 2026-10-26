import { type ReactNode, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { api, type Row } from "../api";
import {
  Badge, BigCount, ErrorBox, FillBox, Grid, PageHead, ProgressBar, ProgressKey, RunStatus, Section, Stamp, autoColumns,
  databasePath, fmtDuration, fmtInt, fmtLeft, fmtSpeed, jobRunName, jobRunPath, plural, progressOf, progressText, type Progress
} from "../components";

/** A finished run's totals in the same shape as a live run's progress, so both read the same way. The run's "ok"
 * counts its unchanged tables too, so loaded is ok less unchanged. */
function finishedProgress(run: Row): Progress {
  const unchanged = Number(run.skipped || 0);
  return { total: Number(run.total || 0), unchanged, loaded: Math.max(0, Number(run.ok || 0) - unchanged),
           inFlight: 0, failed: Number(run.failed || 0) };
}

function Figure({ label, value, sub, tone }: { label: string; value: ReactNode; sub?: string; tone?: string }) {
  return (
    <div className={`metric ${tone ?? ""}`}>
      <div className="metric-label">{label}</div>
      <div className="metric-value">{value || "–"}</div>
      {sub && <div className="metric-sub">{sub}</div>}
    </div>
  );
}

/** A run with no recent loads that has run longer than its group's typical run: no estimate is honest. */
const pastTypical = (r: Row) => r.time_left?.basis === "typical" && Number(r.time_left.seconds) === 0;

/** The run page's title: the group and run, and while running how far it is and about how long is left. */
function title(id: string, r: Row | undefined, p: Progress | null, live: boolean): ReactNode {
  if (!r) return <>Group run {id}</>;
  const head = <>{r.task_collection}, group run {fmtInt(r.run_id)}</>;
  if (!live) return <>{head} <RunStatus status={r.status} outcome={r.outcome} /></>;
  const pct = p && p.total ? Math.floor((100 * (p.unchanged + p.loaded)) / p.total) : null;
  const left = r.time_left && !pastTypical(r) ? `, ${fmtLeft(r.time_left.seconds)} left` : "";
  return <>{head}{pct !== null ? `: ${pct}% checked${left}` : ""} <RunStatus status={r.status} outcome={r.outcome} /></>;
}

/** The tables the run is loading now, longest first: eight, or every one the server sent with "Show all". */
function InFlight({ id }: { id: string }) {
  const [all, setAll] = useState(false);
  const found = useQuery({
    queryKey: ["run-in-flight", id],
    queryFn: () => api.get<{ tables: Row[]; total: number; limit: number }>(`/runs/${encodeURIComponent(id)}/in-flight`),
    refetchInterval: 15_000,
  });
  const tables = found.data?.tables ?? [];
  const shown = all ? tables : tables.slice(0, 8);
  const total = found.data?.total ?? 0;
  const note = <>{total > tables.length ? `The ${fmtInt(tables.length)} running longest of ${fmtInt(total)}. ` : ""}
    Tables in a batched append are not listed: their events are written when the batch commits.</>;
  return (
    <Section title={found.data ? `In flight now: ${shown.length ? `1 to ${fmtInt(shown.length)} of ` : ""}${fmtInt(total)}` : "In flight now"}
      note={note}
      actions={tables.length > 8 ? <button onClick={() => setAll(!all)}>{all ? "Show fewer" : "Show all"}</button> : undefined}>
      <ErrorBox error={found.error} />
      <FillBox min={120}>
        <table className="data">
          <thead><tr><th>Database</th><th>Table</th><th>Load</th><th>Step</th><th className="num">Running for</th>
            <th className="num">Retries</th></tr></thead>
          <tbody>
            {shown.map((t) => (
              <tr key={`${t.src_database}\u001f${t.src_schema}\u001f${t.src_table}`}><td>{t.src_database}</td>
                <td><b>{t.src_schema}.{t.src_table}</b></td><td>{t.action}</td><td>{t.stage || ""}</td>
                <td className="num">{fmtDuration(t.running_s)}</td><td className="num">{fmtInt(t.retry_count)}</td></tr>
            ))}
            {found.data && !tables.length && <tr><td colSpan={6} className="muted">No table is being loaded right now.</td></tr>}
          </tbody>
        </table>
      </FillBox>
    </Section>
  );
}

/** queries.RUN_EVENTS_LIMIT: the most table events the server sends. */
const EVENTS_LIMIT = 500;

const fmtWhen = (v: unknown) => (v ? new Date(String(v)).toLocaleString([], { day: "numeric", month: "short", hour: "2-digit",
  minute: "2-digit", second: "2-digit" }) : "");

export default function RunDetail() {
  const { id = "" } = useParams();
  const [showEvents, setShowEvents] = useState(false);
  const run = useQuery({
    queryKey: ["run", id],
    queryFn: () => api.get<Row>(`/runs/${encodeURIComponent(id)}`),
    refetchInterval: (q) => ((q.state.data as Row | undefined)?.status === "running" ? 15_000 : false),
  });
  const live = run.data?.status === "running";
  const databases = useQuery({
    queryKey: ["run-databases", id],
    queryFn: () => api.get(`/runs/${encodeURIComponent(id)}/databases`),
    enabled: !!run.data,
    refetchInterval: live ? 15_000 : false,
  });
  const groupDatabases = useQuery({
    queryKey: ["collection-databases", run.data?.task_collection],
    queryFn: () => api.get<Row>(`/collections/${encodeURIComponent(run.data!.task_collection)}/databases`),
    enabled: !!run.data && live,
    refetchInterval: live ? 15_000 : false,
  });
  const events = useQuery({
    queryKey: ["events", id],
    queryFn: () => api.get(`/runs/${encodeURIComponent(id)}/events`),
    enabled: !!run.data && showEvents,
  });
  const r = run.data;
  const p = r ? (live ? (r.progress ? progressOf(r.progress) : null) : finishedProgress(r)) : null;
  const checkedBy = new Map<string, Row>((groupDatabases.data?.databases ?? []).map((d: Row) => [`${d.server}\u001f${d.src_database}`, d]));
  const dbs = [...(databases.data ?? [])].sort((a, b) => Number(b.in_flight || 0) - Number(a.in_flight || 0));
  const total = (k: string) => dbs.reduce((a, d) => a + Number(d[k] || 0), 0);
  const manyServers = new Set(dbs.map((d) => d.server)).size > 1;
  const failedEvents = (events.data ?? []).filter((e) => e.status === "failed").length;
  const rows = r ? <>{r.rows_partial ? "at least " : ""}<BigCount value={r.rows_written} /></> : "";
  return (
    <>
      <PageHead crumb={<span className="eyebrow"><Link to="/overview">Overview</Link> ·{" "}
        {r?.job_runs?.length ? r.job_runs.map((j: Row, i: number) => <span key={j.job_id}>{i > 0 && ", "}
          <Link to={`/ingestion-jobs/${j.job_id}`}>{j.job_name}</Link> · <Link to={jobRunPath(j.job_id, j.slot)}>{jobRunName(j.job_name, j.slot)}</Link></span>)
          : <Link to="/runs?job=none">Runs</Link>} · Group run</span>}
        title={title(id, r, p, live)}
        lead={r && <>Ingestion group <Link to={`/collections/${encodeURIComponent(r.task_collection)}`}><strong>{r.task_collection}</strong></Link>
          {dbs.length ? <>, <strong>{fmtInt(dbs.length)}</strong> {plural(dbs.length, "database")}</> : null}
          {p ? <>, <strong>{fmtInt(p.total)}</strong> {plural(p.total, "table")}</> : null}
          , started {fmtWhen(r.started_at)}
          {r.job_run_id ? <>, Databricks job run <strong>{r.job_run_id}</strong></> : null}
          {r.cluster_id ? <>, cluster <strong>{r.cluster_id}</strong></> : null}
          {r.parallelism ? <>, parallelism <strong>{fmtInt(r.parallelism)}</strong></> : null}.
          {!live && r.outcome?.note ? <> {r.outcome.label}: {r.outcome.note}.</> : null}</>}
        right={live ? <Stamp at={new Date(run.dataUpdatedAt).toISOString()} every={15} /> : undefined} />
      <ErrorBox error={run.error} />
      {r && (
        <>
          {live ? (
            <div className="metrics">
              <Figure label="Speed now" value={r.live_speed ? `${fmtSpeed(r.live_speed.tables_per_min)} tables/min` : ""}
                sub={r.live_speed ? "over the last 5 minutes of loading" : "shown after 3 minutes of loading"} />
              <Figure label="Time left" value={r.time_left && !pastTypical(r) ? fmtLeft(r.time_left.seconds) : ""}
                sub={pastTypical(r) ? "past its typical run time" : r.time_left?.basis === "typical" ? "from its typical run"
                  : r.time_left ? "at the speed now" : "no typical run yet"} />
              <Figure label="Running for" value={fmtDuration(r.duration_s)}
                sub={r.progress?.p50_s != null ? `typical ${fmtDuration(r.progress.p50_s)}, slowest 5% over ${fmtDuration(r.progress.p95_s)}` : undefined} />
              <Figure label="Rows written" value={rows} sub="so far" />
            </div>
          ) : (
            <div className="metrics">
              <Figure label="Took" value={r.duration_s === null ? "" : fmtDuration(r.duration_s)}
                sub={r.duration_s === null ? "it ended without finishing" : `ended ${fmtWhen(r.finished_at)}`} />
              <Figure label="Speed" value={r.speed_tables_per_min != null ? `${fmtSpeed(r.speed_tables_per_min)} tables/min` : ""}
                sub={r.speed_tables_per_min != null ? "while loading tables" : "no table was loaded"} />
              <Figure label="Rows written" value={rows} />
            </div>
          )}
          {p && (
            <Section title={live ? "Progress now" : "What the run did"}
              note={live ? "Refreshes every 15 s while the run is going." : undefined}>
              <div className="metrics">
                <Figure label="Loaded" value={fmtInt(p.loaded)} sub="tables read and written" tone={p.loaded ? "ok" : ""} />
                {live && <Figure label="In flight" value={fmtInt(p.inFlight)} sub="tables being read now" tone="run" />}
                <Figure label="Unchanged" value={fmtInt(p.unchanged)} sub="no change at the source" />
                <Figure label="Failed" value={fmtInt(p.failed)} tone={p.failed ? "bad" : ""} />
                <Figure label="Worked" value={fmtInt(p.loaded + p.inFlight + p.failed)} sub="tables that needed a read" />
              </div>
              <div className="count">{live ? progressText(p) : `${fmtInt(p.unchanged + p.loaded)} of ${fmtInt(p.total)} tables checked`}</div>
              <ProgressBar p={p} big /><ProgressKey p={p} />
            </Section>
          )}
          {live && !p && <p className="muted">Progress is not available yet.</p>}
          {live && <InFlight id={id} />}
          <Section title="By database" note="Each database of the run, in progress first: tables loaded, in flight and failed, and the rows its loads wrote. Checked counts its tables stamped since the run started.">
            <ErrorBox error={databases.error} />
            <FillBox min={160}>
              <table className="data">
                <thead><tr><th>Database</th>{live && <th>Progress</th>}{live && <th className="num">Checked</th>}<th className="num">Loaded</th>
                  <th className="num">In flight</th><th className="num">Failed</th><th className="num">Rows written</th></tr></thead>
                <tbody>
                  {dbs.map((d) => {
                    const g = checkedBy.get(`${d.server}\u001f${d.src_database}`);
                    const tables = Number(g?.tables || 0), checked = Number(g?.checked_this_run || 0);
                    const dp: Progress = { total: tables, loaded: Number(d.loaded || 0), unchanged: Math.max(0, checked - Number(d.loaded || 0)),
                                           inFlight: Number(d.in_flight || 0), failed: Number(d.failed || 0) };
                    return (
                    <tr key={`${d.server}\u001f${d.src_database}`}><td className="name">
                      <Link to={databasePath(r!.task_collection, d.server, d.src_database)}><b>{d.src_database || "(none)"}</b></Link>
                      {manyServers && <span className="sub">{d.server || "(no server)"}</span>}</td>
                      {live && <td style={{ minWidth: 140 }}>{g ? <ProgressBar p={dp} /> : null}</td>}
                      {live && <td className="num">{g ? `${fmtInt(checked)} of ${fmtInt(tables)}` : ""}</td>}
                      <td className="num">{fmtInt(d.loaded)}</td>
                      <td className="num">{fmtInt(d.in_flight)}</td>
                      <td className="num" style={{ color: Number(d.failed) ? "var(--bad)" : undefined }}>{fmtInt(d.failed)}</td>
                      <td className="num">{Number(d.uncounted) ? `at least ${fmtInt(d.rows_written || 0)}` : fmtInt(d.rows_written || 0)}</td></tr>
                    );
                  })}
                  {dbs.length > 1 && <tr className="total"><td>All databases</td>{live && <td />}{live && <td />}<td className="num">{fmtInt(total("loaded"))}</td>
                    <td className="num">{fmtInt(total("in_flight"))}</td><td className="num">{fmtInt(total("failed"))}</td>
                    <td className="num">{total("uncounted") ? `at least ${fmtInt(total("rows_written"))}` : fmtInt(total("rows_written"))}</td></tr>}
                  {databases.data && !dbs.length && <tr><td colSpan={live ? 7 : 5} className="muted">No table events: every table was unchanged.</td></tr>}
                </tbody>
              </table>
            </FillBox>
          </Section>
          <Section title={showEvents && events.data ? `Table events: ${fmtInt(events.data.length)}, ${fmtInt(failedEvents)} failed` : "Table events"}
            actions={<button onClick={() => setShowEvents(!showEvents)}>{showEvents ? "Hide" : "Show"} table events</button>}
            note={<>Tables found unchanged by change detection are counted in "Unchanged" without an event.
              {events.data && events.data.length >= EVENTS_LIMIT ? ` The first ${fmtInt(EVENTS_LIMIT)}, failures first.` : ""}</>}>
            {showEvents && (
              <>
                <ErrorBox error={events.error} />
                <Grid rows={events.data}
                  columns={autoColumns(events.data ?? [], { status: { cellRenderer: (q: any) => <Badge value={q.value} /> },
                    error: { flex: 1, minWidth: 400, tooltipField: "error" } })} />
              </>
            )}
          </Section>
        </>
      )}
    </>
  );
}
