import { type ReactNode, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { api, type Row } from "../api";
import {
  Badge, BigCount, ErrorBox, FillBox, Grid, PageHead, ProgressBar, ProgressKey, RunStatus, Section, Stamp, autoColumns,
  fmtDuration, fmtInt, progressOf, progressText, type Progress
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
  const events = useQuery({
    queryKey: ["events", id],
    queryFn: () => api.get(`/runs/${encodeURIComponent(id)}/events`),
    enabled: !!run.data && showEvents,
  });
  const r = run.data;
  const p = r ? (live ? (r.progress ? progressOf(r.progress) : null) : finishedProgress(r)) : null;
  const dbs = databases.data ?? [];
  const total = (k: string) => dbs.reduce((a, d) => a + Number(d[k] || 0), 0);
  const failedEvents = (events.data ?? []).filter((e) => e.status === "failed").length;
  const rows = r ? <>{r.rows_partial ? "at least " : ""}<BigCount value={r.rows_written} /></> : "";
  return (
    <>
      <PageHead crumb={<span className="eyebrow"><Link to="/runs">Runs</Link> · What did this run do?</span>}
        title={<>Run #{id} {r && <RunStatus status={r.status} />}</>}
        lead={r && <>Ingestion group <Link to={`/collections/${encodeURIComponent(r.task_collection)}`}><strong>{r.task_collection}</strong></Link>
          {r.parallelism ? <>, parallelism <strong>{fmtInt(r.parallelism)}</strong></> : null}
          {r.job_run_id ? <>, job run <strong>{r.job_run_id}</strong></> : null}.</>}
        right={live ? <Stamp at={new Date(run.dataUpdatedAt).toISOString()} every={15} /> : undefined} />
      <ErrorBox error={run.error} />
      {r && (
        <>
          <div className="metrics">
            <Figure label="Started" value={fmtWhen(r.started_at)} />
            <Figure label="Ended" value={live ? "still running" : fmtWhen(r.finished_at)} />
            <Figure label={live ? "Running for" : "Took"} value={fmtDuration(r.duration_s)} />
            <Figure label="Rows written" value={rows} sub={live ? "so far" : undefined} />
          </div>
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
          <Section title="By database" note="Each database of the run: tables loaded, in flight and failed, and the rows its loads wrote.">
            <ErrorBox error={databases.error} />
            <FillBox min={160}>
              <table className="data">
                <thead><tr><th>Database</th><th className="num">Loaded</th><th className="num">In flight</th><th className="num">Failed</th>
                  <th className="num">Rows written</th></tr></thead>
                <tbody>
                  {dbs.map((d) => (
                    <tr key={d.src_database}><td><b>{d.src_database || "(none)"}</b></td><td className="num">{fmtInt(d.loaded)}</td>
                      <td className="num">{fmtInt(d.in_flight)}</td>
                      <td className="num" style={{ color: Number(d.failed) ? "var(--bad)" : undefined }}>{fmtInt(d.failed)}</td>
                      <td className="num">{Number(d.uncounted) ? `at least ${fmtInt(d.rows_written || 0)}` : fmtInt(d.rows_written || 0)}</td></tr>
                  ))}
                  {dbs.length > 1 && <tr className="total"><td>All databases</td><td className="num">{fmtInt(total("loaded"))}</td>
                    <td className="num">{fmtInt(total("in_flight"))}</td><td className="num">{fmtInt(total("failed"))}</td>
                    <td className="num">{fmtInt(total("rows_written"))}</td></tr>}
                  {databases.data && !dbs.length && <tr><td colSpan={5} className="muted">No table events: every table was unchanged.</td></tr>}
                </tbody>
              </table>
            </FillBox>
          </Section>
          <Section title={showEvents && events.data ? `Table events: ${fmtInt(events.data.length)}, ${fmtInt(failedEvents)} failed` : "Table events"}
            actions={<button onClick={() => setShowEvents(!showEvents)}>{showEvents ? "Hide" : "Show"} table events</button>}
            note={'Tables found unchanged by change detection are counted in "Unchanged" without an event.'}>
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
