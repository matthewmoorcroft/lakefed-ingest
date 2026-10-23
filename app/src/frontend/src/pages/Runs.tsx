import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { type ColDef } from "ag-grid-community";
import { api, qs, type Row } from "../api";
import { Badge, ErrorBox, Grid, PageHead, RunStatus, Section, autoColumns, fmtDuration, fmtInt } from "../components";

/** Tables that needed a read: the run's total less those found unchanged. */
const worked = (r: Row) => (r.total === null || r.total === undefined ? null : Number(r.total) - Number(r.skipped || 0));

const COLUMNS: ColDef[] = [
  { field: "run_id", headerName: "Run", width: 90, valueFormatter: (p) => `#${p.value}` },
  { field: "task_collection", headerName: "Collection", minWidth: 110 },
  { field: "status", headerName: "Status", minWidth: 120, cellRenderer: (p: any) => <RunStatus status={p.value} /> },
  { field: "started_at", headerName: "Started", minWidth: 120,
    valueFormatter: (p) => (p.value ? new Date(p.value).toLocaleString([], { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" }) : "") },
  { field: "duration_s", headerName: "Took", type: "rightAligned", valueFormatter: (p) => fmtDuration(p.value) },
  { colId: "worked", headerName: "Worked", type: "rightAligned", valueGetter: (p) => worked(p.data), valueFormatter: (p) => fmtInt(p.value) },
  { field: "skipped", headerName: "Unchanged", minWidth: 110, type: "rightAligned", valueFormatter: (p) => fmtInt(p.value) },
  { field: "failed", headerName: "Failed", type: "rightAligned", valueFormatter: (p) => fmtInt(p.value),
    cellStyle: (p) => (Number(p.value) ? { color: "var(--bad)", fontWeight: 600 } : null) },
  { field: "rows_written", headerName: "Rows written", type: "rightAligned", minWidth: 120,
    valueFormatter: (p) => (p.data?.rows_partial ? `at least ${fmtInt(p.value)}` : fmtInt(p.value)) },
  { colId: "tables_per_s", headerName: "Tables/s", type: "rightAligned",
    valueGetter: (p) => { const w = worked(p.data); const s = Number(p.data?.duration_s); return w !== null && s > 0 ? w / s : null; },
    valueFormatter: (p) => (p.value === null || p.value === undefined ? "" : Number(p.value).toFixed(2)) },
  { field: "parallelism", headerName: "Parallelism", type: "rightAligned", minWidth: 110 },
];

export default function Runs() {
  const [collection, setCollection] = useState("");
  const [run, setRun] = useState<Row | null>(null);
  const [showEvents, setShowEvents] = useState(false);
  const collections = useQuery({ queryKey: ["collections"], queryFn: () => api.get<string[]>("/collections") });
  const runs = useQuery({
    queryKey: ["runs", collection],
    queryFn: () => api.get(`/runs${qs({ collection, limit: 200 })}`),
    refetchInterval: 30_000,
  });
  const databases = useQuery({
    queryKey: ["run-databases", run?.run_id],
    queryFn: () => api.get(`/runs/${run!.run_id}/databases`),
    enabled: !!run,
    refetchInterval: run?.status === "running" ? 15_000 : false,
  });
  const events = useQuery({
    queryKey: ["events", run?.run_id],
    queryFn: () => api.get(`/runs/${run!.run_id}/events`),
    enabled: !!run && showEvents,
  });
  const running = (runs.data ?? []).filter((r) => r.status === "running").length;
  const failed = (events.data ?? []).filter((e) => e.status === "failed").length;
  const statusCol = { cellRenderer: (p: any) => <Badge value={p.value} /> };
  const dbs = databases.data ?? [];
  const total = (k: string) => dbs.reduce((a, d) => a + Number(d[k] || 0), 0);
  return (
    <>
      <PageHead crumb={<span className="eyebrow">Runs · What ran, and how fast?</span>} title="Runs" lead={<><strong>{running}</strong> running; the last {fmtInt(runs.data?.length ?? 0)} runs
        {collection ? <> of <strong>{collection}</strong></> : " of every collection"}.</>}
        right={<label className="toolbar" style={{ margin: 0 }}>Collection
          <select value={collection} onChange={(e) => { setCollection(e.target.value); setRun(null); }}>
            <option value="">(all)</option>
            {collections.data?.map((c) => <option key={c}>{c}</option>)}
          </select>
        </label>} />
      <ErrorBox error={runs.error} />
      <Section title="Recent runs" note={<>Worked: tables that needed a read (unchanged tables are advanced in bulk). Tables/s: worked
        tables over the run's time. Rows written: the run's own, per-table counts plus its batches' total, "at least" when some
        loads recorded none. Select a run for its databases.</>}>
        <Grid rows={runs.data} height={360} selection="single" onSelect={(r) => { setRun(r[0] ?? null); setShowEvents(false); }}
          columns={COLUMNS} getRowId={(r) => String(r.run_id)} options={{ autoSizeStrategy: { type: "fitGridWidth" } }} />
      </Section>
      {run && (
        <Section title={<>Run #{run.run_id} by database <span className="muted small">({run.task_collection})</span></>}
          actions={<button onClick={() => setShowEvents(!showEvents)}>{showEvents ? "Hide" : "Show"} its table events</button>}
          note="Each database of the run: tables loaded, in flight and failed, and the rows its loads wrote.">
          <ErrorBox error={databases.error} />
          <div className="table-wrap">
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
                {dbs.length > 1 && <tr className="total"><td>Run #{run.run_id}</td><td className="num">{fmtInt(total("loaded"))}</td>
                  <td className="num">{fmtInt(total("in_flight"))}</td><td className="num">{fmtInt(total("failed"))}</td>
                  <td className="num">{fmtInt(total("rows_written"))}</td></tr>}
                {databases.data && !dbs.length && <tr><td colSpan={5} className="muted">No table events: every table was unchanged.</td></tr>}
              </tbody>
            </table>
          </div>
        </Section>
      )}
      {run && showEvents && (
        <Section title={`Run #${run.run_id}: ${events.data?.length ?? "…"} table events, ${failed} failed`}>
          <p className="muted small">Tables found unchanged by change detection are counted in the run's
            "Unchanged" without an event.</p>
          <ErrorBox error={events.error} />
          <Grid rows={events.data} height={420}
            columns={autoColumns(events.data ?? [], { status: statusCol, error: { flex: 1, minWidth: 400, tooltipField: "error" } })} />
        </Section>
      )}
    </>
  );
}
