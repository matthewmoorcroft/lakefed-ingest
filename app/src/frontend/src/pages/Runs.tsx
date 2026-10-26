import { useState } from "react";
import { Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { type ColDef } from "ag-grid-community";
import { api, qs, type Row } from "../api";
import { ErrorBox, Grid, PageHead, RunStatus, Section, fmtDuration, fmtInt, plural } from "../components";

/** Tables that needed a read: the run's total less those found unchanged. */
const worked = (r: Row) => (r.total === null || r.total === undefined ? null : Number(r.total) - Number(r.skipped || 0));

const COLUMNS: ColDef[] = [
  { field: "run_id", headerName: "Run", width: 90, cellRenderer: (p: any) => <Link to={`/runs/${p.value}`}>#{p.value}</Link> },
  { field: "task_collection", headerName: "Ingestion group", minWidth: 170 },
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
  const collections = useQuery({ queryKey: ["collections"], queryFn: () => api.get<string[]>("/collections") });
  const runs = useQuery({
    queryKey: ["runs", collection],
    queryFn: () => api.get(`/runs${qs({ collection, limit: 200 })}`),
    refetchInterval: 30_000,
  });
  const running = (runs.data ?? []).filter((r) => r.status === "running").length;
  return (
    <>
      <PageHead crumb={<span className="eyebrow">Runs · What ran, and how fast?</span>} title="Runs" lead={<><strong>{running}</strong> running; the last {fmtInt(runs.data?.length ?? 0)} {plural(runs.data?.length ?? 0, "run")}
        {collection ? <> of <strong>{collection}</strong></> : " of every ingestion group"}.</>}
        right={<label className="toolbar" style={{ margin: 0 }}>Ingestion group
          <select value={collection} onChange={(e) => setCollection(e.target.value)}>
            <option value="">(all)</option>
            {collections.data?.map((c) => <option key={c}>{c}</option>)}
          </select>
        </label>} />
      <ErrorBox error={runs.error} />
      <Section title="Recent runs" note={<>Worked: tables that needed a read (unchanged tables are advanced in bulk). Tables/s: worked
        tables over the run's time. Rows written: the run's own, per-table counts plus its batches' total, "at least" when some
        loads recorded none. Open a run by its number for its databases and table events.</>}>
        <Grid rows={runs.data}
          columns={COLUMNS} getRowId={(r) => String(r.run_id)} options={{ autoSizeStrategy: { type: "fitGridWidth" } }} />
      </Section>
    </>
  );
}
