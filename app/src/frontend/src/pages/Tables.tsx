import { useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { type ColDef } from "ag-grid-community";
import { api, qs, type Row } from "../api";
import { Badge, ConfirmButton, ErrorBox, Grid, Notice, Section, autoColumns, fmtInt, plural } from "../components";
import { EditBulk, EditOne } from "./EditTables";

/** Each table's freshness target right after whether it is enabled, so it reads with the table, not off to the right. */
function targetAfterEnabled(columns: ColDef[]): ColDef[] {
  const target = columns.find((c) => c.field === "sla_min");
  if (!target) return columns;
  const rest = columns.filter((c) => c !== target);
  const at = rest.findIndex((c) => c.field === "task_enabled");
  return [...rest.slice(0, at + 1), target, ...rest.slice(at + 1)];
}

const FILTERS = ["all", "failing", "sla_breached", "no_sla", "seeding", "disabled"];

export default function Tables() {
  const [params, setParams] = useSearchParams();
  const collection = params.get("collection") ?? "";
  const [search, setSearch] = useState("");
  const [status, setStatus] = useState(FILTERS.includes(params.get("status") ?? "") ? params.get("status")! : "all");
  const database = params.get("database") ?? undefined;
  const [pages, setPages] = useState<string[]>([]);
  const after = pages[pages.length - 1];
  const [selected, setSelected] = useState<Row[]>([]);
  const [message, setMessage] = useState<string | null>(null);
  const [editing, setEditing] = useState<"one" | "bulk" | null>(null);
  const qc = useQueryClient();
  const collections = useQuery({ queryKey: ["collections"], queryFn: () => api.get<string[]>("/collections") });
  const tables = useQuery({
    queryKey: ["tables", collection, database, search, status, after],
    queryFn: () => api.get<{ tables: Row[]; limit: number; next_after: unknown[] | null }>(`/tables${qs({ collection, database, search, status, after })}`),
  });
  const act = useMutation({
    mutationFn: ({ path, body }: { path: string; body: Row }) => api.post<{ updated: number }>(path, body),
    onSuccess: (r, v) => {
      setMessage(`${v.path.endsWith("reseed") ? "Reseed requested for" : "Updated"} ${r.updated} ${plural(r.updated, "table")}.`);
      qc.invalidateQueries();
    },
  });
  const ids = selected.map((r) => Number(r.id));
  return (
    <>
      <h1>Tables</h1>
      <div className="toolbar">
        <label>Ingestion group
          <select value={collection} onChange={(e) => { setPages([]); setParams(e.target.value ? { collection: e.target.value } : {}); }}>
            <option value="">(all)</option>
            {collections.data?.map((c) => <option key={c}>{c}</option>)}
          </select>
        </label>
        <label className="grow">Search
          <input placeholder="database, table, sink or client_id" value={search} onChange={(e) => { setPages([]); setSearch(e.target.value); }} />
        </label>
        <label>Show
          <select value={status} onChange={(e) => { setPages([]); setStatus(e.target.value); }}>{FILTERS.map((f) => <option key={f}>{f}</option>)}</select>
        </label>
        {database !== undefined && <span className="tag">database {database || "(none)"}
          <button className="ghost" onClick={() => { setPages([]); setParams(collection ? { collection } : {}); }}>clear</button></span>}
      </div>
      <ErrorBox error={tables.error || act.error} />
      {message && <Notice tone="ok">{message}</Notice>}
      <Section title={tables.data ? `Page ${pages.length + 1}: ${tables.data.tables.length} ${plural(tables.data.tables.length, "table")}` : "Tables"}
        actions={<>
          <button className="ghost" disabled={!pages.length} onClick={() => setPages(pages.slice(0, -1))}>Previous</button>
          <button className="ghost" disabled={!tables.data?.next_after} onClick={() => setPages([...pages, JSON.stringify(tables.data!.next_after)])}>Next</button>
          {ids.length > 0 && <>
          <span className="muted">{ids.length} selected</span>
          <button className="primary" onClick={() => setEditing(ids.length === 1 ? "one" : "bulk")}>
            {ids.length === 1 ? "Edit" : "Bulk edit"}
          </button>
          <ConfirmButton label="Enable" onConfirm={() => act.mutateAsync({ path: "/tables/enable", body: { ids, enabled: true } })} />
          <ConfirmButton label="Disable" onConfirm={() => act.mutateAsync({ path: "/tables/enable", body: { ids, enabled: false } })} />
          <ConfirmButton label="Force reseed" danger
            onConfirm={() => act.mutateAsync({ path: "/tables/reseed", body: { ids } })} />
          </>}
        </>}>
        <p className="muted small">Select one row to edit it, or several to bulk edit, enable, disable, or force a reseed
          (the next run copies them again). Changes are refused while the table's ingestion group is running. 100 tables a page,
          by ingestion group, database and table; selections are per page.</p>
        <Grid rows={tables.data?.tables} selection="multiple" onSelect={setSelected} getRowId={(r) => String(r.id)}
          columns={targetAfterEnabled(autoColumns(tables.data?.tables ?? [], {
            task_collection: { headerName: "Ingestion group" },
            sla_min: { headerName: "Freshness target (min)", minWidth: 180, type: "rightAligned", valueFormatter: (p) => fmtInt(p.value) },
            sla_breached: { headerName: "Past target", cellRenderer: (p: any) => p.value ? <Badge value="breached" /> : null },
            status: { cellRenderer: (p: any) => <Badge value={p.value} /> },
            phase: { cellRenderer: (p: any) => <Badge value={p.value} /> },
            detail: { flex: 1, minWidth: 300, tooltipField: "detail" },
          }))} />
      </Section>
      {editing === "one" && ids.length === 1 && <EditOne id={ids[0]} onClose={() => setEditing(null)} />}
      {editing === "bulk" && ids.length > 0 && <EditBulk ids={ids} onClose={() => setEditing(null)} />}
    </>
  );
}
