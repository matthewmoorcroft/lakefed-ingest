import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, type Row } from "../api";
import { usePermissions } from "../permissions";
import { OutLink, PageHead, Stamp, Tip, readAt, Badge, ErrorBox, Grid, Menu, Notice, Section, fmtTime, plural, useConfirm } from "../components";
import { deleteJobRequest } from "./IngestionJob";

// A schedule runs one or more collections at a cadence. The app turns each (schedule, collection)
// into a small Databricks job that starts the Databricks ingestion job; editing here keeps those jobs in sync.

interface Options { cadences: number[]; parameters: string[]; flags: string[]; defaults: Record<string, string>;
  variants: string[]; collections: string[]; scheduled: Record<string, string[]>; view_group: string | null }

const CADENCE_LABEL = (m: number) => (m < 60 ? `every ${m} minutes` : m === 60 ? "hourly" : m === 1440 ? "daily" : `every ${m / 60} hours`);
const PARAM_LABEL = (k: string) => k.replace(/_/g, " ").replace(/^./, (c) => c.toUpperCase());
const paramsText = (p?: Row) => Object.entries(p ?? {}).map(([k, v]) => `${k.replace(/_/g, " ")} ${v}`).join(" · ");
const mins = (s: unknown) => (s === null || s === undefined ? "" : `${Math.round((Number(s) / 60) * 10) / 10} min`);

function Editor({ initial, options, onDone }: { initial?: Row; options: Options; onDone: () => void }) {
  const [f, setF] = useState<Row>(initial ? {
    name: initial.name, cadence_minutes: initial.cadence_minutes, timezone: initial.timezone, stagger: initial.stagger,
    paused: initial.paused, notify_emails: initial.notify_emails ?? "", job_variant: initial.job_variant,
    members: initial.members.map((m: Row) => m.task_collection),
  } : { name: "", cadence_minutes: 15, timezone: "UTC", stagger: true, paused: false, notify_emails: "",
        job_variant: "default", members: [] });
  // Every parameter the schedule has is kept, shown or not; a blank field means the job's default.
  const [params, setParams] = useState<Record<string, string>>(
    Object.fromEntries(Object.entries(initial?.parameters ?? {}).map(([k, v]) => [k, String(v)])));
  const qc = useQueryClient();
  const permissions = usePermissions().data;
  // Anyone may set up a job; one that isn't paused starts runs, so it needs the right to run what it starts.
  const startsRuns = !f.paused && (!initial || (f.job_variant || "default") !== (initial.job_variant || "default"));
  const runRefusal = startsRuns ? permissions?.new_running_job[f.job_variant || "default"] ?? null : null;
  const save = useMutation({
    mutationFn: () => {
      const body = { name: f.name, cadence_minutes: Number(f.cadence_minutes), timezone: f.timezone, stagger: f.stagger && f.job_variant !== "check",
        paused: f.paused, notify_emails: f.notify_emails || null, job_variant: f.job_variant, members: f.members,
        // The start time is set on the job's Edit page; kept here as it is, and dropped when the cadence no longer takes one.
        options: Number(f.cadence_minutes) > 60 ? (initial?.options ?? {}) : {},
        parameters: Object.fromEntries(Object.entries(params).filter(([, v]) => v.trim() !== "").map(([k, v]) => [k, v.trim()])) };
      return initial ? api.post(`/schedules/${initial.id}`, body) : api.post("/schedules", body);
    },
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["schedules"] }); onDone(); },
  });
  const toggle = (c: string) => setF({ ...f, members: f.members.includes(c) ? f.members.filter((x: string) => x !== c) : [...f.members, c] });
  return (
    <div className="drawer">
      <div className="drawer-head">
        <h2>{initial ? `Edit schedule: ${initial.name}` : "New schedule"}</h2>
        <button className="ghost" onClick={onDone}>Close</button>
      </div>
      <form className="form-grid" onSubmit={(e) => { e.preventDefault(); save.mutate(); }}>
        <label>Name *<input value={f.name} required onChange={(e) => setF({ ...f, name: e.target.value })} /></label>
        <label>Cadence
          <select value={f.cadence_minutes} onChange={(e) => setF({ ...f, cadence_minutes: Number(e.target.value) })}>
            {options.cadences.map((c) => <option key={c} value={c}>{CADENCE_LABEL(c)}</option>)}
          </select>
        </label>
        <label>Time zone<input value={f.timezone} onChange={(e) => setF({ ...f, timezone: e.target.value })} />
          <span className="muted small">IANA name; UTC avoids daylight-saving gaps for 24/7 schedules</span></label>
        {[...options.parameters, ...Object.keys(params).filter((k) => !options.parameters.includes(k))].map((k) => (
          <label key={k}>{PARAM_LABEL(k)}
            {options.flags.includes(k) ? (
              <select value={params[k] ?? ""} onChange={(e) => setParams({ ...params, [k]: e.target.value })}>
                <option value="">job default{options.defaults[k] ? ` (${options.defaults[k]})` : ""}</option>
                <option>true</option><option>false</option>
              </select>
            ) : (
              <input value={params[k] ?? ""} placeholder={options.defaults[k] ? `job default (${options.defaults[k]})` : "job default"}
                onChange={(e) => setParams({ ...params, [k]: e.target.value })} />
            )}</label>
        ))}
        {options.variants.length > 1 && (
          <label>Databricks job
            <select value={f.job_variant} onChange={(e) => setF({ ...f, job_variant: e.target.value })}>
              {options.variants.map((v) => (
                <option key={v} value={v}>{v === "check" ? "check first: one job for the whole schedule" : v}</option>
              ))}
            </select>
          </label>
        )}
        <label>Failure emails<input value={f.notify_emails} placeholder="a@example.com, b@example.com"
          onChange={(e) => setF({ ...f, notify_emails: e.target.value })} /></label>
        <label className="checkbox"><input type="checkbox" checked={f.stagger && f.job_variant !== "check"}
          disabled={f.job_variant === "check"} onChange={(e) => setF({ ...f, stagger: e.target.checked })} />
          Stagger start times across the cadence{f.job_variant === "check" ? " (not with check first: the check takes every member at once)" : ""}</label>
        <label className="checkbox"><input type="checkbox" checked={f.paused} onChange={(e) => setF({ ...f, paused: e.target.checked })} />
          Paused</label>
        <div className="wide">
          <div className="muted small">Ingestion groups * ({f.members.length} selected). Each gets its own job, so one slow
            ingestion group never delays the others.</div>
          <div className="checklist">
            {options.collections.map((c) => {
              const elsewhere = (options.scheduled[c] ?? []).filter((n) => n !== initial?.name);
              return (
                <label key={c} className="checkbox">
                  <input type="checkbox" checked={f.members.includes(c)} onChange={() => toggle(c)} />
                  {c}{elsewhere.length > 0 && <span className="muted small">also in {elsewhere.join(", ")}</span>}
                </label>
              );
            })}
          </div>
        </div>
        {runRefusal && <p className="wide muted small">A job that isn't paused starts runs, and {runRefusal}. Tick Paused to set it up
          anyway.</p>}
        <div className="wide"><button className="primary" disabled={save.isPending || !f.members.length || !!runRefusal}>
          {save.isPending ? "Saving and syncing jobs…" : initial ? "Save and sync jobs" : "Create schedule and jobs"}</button></div>
      </form>
      <ErrorBox error={save.error} />
    </div>
  );
}

function SyncResult({ result }: { result?: Row }) {
  if (!result?.sync) return null;
  const errors = (result.sync as Row[]).filter((r) => r.action === "error");
  const changed = (result.sync as Row[]).filter((r) => ["created", "adopted", "updated", "deleted"].includes(r.action));
  return errors.length ? <Notice tone="bad">Job sync failed for {errors.map((e) => `${e.task_collection || "the schedule"}: ${e.detail}`).join("; ")}</Notice>
    : <p className="muted">Jobs in sync{changed.length ? `: ${changed.map((c) => `${c.task_collection} ${c.action}`).join(", ")}` : ""}.</p>;
}

function ScheduleCard({ s, onEdit }: { s: Row; onEdit: () => void }) {
  const qc = useQueryClient();
  const [drift, setDrift] = useState<Row | null>(null);
  const act = useMutation({
    mutationFn: (path: string) => api.post(`/schedules/${s.id}/${path}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["schedules"] }),
  });
  const check = useMutation({ mutationFn: () => api.get<Row>(`/schedules/${s.id}/check`), onSuccess: setDrift });
  const confirm = useConfirm();
  const refusals = usePermissions().data?.jobs[String(s.id)];
  const locked = refusals?.change ?? null;
  const rows = (s.members as Row[]).map((m) => ({
    collection: m.task_collection, starts: m.when, next_run: s.paused ? "Paused, no run until resumed" : fmtTime(m.next_run),
    last_run: m.last_status, last_started: fmtTime(m.last_started), last_duration: mins(m.last_duration_s),
    p95_7d: mins(m.p95_s), runs_24h: m.runs_24h, failed_24h: m.failed_24h, skipped_24h: m.skipped_24h ?? "",
    strictest_sla: m.min_sla_minutes ? `${m.min_sla_minutes} min` : "", job: m.job_url, sync: m.sync_error ? "error" : m.job_id ? "ok" : "not synced",
    sync_drift: drift?.[m.task_collection]?.status ?? "", warnings: (m.warnings as string[]).join(" · "),
  }));
  const warned = rows.filter((r) => r.warnings).length;
  return (
    <Section title={`${s.name}: ${s.description}`} actions={<>
      {s.paused ? <Badge value="paused" /> : <Badge value="ok" />}
      {/* As a Databricks job page: the less common actions and Delete in the more-actions menu. */}
      <Menu label="⋮" title="More actions">
        <button onClick={() => check.mutate()} disabled={check.isPending}>Check jobs</button>
        <button onClick={() => act.mutate("sync")} disabled={act.isPending || !!locked} title={locked ?? undefined}>Sync jobs</button>
        <button className="danger-text" disabled={!!refusals?.delete} title={refusals?.delete ?? undefined}
          onClick={() => confirm.ask(deleteJobRequest(s.name, s.members.length,
            (s.members as Row[]).reduce((sum, m) => sum + Number(m.enabled_tables || 0), 0))).then((ok) => ok && act.mutate("delete"))}>Delete job</button>
      </Menu>
      <button onClick={() => act.mutate(s.paused ? "resume" : "pause")} disabled={act.isPending || !!locked}
        title={locked ?? undefined}>{s.paused ? "Resume" : "Pause"}</button>
      <button onClick={onEdit} disabled={!!locked} title={locked ?? undefined}>Edit</button>
    </>}>
      {confirm.dialog}
      {locked && <p className="muted small">Edit, {s.paused ? "Resume" : "Pause"}, Sync jobs and Delete: {locked}.</p>}
      <p className="muted small">Time zone {s.timezone}{paramsText(s.parameters) ? ` · ${paramsText(s.parameters)}` : ""}
        {s.notify_emails ? ` · failure emails to ${s.notify_emails}` : ""} · settings last changed {fmtTime(s.updated_at)}{s.created_by ? ` · created by ${s.created_by}` : ""}</p>
      {warned > 0 && <Notice tone="warn">{warned} {plural(warned, "ingestion group")} {warned === 1 ? "has" : "have"} warnings: see the warnings column.</Notice>}
      <ErrorBox error={act.error || check.error} />
      <SyncResult result={act.data} />
      {/* The group and its next run stay pinned on the left, so they always show; the other columns scroll. */}
      <Grid rows={rows} fitRows filters={false} columns={[
        { field: "collection", headerName: "Ingestion group", pinned: "left" },
        { field: "next_run", headerName: "Next run", pinned: "left" }, { field: "starts", headerName: "Starts" },
        { field: "last_run", headerName: "Last run", cellRenderer: (p: any) => <Badge value={p.value} /> },
        { field: "last_started", headerName: "Last started" }, { field: "last_duration", headerName: "Last duration" },
        { field: "p95_7d", headerName: "p95 (7 days)" }, { field: "runs_24h", headerName: "Runs (24 h)" },
        { field: "failed_24h", headerName: "Failed (24 h)" }, { field: "skipped_24h", headerName: "Skipped triggers (24 h)" },
        { field: "strictest_sla", headerName: "Strictest freshness target" },
        { field: "job", headerName: "Job", cellRenderer: (p: any) => (p.value ? <OutLink href={p.value}>View job</OutLink> : null) },
        { field: "sync", headerName: "Sync", cellRenderer: (p: any) => <Badge value={p.value === "ok" ? "synced" : p.value === "error" ? "failed" : "not synced"} /> },
        { field: "sync_drift", headerName: "Check", cellRenderer: (p: any) => (p.value ? <Badge value={p.value === "in_sync" ? "in_sync" : "drifted"} /> : null) },
        { field: "warnings", headerName: "Warnings", minWidth: 160, tooltipField: "warnings" },
      ]} />
    </Section>
  );
}

export default function Schedules() {
  const list = useQuery({ queryKey: ["schedules"], queryFn: () => api.get<Row[]>("/schedules"), refetchInterval: 30_000 });
  const options = useQuery({ queryKey: ["schedule-options"], queryFn: () => api.get<Options>("/schedules/options") });
  const [editing, setEditing] = useState<Row | "new" | null>(null);
  const qc = useQueryClient();
  const close = () => { setEditing(null); qc.invalidateQueries({ queryKey: ["schedule-options"] }); };
  return (
    <>
      <PageHead title="Schedules"
        lead={<>Each schedule is an ingestion job: its cadence and its ingestion groups.{" "}
          <Tip focusable text={"Each ingestion group gets its own small wrapper job that starts the Databricks job at the schedule's cadence. "
            + "A run still going at the next trigger makes Databricks skip that trigger (shown as skipped), so a slow group never piles up runs. "
            + "Edit schedules here: changes made in the Jobs UI are reported by Check jobs and undone by the next sync."}>
            <span className="info" aria-label="How schedules run">ⓘ</span></Tip></>}
        right={<Stamp at={readAt(list)} every={30} error={list.error} />} />
      <ErrorBox error={list.error || options.error} />
      <div className="toolbar"><button className="primary" disabled={!options.data} onClick={() => setEditing("new")}>New schedule</button></div>
      {list.data && list.data.length === 0 && <Notice>No schedules yet.</Notice>}
      {list.data?.map((s) => <ScheduleCard key={s.id} s={s} onEdit={() => setEditing(s)} />)}
      {editing && options.data && (
        <Editor initial={editing === "new" ? undefined : editing} options={options.data} onDone={close} />
      )}
    </>
  );
}
