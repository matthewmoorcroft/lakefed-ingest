import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { AgGridReact } from "ag-grid-react";
import { themeQuartz, type ColDef, type GridApi, type GridOptions } from "ag-grid-community";
import { ApiError, type Row } from "./api";

// AG Grid in the workspace look: the design system's font, border, header and accent colors (styles.css).
export const gridTheme = themeQuartz.withParams({
  fontFamily: "var(--db-font-family)", fontSize: 13, headerFontSize: 13, spacing: 5, wrapperBorderRadius: 8,
  borderColor: "var(--db-border-base)", headerBackgroundColor: "var(--db-surface-subtle)", headerFontWeight: 600,
  foregroundColor: "var(--db-text-base)", accentColor: "var(--db-accent-surface)", rowHoverColor: "var(--db-surface-hover)",
  selectedRowBackgroundColor: "var(--db-surface-selected)",
});
const theme = gridTheme;

/** The element's current width in pixels, kept up to date as the window resizes. A callback ref, so it also
 * measures elements that appear later (after data loads). */
export function useWidth<T extends HTMLElement>(): [(element: T | null) => void, number] {
  const [element, setElement] = useState<T | null>(null);
  const [width, setWidth] = useState(0);
  useEffect(() => {
    if (!element) return;
    const observer = new ResizeObserver(([entry]) => setWidth(entry.contentRect.width));
    observer.observe(element);
    return () => observer.disconnect();
  }, [element]);
  return [setElement, width];
}

export function fmtTime(v: unknown): string {
  if (!v) return "";
  const d = new Date(String(v));
  return isNaN(d.getTime()) ? String(v) : d.toLocaleString();
}

/** Column defs from the first row's keys (with per-column overrides), so pages stay short. */
export function autoColumns(rows: Row[], overrides: Record<string, Partial<ColDef>> = {}, hide: string[] = []): ColDef[] {
  const keys = rows.length ? Object.keys(rows[0]) : Object.keys(overrides);
  return keys.filter((k) => !hide.includes(k)).map((k) => ({
    field: k,
    headerName: k,
    ...(/(_at|started|finished|ended)$/.test(k)
      ? { valueFormatter: (p: any) => fmtTime(p.value) } : {}),
    ...overrides[k],
  }));
}

/** Space kept free below a panel that fills the window: the card's padding and the page's bottom margin. */
const FILL_GAP = 64;

/**
 * The height that lets an element reach the bottom of the window from where it starts on the page, so a page's
 * main table uses the screen's height and scrolls inside itself. ``share`` takes a part of the space left (for a
 * page with two panels), ``reserve`` leaves room in pixels for a panel below, ``min`` keeps it usable on short
 * windows. Kept up to date as the window or page changes.
 */
export function useFillHeight<T extends HTMLElement>(min = 280, share = 1, reserve = 0): [(element: T | null) => void, number] {
  const [element, setElement] = useState<T | null>(null);
  const [height, setHeight] = useState(min);
  useEffect(() => {
    if (!element) return;
    const measure = () => {
      const top = element.getBoundingClientRect().top + window.scrollY;
      setHeight(Math.max(min, Math.floor((window.innerHeight - top - FILL_GAP - reserve) * share)));
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(document.body);
    window.addEventListener("resize", measure);
    return () => { observer.disconnect(); window.removeEventListener("resize", measure); };
  }, [element, min, share, reserve]);
  return [setElement, height];
}

/** A box for a plain table that fills the window's height and scrolls inside, with its header kept in view. */
export function FillBox({ children, min, share }: { children: ReactNode; min?: number; share?: number }) {
  const [ref, height] = useFillHeight<HTMLDivElement>(min, share);
  return <div ref={ref} className="table-wrap fill" style={{ maxHeight: height }}>{children}</div>;
}

export function Grid(props: {
  rows: Row[] | undefined;
  columns?: ColDef[];
  /** A fixed height; by default the grid fills the window's height from where it starts. */
  height?: number | string;
  /** Size to the rows when they are few (a short list), never taller than the window allows. */
  fitRows?: boolean;
  /** Part of the window's remaining height to take, when the page has another panel below. */
  fillShare?: number;
  /** Pixels to leave below the grid for a small panel that follows it. */
  fillReserve?: number;
  selection?: "single" | "multiple";
  onSelect?: (rows: Row[]) => void;
  options?: GridOptions;
  getRowId?: (r: Row) => string;
}) {
  const cols = useMemo(() => props.columns ?? autoColumns(props.rows ?? []), [props.columns, props.rows]);
  const box = useRef<HTMLDivElement>(null);
  const [fillRef, fillHeight] = useFillHeight<HTMLDivElement>(props.fitRows ? 140 : 280, props.fillShare ?? 1, props.fillReserve ?? 0);
  const rowsHeight = 52 + (props.rows?.length ?? 0) * 42 + 4;
  const height = props.height ?? (props.fitRows ? Math.min(rowsHeight, fillHeight) : fillHeight);
  const byContent = (props.options?.autoSizeStrategy?.type ?? "fitCellContents") === "fitCellContents";
  /** Columns sized to their content, then stretched when they leave the grid's width unused (wide screens). */
  const fill = (gridApi: GridApi) => {
    const width = box.current?.clientWidth ?? 0;
    if (!width || gridApi.isDestroyed()) return;
    if (byContent) gridApi.autoSizeAllColumns();
    const used = gridApi.getAllDisplayedColumns().reduce((sum, column) => sum + column.getActualWidth(), 0);
    if (used < width - 2) gridApi.sizeColumnsToFit();
  };
  return (
    <div ref={(element) => { box.current = element; fillRef(element); }} style={{ height, width: "100%" }}>
      <AgGridReact
        theme={theme}
        rowData={props.rows ?? []}
        columnDefs={cols}
        defaultColDef={{ sortable: true, filter: true, resizable: true, minWidth: 90 }}
        autoSizeStrategy={{ type: "fitCellContents" }}
        rowSelection={props.selection
          ? { mode: props.selection === "single" ? "singleRow" : "multiRow", enableClickSelection: props.selection === "single" }
          : undefined}
        onSelectionChanged={(e) => props.onSelect?.(e.api.getSelectedRows())}
        getRowId={props.getRowId ? (p) => props.getRowId!(p.data) : undefined}
        loading={props.rows === undefined}
        onFirstDataRendered={(e) => fill(e.api)}
        onRowDataUpdated={(e) => fill(e.api)}
        onGridSizeChanged={(e) => fill(e.api)}
        {...props.options}
      />
    </div>
  );
}

export function Metric({ label, value, tone }: { label: string; value: ReactNode; tone?: "bad" | "warn" | "ok" }) {
  return (
    <div className={`metric ${tone ?? ""}`}>
      <div className="metric-label">{label}</div>
      <div className="metric-value">{value}</div>
    </div>
  );
}

export function Badge({ value }: { value: unknown }) {
  const v = String(value ?? "");
  const tone = ["failed", "FAILED", "invalid", "INTERNAL_ERROR"].includes(v) ? "bad"
    : ["running", "RUNNING", "PENDING", "seeding", "skipped", "breached"].includes(v) ? "warn"
    : ["ok", "completed", "SUCCESS", "registered", "valid", "incremental", "loaded"].includes(v) ? "ok" : "";
  return <span className={`badge ${tone}`}>{v}</span>;
}

// ---- monitoring views --------------------------------------------------------------------------

const NUMBER = new Intl.NumberFormat("en-US");

/** 1234567 -> "1,234,567"; empty for null. */
export function fmtInt(v: unknown): string {
  return v === null || v === undefined || v === "" ? "" : NUMBER.format(Number(v));
}

const COMPACT = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });

/** A count for a metric tile: in full below a billion, else short ("296.3B") with the full count on hover, so it fits. */
export function BigCount({ value }: { value: unknown }) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Math.abs(n) < 1e9 ? <>{fmtInt(n)}</> : <span title={fmtInt(n)}>{COMPACT.format(n)}</span>;
}

/** A host name that wraps only after a dot, never inside a name or the port. */
export function Host({ host, port }: { host: string; port?: unknown }) {
  const parts = host.split(".");
  return (
    <span className="host" title={port ? `${host}:${port}` : host}>
      {parts.map((part, i) => <span key={i}>{part}{i < parts.length - 1 ? <>.<wbr /></> : port ? `:${port}` : ""}</span>)}
    </span>
  );
}

/** "database" for one, "databases" for any other count. */
export function plural(count: unknown, word: string): string {
  return Number(count) === 1 ? word : `${word}s`;
}

/** Seconds -> "8m 12s", "1h 4m", "41s". */
export function fmtDuration(seconds: unknown): string {
  const s = Math.max(0, Math.round(Number(seconds) || 0));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}

/** A timestamp -> "4 s ago", "38 min ago", "2.0 h ago", "3 days ago"; "never" without one. */
export function fmtAgo(v: unknown, now = Date.now()): string {
  if (!v) return "never";
  const s = (now - new Date(String(v)).getTime()) / 1000;
  if (!isFinite(s)) return String(v);
  if (s < 60) return `${Math.max(0, Math.round(s))} s ago`;
  if (s < 3600) return `${Math.round(s / 60)} min ago`;
  if (s < 48 * 3600) return `${(s / 3600).toFixed(1)} h ago`;
  return `${Math.round(s / 86400)} days ago`;
}

/** Hours and minutes of a timestamp, local time. */
export function fmtClock(v: unknown): string {
  return v ? new Date(String(v)).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : "";
}

export type Tone = "ok" | "warn" | "bad" | "run" | "first" | "";

/** A status tag with a dot and a label, never color alone. */
export function Tag({ tone = "", children }: { tone?: Tone; children: ReactNode }) {
  return <span className={`tag ${tone}`}><span className="dot" />{children}</span>;
}

export interface Outcome { label: string; tone: string; note?: string | null }

/** A run's status as a tag: its outcome (queries.run_outcome) when the server sent one, else its engine status. */
export function RunStatus({ status, outcome }: { status: unknown; outcome?: Outcome | null }) {
  if (outcome) {
    const tone = (["ok", "warn", "bad", "run"].includes(outcome.tone) ? outcome.tone : "") as Tone;
    return <span title={outcome.note ?? undefined}><Tag tone={tone}>{outcome.label}</Tag></span>;
  }
  const v = String(status ?? "");
  const tone: Tone = v === "completed" ? "ok" : v === "running" ? "run" : v === "failed" || v === "abandoned" ? "bad"
    : v === "skipped" ? "warn" : "";
  return <Tag tone={tone}>{v ? v[0].toUpperCase() + v.slice(1) : "Unknown"}</Tag>;
}

/** Tables a minute -> "12", "2.5", "0.4". */
export function fmtSpeed(perMinute: unknown): string {
  const v = Number(perMinute) || 0;
  return v >= 10 ? fmtInt(Math.round(v)) : v.toFixed(1);
}

/** Seconds left -> "about 7 min", "under a minute", "about 2.5 h". */
export function fmtLeft(seconds: unknown): string {
  const s = Math.max(0, Number(seconds) || 0);
  if (s < 60) return "under a minute";
  if (s < 5400) return `about ${Math.round(s / 60)} min`;
  return `about ${(s / 3600).toFixed(1)} h`;
}

export interface StateCounts { state_current?: unknown; state_late?: unknown; state_first_load?: unknown; state_failing?: unknown }

const STATES = [
  ["state_current", "seg-current", "current"], ["state_late", "seg-late", "late"],
  ["state_first_load", "seg-first", "waiting for a first load"], ["state_failing", "seg-failing", "failing"],
] as const;

/** Each enabled table's state now, as one bar. */
export function StateBar({ row }: { row: StateCounts }) {
  const counts = STATES.map(([k]) => Number((row as any)[k] || 0));
  const total = counts.reduce((a, b) => a + b, 0);
  const title = STATES.map(([, , label], i) => `${fmtInt(counts[i])} ${label}`).join(", ");
  return (
    <div className="bar" title={title} role="img" aria-label={title}>
      {total > 0 && STATES.map(([k, cls], i) => counts[i] > 0 && <span key={k} className={cls} style={{ width: `${(100 * counts[i]) / total}%` }} />)}
    </div>
  );
}

export function StateKey() {
  return (
    <div className="key">
      <span><i className="seg-current" />Current</span><span><i className="seg-late" />Late</span>
      <span><i className="seg-first" />Waiting for a first load</span><span><i className="seg-failing" />Failing</span>
    </div>
  );
}

export interface Progress { unchanged: number; loaded: number; inFlight: number; failed: number; total: number }

/** A run's progress over its collection: checked (unchanged + loaded), in flight, failed, to go. */
export function progressOf(run: Row): Progress {
  const total = Number(run.enabled_tables || 0);
  const loaded = Number(run.tables_ok || 0);
  const checked = Number(run.tables_checked || 0);
  return { total, loaded, unchanged: Math.max(0, checked - loaded), inFlight: Number(run.tables_running || 0),
           failed: Number(run.tables_failed || 0) };
}

export function ProgressBar({ p, big }: { p: Progress; big?: boolean }) {
  const pct = (n: number) => `${p.total ? (100 * n) / p.total : 0}%`;
  const left = Math.max(0, p.total - p.unchanged - p.loaded - p.inFlight - p.failed);
  const title = `${fmtInt(p.unchanged)} unchanged, ${fmtInt(p.loaded)} loaded, ${fmtInt(p.inFlight)} in flight, `
    + `${fmtInt(p.failed)} failed, ${fmtInt(left)} to go`;
  return (
    <div className={`bar${big ? " big" : ""}`} title={title} role="img" aria-label={title}>
      {p.unchanged > 0 && <span className="seg-unchanged" style={{ width: pct(p.unchanged) }} />}
      {p.loaded > 0 && <span className="seg-loaded" style={{ width: pct(p.loaded) }} />}
      {p.inFlight > 0 && <span className="seg-flight" style={{ width: pct(p.inFlight) }} />}
      {p.failed > 0 && <span className="seg-failing" style={{ width: pct(p.failed) }} />}
    </div>
  );
}

export function ProgressKey({ p }: { p?: Progress }) {
  const left = p ? Math.max(0, p.total - p.unchanged - p.loaded - p.inFlight - p.failed) : undefined;
  const n = (v?: number) => (v === undefined ? null : <> <b>{fmtInt(v)}</b></>);
  return (
    <div className="key">
      <span><i className="seg-unchanged" />Unchanged{n(p?.unchanged)}</span><span><i className="seg-loaded" />Loaded{n(p?.loaded)}</span>
      <span><i className="seg-flight" />In flight{n(p?.inFlight)}</span><span><i className="seg-failing" />Failed{n(p?.failed)}</span>
      <span><i className="seg-track" />{p ? "To go" : "Not checked yet"}{n(left)}</span>
    </div>
  );
}

/** "12,094 of 13,000 tables checked (93%), 96 in flight, 810 to go". */
export function progressText(p: Progress): string {
  const checked = p.unchanged + p.loaded;
  const left = Math.max(0, p.total - checked - p.inFlight - p.failed);
  const pct = p.total ? Math.floor((100 * checked) / p.total) : 0;
  return `${fmtInt(checked)} of ${fmtInt(p.total)} tables checked (${pct}%), ${fmtInt(p.inFlight)} in flight, ${fmtInt(left)} to go`;
}

/** When the shown data was read, and how often it refreshes. */
export function Stamp({ at, every }: { at?: string; every?: number }) {
  if (!at) return null;
  return <span className="stamp">Updated {fmtAgo(at)}{every ? `, every ${every} s` : ""}</span>;
}

export function PageHead({ title, lead, crumb, right }: { title: ReactNode; lead?: ReactNode; crumb?: ReactNode; right?: ReactNode }) {
  return (
    <div className="page-head">
      <div>{crumb && <div className="crumb">{crumb}</div>}<h1>{title}</h1>{lead && <p className="lead">{lead}</p>}</div>
      {right && <div>{right}</div>}
    </div>
  );
}

export function ErrorBox({ error }: { error: unknown }) {
  if (!error) return null;
  const e = error as ApiError;
  return (
    <div className="alert bad">
      <strong>Error:</strong> {e.message ?? String(error)}
      {e.hint && <div className="hint">{e.hint}</div>}
    </div>
  );
}

export function Notice({ tone = "info", children }: { tone?: "info" | "ok" | "warn" | "bad"; children: ReactNode }) {
  return <div className={`alert ${tone}`}>{children}</div>;
}

/** A button that asks for a second click before running a write. */
export function ConfirmButton(props: {
  label: string;
  confirmLabel?: string;
  onConfirm: () => unknown;
  disabled?: boolean;
  danger?: boolean;
}) {
  const [armed, setArmed] = useState(false);
  const [busy, setBusy] = useState(false);
  if (!armed)
    return (
      <button className={props.danger ? "danger" : "primary"} disabled={props.disabled || busy} onClick={() => setArmed(true)}>
        {props.label}
      </button>
    );
  return (
    <span className="confirm">
      <button
        className={props.danger ? "danger" : "primary"}
        disabled={busy}
        onClick={async () => {
          setBusy(true);
          try { await props.onConfirm(); } finally { setBusy(false); setArmed(false); }
        }}
      >
        {busy ? "Working…" : props.confirmLabel ?? `Confirm: ${props.label}`}
      </button>
      <button className="ghost" disabled={busy} onClick={() => setArmed(false)}>Cancel</button>
    </span>
  );
}

export function Section({ title, actions, note, children }: { title: ReactNode; actions?: ReactNode; note?: ReactNode;
                          children: ReactNode }) {
  return (
    <section className="card">
      <div className="card-head">
        <div><h2>{title}</h2>{note && <p>{note}</p>}</div>
        <div className="card-actions">{actions}</div>
      </div>
      {children}
    </section>
  );
}

/** The server's progress shape (ingestion_jobs._progress) as the progress bar's. */
export function progressFrom(p?: Row | null): Progress {
  return { total: Number(p?.total || 0), unchanged: Number(p?.unchanged || 0), loaded: Number(p?.loaded || 0),
           inFlight: Number(p?.in_flight || 0), failed: Number(p?.failed || 0) };
}

/** A job run's name: its job and its slot's start, "East hourly, 14:00 job run" (with the day when not today). */
export function jobRunName(jobName: unknown, slot: unknown): string {
  const at = new Date(String(slot));
  const today = new Date().toDateString() === at.toDateString();
  const when = at.toLocaleString([], today ? { hour: "2-digit", minute: "2-digit" }
    : { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" });
  return `${jobName}, ${when} job run`;
}

export const jobRunPath = (jobId: unknown, slot: unknown) =>
  `/ingestion-jobs/${encodeURIComponent(String(jobId))}/runs/${encodeURIComponent(String(slot))}`;

/** A job's Status cell (ingestion_jobs.status): its state and issues together, never color alone. */
export function JobStatus({ status }: { status?: Row | null }) {
  if (!status) return null;
  const tone = (["ok", "warn", "bad", "run"].includes(status.tone) ? status.tone : "") as Tone;
  const detail: string[] = [...(status.detail ?? [])];
  if (status.paused_since) detail[detail.length - 1] = `paused since ${fmtClock(status.paused_since)}`;
  if (status.expected_end) detail.push(`ends about ${fmtClock(status.expected_end)}`);
  return (
    <div className="status-cell">
      <Tag tone={tone}>{status.label}</Tag>
      {detail.length > 0 && <span className="sub">{detail.join(" · ")}</span>}
      {status.cause && <span className="sub">{status.cause}</span>}
    </div>
  );
}

/** "1 to 25 of 40 ingestion jobs", with previous and next. */
export function Pager({ offset, limit, total, noun, onPage }: { offset: number; limit: number; total: number; noun: string;
                        onPage: (offset: number) => void }) {
  const last = Math.min(offset + limit, total);
  return (
    <div className="pager">
      <span className="muted">{total ? `${fmtInt(offset + 1)} to ${fmtInt(last)} of ${fmtInt(total)}` : "0"} {plural(total, noun)}</span>
      {total > limit && (
        <span className="pager-buttons">
          <button className="ghost" disabled={offset === 0} onClick={() => onPage(Math.max(0, offset - limit))}>Previous</button>
          <button className="ghost" disabled={last >= total} onClick={() => onPage(offset + limit)}>Next</button>
        </span>
      )}
    </div>
  );
}

/** One database's page: its group, server ("-" when blank) and name. */
export const databasePath = (group: unknown, server: unknown, database: unknown) =>
  `/collections/${encodeURIComponent(String(group))}/databases/${encodeURIComponent(String(server || "-"))}/${encodeURIComponent(String(database ?? ""))}`;
