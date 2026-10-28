import { Fragment, useCallback, useEffect, useId, useLayoutEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { createPortal } from "react-dom";
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

/** "28 Oct, 16:01": a short date and time for a table cell, the full one on hover (fmtTime). */
export function fmtShort(v: unknown): string {
  if (!v) return "";
  const d = new Date(String(v));
  return isNaN(d.getTime()) ? String(v) : d.toLocaleString([], { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" });
}

// Headings in words for the state database's column names, as the Databricks lists and Catalog Explorer show them.
const HEADINGS: Record<string, string> = {
  id: "Table ID", control_id: "Table ID", task_enabled: "Enabled", task_collection: "Ingestion group", src_type: "Source type",
  load_type: "Load type", src_database: "Source database", src_schema: "Source schema", src_table: "Source table",
  src_host: "Server", src_port: "Port", src_connection: "UC connection", sink_catalog: "Target catalog",
  sink_schema: "Target schema", sink_table: "Target table", sink_fqn: "Target table", primary_key: "Primary key",
  changed_at: "Changed at", changed_by: "Changed by", action: "Action", old_value: "Old value", new_value: "New value",
  run_id: "Run ID", state: "Status", result: "Result", url: "Link", started: "Started", ended: "Ended", trigger: "Trigger",
  schedule: "Schedule", freshness_sla_minutes: "Freshness target (minutes)", last_success_at: "Last loaded",
  status: "Status", phase: "Phase", created_at: "Created", updated_at: "Last updated", client_id: "Client ID",
  write_mode: "Write mode", cursor_column: "Cursor column", select_list: "Columns", secret_key: "Secret key",
  use_remote_query: "Read through UC connection", error: "Error", stage: "Stage", position: "Position",
};

/** A column name in words: its heading, else the name with spaces and a capital ("rows_merged" -> "Rows merged"). */
export function heading(key: string): string {
  return HEADINGS[key] ?? (key.replace(/_/g, " ").replace(/^./, (c) => c.toUpperCase()) || " ");
}

/** Column defs from the first row's keys (with per-column overrides), so pages stay short. */
export function autoColumns(rows: Row[], overrides: Record<string, Partial<ColDef>> = {}, hide: string[] = []): ColDef[] {
  const keys = rows.length ? Object.keys(rows[0]) : Object.keys(overrides);
  return keys.filter((k) => !hide.includes(k)).map((k) => ({
    field: k,
    headerName: heading(k),
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

// Status names as the Databricks Jobs UI has them, in sentence case and the same on every page, for the engine's and
// the Jobs API's values alike.
const STATUS_NAMES: Record<string, string> = {
  completed: "Succeeded", success: "Succeeded", succeeded: "Succeeded", failed: "Failed", internal_error: "Failed",
  running: "Running", pending: "Pending", queued: "Queued", blocked: "Blocked", skipped: "Skipped", terminated: "Terminated",
  terminating: "Terminating", canceled: "Canceled", cancelled: "Canceled", timedout: "Timed out", timed_out: "Timed out",
  paused: "Paused", unpaused: "Active", ok: "Active", abandoned: "Ended without finishing", "no run": "Not run",
  success_with_failures: "Succeeded with failures", synced: "Synced", in_sync: "In sync", drifted: "Changed outside the app",
};

/** A status value in the Databricks UI's words ("SUCCESS" and "completed" -> "Succeeded"); other values capitalized. */
export function statusName(value: unknown): string {
  const v = String(value ?? "");
  return STATUS_NAMES[v.toLowerCase()] ?? (v ? v[0].toUpperCase() + v.slice(1) : "");
}

export function Badge({ value }: { value: unknown }) {
  const v = String(value ?? "").toLowerCase();
  const tone = ["failed", "invalid", "internal_error", "timedout"].includes(v) ? "bad"
    : ["running", "pending", "queued", "seeding"].includes(v) ? "run"
    : ["skipped", "breached", "drifted"].includes(v) ? "warn"
    : ["completed", "success", "succeeded", "registered", "valid", "incremental", "loaded", "synced", "in_sync"].includes(v) ? "ok" : "";
  return <span className={`badge ${tone}`}>{statusName(value)}</span>;
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
/** Text shown on hover, focus or tap in the page itself (data-tip), not the browser's title: drawn at the page's top
 * level, below its target or above it when there is no room, and always kept inside the window, so no table or
 * scrolling box can cut it off. It closes on scroll, resize, Escape or a tap elsewhere, so it never stays behind
 * at an old place. `focusable` makes it a tab stop (for text that is cut off, or a help note). */
export function Tip({ text, children, focusable = false }: { text: string; children: ReactNode; focusable?: boolean }) {
  const ref = useRef<HTMLSpanElement>(null);
  const id = useId();
  const [at, setAt] = useState<DOMRect | null>(null);
  const show = () => ref.current && setAt(ref.current.getBoundingClientRect());
  const hide = () => setAt(null);
  useEffect(() => {
    if (!at) return;
    const away = (e: Event) => { if (!ref.current?.contains(e.target as Node)) hide(); };
    const key = (e: KeyboardEvent) => { if (e.key === "Escape") hide(); };
    window.addEventListener("scroll", hide, true);
    window.addEventListener("resize", hide);
    document.addEventListener("pointerdown", away);
    document.addEventListener("keydown", key);
    return () => {
      window.removeEventListener("scroll", hide, true);
      window.removeEventListener("resize", hide);
      document.removeEventListener("pointerdown", away);
      document.removeEventListener("keydown", key);
    };
  }, [at]);
  return (
    <span ref={ref} className="tip-target" data-tip={text} tabIndex={focusable ? 0 : undefined}
      aria-describedby={at ? id : undefined} onMouseEnter={show} onFocus={show} onMouseLeave={hide} onBlur={hide}
      onClick={(e) => { e.stopPropagation(); if (at) hide(); else show(); }}>
      {children}
      {at && createPortal(<TipPop id={id} text={text} target={at} />, document.body)}
    </span>
  );
}

function TipPop({ id, text, target }: { id: string; text: string; target: DOMRect }) {
  const ref = useRef<HTMLDivElement>(null);
  const [place, setPlace] = useState({ left: target.left, top: target.bottom + 6, ready: false });
  useLayoutEffect(() => {
    const box = ref.current?.getBoundingClientRect();
    if (!box) return;
    const gap = 6, edge = 4;
    const below = target.bottom + gap;
    const top = below + box.height <= window.innerHeight - edge ? below : Math.max(edge, target.top - gap - box.height);
    const left = Math.min(Math.max(edge, target.left), Math.max(edge, window.innerWidth - box.width - edge));
    setPlace({ left, top, ready: true });
  }, [target]);
  return <div ref={ref} id={id} className="tip-pop" role="tooltip"
    style={{ left: place.left, top: place.top, visibility: place.ready ? "visible" : "hidden" }}>{text}</div>;
}

/** A host name on one line: cut with an ellipsis when it does not fit, its full name (and port) on hover. */
export function Host({ host, port }: { host: string; port?: unknown }) {
  const full = port ? `${host}:${port}` : host;
  const ref = useRef<HTMLSpanElement>(null);
  const [cut, setCut] = useState(false);       // only a name that is cut off is a tab stop
  useLayoutEffect(() => {
    const measure = () => ref.current && setCut(ref.current.scrollWidth > ref.current.clientWidth + 1);
    measure();
    window.addEventListener("resize", measure);
    return () => window.removeEventListener("resize", measure);
  }, [full]);
  return <Tip text={full} focusable={cut}><span ref={ref} className="host">{full}</span></Tip>;
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
  return <Tag tone={tone}>{statusName(v) || "Unknown"}</Tag>;
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

export interface Progress { unchanged: number; loaded: number; inFlight: number; failed: number; total: number; notRun?: number }

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
  const notRun = p.notRun ?? 0;
  const left = Math.max(0, p.total - p.unchanged - p.loaded - p.inFlight - p.failed - notRun);
  const title = `${fmtInt(p.unchanged)} unchanged, ${fmtInt(p.loaded)} loaded, ${fmtInt(p.inFlight)} in flight, `
    + `${fmtInt(p.failed)} failed${notRun ? `, ${fmtInt(notRun)} not run` : ""}${left ? `, ${fmtInt(left)} to go` : ""}`;
  return (
    <div className={`bar${big ? " big" : ""}`} title={title} role="img" aria-label={title}>
      {p.unchanged > 0 && <span className="seg-unchanged" style={{ width: pct(p.unchanged) }} />}
      {p.loaded > 0 && <span className="seg-loaded" style={{ width: pct(p.loaded) }} />}
      {p.inFlight > 0 && <span className="seg-flight" style={{ width: pct(p.inFlight) }} />}
      {p.failed > 0 && <span className="seg-failing" style={{ width: pct(p.failed) }} />}
      {notRun > 0 && <span className="seg-notrun" style={{ width: pct(notRun) }} />}
    </div>
  );
}

/** The bar's key; a finished run has nothing to go, so it shows what did not run instead. */
export function ProgressKey({ p, finished }: { p?: Progress; finished?: boolean }) {
  const left = p ? Math.max(0, p.total - p.unchanged - p.loaded - p.inFlight - p.failed - (p.notRun ?? 0)) : undefined;
  const n = (v?: number) => (v === undefined ? null : <> <b>{fmtInt(v)}</b></>);
  return (
    <div className="key">
      <span><i className="seg-unchanged" />Unchanged{n(p?.unchanged)}</span><span><i className="seg-loaded" />Loaded{n(p?.loaded)}</span>
      {!finished && <span><i className="seg-flight" />In flight{n(p?.inFlight)}</span>}<span><i className="seg-failing" />Failed{n(p?.failed)}</span>
      {finished ? <span><i className="seg-notrun" />Not run{n(p?.notRun ?? 0)}</span>
        : <span><i className="seg-track" />{p ? "To go" : "Not checked yet"}{n(left)}</span>}
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

/** When the shown data was read, and how often it refreshes. When the latest refresh failed, it says the page
 * shows older data and why, so a broken read never looks calm. */
export function Stamp({ at, every, error }: { at?: string; every?: number; error?: unknown }) {
  if (error && at) return <span className="stamp stale" title={(error as ApiError).message ?? String(error)}>
    Not updated since {fmtClock(at)}: {(error as ApiError).message ?? String(error)}</span>;
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

/** A dialog as the Databricks UI asks before a change: a title, what it does, then Cancel and the action at the right,
 * red when it destroys or disables something. Escape, a click outside and Cancel all cancel. */
export function Modal({ title, action, danger, onClose, children }: { title: string; action: string; danger?: boolean;
                        onClose: (confirmed: boolean) => void; children?: ReactNode }) {
  const id = useId();
  useEffect(() => {
    const key = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(false); };
    document.addEventListener("keydown", key);
    return () => document.removeEventListener("keydown", key);
  }, [onClose]);
  return createPortal(
    <div className="modal-backdrop" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(false); }}>
      <div className="modal" role="dialog" aria-modal="true" aria-labelledby={id}>
        <h2 id={id}>{title}</h2>
        {children && <div className="modal-body">{children}</div>}
        <div className="modal-actions">
          <button autoFocus onClick={() => onClose(false)}>Cancel</button>
          <button className={danger ? "danger" : "primary"} onClick={() => onClose(true)}>{action}</button>
        </div>
      </div>
    </div>, document.body);
}

export interface ConfirmRequest { title: string; body?: ReactNode; action: string; danger?: boolean }

/** Ask with a Modal from an event handler: `confirm.ask({...}).then((ok) => ok && act())`, with `confirm.dialog` rendered
 * on the page. */
export function useConfirm(): { ask: (request: ConfirmRequest) => Promise<boolean>; dialog: ReactNode } {
  const [request, setRequest] = useState<(ConfirmRequest & { resolve: (ok: boolean) => void }) | null>(null);
  const ask = useCallback((r: ConfirmRequest) => new Promise<boolean>((resolve) => setRequest({ ...r, resolve })), []);
  const close = useCallback((ok: boolean) => { setRequest((r) => { r?.resolve(ok); return null; }); }, []);
  return { ask, dialog: request && <Modal title={request.title} action={request.action} danger={request.danger}
    onClose={close}>{request.body}</Modal> };
}

/** A button that asks in a Modal before running a write: its title (default "<label>?"), what it does and its action. */
export function ConfirmButton(props: {
  label: string;
  confirmLabel?: string;
  title?: string;
  message?: ReactNode;
  onConfirm: () => unknown;
  disabled?: boolean;
  danger?: boolean;
  plain?: boolean;
}) {
  const confirm = useConfirm();
  const [busy, setBusy] = useState(false);
  const run = async () => {
    if (!(await confirm.ask({ title: props.title ?? `${props.label}?`, body: props.message,
                              action: props.confirmLabel ?? props.label, danger: props.danger }))) return;
    setBusy(true);
    try { await props.onConfirm(); } finally { setBusy(false); }
  };
  return (
    <>
      <button className={props.danger ? "danger" : props.plain ? "" : "primary"} disabled={props.disabled || busy} onClick={run}>
        {busy ? "Working…" : props.label}
      </button>
      {confirm.dialog}
    </>
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
           inFlight: Number(p?.in_flight || 0), failed: Number(p?.failed || 0), notRun: Number(p?.not_run || 0) };
}

/** A job run's name: its job and its slot's start, "East hourly, 14:00 job run" (with the day when not today). */
/** A time as a clock reading today, and with its day otherwise ("23 Oct, 21:00"). */
export function fmtSlot(slot: unknown): string {
  if (!slot) return "";
  const at = new Date(String(slot));
  const today = new Date().toDateString() === at.toDateString();
  return at.toLocaleString([], today ? { hour: "2-digit", minute: "2-digit" }
    : { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" });
}

export function jobRunName(jobName: unknown, slot: unknown): string {
  return `${jobName}, ${fmtSlot(slot)} job run`;
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

/** A name that wraps only between words, so a hyphenated or underscored name never breaks mid-word. */
export function Name({ children }: { children: unknown }) {
  const words = String(children ?? "").split(" ");
  return <>{words.map((w, i) => <Fragment key={i}>{i > 0 && " "}<span className="word">{w}</span></Fragment>)}</>;
}

/** A menu behind a button, as the Databricks UI keeps a job's less common and destructive actions (the kebab) and the
    user's details (the avatar). It closes on a click outside or Escape. */
export function Menu({ label, title, buttonClass = "kebab", children }: { label: ReactNode; title: string;
                     buttonClass?: string; children: ReactNode }) {
  const [open, setOpen] = useState(false);
  const box = useRef<HTMLSpanElement>(null);
  useEffect(() => {
    if (!open) return;
    const close = (e: MouseEvent | KeyboardEvent) => {
      if (e instanceof KeyboardEvent ? e.key === "Escape" : !box.current?.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", close);
    document.addEventListener("keydown", close);
    return () => { document.removeEventListener("mousedown", close); document.removeEventListener("keydown", close); };
  }, [open]);
  return (
    <span className="menu" ref={box}>
      <button className={buttonClass} aria-haspopup="menu" aria-expanded={open} aria-label={title} title={title}
        onClick={() => setOpen(!open)}>{label}</button>
      {open && <div className="menu-items" role="menu" onClick={(e) => (e.target as HTMLElement).closest("button") && setOpen(false)}>{children}</div>}
    </span>
  );
}
