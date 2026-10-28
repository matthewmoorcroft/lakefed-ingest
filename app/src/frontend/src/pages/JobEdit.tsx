import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate, useParams } from "react-router-dom";
import { api, qs, type Row } from "../api";
import { Crumbs, ConfirmButton, ErrorBox, Notice, PageHead, Pager, Section, StateBar, fmtInt, plural, useConfirm } from "../components";
import { usePermissions } from "../permissions";

// Edit an ingestion job: what it ingests (a tree from its servers), how it writes, its identity columns, when it
// runs and its advanced configuration. A change in the tree applies once its dialog is confirmed, as Catalog Explorer
// asks before a change to data; the rest is saved with Save.

const TAGS = "{server} {database} {schema} {table}";
const CADENCE_LABEL = (m: number) => (m < 60 ? `${m} minutes` : m === 60 ? "hour" : m === 1440 ? "day" : `${m / 60} hours`);
const PAGE = 25;
const LOAD_TYPES: Record<string, string> = { ct: "Change tracking", cursor: "Cursor column", full: "Full reload" };

/** Why the signed-in user may not change this job (their Databricks permission), or null when they may. */
type Locked = string | null;

/** When it runs, its parameters and its name as the form shows them for a stored schedule: the baseline Save compares with. */
function scheduleForm(schedule: Row) {
  // A set-time schedule keeps its stored zone; Europe/London is only the default offered when a minute or
  // hourly schedule (always UTC) becomes a set-time one.
  const start = Number(schedule.options?.start_minute ?? 0);
  return {
    run: { cadence_minutes: schedule.cadence_minutes, start: `${String(Math.floor(start / 60)).padStart(2, "0")}:${String(start % 60).padStart(2, "0")}`,
           timezone: Number(schedule.cadence_minutes) > 60 ? schedule.timezone : "Europe/London",
           paused: schedule.paused, notify_emails: schedule.notify_emails ?? "" } as Row,
    params: Object.fromEntries(Object.entries(schedule.parameters ?? {}).map(([k, v]) => [k, String(v)])) as Record<string, string>,
    name: String(schedule.name),
  };
}

const startMinute = (hhmm: string) => { const [h, m] = String(hhmm || "00:00").split(":").map(Number); return (h || 0) * 60 + (m || 0); };
const value = (s?: Row) => (s && "value" in s ? s.value : undefined);
const mixedText = (s?: Row) => (s?.mixed ? Object.entries(s.mixed).map(([v, n]) => `${v || "(none)"}: ${fmtInt(n)}`).join(", ") : "");

function Mixed({ s }: { s?: Row }) {
  return s?.mixed ? <span className="help">Differs between tables ({mixedText(s)}); a value here sets it for all.</span> : null;
}

function TableRows({ jobId, server, database, locked }: { jobId: string; server: string; database: string; locked: Locked }) {
  const [offset, setOffset] = useState(0);
  const qc = useQueryClient();
  const tables = useQuery({
    queryKey: ["job-tree", jobId, server, database, offset],
    queryFn: () => api.get<Row>(`/ingestion-jobs/${jobId}/tree${qs({ server, database, offset, limit: PAGE })}`),
  });
  const confirm = useConfirm();
  // A load type is the same in every database of the job: the preview counts the tables it changes and reloads.
  const edit = useMutation({
    mutationFn: async ({ t, load_type }: { t: Row; load_type: string }) => {
      const body = { schema: t.schema, table: t.table, changes: { load_type } };
      const preview = (await api.post<Row>(`/ingestion-jobs/${jobId}/tables/edit`, { ...body, dry_run: true })).summary;
      const ok = await confirm.ask({ title: `Change the load type of ${t.schema}.${t.table}?`, action: "Change load type",
        body: <><p>It becomes {LOAD_TYPES[load_type] ?? load_type} in all {fmtInt(preview.changed)} {plural(preview.changed, "table")} of
          this name in the job's databases.</p>
          {Number(preview.reseed) > 0 && <p>The next run loads {fmtInt(preview.reseed)} of them again from their sources.</p>}</> });
      return ok ? api.post(`/ingestion-jobs/${jobId}/tables/edit`, { ...body, dry_run: false }) : null;
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["job-tree", jobId] }),
  });
  const enable = useMutation({
    mutationFn: async (t: Row) => {
      const ok = await confirm.ask(t.enabled
        ? { title: `Disable ${t.schema}.${t.table} in ${database}?`, action: "Disable", danger: true,
            body: "Runs stop loading it. Its target table, checkpoint and history stay." }
        : { title: `Enable ${t.schema}.${t.table} in ${database}?`, action: "Enable", body: "Its group's next run loads it." });
      return ok ? api.post(`/ingestion-jobs/${jobId}/enable`, { server, database, schema: t.schema, table: t.table, enabled: !t.enabled }) : null;
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["job-tree", jobId] }),
  });
  return (
    <>
      {(tables.data?.items ?? []).map((t: Row) => (
        <tr key={t.id} className="l3">
          <td className="name">{t.schema}.{t.table}</td>
          <td><select value={t.load_type} disabled={edit.isPending || !!locked} title={locked ?? "Changes this table in every database of the job"}
            onChange={(e) => edit.mutate({ t, load_type: e.target.value })}>
            <option value="ct">Change tracking</option><option value="cursor">Cursor column</option>
            <option value="full">Full reload</option></select></td>
          <td className="mono">{t.primary_key}</td>
          <td className="num">{fmtInt(t.columns)} columns</td>
          <td className="mono">{t.target}</td>
          <td><input type="checkbox" checked={!!t.enabled} disabled={enable.isPending || !!locked} title={locked ?? undefined}
            onChange={() => enable.mutate(t)} /></td>
          <td><StateBar row={t} /></td>
        </tr>
      ))}
      {(tables.data?.total ?? 0) > PAGE && (
        <tr className="l3"><td colSpan={7}><Pager offset={offset} limit={PAGE} total={tables.data!.total} noun="table" onPage={setOffset} /></td></tr>
      )}
      {(edit.error || enable.error || tables.error) && <tr className="l3"><td colSpan={7}><ErrorBox error={edit.error || enable.error || tables.error} /></td></tr>}
      {confirm.dialog}
    </>
  );
}

function DatabaseRows({ jobId, server, locked }: { jobId: string; server: string; locked: Locked }) {
  const [offset, setOffset] = useState(0);
  const [search, setSearch] = useState("");
  const [open, setOpen] = useState<string | null>(null);
  const qc = useQueryClient();
  const dbs = useQuery({
    queryKey: ["job-tree", jobId, server, "", search, offset],
    queryFn: () => api.get<Row>(`/ingestion-jobs/${jobId}/tree${qs({ server, search, offset, limit: PAGE })}`),
  });
  const confirm = useConfirm();
  const enable = useMutation({
    mutationFn: async (d: Row) => {
      const on = Number(d.enabled_tables) === 0;
      const ok = await confirm.ask(on
        ? { title: `Enable all ${fmtInt(d.tables)} ${plural(d.tables, "table")} of ${d.database}?`, action: "Enable",
            body: "Its group's next run loads them." }
        : { title: `Disable all ${fmtInt(d.enabled_tables)} enabled ${plural(d.enabled_tables, "table")} of ${d.database}?`,
            action: "Disable", danger: true, body: "Runs stop loading them. Their target tables, checkpoints and history stay." });
      return ok ? api.post(`/ingestion-jobs/${jobId}/enable`, { server, database: d.database, enabled: on }) : null;
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["job-tree", jobId] }),
  });
  return (
    <>
      <tr className="l2"><td colSpan={7}><input className="search tree-search" placeholder={`Find a database on ${server}`} value={search}
        onChange={(e) => { setSearch(e.target.value); setOffset(0); }} /></td></tr>
      {(dbs.data?.items ?? []).map((d: Row) => (
        <Fragment key={d.database}>
          <tr className="l2">
            <td className="name"><button className="twisty" aria-expanded={open === d.database}
              onClick={() => setOpen(open === d.database ? null : d.database)}>{open === d.database ? "▾" : "▸"}</button>{d.database}</td>
            <td className="muted">{d.client_id ? `client_id ${d.client_id}` : ""}</td>
            <td></td>
            <td className="num">{fmtInt(d.tables)} {Number(d.missing) > 0 && <span className="tag warn">{fmtInt(d.missing)} missing</span>}</td>
            <td></td>
            <td><input type="checkbox" checked={Number(d.enabled_tables) > 0} disabled={enable.isPending || !!locked}
              title={locked ?? "Enables or disables every table of this database"} onChange={() => enable.mutate(d)} /></td>
            <td>{Number(d.enabled_tables) > 0 ? <StateBar row={d} /> : <span className="muted">disabled</span>}</td>
          </tr>
          {open === d.database && <TableRows jobId={jobId} server={server} database={d.database} locked={locked} />}
        </Fragment>
      ))}
      {(dbs.data?.total ?? 0) > PAGE && (
        <tr className="l2"><td colSpan={7}><Pager offset={offset} limit={PAGE} total={dbs.data!.total} noun="database" onPage={setOffset} /></td></tr>
      )}
      {(dbs.error || enable.error) && <tr className="l2"><td colSpan={7}><ErrorBox error={dbs.error || enable.error} /></td></tr>}
      {confirm.dialog}
    </>
  );
}

function Tree({ jobId, locked }: { jobId: string; locked: Locked }) {
  const [open, setOpen] = useState<string | null>(null);
  const servers = useQuery({ queryKey: ["job-tree", jobId], queryFn: () => api.get<Row>(`/ingestion-jobs/${jobId}/tree${qs({ limit: 200 })}`) });
  return (
    <div className="table-wrap">
      <table className="data tree">
        <thead><tr><th>Server › database › table</th><th>Read through · load type</th><th>Key</th>
          <th className="num">Databases · tables</th><th>Target table</th><th>Enabled</th><th>State now</th></tr></thead>
        <tbody>
          {(servers.data?.items ?? []).map((s: Row) => (
            <Fragment key={s.server}>
              <tr>
                <td className="name"><button className="twisty" aria-expanded={open === s.server}
                  onClick={() => setOpen(open === s.server ? null : s.server)}>{open === s.server ? "▾" : "▸"}</button><b>{s.server}</b></td>
                <td>{s.src_connection ? `UC connection ${s.src_connection}` : `JDBC ${s.src_host}`}</td>
                <td></td>
                <td className="num">{fmtInt(s.databases)} · {fmtInt(s.tables)}</td>
                <td></td>
                <td className="muted">{fmtInt(s.enabled_tables)} on</td>
                <td><StateBar row={s} /></td>
              </tr>
              {open === s.server && <DatabaseRows jobId={jobId} server={s.server} locked={locked} />}
            </Fragment>
          ))}
          {servers.data && !servers.data.items.length && <tr><td colSpan={7} className="muted">No tables yet: import one database's tables (Import CSV) into one of the job's groups, then add databases here.</td></tr>}
        </tbody>
      </table>
      <ErrorBox error={servers.error} />
    </div>
  );
}

function AddDatabases({ jobId, servers }: { jobId: string; servers: string[] }) {
  const [how, setHow] = useState<"csv" | "hand">("csv");
  const [csv, setCsv] = useState("");
  const [columns, setColumns] = useState({ server: "server", database: "database", enabled: "" });
  const [server, setServer] = useState(servers[0] ?? "");
  const [names, setNames] = useState("");
  const [fullList, setFullList] = useState(false);
  const [perServer, setPerServer] = useState(4);
  const [preview, setPreview] = useState<Row | null>(null);
  const [checked, setChecked] = useState<Row | null>(null);
  const qc = useQueryClient();
  const body = (dry_run: boolean): Row => ({
    ...(how === "csv"
      ? { csv, columns: Object.fromEntries(Object.entries(columns).filter(([, v]) => v.trim())) }
      : { databases: names.split(/[\n,]+/).map((n) => n.trim()).filter(Boolean).map((database) => ({ server, database })) }),
    full_list: how === "csv" && fullList, groups_per_server: perServer, dry_run });
  // The confirm applies exactly the request that was checked; any change to the inputs clears the check.
  const check = useMutation({
    mutationFn: () => { const b = body(false); return api.post<Row>(`/ingestion-jobs/${jobId}/databases`, { ...b, dry_run: true }).then((r) => ({ r, b })); },
    onSuccess: ({ r, b }) => { setPreview(r.summary); setChecked(b); },
  });
  useEffect(() => { setPreview(null); setChecked(null); }, [how, csv, JSON.stringify(columns), server, names, fullList, perServer]);
  const apply = useMutation({
    mutationFn: () => api.post<Row>(`/ingestion-jobs/${jobId}/databases`, checked!),
    onSuccess: (r) => { setPreview(r.summary); qc.invalidateQueries({ queryKey: ["job-tree", jobId] }); },
  });
  return (
    <div className="add-databases">
      <div className="toolbar">
        <label className="checkbox"><input type="radio" checked={how === "csv"} onChange={() => { setHow("csv"); setPreview(null); }} /> By a databases file</label>
        <label className="checkbox"><input type="radio" checked={how === "hand"} onChange={() => { setHow("hand"); setPreview(null); }} /> By hand</label>
      </div>
      {how === "csv" ? (
        <div className="form-grid">
          <label className="wide">Databases file (CSV, one line per database)
            <input type="file" accept=".csv,text/csv" onChange={async (e) => { const f = e.target.files?.[0]; if (f) setCsv(await f.text()); setPreview(null); }} />
            <textarea rows={4} value={csv} placeholder={"server,database\nsql_01,sales_01"} onChange={(e) => { setCsv(e.target.value); setPreview(null); }} /></label>
          <label>Column for the server<input value={columns.server} onChange={(e) => setColumns({ ...columns, server: e.target.value })} /></label>
          <label>Column for the database<input value={columns.database} onChange={(e) => setColumns({ ...columns, database: e.target.value })} /></label>
          <label>Column for enabled (optional)<input value={columns.enabled} placeholder="e.g. isActive" onChange={(e) => setColumns({ ...columns, enabled: e.target.value })} /></label>
          <label className="checkbox wide"><input type="checkbox" checked={fullList} onChange={(e) => setFullList(e.target.checked)} />
            The file is the full list: databases of this job that are not in it are disabled, never deleted</label>
        </div>
      ) : (
        <div className="form-grid">
          <label>Server (a UC connection, or one of the job's servers)
            <input list="job-servers" value={server} onChange={(e) => setServer(e.target.value)} />
            <datalist id="job-servers">{servers.map((s) => <option key={s} value={s} />)}</datalist></label>
          <label className="wide">Database names, one per line<textarea rows={4} value={names} onChange={(e) => setNames(e.target.value)} /></label>
        </div>
      )}
      <div className="form-grid">
        <label>Groups per new server<input type="number" min={1} max={64} value={perServer} onChange={(e) => setPerServer(Number(e.target.value) || 1)} />
          <span className="help">A server new to the job gets this many groups; each database joins one by its name.</span></label>
      </div>
      <p className="help">Each database gets every table of the job, with the job's target names and identity columns filled in.</p>
      <div className="toolbar">
        <button className="ghost" disabled={check.isPending} onClick={() => check.mutate()}>Check</button>
        {preview && checked && !preview.applied && <ConfirmButton label={`Add ${fmtInt(preview.new_tables)} ${plural(preview.new_tables, "table")}`}
          disabled={!preview.new_tables && !preview.enable && !preview.disable} onConfirm={() => apply.mutate()} />}
      </div>
      {preview && <Notice tone={preview.applied ? "ok" : "info"}>
        {preview.applied ? "Added: " : "Would add: "}{fmtInt(preview.new_tables)} {plural(preview.new_tables, "table")} from {fmtInt(preview.databases)} {plural(preview.databases, "database")}
        {preview.skipped ? `, ${fmtInt(preview.skipped)} skipped (target taken)` : ""}; enable {fmtInt(preview.enable)}, disable {fmtInt(preview.disable)}
        {preview.new_groups?.length ? `; new groups ${preview.new_groups.join(", ")}` : ""}.</Notice>}
      <ErrorBox error={check.error || apply.error} />
    </div>
  );
}

export default function JobEdit() {
  const { id = "" } = useParams();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const settings = useQuery({ queryKey: ["job-settings", id], queryFn: () => api.get<Row>(`/ingestion-jobs/${id}/settings`) });
  const schedules = useQuery({ queryKey: ["schedules"], queryFn: () => api.get<Row>("/schedules") });
  const options = useQuery({ queryKey: ["schedule-options"], queryFn: () => api.get<Row>("/schedules/options") });
  const servers = useQuery({ queryKey: ["job-tree", id], queryFn: () => api.get<Row>(`/ingestion-jobs/${id}/tree${qs({ limit: 200 })}`) });
  const schedule: Row | undefined = (schedules.data?.schedules ?? schedules.data ?? []).find?.((s: Row) => String(s.id) === id);
  const s = settings.data?.settings as Row | undefined;
  const [w, setW] = useState<Row>({});
  const [identity, setIdentity] = useState<[string, string][]>([]);
  // Identity columns whose values differ between tables: a value typed here sets the column for all of them.
  const [differing, setDiffering] = useState<Record<string, string>>({});
  const [run, setRun] = useState<Row>({});
  const [name, setName] = useState("");
  const [params, setParams] = useState<Record<string, string>>({});
  const [addOpen, setAddOpen] = useState(false);
  // The settings form as it loaded, so "unsaved" means the user changed something, not that a default differs.
  const [loaded, setLoaded] = useState("");
  const locked: Locked = usePermissions().data?.jobs[id]?.change ?? null;

  useEffect(() => {
    if (!s) return;
    const form = { fan_in: value(s.fan_in), write_mode_changes: value(s.write_mode_changes), write_mode_full: value(s.write_mode_full),
      freshness_sla_minutes: value(s.freshness_sla_minutes) ?? "", sink_catalog: value(s.sink_catalog) ?? "",
      sink_schema: value(s.sink_schema) ?? "", sink_table: value(s.sink_table) ?? "", lowercase: value(s.lowercase) ?? true,
      client_id: value(s.client_id) ?? "{database}" };
    const ident = Object.entries((value(s.identity) as Record<string, string>) ?? {});
    const differs = Object.fromEntries((settings.data?.identity_differs ?? []).map((name: string) => [name, ""]));
    setW(form); setIdentity(ident); setDiffering(differs);
    setLoaded(JSON.stringify({ w: form, identity: ident, differing: differs }));
  }, [settings.data]);
  const baseline = useMemo(() => (schedule ? scheduleForm(schedule) : null), [schedule?.id, schedule?.updated_at]);
  useEffect(() => {
    if (!baseline) return;
    setRun(baseline.run); setParams(baseline.params); setName(baseline.name);
  }, [baseline]);

  // Only what differs from the job's rows is sent, so an untouched setting never rewrites a mixed one.
  const changed = (): Row => {
    if (!s) return {};
    const out: Row = {};
    for (const k of ["fan_in", "write_mode_changes", "write_mode_full", "sink_catalog", "sink_schema", "sink_table", "lowercase", "client_id"])
      if (w[k] !== undefined && w[k] !== value(s[k]) && !(w[k] === "" && value(s[k]) == null)) out[k] = w[k];
    if (String(w.freshness_sla_minutes ?? "") !== String(value(s.freshness_sla_minutes) ?? ""))
      out.freshness_sla_minutes = w.freshness_sla_minutes === "" ? null : Number(w.freshness_sla_minutes);
    const typed = Object.entries(differing).filter(([, t]) => t.trim());
    const ident = Object.fromEntries([...typed, ...identity].filter(([n]) => n.trim()).map(([n, t]) => [n.trim(), t.trim()]));
    if (typed.length || JSON.stringify(ident) !== JSON.stringify(value(s.identity) ?? {})) out.identity = ident;
    return out;
  };
  const writeModeMoves = s && ((w.write_mode_changes && w.write_mode_changes !== value(s.write_mode_changes))
    || (w.write_mode_full && w.write_mode_full !== value(s.write_mode_full)));
  const targetsChanged = s && ["sink_catalog", "sink_schema", "sink_table"].some((k) => w[k] !== value(s[k]));

  const save = useMutation({
    mutationFn: async () => {
      const edits = changed();
      let result: Row | null = null;
      if (Object.keys(edits).length) {
        // The preview refuses (422, with its reasons) anything the tables can't take, before any write.
        await api.post<Row>(`/ingestion-jobs/${id}/settings`, { settings: edits, dry_run: true });
        result = await api.post<Row>(`/ingestion-jobs/${id}/settings`, { settings: edits, dry_run: false });
      }
      if (schedule) {
        const cadence = Number(run.cadence_minutes);
        await api.post(`/schedules/${schedule.id}`, {
          name: name.trim() || schedule.name, cadence_minutes: cadence, timezone: cadence <= 60 ? "UTC" : run.timezone || "Europe/London",
          stagger: schedule.stagger, paused: run.paused, notify_emails: run.notify_emails || null, job_variant: schedule.job_variant,
          options: { ...(schedule.options ?? {}), ...(cadence > 60 ? { start_minute: startMinute(run.start) } : { start_minute: undefined }) },
          members: (schedule.members as Row[]).map((m) => m.task_collection),
          parameters: Object.fromEntries(Object.entries(params).filter(([, v]) => v.trim() !== "").map(([k, v]) => [k, v.trim()])),
        }).catch((e: Error) => {
          throw new Error(result ? `The table settings were saved, but When it runs and Advanced configuration were not: ${e.message}` : e.message);
        });
      }
      return result;
    },
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["job-settings", id] }); qc.invalidateQueries({ queryKey: ["schedules"] });
      qc.invalidateQueries({ queryKey: ["job-tree", id] }); qc.invalidateQueries({ queryKey: ["ingestion-job", id] }); },
  });

  // As the Databricks editors do: Save only with something to save, a sign of unsaved changes, and a question before
  // leaving with them (a link in the app, or closing the tab).
  const dirty = !!s && (JSON.stringify({ w, identity, differing }) !== loaded
    || (!!baseline && JSON.stringify({ run, params, name }) !== JSON.stringify(baseline)));
  const confirm = useConfirm();
  const leaving = useRef(false);
  // navigate() takes a path or a history step, not a value that may be either.
  const go = (to: string | number) => { leaving.current = true; if (typeof to === "number") navigate(to); else navigate(to); };
  const leave = (to: string | number) => {
    if (!dirty || leaving.current) return go(to);
    confirm.ask({ title: "Leave without saving?", action: "Leave", danger: true,
                  body: "Your changes to this job's settings are not saved. Changes you confirmed in What it ingests are already applied." })
      .then((ok) => ok && go(to));
  };
  // The browser's Back button too: with unsaved changes a guard entry sits on top of this page's history entry, so Back
  // lands on it and asks; staying puts the guard back, leaving goes back past it. Once the form is clean again (saved,
  // or the edit undone) the guard comes off, so Back always goes back one page.
  useEffect(() => {
    if (!dirty) return;
    window.history.pushState({ unsavedGuard: true }, "", window.location.href);
    const back = () => {
      if (leaving.current) return;
      window.history.pushState({ unsavedGuard: true }, "", window.location.href);
      leave(-2);
    };
    window.addEventListener("popstate", back);
    return () => {
      window.removeEventListener("popstate", back);
      if (!leaving.current && window.history.state?.unsavedGuard) window.history.back();
    };
  }, [dirty]);
  useEffect(() => {
    if (!dirty) return;
    const unload = (e: BeforeUnloadEvent) => { e.preventDefault(); };
    const click = (e: MouseEvent) => {
      const link = (e.target as HTMLElement).closest?.("a[href]") as HTMLAnchorElement | null;
      if (!link || link.target === "_blank" || link.origin !== window.location.origin || e.defaultPrevented) return;
      e.preventDefault(); e.stopPropagation();
      leave(link.pathname + link.search);
    };
    window.addEventListener("beforeunload", unload);
    document.addEventListener("click", click, true);
    return () => { window.removeEventListener("beforeunload", unload); document.removeEventListener("click", click, true); };
  });

  const cadences: number[] = options.data?.cadences ?? [5, 10, 15, 20, 30, 60, 120, 180, 240, 360, 720, 1440];
  const paramNames: string[] = (options.data?.parameters ?? []).filter((k: string) => k !== "fail_on_table_errors");
  const atSetTimes = Number(run.cadence_minutes) > 60;
  const tablesFail = params.fail_on_table_errors === "true";
  return (
    <>
      <PageHead crumb={<Crumbs steps={[["Ingestion jobs", "/ingestion-jobs"], [schedule?.name ?? "Ingestion job", `/ingestion-jobs/${id}`], ["Edit"]]} />}
        title={`Edit ${schedule?.name ?? "ingestion job"}`}
        lead={settings.data && <><strong>{fmtInt(settings.data.tables)}</strong> {plural(settings.data.tables, "table")} in{" "}
          <strong>{fmtInt(settings.data.groups.length)}</strong> {plural(settings.data.groups.length, "group")}.</>}
        right={<span className="card-actions">{dirty && <span className="muted">Unsaved changes</span>}
          <button onClick={() => leave(`/ingestion-jobs/${id}`)}>Cancel</button>
          <button className="primary" disabled={save.isPending || !s || !!locked || !dirty} title={locked ?? (!dirty ? "Nothing to save yet" : undefined)}
            onClick={() => save.mutate()}>{save.isPending ? "Saving…" : "Save"}</button></span>} />
      {confirm.dialog}
      {locked && <Notice tone="info">You can see this job's settings but not change them: {locked}.</Notice>}
      <ErrorBox error={settings.error || save.error} />
      {save.isSuccess && <Notice tone="ok">Saved{save.data ? `: ${fmtInt(save.data.summary.changed)} ${plural(save.data.summary.changed, "table")} changed${save.data.summary.reseed ? `, ${fmtInt(save.data.summary.reseed)} to load again` : ""}` : ""}.</Notice>}

      {schedule && <Section title="Name">
        <div className="form-grid"><label>Job name<input value={name} onChange={(e) => setName(e.target.value)} />
          <span className="help">Its Databricks jobs are renamed with it when you press Save.</span></label></div>
      </Section>}

      <Section title="What it ingests" note="Open a server to see its databases, and a database to see its tables. A table's load type is the same in every database: change it and it changes in all of them."
        actions={<button className="ghost" disabled={!!locked} title={locked ?? undefined} onClick={() => setAddOpen(!addOpen)}>{addOpen ? "Close" : "Add databases"}</button>}>
        <p className="impact">Changes here apply as soon as you confirm them; Save does not cover them.</p>
        {addOpen && <AddDatabases jobId={id} servers={(servers.data?.items ?? []).map((x: Row) => x.server)} />}
        <Tree jobId={id} locked={locked} />
      </Section>

      {s && <Section title="How it writes" note="For every table of the job.">
        <div className="form-grid">
          <label>Fan-in<select value={w.fan_in === undefined || w.fan_in === null ? "" : String(w.fan_in)} onChange={(e) => setW({ ...w, fan_in: e.target.value === "true" })}>
            {w.fan_in == null && <option value="">choose</option>}
            <option value="true">Yes: shared tables</option><option value="false">No: own tables</option></select>
            <span className="help">With fan-in, each database writes only its own rows of a shared table.</span><Mixed s={s.fan_in} /></label>
          <label>Change-tracked tables<select value={w.write_mode_changes ?? ""} onChange={(e) => setW({ ...w, write_mode_changes: e.target.value })}>
            {w.write_mode_changes == null && <option value="">none in this job</option>}
            <option value="append">Append: change log</option><option value="merge">Merge: current state</option></select><Mixed s={s.write_mode_changes} /></label>
          <label>Full-reload tables<select value={w.write_mode_full ?? ""} onChange={(e) => setW({ ...w, write_mode_full: e.target.value })}>
            {w.write_mode_full == null && <option value="">none in this job</option>}
            <option value="append">Append: a snapshot each run</option><option value="merge">Merge: replace the rows</option></select><Mixed s={s.write_mode_full} /></label>
          <label>Freshness target (minutes)<input type="number" min={1} value={w.freshness_sla_minutes ?? ""} placeholder="none"
            onChange={(e) => setW({ ...w, freshness_sla_minutes: e.target.value })} /><Mixed s={s.freshness_sla_minutes} /></label>
        </div>
        {writeModeMoves && <p className={targetsChanged ? "help" : "impact"}>A new write mode makes new target tables and loads every
          table again{targetsChanged ? "." : ": change the target names below too."}</p>}
        <div className="form-grid">
          <label>Target catalog<input value={w.sink_catalog} onChange={(e) => setW({ ...w, sink_catalog: e.target.value })} /><Mixed s={s.sink_catalog} /></label>
          <label>Target schema<input value={w.sink_schema} onChange={(e) => setW({ ...w, sink_schema: e.target.value })} /><Mixed s={s.sink_schema} /></label>
          <label>Target table<input value={w.sink_table} onChange={(e) => setW({ ...w, sink_table: e.target.value })} /><Mixed s={s.sink_table} /></label>
          <label className="checkbox"><input type="checkbox" checked={!!w.lowercase} onChange={(e) => setW({ ...w, lowercase: e.target.checked })} /> Lower-case names</label>
        </div>
        <p className="help"><span className="info" aria-hidden>ⓘ</span> Valid tags: <span className="mono">{TAGS}</span>, with any text between them.
          With fan-in, {"{server}"} and {"{database}"} can't be used in target names: they would give every database its own table.</p>
        {targetsChanged && <p className="impact">New target names make new tables and load their tables again. The old tables are left in place.</p>}
      </Section>}

      {s && <Section title="Identity columns" note="Written on every row next to __client_id__, so the shared tables say whose rows they are.">
        <table className="data identity">
          <thead><tr><th>Column</th><th>Value</th><th></th></tr></thead>
          <tbody>
            {s.identity?.mixed && !Object.keys(differing).length && <tr><td colSpan={3}><Mixed s={s.identity} /></td></tr>}
            <tr><td className="mono">__client_id__</td><td><input className="mono" value={w.client_id} disabled={w.fan_in === false}
              onChange={(e) => setW({ ...w, client_id: e.target.value })} /> <span className="help">keeps each database's rows apart{w.fan_in === false ? " (fan-in only)" : ""}</span></td><td></td></tr>
            {Object.entries(differing).map(([name, template]) => (
              <tr key={`differs-${name}`}>
                <td className="mono">{name}</td>
                <td><input className="mono" value={template} placeholder="differs between tables" aria-label={`${name} value`}
                  onChange={(e) => setDiffering({ ...differing, [name]: e.target.value })} />
                  <span className="help">a value here sets it for all {fmtInt(settings.data?.tables)} {plural(settings.data?.tables, "table")}</span></td>
                <td></td>
              </tr>
            ))}
            {identity.map(([name, template], i) => (
              <tr key={i}>
                <td><input className="mono" value={name} placeholder="column name" aria-label="Identity column name" onChange={(e) => setIdentity(identity.map((x, j) => (j === i ? [e.target.value, x[1]] : x)))} /></td>
                <td><input className="mono" value={template} placeholder="{database}" aria-label="Identity column value" onChange={(e) => setIdentity(identity.map((x, j) => (j === i ? [x[0], e.target.value] : x)))} /></td>
                <td><button className="ghost" onClick={() => setIdentity(identity.filter((_, j) => j !== i))}>Remove</button></td>
              </tr>
            ))}
          </tbody>
        </table>
        <div className="toolbar"><button className="ghost" onClick={() => setIdentity([...identity, ["", "{database}"]])}>Add a column</button></div>
        <p className="help"><span className="info" aria-hidden>ⓘ</span> Valid tags: <span className="mono">{TAGS}</span>, for example{" "}
          <span className="mono">{"{server}/{database}"}</span>. A column added later is empty for rows loaded before it.</p>
      </Section>}

      {schedule && <Section title="When it runs">
        <div className="form-grid">
          <label>Every<select value={run.cadence_minutes} onChange={(e) => setRun({ ...run, cadence_minutes: Number(e.target.value) })}>
            {cadences.map((c) => <option key={c} value={c}>{CADENCE_LABEL(c)}</option>)}</select>
            <span className="help">{atSetTimes ? "Tied to the time of day, so it follows the time zone below."
              : "In UTC: London is a whole hour off, so the runs fall on the same minutes there, and none is skipped on a clock-change night."}</span></label>
          {atSetTimes && <label>{Number(run.cadence_minutes) === 1440 ? "Daily at" : "Starting from"}
            <input type="time" value={run.start} onChange={(e) => setRun({ ...run, start: e.target.value })} />
            <span className="help">{Number(run.cadence_minutes) === 1440 ? "The time of day it runs."
              : `Then every ${CADENCE_LABEL(Number(run.cadence_minutes))}, round the clock.`}</span></label>}
          {atSetTimes && <label>Time zone<input value={run.timezone} onChange={(e) => setRun({ ...run, timezone: e.target.value })} />
            <span className="help">A run between 01:00 and 02:00 can be skipped or run an hour late on the night the clocks change.</span></label>}
          <label className="checkbox wide"><input type="checkbox" checked={!!run.paused} onChange={(e) => setRun({ ...run, paused: e.target.checked })} /> Paused</label>
          <label className="wide emails">Emails to<input value={run.notify_emails} placeholder="a@example.com, b@example.com" onChange={(e) => setRun({ ...run, notify_emails: e.target.value })} />
            <span className="help">Sent when a run fails.</span></label>
          <label className="checkbox wide"><input type="checkbox" checked={tablesFail}
            onChange={(e) => setParams({ ...params, fail_on_table_errors: e.target.checked ? "true" : "" })} />
            Also email when tables fail in a run <span className="help">(the run then ends as failed)</span></label>
        </div>
      </Section>}

      {schedule && <details className="card advanced">
        <summary><b>Advanced configuration</b> <span className="muted">{Object.entries(params).filter(([k, v]) => v && k !== "fail_on_table_errors")
          .map(([k, v]) => `${k.replace(/_/g, " ")} ${v}`).join(" · ") || "every key at its default"}</span></summary>
        <div className="form-grid">
          {paramNames.map((k) => (
            <label key={k}>{k.replace(/_/g, " ").replace(/^./, (c) => c.toUpperCase())}
              {(options.data?.flags ?? []).includes(k)
                ? <select value={params[k] ?? ""} onChange={(e) => setParams({ ...params, [k]: e.target.value })}>
                    <option value="">default{options.data?.defaults?.[k] ? ` (${options.data.defaults[k]})` : ""}</option><option>true</option><option>false</option></select>
                : <input value={params[k] ?? ""} placeholder={options.data?.defaults?.[k] ? `default (${options.data.defaults[k]})` : "default"}
                    onChange={(e) => setParams({ ...params, [k]: e.target.value })} />}
            </label>
          ))}
        </div>
        <Compute jobId={id} />
      </details>}
    </>
  );
}

// What the job's runs execute on, read live from the Jobs API. Compute is set in the bundle, so it is shown, not edited.
function Compute({ jobId }: { jobId: string }) {
  const compute = useQuery({ queryKey: ["job-compute", jobId], queryFn: () => api.get<Row>(`/ingestion-jobs/${jobId}/compute`) });
  return (
    <div className="compute">
      <h3>Compute</h3>
      <p className="muted">What this job's runs execute on, read from the Databricks jobs now. It is set in the bundle that deploys them, not here.</p>
      <ErrorBox error={compute.error} />
      {(compute.data?.jobs ?? []).map((job: Row) => (
        <Fragment key={job.role}>
          <h4>{job.role === "check" ? "Check job" : "Ingestion job"} <span className="muted mono">{job.name ?? job.job_id}</span></h4>
          {job.error && <p className="muted">{job.error}</p>}
          <table className="data">
            <thead><tr><th>Task</th><th>Setting</th><th>Value</th><th>Set by</th></tr></thead>
            <tbody>
              {job.compute.map((task: Row) => task.fields.map((f: Row, i: number) => (
                <tr key={`${task.task}-${f.name}`}>
                  <td>{i === 0 ? <><b>{task.task}</b> <span className="muted">{task.kind}</span></> : ""}</td>
                  <td>{f.name}</td><td className="mono">{f.value}</td>
                  <td className="muted">{f.source}</td>
                </tr>
              )))}
            </tbody>
          </table>
        </Fragment>
      ))}
    </div>
  );
}
