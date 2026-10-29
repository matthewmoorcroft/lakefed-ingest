import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, type Row } from "../api";
import { Crumbs, ErrorBox, Menu, Modal, Notice, PageHead, Section, fmtInt, plural, useConfirm } from "../components";
import { CADENCES, IdentityFields, ParameterFields, RunFields, WritesFields, paramsSummary } from "./jobForm";
import { DiscoverDialog } from "./Discover";
import { ClusterFields, clusterSummary, ownCluster } from "./JobEdit";

// New ingestion job: the Edit view, empty, with Create in place of Save. Its tables are added in What it ingests
// (Add tables: a CSV of where each table is), the rest is set as in Edit, and nothing is written until Create, which
// makes the job, its tables and its groups in one go (POST /api/ingestion-jobs).

const LOAD_TYPES: [string, string][] = [["ct", "Change tracking"], ["cursor", "Cursor column"], ["full", "Full reload"]];
const CSV_EXAMPLE = "src_connection,src_database,src_schema,src_table,select_list,primary_key\nsql_01,sales_01,dbo,orders,\"id, amount\",id";

/** A short fingerprint of a large text (its length and a 32-bit FNV-1a hash), cheap to compare on every change. */
function fingerprint(text: string): string {
  let hash = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) hash = Math.imul(hash ^ text.charCodeAt(i), 0x01000193);
  return `${text.length}:${(hash >>> 0).toString(16)}`;
}

/** The value once it has stayed the same for `ms`, so typing in a field doesn't ask for a preview per key. */
function useSettled<T>(value: T, ms: number): T {
  const [settled, setSettled] = useState(value);
  useEffect(() => { const timer = setTimeout(() => setSettled(value), ms); return () => clearTimeout(timer); }, [value, ms]);
  return settled;
}

export default function NewJob() {
  const navigate = useNavigate();
  const qc = useQueryClient();
  const options = useQuery({ queryKey: ["schedule-options"], queryFn: () => api.get<Row>("/schedules/options") });
  const [name, setName] = useState("");
  const [csv, setCsv] = useState("");
  const [loadType, setLoadType] = useState("ct");
  const [w, setW] = useState<Row>({ fan_in: false, write_mode_changes: "merge", write_mode_full: "merge", sink_catalog: "",
                                    sink_schema: "", sink_table: "{table}", lowercase: true, client_id: "{database}",
                                    freshness_sla_minutes: "" });
  const [identity, setIdentity] = useState<[string, string][]>([]);
  const [run, setRun] = useState<Row>({ cadence_minutes: 60, start: "00:00", timezone: "Europe/London", paused: true, notify_emails: "" });
  const [params, setParams] = useState<Record<string, string>>({});
  const [variant, setVariant] = useState<string | null>(null);
  const [groupsPerServer, setGroupsPerServer] = useState(4);
  const [importing, setImporting] = useState(false);
  const [discovering, setDiscovering] = useState(false);
  // The job's own cluster, as in Edit's Job cluster: at the Databricks job's values unless changed here.
  const clusterInfo = useQuery({ queryKey: ["cluster-options"], queryFn: () => api.get<Row>("/cluster-options") });
  const [clusterChoice, setCluster] = useState<Row | null>(null);
  const cluster = clusterChoice ?? clusterInfo.data?.defaults ?? null;
  const own = clusterInfo.data && cluster ? ownCluster(cluster, clusterInfo.data.defaults) : null;
  const variants: string[] = options.data?.variants ?? [];
  // The check job is how the 1,000- and 100,000-table jobs were tested, so it is the default where there is one.
  const jobVariant = variant ?? (variants.includes("check") ? "check" : "default");

  const body = (dryRun: boolean) => {
    const cadence = Number(run.cadence_minutes);
    const [h, m] = String(run.start || "00:00").split(":").map(Number);
    return {
      name: name.trim(), csv, groups_per_server: groupsPerServer, dry_run: dryRun,
      settings: { ...w, load_type: loadType, freshness_sla_minutes: w.freshness_sla_minutes === "" ? null : Number(w.freshness_sla_minutes),
                  identity: Object.fromEntries(identity.filter(([n]) => n.trim()).map(([n, t]) => [n.trim(), t.trim()])) },
      run: { cadence_minutes: cadence, timezone: cadence <= 60 ? "UTC" : run.timezone || "Europe/London", paused: run.paused,
             notify_emails: run.notify_emails || null, job_variant: jobVariant,
             options: { ...(cadence > 60 ? { start_minute: (h || 0) * 60 + (m || 0) } : {}), ...(own ? { cluster: own } : {}) },
             parameters: Object.fromEntries(Object.entries(params).filter(([, v]) => v.trim() !== "").map(([k, v]) => [k, v.trim()])) },
    };
  };
  // The preview follows every change, so the page always says what Create would write. A whole server is 100,000
  // lines (9 MB), so the file is keyed by a fingerprint, not by its text, and a preview is asked for only once the
  // changes have settled.
  const previewBody = body(true);
  const csvKey = useMemo(() => fingerprint(csv), [csv]);
  const current = JSON.stringify({ ...previewBody, csv: csvKey });
  const settled = useSettled(current, 600);
  const preview = useQuery({
    queryKey: ["new-job-preview", settled], enabled: !!csv.trim() && !!name.trim(), retry: false,
    queryFn: () => api.post<Row>("/ingestion-jobs", previewBody),
  });
  const create = useMutation({
    mutationFn: () => api.post<Row>("/ingestion-jobs", body(false)),
    onSuccess: (r) => {
      qc.invalidateQueries({ queryKey: ["ingestion-jobs"] }); qc.invalidateQueries({ queryKey: ["schedules"] });
      if (r.created) navigate(`/ingestion-jobs/${r.id}`);
    },
  });
  const summary: Row | undefined = preview.data?.summary;
  const blocked = !name.trim() ? "Name the job first" : !csv.trim() ? "Add its tables first"
    : preview.isFetching || settled !== current ? "Checking the tables" : preview.error ? "Fix what the check found first"
    : summary?.invalid?.length ? "Fix the invalid lines first" : null;
  const confirm = useConfirm();
  const cancel = () => {
    if (!name && !csv) return navigate("/ingestion-jobs");
    confirm.ask({ title: "Leave without creating the job?", action: "Leave", danger: true, body: "Nothing of it is saved." })
      .then((ok) => ok && navigate("/ingestion-jobs"));
  };

  return (
    <>
      <PageHead crumb={<Crumbs steps={[["Ingestion jobs", "/ingestion-jobs"], ["New"]]} />} title="New ingestion job"
        lead={summary ? <><strong>{fmtInt(summary.tables)}</strong> {plural(summary.tables, "table")} in{" "}
          <strong>{fmtInt(summary.groups.length)}</strong> {plural(summary.groups.length, "group")}.</> : "No tables yet: add them in What it ingests."}
        right={<span className="card-actions">
          <button onClick={cancel}>Cancel</button>
          <button className="primary" disabled={!!blocked || create.isPending} title={blocked ?? undefined}
            onClick={() => create.mutate()}>{create.isPending ? "Creating…" : "Create"}</button></span>} />
      {confirm.dialog}
      <ErrorBox error={create.error} />
      {create.data && !create.data.created && <Notice tone="warn">Not created: fix the invalid lines below first.</Notice>}

      <Section title="Name">
        <div className="form-grid"><label>Job name<input value={name} placeholder="e.g. Engagement extraction" onChange={(e) => setName(e.target.value)} />
          <span className="help">Its Databricks jobs and its groups are named after it.</span></label></div>
      </Section>

      <Section title="What it ingests" note="The servers, databases and tables this job ingests."
        actions={<Menu label="Add tables ▾" buttonClass="ghost" title="Add tables">
          <button onClick={() => setImporting(true)}>Import a CSV<span className="sub">one line per table: where it is; the job says how to ingest it</span></button>
          <button onClick={() => setDiscovering(true)}>Discover on a source<span className="sub">a UC connection, tested first; then the whole server, whole databases or single tables</span></button>
        </Menu>}>
        {!csv.trim() ? <p className="empty-drop">No tables yet. <b>Add tables</b>: import a CSV of where each table is, or discover them on a source.</p> : (
          <>
            <div className="table-wrap"><table className="data">
              <thead><tr><th>Server</th><th>Read through · load type</th><th className="num">Databases · tables</th><th className="num">Groups</th></tr></thead>
              <tbody>
                {(summary?.servers ?? []).map((s: Row) => (
                  <tr key={s.server}><td className="name"><b>{s.server}</b></td>
                    <td>{s.read_through} · {Object.entries(s.load_types as Record<string, number>).map(([k, n]) =>
                      `${LOAD_TYPES.find(([key]) => key === k)?.[1] ?? k}${Object.keys(s.load_types).length > 1 ? ` ${fmtInt(n)}` : ""}`).join(", ")}</td>
                    <td className="num">{fmtInt(s.databases)} · {fmtInt(s.tables)}</td><td className="num">{fmtInt(s.groups.length)}</td></tr>
                ))}
              </tbody>
            </table></div>
            {!name.trim() && <p className="muted">Name the job to check its tables.</p>}
            {(summary?.invalid ?? []).length > 0 && <Notice tone="bad">{fmtInt(summary!.invalid.length)} {plural(summary!.invalid.length, "line")} can't be
              added:<ul>{summary!.invalid.slice(0, 20).map((l: Row) => <li key={l.line}>line {l.line}, {l.table}: {l.errors.join("; ")}</li>)}</ul>
              {summary!.invalid.length > 20 && <span className="muted">and {fmtInt(summary!.invalid.length - 20)} more.</span>}</Notice>}
            <ErrorBox error={preview.error} />
            <div className="toolbar"><button className="ghost" onClick={() => setImporting(true)}>Import a file instead</button>
              <button className="ghost" onClick={() => setCsv("")}>Remove the tables</button></div>
          </>
        )}
      </Section>
      {discovering && <DiscoverDialog onClose={(lines) => { setDiscovering(false); if (lines) setCsv(lines); }} />}
      {importing && <ImportCsvDialog csv={csv} loadType={loadType} onClose={(next) => {
        setImporting(false); if (next) { setCsv(next.csv); setLoadType(next.loadType); } }} />}

      <Section title="How it writes" note="For every table of the job."><WritesFields w={w} setW={setW} /></Section>
      <Section title="Identity columns" note="Written on every row next to __client_id__, so the shared tables say whose rows they are.">
        <IdentityFields w={w} setW={setW} identity={identity} setIdentity={setIdentity} />
      </Section>
      <Section title="When it runs" note="A new job starts paused: resume it from its page once you have checked it.">
        <RunFields run={run} setRun={setRun} params={params} setParams={setParams} cadences={options.data?.cadences ?? CADENCES} />
      </Section>

      <details className="card advanced">
        <summary><b>Advanced configuration</b> <span className="muted">compute and tuning, at the defaults we tested: {groupsPerServer} groups per
          server, runs through the {jobVariant === "check" ? "check job" : "Databricks job"}, {clusterSummary(clusterInfo.data, cluster)};
          {" "}{paramsSummary(params) || "every key at its default"}</span></summary>
        <h3>Compute</h3>
        <div className="form-grid">
          <label>Groups per server<input type="number" min={1} max={64} value={groupsPerServer}
            onChange={(e) => setGroupsPerServer(Number(e.target.value) || 1)} />
            <span className="help">Each server's databases are split between this many groups, which run side by side.</span></label>
          {variants.length > 1 && <label>Runs start<select value={jobVariant} onChange={(e) => setVariant(e.target.value)}>
            {variants.map((v) => <option key={v} value={v}>{v === "check" ? "through the check job: only databases with changes" : "the Databricks job for every group"}</option>)}</select></label>}
        </div>
        <ClusterFields info={clusterInfo.data} error={clusterInfo.error} cluster={cluster} setCluster={setCluster} locked={null} after="Create" />
        <h3>Tuning</h3>
        <ParameterFields params={params} setParams={setParams} options={options.data} />
      </details>
    </>
  );
}

/** Import a CSV: a file of where each table is, and how its tables load. New and Edit both use it. */
export function ImportCsvDialog({ csv, loadType, onClose }: { csv: string; loadType: string;
                         onClose: (next: { csv: string; loadType: string } | null) => void }) {
  const [text, setText] = useState(csv);
  const [load, setLoad] = useState(loadType);
  return (
    <Modal title="Import a CSV" action="Add tables" actionDisabled={text.trim() ? undefined : "Choose a file or paste its lines first"}
      onClose={(ok) => onClose(ok ? { csv: text, loadType: load } : null)}>
      <p>One line per table: where it is. Columns: <span className="mono">src_connection</span> (a UC connection) or{" "}
        <span className="mono">src_host</span>, <span className="mono">src_port</span> and <span className="mono">secret_key</span>;
        then <span className="mono">src_database</span>, <span className="mono">src_schema</span>, <span className="mono">src_table</span>,{" "}
        <span className="mono">select_list</span> and <span className="mono">primary_key</span>; optionally <span className="mono">cursor_column</span>,{" "}
        <span className="mono">client_id</span> and <span className="mono">attr:&lt;column&gt;</span> identity values.</p>
      <div className="form-grid">
        <label className="wide">File<input type="file" accept=".csv,text/csv" onChange={async (e) => { const f = e.target.files?.[0]; if (f) setText(await f.text()); }} />
          <textarea rows={6} value={text} placeholder={CSV_EXAMPLE} onChange={(e) => setText(e.target.value)} /></label>
        <label>How its tables load<select value={load} onChange={(e) => setLoad(e.target.value)}>
          {LOAD_TYPES.map(([k, label]) => <option key={k} value={k}>{label}</option>)}</select>
          <span className="help">Change a table's load type later in Edit.</span></label>
      </div>
    </Modal>
  );
}
