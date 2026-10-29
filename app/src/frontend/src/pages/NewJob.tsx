import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, type Row } from "../api";
import { Crumbs, ErrorBox, Menu, Modal, Notice, PageHead, Section, fmtInt, plural, useConfirm } from "../components";
import { CADENCES, IdentityFields, ParameterFields, RunFields, WritesFields, paramsSummary } from "./jobForm";
import { DiscoverDialog } from "./Discover";
import { ClusterFields, clusterSummary, ownCluster } from "./JobEdit";
import { SourcePick, nothingPicked, pickedAny, pickedText, type Picked } from "./SourcePick";
import { TaskProgress, finished, newRequestId, resumableWrite } from "./tasks";

// New ingestion job: the Edit view, empty, with Create in place of Save. Its tables are added in What it ingests
// (Add tables: a CSV uploaded once, or a discovery), the rest is set as in Edit, and nothing is written until Create,
// which makes the job, its tables and its groups in one go (POST /api/ingestion-jobs). The page holds only what was
// chosen (an upload's or a discovery's id and the servers, databases or tables picked), never the tables: the preview
// and Create name it, and the server reads the tables from Lakebase. A large Create is written by a background task,
// whose progress the page shows before it opens the job.

const LOAD_TYPES: [string, string][] = [["ct", "Change tracking"], ["cursor", "Cursor column"], ["full", "Full reload"]];
const CSV_EXAMPLE = "src_connection,src_database,src_schema,src_table,select_list,primary_key\nsql_01,sales_01,dbo,orders,\"id, amount\",id";

/** Tables chosen by reference: what Create sends as its source, and how the page names it. */
export type Chosen = { source: Row; label: string };

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
  const [chosen, setChosen] = useState<Chosen | null>(null);
  // Made when the page opens; a Create sent again (a retry after a timeout) finds the task the first one made.
  const [requestId, setRequestId] = useState(newRequestId);
  const [task, setTask] = useState<Row | null>(null);
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
      name: name.trim(), source: chosen?.source, groups_per_server: groupsPerServer, dry_run: dryRun, request_id: requestId,
      settings: { ...w, load_type: loadType, freshness_sla_minutes: w.freshness_sla_minutes === "" ? null : Number(w.freshness_sla_minutes),
                  identity: Object.fromEntries(identity.filter(([n]) => n.trim()).map(([n, t]) => [n.trim(), t.trim()])) },
      run: { cadence_minutes: cadence, timezone: cadence <= 60 ? "UTC" : run.timezone || "Europe/London", paused: run.paused,
             notify_emails: run.notify_emails || null, job_variant: jobVariant,
             options: { ...(cadence > 60 ? { start_minute: (h || 0) * 60 + (m || 0) } : {}), ...(own ? { cluster: own } : {}) },
             parameters: Object.fromEntries(Object.entries(params).filter(([, v]) => v.trim() !== "").map(([k, v]) => [k, v.trim()])) },
    };
  };
  // The preview follows every change, so the page always says what Create would write; it is asked for only once the
  // changes have settled, and it names the tables by reference, so it stays small whatever their number.
  const previewBody = body(true);
  const current = JSON.stringify({ ...previewBody, request_id: null });
  const settled = useSettled(current, 600);
  const preview = useQuery({
    queryKey: ["new-job-preview", settled], enabled: !!chosen && !!name.trim(), retry: false,
    queryFn: () => api.post<Row>("/ingestion-jobs", previewBody),
  });
  // The body is the variables, so a Create that stopped part way is finished by sending the same body again.
  const create = useMutation({
    mutationFn: async (sent: Row) => { setTask(null); return finished(await api.post<Row>("/ingestion-jobs", sent), setTask); },
    onSuccess: (r) => {
      qc.invalidateQueries({ queryKey: ["ingestion-jobs"] }); qc.invalidateQueries({ queryKey: ["schedules"] });
      if (r.created) navigate(`/ingestion-jobs/${r.id}`);
    },
    // A Create that failed is a new request when it is sent again, unless it stopped part way: its databases are kept
    // and the same request finishes it.
    onError: (error) => { if (!resumableWrite(error)) setRequestId(newRequestId()); },
  });
  const summary: Row | undefined = preview.data?.summary;
  const blocked = !name.trim() ? "Name the job first" : !chosen ? "Add its tables first"
    : preview.isFetching || settled !== current ? "Checking the tables" : preview.error ? "Fix what the check found first"
    : summary?.invalid?.length ? "Fix the invalid lines first" : null;
  const confirm = useConfirm();
  const cancel = () => {
    if (!name && !chosen) return navigate("/ingestion-jobs");
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
            onClick={() => create.mutate(body(false))}>{create.isPending ? "Creating…" : "Create"}</button></span>} />
      {confirm.dialog}
      {create.isPending && <TaskProgress task={task} />}
      {!create.isPending && resumableWrite(create.error)
        ? <TaskProgress task={task} onResume={() => create.mutate(create.variables as Row)} />
        : <ErrorBox error={create.error} />}
      {create.data && !create.data.created && <Notice tone="warn">Not created: fix the invalid lines below first.</Notice>}

      <Section title="Name">
        <div className="form-grid"><label>Job name<input value={name} placeholder="e.g. Engagement extraction" onChange={(e) => setName(e.target.value)} />
          <span className="help">Its Databricks jobs and its groups are named after it.</span></label></div>
      </Section>

      <Section title="What it ingests" note="The servers, databases and tables this job ingests."
        actions={<Menu label="Add tables ▾" buttonClass="ghost" title="Add tables">
          <button onClick={() => setImporting(true)}>Import a CSV<span className="sub">one line per table: where it is; uploaded once, then choose from it</span></button>
          <button onClick={() => setDiscovering(true)}>Discover on a source<span className="sub">a UC connection, tested first; then the whole server, whole databases or single tables</span></button>
        </Menu>}>
        {!chosen ? <p className="empty-drop">No tables yet. <b>Add tables</b>: import a CSV of where each table is, or discover them on a source.</p> : (
          <>
            <p className="muted">From {chosen.label}.</p>
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
              <button className="ghost" onClick={() => setChosen(null)}>Remove the tables</button></div>
          </>
        )}
      </Section>
      {discovering && <DiscoverDialog onClose={(pick) => { setDiscovering(false); if (pick) setChosen(pick); }} />}
      {importing && <ImportCsvDialog loadType={loadType} onClose={(next) => {
        setImporting(false); if (next) { setChosen(next); setLoadType(next.loadType); } }} />}

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

/** Import a CSV: a file of where each table is, and how its tables load, uploaded once (POST /api/uploads); then its
 *  servers and databases to choose from (SourcePick). New and Edit both use it. */
export function ImportCsvDialog({ loadType, onClose }: { loadType: string;
                         onClose: (next: (Chosen & { loadType: string }) | null) => void }) {
  const [text, setText] = useState("");
  const [fileName, setFileName] = useState<string | null>(null);
  const [load, setLoad] = useState(loadType);
  const [upload, setUpload] = useState<Row | null>(null);
  const [picked, setPicked] = useState<Picked>(nothingPicked);
  const send = useMutation({
    mutationFn: () => api.post<Row>("/uploads", { csv: text, file_name: fileName }),
    onSuccess: (r) => { setUpload(r); setText(""); },
  });
  const step1 = !upload;
  return (
    <Modal title={step1 ? "Import a CSV · Step 1 of 2: the file" : "Import a CSV · Step 2 of 2: choose its tables"}
      action={step1 ? (send.isPending ? "Uploading…" : "Next: choose tables") : "Add the tables chosen"}
      actionDisabled={step1 ? (send.isPending ? "Uploading the file" : text.trim() ? undefined : "Choose a file or paste its lines first")
        : pickedAny(picked) ? undefined : "Choose the server, databases or tables first"}
      onClose={(ok) => {
        if (!ok) return onClose(null);
        if (step1) return send.mutate();
        onClose({ source: { upload_id: upload!.upload_id, ...picked }, loadType: load,
                  label: `${upload!.file_name ?? "the uploaded file"}: ${pickedText(picked)}` });
      }}>
      {step1 ? (
        <>
          <p>One line per table: where it is. Columns: <span className="mono">src_connection</span> (a UC connection) or{" "}
            <span className="mono">src_host</span>, <span className="mono">src_port</span> and <span className="mono">secret_key</span>;
            then <span className="mono">src_database</span>, <span className="mono">src_schema</span>, <span className="mono">src_table</span>,{" "}
            <span className="mono">select_list</span> and <span className="mono">primary_key</span>; optionally <span className="mono">cursor_column</span>,{" "}
            <span className="mono">client_id</span>, <span className="mono">load_type</span> and <span className="mono">attr:&lt;column&gt;</span> identity
            values. The file is uploaded once and kept; then you choose its servers, databases or tables.</p>
          <div className="form-grid">
            <label className="wide">File<input type="file" accept=".csv,text/csv" onChange={async (e) => {
                const f = e.target.files?.[0]; if (f) { setText(await f.text()); setFileName(f.name); } }} />
              <textarea rows={6} value={text.length > 20000 ? `${text.slice(0, 20000)}\n…` : text} placeholder={CSV_EXAMPLE}
                readOnly={text.length > 20000} onChange={(e) => { setText(e.target.value); setFileName(null); }} /></label>
            <label>How its tables load<select value={load} onChange={(e) => setLoad(e.target.value)}>
              {LOAD_TYPES.map(([k, label]) => <option key={k} value={k}>{label}</option>)}</select>
              <span className="help">A line's own load_type wins. Change a table's load type later in Edit.</span></label>
          </div>
          <ErrorBox error={send.error} />
        </>
      ) : (
        <div className="discover">
          <p className="muted">Uploaded: {fmtInt(upload!.tables)} {plural(upload!.tables, "table")}
            {upload!.invalid ? `, ${fmtInt(upload!.invalid)} ${plural(upload!.invalid, "line")} that can't be taken` : ""}.</p>
          <SourcePick kind="upload" sourceId={upload!.upload_id} picked={picked} setPicked={setPicked} />
          <p className="muted">Chosen: {pickedText(picked)}.</p>
        </div>
      )}
    </Modal>
  );
}
