import type { ReactNode } from "react";
import type { Row } from "../api";
import { Info, Required, fmtInt, plural } from "../components";

// The sections New ingestion job and Edit share, so a job is set up and changed in one editor: how it writes, its
// identity columns, when it runs and the advanced configuration. Each takes the form's values and setters; Edit adds
// what only a job with tables has (values that differ between tables, notes on what a change reloads).

export const TAGS = "{server} {database} {schema} {table}";
export const CADENCE_LABEL = (m: number) => (m < 60 ? `${m} minutes` : m === 60 ? "hour" : m === 1440 ? "day" : `${m / 60} hours`);
export const CADENCES = [5, 10, 15, 20, 30, 60, 120, 180, 240, 360, 720, 1440];

const mixedText = (s?: Row) => (s?.mixed ? Object.entries(s.mixed).map(([v, n]) => `${v || "(none)"}: ${fmtInt(n)}`).join(", ") : "");

/** A setting whose value differs between the job's tables (Edit only). */
export function Mixed({ s }: { s?: Row }) {
  return s?.mixed ? <span className="help">Differs between tables ({mixedText(s)}); a value here sets it for all.</span> : null;
}

const TAGS_HELP = <>Valid tags: <span className="mono">{TAGS}</span>, with any text between them. With fan-in, {"{server}"} and
  {" {database}"} can't be used in target names: they would give every database its own table.</>;

/** How it writes: fan-in, the write modes, the freshness target and the target names, in one grid. */
export function WritesFields({ w, setW, stored, modeNote, targetsNote }: { w: Row; setW: (w: Row) => void; stored?: Row;
                              modeNote?: ReactNode; targetsNote?: ReactNode }) {
  return (
    <>
      <div className="form-grid">
        <label><span>Fan-in<Info label="About fan-in">With fan-in, every database writes only its own rows of shared
          target tables. Without it, each source table has its own target table.</Info></span><select value={w.fan_in === undefined || w.fan_in === null ? "" : String(w.fan_in)} onChange={(e) => setW({ ...w, fan_in: e.target.value === "true" })}>
          {w.fan_in == null && <option value="">choose</option>}
          <option value="true">Yes: shared tables</option><option value="false">No: own tables</option></select><Mixed s={stored?.fan_in} /></label>
        <label>Change-tracked tables<select value={w.write_mode_changes ?? ""} onChange={(e) => setW({ ...w, write_mode_changes: e.target.value })}>
          {w.write_mode_changes == null && <option value="">none in this job</option>}
          <option value="append">Append: change log</option><option value="merge">Merge: current state</option></select><Mixed s={stored?.write_mode_changes} /></label>
        <label>Full-reload tables<select value={w.write_mode_full ?? ""} onChange={(e) => setW({ ...w, write_mode_full: e.target.value })}>
          {w.write_mode_full == null && <option value="">none in this job</option>}
          <option value="append">Append: a snapshot each run</option><option value="merge">Merge: replace the rows</option></select><Mixed s={stored?.write_mode_full} /></label>
        <label>Freshness target (minutes)<input type="number" min={1} value={w.freshness_sla_minutes ?? ""} placeholder="none"
          onChange={(e) => setW({ ...w, freshness_sla_minutes: e.target.value })} /><Mixed s={stored?.freshness_sla_minutes} /></label>
        <label><span>Target catalog<Required /></span><input value={w.sink_catalog ?? ""} placeholder="catalog" onChange={(e) => setW({ ...w, sink_catalog: e.target.value })} /><Mixed s={stored?.sink_catalog} /></label>
        <label><span>Target schema<Required /></span><input value={w.sink_schema ?? ""} placeholder="schema" onChange={(e) => setW({ ...w, sink_schema: e.target.value })} /><Mixed s={stored?.sink_schema} /></label>
        <label><span>Target table<Required /><Info label="About target names">{TAGS_HELP}</Info></span><input value={w.sink_table ?? ""} onChange={(e) => setW({ ...w, sink_table: e.target.value })} /><Mixed s={stored?.sink_table} /></label>
        <label className="checkbox"><input type="checkbox" checked={!!w.lowercase} onChange={(e) => setW({ ...w, lowercase: e.target.checked })} /> Lower-case names</label>
      </div>
      {modeNote}
      {targetsNote}
    </>
  );
}

/** Identity columns: __client_id__ (fan-in only) and any others, each a template filled per database. */
export function IdentityFields({ w, setW, identity, setIdentity, differing = {}, setDiffering, stored, tables }: {
  w: Row; setW: (w: Row) => void; identity: [string, string][]; setIdentity: (i: [string, string][]) => void;
  differing?: Record<string, string>; setDiffering?: (d: Record<string, string>) => void; stored?: Row; tables?: number }) {
  return (
    <>
      <table className="data identity">
        <thead><tr><th>Column</th><th>Value<Info label="About identity values">Valid tags: <span className="mono">{TAGS}</span>,
          for example <span className="mono">{"{server}/{database}"}</span>. A column added later is empty for rows loaded before
          it.</Info></th><th></th></tr></thead>
        <tbody>
          {stored?.identity?.mixed && !Object.keys(differing).length && <tr><td colSpan={3}><Mixed s={stored.identity} /></td></tr>}
          <tr><td className="mono">__client_id__<Info label="About __client_id__">Keeps each database's rows apart in the shared
            tables (fan-in only).</Info></td><td><input className="mono" value={w.client_id ?? ""} disabled={w.fan_in === false}
            onChange={(e) => setW({ ...w, client_id: e.target.value })} /></td><td></td></tr>
          {Object.entries(differing).map(([name, template]) => (
            <tr key={`differs-${name}`}>
              <td className="mono">{name}</td>
              <td><input className="mono" value={template} placeholder="differs between tables" aria-label={`${name} value`}
                onChange={(e) => setDiffering?.({ ...differing, [name]: e.target.value })} />
                <span className="help">a value here sets it for all {fmtInt(tables)} {plural(tables, "table")}</span></td>
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
    </>
  );
}

/** When it runs: the cadence, a start time and zone for set-time cadences, paused, and who is emailed on failure. */
export function RunFields({ run, setRun, params, setParams, cadences = CADENCES, defaults }: { run: Row; setRun: (r: Row) => void;
                           params: Record<string, string>; setParams: (p: Record<string, string>) => void; cadences?: number[];
                           defaults?: Record<string, string> }) {
  // Blank follows the ingestion job's default (on); unticking keeps "false" for this job.
  const failOnTables = (params.fail_on_table_errors || defaults?.fail_on_table_errors || "true") === "true";
  const atSetTimes = Number(run.cadence_minutes) > 60;
  return (
    <div className="form-grid">
      <label><span>Every<Info label="About the cadence">{atSetTimes ? "Tied to the time of day, so it follows the time zone."
        : "In UTC: London is a whole hour off, so the runs fall on the same minutes there, and none is skipped on a clock-change night."}</Info></span>
        <select value={run.cadence_minutes} onChange={(e) => setRun({ ...run, cadence_minutes: Number(e.target.value) })}>
        {cadences.map((c) => <option key={c} value={c}>{CADENCE_LABEL(c)}</option>)}</select></label>
      {atSetTimes && <label>{Number(run.cadence_minutes) === 1440 ? "Daily at" : "Starting from"}
        <input type="time" value={run.start} onChange={(e) => setRun({ ...run, start: e.target.value })} /></label>}
      {atSetTimes && <label><span>Time zone<Info label="About the time zone">A run between 01:00 and 02:00 can be skipped or run an
        hour late on the night the clocks change.</Info></span><input value={run.timezone} onChange={(e) => setRun({ ...run, timezone: e.target.value })} /></label>}
      <label className="checkbox wide"><input type="checkbox" checked={!!run.paused} onChange={(e) => setRun({ ...run, paused: e.target.checked })} /> Paused</label>
      <label className="wide emails">Emails when a run fails<input value={run.notify_emails} placeholder="a@example.com, b@example.com"
        onChange={(e) => setRun({ ...run, notify_emails: e.target.value })} /></label>
      <label className="checkbox wide"><input type="checkbox" checked={failOnTables}
        onChange={(e) => setParams({ ...params, fail_on_table_errors: e.target.checked ? "" : "false" })} />
        Also email when tables fail in a run</label>
    </div>
  );
}

/** Each tuning key in plain words with its unit, and what it does, for its (i). */
export const TUNING: Record<string, [string, string]> = {
  parallelism: ["Concurrent queries per run", "How many source queries one run has going at once, across its tables."],
  detection_chunk: ["Tables per change check", "Tables asked for their changes in one source query."],
  trust_server_certificate: ["Trust the server certificate", "Accept the SQL Server's TLS certificate without checking it against a trusted authority."],
  secret_scope: ["Secret scope", "The secret scope holding each database's login, for tables read by host and password."],
  snapshot_isolation: ["Read changes under snapshot isolation", "Needs ALLOW_SNAPSHOT_ISOLATION on every source database."],
  reseed_on_new_columns: ["Reload when columns are added", "Off: a table that gains columns carries on, the new columns empty for the rows loaded before."],
  append_batch_size: ["Tables per append batch", "Tables appending to the same shared table that are read and written together."],
  max_seeds_per_run: ["First loads per run", "At most this many first loads in one run; the rest wait for later runs. 0 is no limit."],
  freshness_interval_s: ["Freshness interval (seconds)", "An unchanged table's last check is written at most this often. 0 writes it every run."],
  max_queries_per_source: ["Queries per source server", "At most this many queries on one source server at once, across every run. 0 is no limit."],
  table_retries: ["Retries per table", "Extra attempts at a table after a passing source error. 0 is off."],
  table_retry_budget: ["Retries per run", "Retries in a whole run; once spent, failures fail at once."],
};

/** The ingestion job's tuning keys, each showing its value: the Databricks job's default until changed. A value left at
 * the default is not saved, so the job follows the default if it changes; a changed one has Reset beside it. */
export function ParameterFields({ params, setParams, options }: { params: Record<string, string>;
                                 setParams: (p: Record<string, string>) => void; options?: Row }) {
  const names: string[] = (options?.parameters ?? []).filter((k: string) => k !== "fail_on_table_errors");
  const defaults: Record<string, string> = options?.defaults ?? {};
  const set = (k: string, value: string) => {
    const next = { ...params };
    if (value === "" || value === defaults[k]) delete next[k]; else next[k] = value;
    setParams(next);
  };
  const reset = (k: string) => { const next = { ...params }; delete next[k]; setParams(next); };
  return (
    <div className="form-grid">
      {names.map((k) => {
        const [label, about] = TUNING[k] ?? [k.replace(/_/g, " ").replace(/^./, (c) => c.toUpperCase()), ""];
        const value = params[k] ?? defaults[k] ?? "";
        const changed = params[k] !== undefined && params[k] !== "";
        const resetButton = changed && <button className="ghost link reset" onClick={() => reset(k)}
          title={`Back to the default${defaults[k] ? `, ${defaults[k]}` : ""}`}>Reset</button>;
        return (options?.flags ?? []).includes(k)
          ? <label key={k} className="checkbox"><input type="checkbox" checked={value === "true"}
              onChange={(e) => set(k, e.target.checked ? "true" : "false")} /> {label}
              {about && <Info label={`About ${label}`}>{about}</Info>}{resetButton}</label>
          : <label key={k}><span>{label}{about && <Info label={`About ${label}`}>{about}</Info>}{resetButton}</span>
              <input value={value} onChange={(e) => set(k, e.target.value)} /></label>;
      })}
    </div>
  );
}
