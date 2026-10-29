import type { ReactNode } from "react";
import type { Row } from "../api";
import { fmtInt, plural } from "../components";

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

/** How it writes: fan-in, the write modes, the freshness target and the target names. */
export function WritesFields({ w, setW, stored, modeNote, targetsNote }: { w: Row; setW: (w: Row) => void; stored?: Row;
                              modeNote?: ReactNode; targetsNote?: ReactNode }) {
  return (
    <>
      <div className="form-grid">
        <label>Fan-in<select value={w.fan_in === undefined || w.fan_in === null ? "" : String(w.fan_in)} onChange={(e) => setW({ ...w, fan_in: e.target.value === "true" })}>
          {w.fan_in == null && <option value="">choose</option>}
          <option value="true">Yes: shared tables</option><option value="false">No: own tables</option></select>
          <span className="help">{w.fan_in === false ? "Without fan-in, each source table has its own target table."
            : "With fan-in, each database writes only its own rows of a shared table."}</span><Mixed s={stored?.fan_in} /></label>
        <label>Change-tracked tables<select value={w.write_mode_changes ?? ""} onChange={(e) => setW({ ...w, write_mode_changes: e.target.value })}>
          {w.write_mode_changes == null && <option value="">none in this job</option>}
          <option value="append">Append: change log</option><option value="merge">Merge: current state</option></select><Mixed s={stored?.write_mode_changes} /></label>
        <label>Full-reload tables<select value={w.write_mode_full ?? ""} onChange={(e) => setW({ ...w, write_mode_full: e.target.value })}>
          {w.write_mode_full == null && <option value="">none in this job</option>}
          <option value="append">Append: a snapshot each run</option><option value="merge">Merge: replace the rows</option></select><Mixed s={stored?.write_mode_full} /></label>
        <label>Freshness target (minutes)<input type="number" min={1} value={w.freshness_sla_minutes ?? ""} placeholder="none"
          onChange={(e) => setW({ ...w, freshness_sla_minutes: e.target.value })} /><Mixed s={stored?.freshness_sla_minutes} /></label>
      </div>
      {modeNote}
      <div className="form-grid">
        <label>Target catalog<input value={w.sink_catalog ?? ""} placeholder="catalog" onChange={(e) => setW({ ...w, sink_catalog: e.target.value })} /><Mixed s={stored?.sink_catalog} /></label>
        <label>Target schema<input value={w.sink_schema ?? ""} placeholder="schema" onChange={(e) => setW({ ...w, sink_schema: e.target.value })} /><Mixed s={stored?.sink_schema} /></label>
        <label>Target table<input value={w.sink_table ?? ""} onChange={(e) => setW({ ...w, sink_table: e.target.value })} /><Mixed s={stored?.sink_table} /></label>
        <label className="checkbox"><input type="checkbox" checked={!!w.lowercase} onChange={(e) => setW({ ...w, lowercase: e.target.checked })} /> Lower-case names</label>
      </div>
      <p className="help"><span className="info" aria-hidden>ⓘ</span> Valid tags: <span className="mono">{TAGS}</span>, with any text between them.
        With fan-in, {"{server}"} and {"{database}"} can't be used in target names: they would give every database its own table.</p>
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
        <thead><tr><th>Column</th><th>Value</th><th></th></tr></thead>
        <tbody>
          {stored?.identity?.mixed && !Object.keys(differing).length && <tr><td colSpan={3}><Mixed s={stored.identity} /></td></tr>}
          <tr><td className="mono">__client_id__</td><td><input className="mono" value={w.client_id ?? ""} disabled={w.fan_in === false}
            onChange={(e) => setW({ ...w, client_id: e.target.value })} /> <span className="help">keeps each database's rows apart{w.fan_in === false ? " (fan-in only)" : ""}</span></td><td></td></tr>
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
      <p className="help"><span className="info" aria-hidden>ⓘ</span> Valid tags: <span className="mono">{TAGS}</span>, for example{" "}
        <span className="mono">{"{server}/{database}"}</span>. A column added later is empty for rows loaded before it.</p>
    </>
  );
}

/** When it runs: the cadence, a start time and zone for set-time cadences, paused, and who is emailed on failure. */
export function RunFields({ run, setRun, params, setParams, cadences = CADENCES }: { run: Row; setRun: (r: Row) => void;
                           params: Record<string, string>; setParams: (p: Record<string, string>) => void; cadences?: number[] }) {
  const atSetTimes = Number(run.cadence_minutes) > 60;
  return (
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
      <label className="checkbox wide"><input type="checkbox" checked={params.fail_on_table_errors === "true"}
        onChange={(e) => setParams({ ...params, fail_on_table_errors: e.target.checked ? "true" : "" })} />
        Also email when tables fail in a run <span className="help">(the run then ends as failed)</span></label>
    </div>
  );
}

/** The ingestion job's tuning keys, each at the Databricks job's default unless set. */
export function ParameterFields({ params, setParams, options }: { params: Record<string, string>;
                                 setParams: (p: Record<string, string>) => void; options?: Row }) {
  const names: string[] = (options?.parameters ?? []).filter((k: string) => k !== "fail_on_table_errors");
  return (
    <div className="form-grid">
      {names.map((k) => (
        <label key={k}>{k.replace(/_/g, " ").replace(/^./, (c) => c.toUpperCase())}
          {(options?.flags ?? []).includes(k)
            ? <select value={params[k] ?? ""} onChange={(e) => setParams({ ...params, [k]: e.target.value })}>
                <option value="">default{options?.defaults?.[k] ? ` (${options.defaults[k]})` : ""}</option><option>true</option><option>false</option></select>
            : <input value={params[k] ?? ""} placeholder={options?.defaults?.[k] ? `default (${options.defaults[k]})` : "default"}
                onChange={(e) => setParams({ ...params, [k]: e.target.value })} />}
        </label>
      ))}
    </div>
  );
}

/** The Advanced configuration's one-line summary: the keys set away from their defaults. */
export const paramsSummary = (params: Record<string, string>) => Object.entries(params)
  .filter(([k, v]) => v && k !== "fail_on_table_errors").map(([k, v]) => `${k.replace(/_/g, " ")} ${v}`).join(" · ");
