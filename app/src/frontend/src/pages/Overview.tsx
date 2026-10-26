import { useQuery } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";
import { api, type Row } from "../api";
import {
  BigCount, ErrorBox, Notice, PageHead, ProgressBar, ProgressKey, RunStatus, Section, Stamp, StateBar, StateKey, Tag,
  fmtAgo, fmtClock, fmtDuration, fmtInt, progressOf, progressText, plural, type Tone
} from "../components";

interface Overview { collections: Row[]; running: Row[]; servers: Row[]; read_at: string; cache_s: number }

const sum = (rows: Row[], k: string) => rows.reduce((a, r) => a + Number(r[k] || 0), 0);

/** The tone of a server's health tag (queries.server_health). */
const HEALTH_TONE: Record<string, Tone> = { healthy: "ok", failing: "bad", late: "warn", "first load": "first", "no tables": "" };

export default function OverviewPage() {
  const nav = useNavigate();
  const ov = useQuery({ queryKey: ["overview"], queryFn: () => api.get<Overview>("/overview"), refetchInterval: 30_000 });
  const groups = useQuery({ queryKey: ["error-groups"], queryFn: () => api.get<{ groups: Row[]; tables_shown: number; truncated: boolean }>("/errors/groups?hours=24"),
                            refetchInterval: 60_000 });
  const cols = ov.data?.collections ?? [];
  const running = ov.data?.running ?? [];
  const enabled = sum(cols, "enabled_tables");
  const noTarget = sum(cols, "no_sla_tables");
  const current = sum(cols, "state_current");
  const databases = sum(cols, "databases");
  const servers = ov.data?.servers.length ?? 0;
  const pct = enabled ? ((100 * current) / enabled).toFixed(1) : "0.0";
  return (
    <>
      <PageHead crumb={<span className="eyebrow">Overview · Is everything healthy now?</span>} title={!ov.data ? "Overview" : enabled ? `${pct}% of ${fmtInt(enabled)} enabled ${plural(enabled, "table")} ${enabled === 1 ? "is" : "are"} current` : "No enabled tables yet"}
        lead={ov.data && <>Now: <strong>{fmtInt(sum(cols, "state_first_load"))}</strong> waiting for a first load,{" "}
          <strong>{fmtInt(sum(cols, "state_failing"))}</strong> failing, <strong>{fmtInt(sum(cols, "state_late"))}</strong> late,
          across <strong>{fmtInt(databases)}</strong> {plural(databases, "database")} in <strong>{cols.length}</strong> {plural(cols.length, "ingestion group")}{" "}
          on <strong>{servers}</strong> {servers === 1 ? "server" : "servers"}.</>}
        right={<Stamp at={ov.data?.read_at} every={30} />} />
      <ErrorBox error={ov.error} />
      {ov.data && !cols.length && <Notice tone="info">No tables are set up yet. Add them with <Link to="/bulk">Bulk import</Link> or <Link to="/add">Add table</Link>.</Notice>}
      <div className="metrics">
        <div className="metric ok"><div className="metric-label">Current</div>
          <div className="metric-value">{fmtInt(current)}<small>tables</small></div><div className="metric-sub">{pct}% of enabled, now</div></div>
        <div className="metric first"><div className="metric-label">Waiting for a first load</div>
          <div className="metric-value">{fmtInt(sum(cols, "state_first_load"))}<small>tables</small></div><div className="metric-sub">now</div></div>
        <div className={`metric ${sum(cols, "state_failing") ? "bad" : ""}`}><div className="metric-label">Failing</div>
          <div className="metric-value">{fmtInt(sum(cols, "state_failing"))}<small>tables</small></div>
          <div className="metric-sub">latest attempt failed</div></div>
        <div className={`metric ${sum(cols, "state_late") ? "warn" : ""}`}><div className="metric-label">Late</div>
          <div className="metric-value">{fmtInt(sum(cols, "state_late"))}<small>tables</small></div>
          <div className="metric-sub">past their freshness target</div></div>
        <div className="metric"><div className="metric-label">Running now</div>
          <div className="metric-value">{running.length}<small>{running.length === 1 ? "run" : "runs"}</small></div>
          <div className="metric-sub">{running.map((r) => <Link key={r.run_id} to={`/collections/${encodeURIComponent(r.task_collection)}`}
            style={{ marginRight: 8 }}>{r.task_collection}</Link>)}{!running.length && "none"}</div></div>
        <div className="metric"><div className="metric-label">Rows written</div>
          <div className="metric-value"><BigCount value={sum(cols, "rows_24h")} /></div><div className="metric-sub">last 24 h</div></div>
      </div>
      {/* Only once freshness targets are in use: until then no table can be late, and there is nothing to warn about. */}
      {noTarget > 0 && noTarget < enabled && (
        <Notice tone="warn">{fmtInt(noTarget)} enabled {plural(noTarget, "table")} {noTarget === 1 ? "has" : "have"} no freshness
          target, so {noTarget === 1 ? "it is" : "they are"} never flagged as late. To set one: Tables, select the tables, Bulk edit,
          then Freshness target.</Notice>
      )}

      <Section title="Running now" actions={<Link to="/runs">All runs</Link>}
        note="Checked counts the ingestion group's tables this run has stamped: unchanged ones in bulk once change detection ends, loaded ones as each finishes.">
        {!running.length && <div className="empty">Nothing is running. A run appears here as soon as it starts.</div>}
        {running.map((r) => {
          const p = progressOf(r);
          const slow = r.typical_s && r.elapsed_s > 2 * Math.max(r.typical_s, 60);
          return (
            <div className="run-row" key={r.run_id}>
              <div>
                <div className="title"><Link to={`/collections/${encodeURIComponent(r.task_collection)}`}>{r.task_collection}</Link></div>
                <div className="kv"><span>Run <b>#{r.run_id}</b></span>{r.server && <span>{r.server}</span>}
                  {r.parallelism && <span>Parallelism <b>{r.parallelism}</b></span>}</div>
                <div style={{ marginTop: 6 }}>{slow ? <Tag tone="warn">Longer than usual</Tag> : <Tag tone="run">Running</Tag>}</div>
              </div>
              <div><div className="count">{progressText(p)}</div><ProgressBar p={p} big /><ProgressKey p={p} /></div>
              <div className="kv stack">
                <span>Running <b>{fmtDuration(r.elapsed_s)}</b>{r.typical_s ? <> (typical <b>{fmtDuration(r.typical_s)}</b>)</> : null}</span>
                <span>Last table activity <b>{fmtAgo(r.last_activity_at)}</b></span>
                <span>Started <b>{fmtClock(r.started_at)}</b></span>
              </div>
            </div>
          );
        })}
      </Section>

      <Section title="Servers" actions={<StateKey />}
        note="Each source server (the tables' instance group, else their connection or host) with its enabled tables by state.">
        <div className="server-cards">
          {(ov.data?.servers ?? []).map((s) => (
            <div className="server-card" key={s.server}>
              <div className="top"><div><h3>{s.server || "(no server)"}</h3>
                <div className="meta">{fmtInt(s.databases)} {plural(s.databases, "database")} · {fmtInt(s.tables)} {plural(s.tables, "table")} · {s.collections} {plural(s.collections, "ingestion group")}</div></div>
                {Number(s.running_collections) > 0 ? <Tag tone="run">{s.running_collections} running</Tag>
                  : <Tag tone={HEALTH_TONE[s.health] ?? ""}>{s.health ? s.health[0].toUpperCase() + s.health.slice(1) : "Unknown"}</Tag>}</div>
              <StateBar row={s} />
              <div className="nums">
                <div><b>{Number(s.tables) ? Math.floor((100 * Number(s.state_current)) / Number(s.tables)) : 0}%</b><span>current</span></div>
                <div><b>{fmtInt(s.state_first_load)}</b><span>first load</span></div>
                <div><b>{fmtInt(s.state_failing)}</b><span>failing</span></div>
                <div><b>{fmtInt(s.state_late)}</b><span>late</span></div>
              </div>
              <div className="meta">Last success {fmtAgo(s.newest_success_at)} · oldest {fmtAgo(s.oldest_success_at)}</div>
            </div>
          ))}
        </div>
      </Section>

      <Section title="Ingestion groups" note="Select an ingestion group for its databases. Its tables and schedule stay on the Tables and Schedules pages.">
        <div className="table-wrap">
          <table className="data">
            <thead><tr><th>Ingestion group</th><th>Server</th><th className="num">Databases</th><th className="num">Tables</th>
              <th>State now</th><th className="num">Failing</th><th className="num">First load</th><th className="num">Late</th>
              <th>Last run</th><th className="num">Rows, 24 h</th></tr></thead>
            <tbody>
              {cols.map((c) => {
                const run = running.find((r) => r.task_collection === c.task_collection);
                return (
                  <tr key={c.task_collection} className="link" onClick={() => nav(`/collections/${encodeURIComponent(c.task_collection)}`)}>
                    <td><Link to={`/collections/${encodeURIComponent(c.task_collection)}`}><b>{c.task_collection}</b></Link></td>
                    <td>{Number(c.servers) > 1 ? `${c.servers} servers` : c.server}</td>
                    <td className="num">{fmtInt(c.databases)}</td><td className="num">{fmtInt(c.enabled_tables)}</td>
                    <td><StateBar row={c} /></td>
                    <td className="num" style={{ color: Number(c.state_failing) ? "var(--bad)" : undefined }}>{fmtInt(c.state_failing)}</td>
                    <td className="num">{fmtInt(c.state_first_load)}</td><td className="num">{fmtInt(c.state_late)}</td>
                    <td>{run ? <><Tag tone="run">Running</Tag><span className="sub">#{run.run_id} since {fmtClock(run.started_at)},{" "}
                      {Math.floor((100 * Number(run.tables_checked || 0)) / Math.max(1, Number(run.enabled_tables || 0)))}% checked</span></>
                      : c.last_run_id ? <><RunStatus status={c.last_run_status} /><span className="sub">#{c.last_run_id} at {fmtClock(c.last_run_started)}
                        {c.last_run_finished && `, took ${fmtDuration((new Date(c.last_run_finished).getTime() - new Date(c.last_run_started).getTime()) / 1000)}`}
                        {Number(c.last_run_failed) > 0 && `, ${c.last_run_failed} failed`}</span></>
                      : <span className="muted">never run</span>}</td>
                    <td className="num">{fmtInt(c.rows_24h)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <StateKey />
      </Section>

      <Section title="Failures, last 24 hours" actions={<Link to="/tables?status=failing">Failing tables</Link>}
        note="Failures with the same cause are grouped. Connection and timeout failures usually clear on the next run; the others need a change at the source or in config.">
        <ErrorBox error={groups.error} />
        {groups.data && groups.data.groups.length === 0 && <Notice tone="ok">No failed tables in the last 24 hours.</Notice>}
        {groups.data?.truncated && <Notice tone="warn">Showing the {fmtInt(groups.data.tables_shown)} tables that failed most
          recently; more failed in the last 24 hours. The Tables page's failing filter lists them all.</Notice>}
        {(groups.data?.groups ?? []).map((g) => (
          <div className="err-group" key={`${g.error_class}:${g.signature}`}>
            <div><div className="what">{g.label}{" "}
              <Tag tone={g.error_class === "connection" || g.error_class === "timeout" ? "warn" : "bad"}>{g.error_class}</Tag></div>
              <div className="where">{fmtInt(g.tables)} {plural(g.tables, "table")}
                {Number(g.failures) > Number(g.tables) ? <> ({fmtInt(g.failures)} failures)</> : null} in {g.databases.length}{" "}
                {g.databases.length === 1 ? "database" : "databases"}:{" "}
                {g.databases.slice(0, 6).map((d: Row) => `${d.src_database} (${d.task_collection}): ${d.tables.slice(0, 4).join(", ")}`
                  + (d.tables.length > 4 ? ` and ${d.tables.length - 4} more` : "")).join("; ")}
                {g.databases.length > 6 && `; and ${g.databases.length - 6} more databases`}</div></div>
            <div className="where" style={{ textAlign: "right" }}>first {fmtClock(g.first_at)}<br />last {fmtClock(g.last_at)}</div>
            <div className="raw">{g.example}</div>
          </div>
        ))}
      </Section>
    </>
  );
}
