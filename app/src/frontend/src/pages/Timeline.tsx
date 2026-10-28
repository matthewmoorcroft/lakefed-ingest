import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api, qs, type Row } from "../api";
import { PageHead, Stamp, readAt, ErrorBox, Notice, fmtTime, useWidth, plural } from "../components";

// Runs per collection over a window: planned triggers (ticks), runs (bars by outcome) and triggers
// Databricks skipped because the previous run was still going (red markers).

interface Timeline { start: string; end: string; collections: { task_collection: string; runs: Row[]; planned: string[]; skipped: string[]; schedules: string[] }[] }

const LABEL_W = 230, MIN_CHART_W = 600, ROW_H = 26, AXIS_H = 26;
const COLOR: Record<string, string> = { completed: "var(--s-current)", failed: "var(--s-failing)", running: "var(--s-flight)", skipped: "var(--s-late)" };

export default function TimelinePage() {
  const [hours, setHours] = useState(24);
  // The chart is drawn at the card's own width, so it uses the whole screen without scaling its text.
  const [box, boxWidth] = useWidth<HTMLDivElement>();
  const CHART_W = Math.max(MIN_CHART_W, boxWidth - LABEL_W - 26);
  const t = useQuery({ queryKey: ["timeline", hours], queryFn: () => api.get<Timeline>(`/timeline${qs({ hours })}`), refetchInterval: 60_000 });
  const d = t.data;
  const t0 = d ? new Date(d.start).getTime() : 0, t1 = d ? new Date(d.end).getTime() : 1;
  const x = (iso: string | number) => LABEL_W + ((new Date(iso).getTime() - t0) / (t1 - t0)) * CHART_W;
  const rows = d?.collections ?? [];
  const height = AXIS_H + rows.length * ROW_H + 8;
  const step = hours <= 6 ? 1 : hours <= 24 ? 2 : 6;
  const ticks: number[] = [];
  if (d) {
    const first = new Date(t0); first.setMinutes(0, 0, 0);
    for (let ms = first.getTime() + 3600_000; ms < t1; ms += step * 3600_000) ticks.push(ms);
  }
  const stats = rows.reduce((a, r) => ({ runs: a.runs + r.runs.length, failed: a.failed + r.runs.filter((x) => x.status === "failed").length,
    skipped: a.skipped + r.skipped.length }), { runs: 0, failed: 0, skipped: 0 });
  return (
    <>
      <PageHead title="Timeline" lead="Every group run and trigger in the window, by ingestion group." right={<Stamp at={readAt(t)} every={60} error={t.error} />} />
      <div className="toolbar">
        <label>Window
          <select value={hours} onChange={(e) => setHours(Number(e.target.value))}>
            {[6, 12, 24, 48, 168].map((h) => <option key={h} value={h}>last {h < 48 ? `${h} hours` : `${h / 24} days`}</option>)}
          </select>
        </label>
        <span className="legend">
          <i style={{ background: COLOR.completed }} /> completed <i style={{ background: COLOR.failed }} /> failed
          <i style={{ background: COLOR.running }} /> running <i style={{ background: COLOR.skipped }} /> skipped (ingestion group already running)
          <i className="tick" /> planned trigger (current schedule, since its last change) <b className="skipmark">▼</b> trigger skipped by Databricks
        </span>
      </div>
      <ErrorBox error={t.error} />
      {d && <p className="muted small">{stats.runs} {plural(stats.runs, "run")}, {stats.failed} failed, {stats.skipped} skipped {plural(stats.skipped, "trigger")}, {fmtTime(d.start)} – {fmtTime(d.end)}.
        Hover a bar for details.</p>}
      {d && rows.length === 0 && <Notice>No runs or schedules in this window.</Notice>}
      {d && rows.length > 0 && (
        <div className="card timeline" ref={box}>
          <svg viewBox={`0 0 ${LABEL_W + CHART_W + 10} ${height}`} width={LABEL_W + CHART_W + 10} height={height} role="img" aria-label="Runs per ingestion group over time">
            {ticks.map((ms) => (
              <g key={ms}>
                <line x1={x(ms)} x2={x(ms)} y1={AXIS_H - 4} y2={height} style={{ stroke: "var(--border)" }} />
                <text x={x(ms)} y={14} fontSize={11} textAnchor="middle" style={{ fill: "var(--muted)" }}>
                  {new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
                </text>
              </g>
            ))}
            {rows.map((r, i) => {
              const y = AXIS_H + i * ROW_H;
              return (
                <g key={r.task_collection}>
                  <rect x={0} y={y} width={LABEL_W + CHART_W + 10} height={ROW_H} style={{ fill: i % 2 ? "var(--db-surface-subtle)" : "var(--db-surface-base)" }} />
                  <text x={6} y={y + 17} fontSize={12} style={{ fill: "var(--text)" }}>
                    {r.task_collection.length > 30 ? r.task_collection.slice(0, 29) + "…" : r.task_collection}
                    <title>{r.task_collection}{r.schedules.length ? `\n${r.schedules.join("\n")}` : "\nnot scheduled"}</title>
                  </text>
                  {r.planned.map((p) => <line key={p} x1={x(p)} x2={x(p)} y1={y + 3} y2={y + ROW_H - 3} strokeWidth={1} style={{ stroke: "var(--db-border-strong, #9aa5b1)" }} />)}
                  {r.runs.map((run) => {
                    const sx = Math.max(LABEL_W, x(run.started_at)), ex = x(run.finished_at ?? d.end);
                    return (
                      <rect key={run.run_id} x={sx} y={y + 6} width={Math.max(2, ex - sx)} height={ROW_H - 12} rx={2}
                        opacity={0.85} style={{ fill: COLOR[run.status] ?? "var(--muted)" }}>
                        <title>{`run ${run.run_id}: ${run.status}\n${fmtTime(run.started_at)} – ${run.finished_at ? fmtTime(run.finished_at) : "running"}`
                          + `\n${Math.round(((run.finished_at ? new Date(run.finished_at).getTime() : t1) - new Date(run.started_at).getTime()) / 6000) / 10} min`
                          + (run.ok !== null && run.ok !== undefined ? `\nok ${run.ok}, failed ${run.failed}, unchanged ${run.skipped}` : "")}</title>
                      </rect>
                    );
                  })}
                  {r.skipped.map((s) => (
                    <text key={s} x={x(s)} y={y + 10} fontSize={11} textAnchor="middle" style={{ fill: "var(--bad)" }}>▼<title>{`trigger skipped at ${fmtTime(s)}: previous run still going`}</title></text>
                  ))}
                </g>
              );
            })}
          </svg>
        </div>
      )}
    </>
  );
}
