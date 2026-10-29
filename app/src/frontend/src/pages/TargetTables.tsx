import { Fragment, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api, qs, type Row } from "../api";
import { ErrorBox, OutLink, Pager, Section, StateBar, StateKey, fmtAgo, fmtInt, plural } from "../components";

// A job's target tables: where its data lands. Each target says whether it is fanned in from many source tables or
// written one-to-one, lists the job's source tables writing it (opened to the list), any written from outside the
// job, their states, and the rows the run records say were written over 7 days. Names follow Catalog Explorer.
// Exact row counts would need a SQL warehouse, which the app avoids; Catalog Explorer has them, one click away.

const PAGE = 25;

function SourceRows({ jobId, target }: { jobId: string; target: string }) {
  const [offset, setOffset] = useState(0);
  const sources = useQuery({
    queryKey: ["job-target-sources", jobId, target, offset],
    queryFn: () => api.get<Row>(`/ingestion-jobs/${jobId}/targets/sources${qs({ target, offset, limit: PAGE })}`),
  });
  return (
    <>
      {(sources.data?.items ?? []).map((s: Row) => (
        <tr key={s.id} className="l2">
          <td className="name">{s.server} › {s.database} › {s.schema}.{s.table}
            {s.client_id && <span className="sub">client_id {s.client_id}</span>}</td>
          <td><Link to={`/collections/${encodeURIComponent(s.task_collection)}`}>{s.task_collection}</Link></td>
          <td>{s.enabled ? "" : <span className="muted">disabled</span>}</td>
          <td><StateBar row={s} /></td>
          <td className="num">{s.rows_written_7d != null ? fmtInt(s.rows_written_7d) : ""}
            {s.last_written && <span className="sub">last {fmtAgo(s.last_written)}</span>}</td>
          <td></td>
        </tr>
      ))}
      {(sources.data?.total ?? 0) > PAGE && (
        <tr className="l2"><td colSpan={6}><Pager offset={offset} limit={PAGE} total={sources.data!.total} noun="source table" onPage={setOffset} /></td></tr>
      )}
      {sources.error && <tr className="l2"><td colSpan={6}><ErrorBox error={sources.error} /></td></tr>}
    </>
  );
}

/** Whether a target is fanned in from many source tables (counting those outside the job) or written one-to-one. */
function WrittenFrom({ t }: { t: Row }) {
  const writers = Number(t.sources) + Number(t.outside_sources);
  if (writers <= 1) return <>One-to-one<span className="sub">1 source table</span></>;
  const databases = `${fmtInt(t.databases)} ${plural(t.databases, "database")}${Number(t.outside_sources) ? " in this job" : ""}`;
  return <>Fan-in from {fmtInt(writers)} source tables
    <span className="sub">{databases}{Number(t.with_client_id) > 0 ? ", kept apart by client_id" : ""}</span></>;
}

/** "3 fan in, 2 are one-to-one", over the targets the search matches. */
function fanInSummary(total: number, fanIn: number) {
  const oneToOne = total - fanIn;
  if (!fanIn) return `${total === 1 ? "It is" : "All are"} one-to-one: each is written from one source table.`;
  if (!oneToOne) return `${fanIn === 1 ? "It fans in" : "All fan in"} from many source tables.`;
  return `${fmtInt(fanIn)} fan in from many source tables; ${fmtInt(oneToOne)} ${oneToOne === 1 ? "is" : "are"} one-to-one.`;
}

export function TargetTables({ jobId }: { jobId: string }) {
  const [offset, setOffset] = useState(0);
  const [search, setSearch] = useState("");
  const [open, setOpen] = useState<string | null>(null);
  const targets = useQuery({
    queryKey: ["job-targets", jobId, search, offset],
    queryFn: () => api.get<Row>(`/ingestion-jobs/${jobId}/targets${qs({ search, offset, limit: PAGE })}`),
  });
  const total = targets.data?.total;
  return (
    <Section title={`Target tables${total != null ? `: ${fmtInt(total)}` : ""}`} actions={<StateKey />}
      note={<>Where this job's data lands.{total ? ` ${fanInSummary(total, Number(targets.data?.fan_in ?? 0))}` : ""} Rows
        written are from the run records over the last 7 days; Catalog Explorer has each table's exact size.</>}>
      <input className="search tree-search" placeholder="Find a target table" value={search}
        onChange={(e) => { setSearch(e.target.value); setOffset(0); setOpen(null); }} />
      <div className="table-wrap"><table className="data tree">
        <thead><tr><th>Name › its source tables</th><th>Written from</th>
          <th>Outside this job</th><th>State now</th><th className="num">Rows written (7 days)</th><th></th></tr></thead>
        <tbody>
          {(targets.data?.items ?? []).map((t: Row) => (
            <Fragment key={t.target}>
              <tr>
                <td className="name"><button className="twisty" aria-expanded={open === t.target} title={t.target}
                  onClick={() => setOpen(open === t.target ? null : t.target)}>{open === t.target ? "▾" : "▸"}</button><b>{t.table}</b>
                  <span className="sub target-path">{t.catalog}.{t.schema}</span></td>
                <td><WrittenFrom t={t} /></td>
                <td>{Number(t.outside_sources) > 0
                  ? <span className="tag warn">{fmtInt(t.outside_sources)} {plural(t.outside_sources, "source table")}</span>
                  : <span className="muted">none</span>}</td>
                <td><StateBar row={t} /></td>
                <td className="num">{t.rows_written_7d != null ? fmtInt(t.rows_written_7d) : ""}
                  <span className="sub">{t.last_written ? `last ${fmtAgo(t.last_written)}` : "none in 7 days"}</span></td>
                <td><OutLink href={t.url}>Catalog Explorer</OutLink></td>
              </tr>
              {open === t.target && <SourceRows jobId={jobId} target={t.target} />}
            </Fragment>
          ))}
          {targets.data && !targets.data.items.length && (
            <tr><td colSpan={6} className="muted">{search ? "No target table matches." : "No tables yet."}</td></tr>
          )}
        </tbody>
      </table></div>
      {(total ?? 0) > PAGE && <Pager offset={offset} limit={PAGE} total={total!} noun="target table" onPage={setOffset} />}
      <ErrorBox error={targets.error} />
    </Section>
  );
}
