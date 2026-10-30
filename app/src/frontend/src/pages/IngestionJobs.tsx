import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate, useSearchParams } from "react-router-dom";
import { api, qs, type Row } from "../api";
import { ErrorBox, Info, OutLink, PageHead, Pager, RunStatus, Section, Twisty, fmtInt, fmtShort, fmtTime, usePageStamp,
         type Outcome } from "../components";
import { JobsTable, type JobSort } from "./Overview";

const FILTERS = [["all", "All"], ["running", "Running"], ["failing", "Failing"], ["late", "Late"], ["paused", "Paused"]] as const;
const PAGE = 25;

interface JobsPage { jobs: Row[]; total: number; offset: number; limit: number; not_in_job: Row | null; counts: Record<string, number>;
                   read_at: string }

/** Every ingestion job, failing first, filtered and searched, in a table that fills the page. */
export default function IngestionJobs() {
  const [params, setParams] = useSearchParams();
  const navigate = useNavigate();
  const filter = params.get("filter") || "all";
  const [search, setSearch] = useState("");
  const [offset, setOffset] = useState(0);
  const [sort, setSort] = useState<JobSort>({ by: "", descending: true });
  const jobs = useQuery({
    queryKey: ["ingestion-jobs", filter, search, offset, sort],
    queryFn: () => api.get<JobsPage>(`/ingestion-jobs${qs({ filter, search, offset, limit: PAGE, sort: sort.by, descending: sort.descending })}`),
    refetchInterval: 15_000,
  });
  const counts = jobs.data?.counts ?? {};
  usePageStamp(jobs.data?.read_at, 30, jobs.error);
  return (
    <>
      <PageHead title="Ingestion jobs" />
      <ErrorBox error={jobs.error} />
      <div className="toolbar">
        <span className="chips">
          {FILTERS.map(([key, label]) => (
            <button key={key} className={`chip${filter === key ? " active" : ""}`}
              onClick={() => { setOffset(0); setParams(key === "all" ? {} : { filter: key }); }}>
              {label} <b>{fmtInt(counts[key] ?? 0)}</b>
            </button>
          ))}
        </span>
        <input type="search" className="search-wide" placeholder="Search jobs and groups" value={search}
          onChange={(e) => { setOffset(0); setSearch(e.target.value); }} />
        <button className="primary toolbar-end" onClick={() => navigate("/ingestion-jobs/new")}>New ingestion job</button>
      </div>
      <Section title="Jobs">
        {jobs.data && <JobsTable jobs={jobs.data.jobs} notInJob={offset + PAGE >= jobs.data.total ? jobs.data.not_in_job : null}
          total={jobs.data.total} offset={offset} limit={PAGE} onPage={setOffset} fill
          sort={sort} onSort={(next) => { setOffset(0); setSort(next); }} />}
      </Section>
      <Deletions />
    </>
  );
}

const DELETED_PAGE = 10;

/** The deleted jobs whose Delete finished: history only, closed until opened, its count in the header, a page at a time.
 * A job whose Delete is still pending, or failed, stays in the jobs list (with its Deleting or Delete failed state and
 * Run again on its own page). Shown only when there is one. */
function Deletions() {
  const [open, setOpen] = useState(false);
  const [offset, setOffset] = useState(0);
  const page = useQuery({ queryKey: ["ingestion-job-deletions", open, offset],
    queryFn: () => api.get<Row>(`/ingestion-jobs/deletions${qs({ offset, limit: DELETED_PAGE, rows: open })}`),
    refetchInterval: 60_000 });
  const total = Number(page.data?.total ?? 0);
  const rows: Row[] = page.data?.deletions ?? [];
  if (!total && !page.error) return null;
  const title = <span className="section-toggle"><Twisty open={open} onToggle={() => setOpen(!open)}
    label={open ? "Close the deleted jobs" : "Open the deleted jobs"}>Deleted jobs ({fmtInt(total)})</Twisty>
    <Info label="About deleted jobs">Jobs whose Delete has finished: their tables and their state are gone, and the target
      tables too when ticked. Run history stays.</Info></span>;
  return (
    <Section title={title}>
      <ErrorBox error={page.error} />
      {open && <>
        <div className="table-wrap"><table className="data">
          <thead><tr><th>Ingestion job</th><th>Deleted</th><th>Target tables</th><th>Delete job</th><th className="num">Tables left</th></tr></thead>
          <tbody>
            {rows.map((d) => {
              const outcome = deletionOutcome(d);
              return (
                <tr key={d.id}>
                  <td className="name"><b>{d.name}</b></td>
                  <td title={fmtTime(d.deleted_at)}>{fmtShort(d.deleted_at)}</td>
                  <td>{d.drop_targets ? "Dropped" : "Kept"}</td>
                  <td><OutLink href={d.run?.url}><RunStatus status={null} outcome={outcome} /></OutLink>
                    {outcome.note && outcome.tone === "bad" && <span className="sub">{outcome.note}</span>}</td>
                  <td className="num">{fmtInt(d.tables_left)}</td>
                </tr>
              );
            })}
          </tbody>
        </table></div>
        <Pager offset={offset} limit={DELETED_PAGE} total={total} noun="deleted job" onPage={setOffset} />
      </>}
    </Section>
  );
}

/** A deleted job's delete job run as a status: not started, deleting, deleted, or failed with its message. */
export function deletionOutcome(d: Row): Outcome {
  const run = d.run;
  if (!run) return { label: "Not started", tone: "bad", note: "The delete job didn't start: Run again starts it" };
  if (!run.result && ["QUEUED", "PENDING", "RUNNING", "TERMINATING", "BLOCKED", "WAITING_FOR_RETRY"].includes(run.state))
    return { label: "Deleting", tone: "run", note: null };
  if (run.result === "SUCCESS") return { label: "Deleted", tone: "ok", note: null };
  return { label: run.result ? "Failed" : "Unknown", tone: "bad", note: run.message || null };
}
