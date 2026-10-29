import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate, useSearchParams } from "react-router-dom";
import { api, qs, type Row } from "../api";
import { ErrorBox, OutLink, PageHead, RunStatus, Section, Stamp, StateKey, fmtInt, fmtShort, fmtTime, type Outcome } from "../components";
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
    refetchInterval: 30_000,
  });
  const counts = jobs.data?.counts ?? {};
  return (
    <>
      <PageHead title="Ingestion jobs"
        lead="Each ingestion job is a schedule: a cadence and its ingestion groups. Failing first, then late, then by name."
        right={<span className="card-actions"><Stamp at={jobs.data?.read_at} every={30} error={jobs.error} />
          <button className="primary" onClick={() => navigate("/ingestion-jobs/new")}>New ingestion job</button></span>} />
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
      </div>
      <Section title={null} actions={<StateKey />}>
        {jobs.data && <JobsTable jobs={jobs.data.jobs} notInJob={offset + PAGE >= jobs.data.total ? jobs.data.not_in_job : null}
          total={jobs.data.total} offset={offset} limit={PAGE} onPage={setOffset} fill
          sort={sort} onSort={(next) => { setOffset(0); setSort(next); }} />}
      </Section>
      <Deletions />
    </>
  );
}

/** The latest deleted jobs and their delete job runs: going, done, or failed with why, and Run again for one whose
 * tables are still there. Shown only when there is one. */
function Deletions() {
  const qc = useQueryClient();
  const page = useQuery({ queryKey: ["ingestion-job-deletions"], queryFn: () => api.get<Row>("/ingestion-jobs/deletions"),
    refetchInterval: (q) => (((q.state.data as Row | undefined)?.deletions ?? []).some((d: Row) => deletionOutcome(d).tone === "run") ? 5_000 : 60_000) });
  const again = useMutation({
    mutationFn: (d: Row) => api.post<Row>(`/schedules/${encodeURIComponent(d.id)}/delete`, { drop_targets: !!d.drop_targets }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["ingestion-job-deletions"] }),
  });
  const rows: Row[] = page.data?.deletions ?? [];
  if (!rows.length && !page.error) return null;
  return (
    <Section title="Deleted jobs" note="Delete starts the delete job, which deletes the job's tables and their state, and the target tables when ticked. Run history stays.">
      <ErrorBox error={page.error ?? again.error} />
      <div className="table-wrap"><table className="data">
        <thead><tr><th>Ingestion job</th><th>Deleted</th><th>Target tables</th><th>Delete job</th><th className="num">Tables left</th><th /></tr></thead>
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
                <td>{outcome.tone !== "run" && Number(d.tables_left) > 0 &&
                  <button className="ghost" disabled={again.isPending} onClick={() => again.mutate(d)}>Run again</button>}</td>
              </tr>
            );
          })}
        </tbody>
      </table></div>
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
