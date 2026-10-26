import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { api, qs, type Row } from "../api";
import { ErrorBox, PageHead, Section, Stamp, StateKey, fmtInt } from "../components";
import { JobsTable } from "./Overview";

const FILTERS = [["all", "All"], ["running", "Running"], ["failing", "Failing"], ["late", "Late"], ["paused", "Paused"]] as const;
const PAGE = 25;

interface JobsPage { jobs: Row[]; total: number; offset: number; limit: number; not_in_job: Row | null; counts: Record<string, number>;
                   read_at: string }

/** Every ingestion job, failing first, filtered and searched, in a table that fills the page. */
export default function IngestionJobs() {
  const [params, setParams] = useSearchParams();
  const filter = params.get("filter") || "all";
  const [search, setSearch] = useState("");
  const [offset, setOffset] = useState(0);
  const jobs = useQuery({
    queryKey: ["ingestion-jobs", filter, search, offset],
    queryFn: () => api.get<JobsPage>(`/ingestion-jobs${qs({ filter, search, offset, limit: PAGE })}`),
    refetchInterval: 30_000,
  });
  const counts = jobs.data?.counts ?? {};
  return (
    <>
      <PageHead crumb={<span className="eyebrow">Ingestion jobs · Which jobs need me?</span>} title="Ingestion jobs"
        lead="Each ingestion job is a schedule: a cadence and its ingestion groups. Failing first, then late, then by name."
        right={<Stamp at={jobs.data?.read_at} every={30} />} />
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
        <input type="search" placeholder="Search jobs and groups" value={search}
          onChange={(e) => { setOffset(0); setSearch(e.target.value); }} />
      </div>
      <Section title="Jobs" actions={<StateKey />}>
        {jobs.data && <JobsTable jobs={jobs.data.jobs} notInJob={offset + PAGE >= jobs.data.total ? jobs.data.not_in_job : null}
          total={jobs.data.total} offset={offset} limit={PAGE} onPage={setOffset} fill />}
      </Section>
    </>
  );
}
