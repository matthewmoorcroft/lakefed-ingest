import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, type Row } from "../api";
import { Badge, ConfirmButton, ErrorBox, Grid, Metric, Notice, Section, autoColumns } from "../components";
import { usePermissions } from "../permissions";

interface IngestJob { job: Row; active: Row[]; recent: Row[] }

export default function Jobs() {
  const qc = useQueryClient();
  const job = useQuery({ queryKey: ["ingest-job"], queryFn: () => api.get<IngestJob>("/jobs/ingest"), refetchInterval: 15_000 });
  const collections = useQuery({ queryKey: ["collections"], queryFn: () => api.get<string[]>("/collections") });
  const [collection, setCollection] = useState("");
  const [parallelism, setParallelism] = useState<number | "">("");
  const [message, setMessage] = useState<string | null>(null);
  const post = useMutation({
    mutationFn: ({ path, body }: { path: string; body?: Row }) => api.post(path, body),
    onSuccess: (r: Row, v) => {
      setMessage(v.path.includes("cancel") ? `Cancel requested for run ${r.cancelled}.` : `Started run ${r.run_id}.`);
      qc.invalidateQueries({ queryKey: ["ingest-job"] });
    },
  });
  const j = job.data?.job;
  const refusals = usePermissions().data?.run;
  const linkCol = { cellRenderer: (p: any) => p.value ? <a href={p.value} target="_blank" rel="noreferrer">open</a> : null };
  const stateCols = { state: { cellRenderer: (p: any) => <Badge value={p.value} /> }, result: { cellRenderer: (p: any) => <Badge value={p.value} /> }, url: linkCol };
  const coll = collection || collections.data?.[0] || "";
  const par = parallelism === "" ? Number(j?.parameters?.parallelism || 32) : parallelism;
  return (
    <>
      <h1>Databricks jobs</h1>
      <p className="muted">The bundle-deployed Databricks job that ingests one ingestion group a run, and the schema setup job. Ingestion jobs (schedules and their groups) are on the Overview and Ingestion jobs.</p>
      <ErrorBox error={job.error || post.error} />
      {message && <Notice tone="ok">{message}</Notice>}
      {j && (
        <Section title={j.name} actions={<a href={j.url} target="_blank" rel="noreferrer">Open in Jobs</a>}>
          <div className="metrics">
            <Metric label="Schedule" value={j.schedule ?? "none"} />
            <Metric label="Schedule status" value={j.schedule_status ?? "–"} />
            <Metric label="Scheduled ingestion group" value={j.default_task_collection ?? "–"} />
          </div>
          <p className="muted small">The Databricks job's own schedule only runs its default ingestion group; the Schedules page
            runs any ingestion group on a cadence (runs it starts show their schedule).</p>
          <div className="toolbar">
            <label>Ingestion group
              <select value={coll} onChange={(e) => setCollection(e.target.value)}>
                {collections.data?.map((c) => <option key={c}>{c}</option>)}
              </select>
            </label>
            <label>Parallelism
              <input type="number" min={1} max={256} value={par} onChange={(e) => setParallelism(Number(e.target.value) || "")} />
            </label>
            <ConfirmButton label="Run now" disabled={!coll || !!refusals?.ingestion}
              onConfirm={() => post.mutateAsync({ path: "/jobs/ingest/run", body: { task_collection: coll, parallelism: par } })} />
          </div>
          {refusals?.ingestion && <p className="muted small">Run now and Cancel: {refusals.ingestion}.</p>}
        </Section>
      )}
      <Section title={`Active runs (${job.data?.active.length ?? "…"})`}>
        {job.data?.active.length ? (
          <Grid rows={job.data.active} fitRows
            columns={[...autoColumns(job.data.active, stateCols), {
              headerName: "", field: "run_id", width: 150, sortable: false, filter: false,
              cellRenderer: (p: any) => (
                <ConfirmButton label="Cancel" danger confirmLabel="Confirm" disabled={!!refusals?.ingestion}
                  onConfirm={() => post.mutateAsync({ path: `/jobs/runs/${p.value}/cancel` })} />
              ),
            }]} />
        ) : <p className="muted">No active runs.</p>}
      </Section>
      <Section title="Recent runs">
        <Grid rows={job.data?.recent ?? (job.error ? [] : undefined)} fillReserve={150} columns={autoColumns(job.data?.recent ?? [], stateCols)} />
      </Section>
      <Section title="Schema setup">
        <p className="muted small">Creates or migrates the Lakebase schema. Safe to re-run; upgrades also migrate on the next
          ingestion run.</p>
        <ConfirmButton label="Run setup job" disabled={!!refusals?.setup} onConfirm={() => post.mutateAsync({ path: "/jobs/setup/run" })} />
        {refusals?.setup && <p className="muted small">Run setup job: {refusals.setup}.</p>}
      </Section>
    </>
  );
}
