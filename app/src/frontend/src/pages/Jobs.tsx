import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, type Row } from "../api";
import { Tip, OutLink, PageHead, Stamp, readAt, Badge, ConfirmButton, ErrorBox, Grid, Metric, Notice, Section, autoColumns, statusName } from "../components";
import { usePermissions } from "../permissions";
import { useSettings } from "../App";

interface IngestJob { job: Row; active: Row[]; recent: Row[] }

/** A Quartz schedule in words for its common shapes, as Job details shows a schedule; the cron itself is on hover. */
function cronWords(cron: string): string {
  const [sec, min, hour, day, month, weekday] = cron.trim().split(/\s+/);
  const plain = sec === "0" && day === "*" && month === "*" && (weekday === "?" || weekday === "*");
  const every = /^(?:0\/|\*\/)(\d+)$/.exec(min ?? "");
  if (plain && every && hour === "*") return `Every ${every[1]} minutes`;
  if (plain && /^\d+$/.test(min) && hour === "*") return min === "0" ? "Every hour, on the hour" : `Every hour at :${min.padStart(2, "0")}`;
  if (plain && /^\d+$/.test(min) && /^\d+$/.test(hour)) return `Every day at ${hour.padStart(2, "0")}:${min.padStart(2, "0")}`;
  return `Cron: ${cron}`;
}

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
  const { config } = useSettings();
  const linkCol = { cellRenderer: (p: any) => p.value ? <OutLink href={p.value}>View run</OutLink> : null };
  const stateCols = { state: { cellRenderer: (p: any) => <Badge value={p.value} /> }, result: { cellRenderer: (p: any) => <Badge value={p.value} /> }, url: linkCol };
  const coll = collection || collections.data?.[0] || "";
  const par = parallelism === "" ? Number(j?.parameters?.parallelism || 32) : parallelism;
  return (
    <>
      <PageHead title="Databricks jobs" lead="The Databricks job that ingests one ingestion group a run, and the schema setup job."
        right={<Stamp at={readAt(job)} every={15} error={job.error} />} />
      <ErrorBox error={job.error || post.error} />
      {message && <Notice tone="ok">{message}</Notice>}
      {j && (
        <Section title={j.name} actions={<a href={j.url} target="_blank" rel="noreferrer">Open in Jobs</a>}>
          <div className="metrics">
            <Metric label="Schedule" value={j.schedule ? <Tip focusable text={`Cron syntax: ${j.schedule}${j.timezone ? ` (${j.timezone})` : ""}`}>
              {cronWords(j.schedule)}</Tip> : "None"} />
            <Metric label="Schedule status" value={j.schedule_status ? statusName(j.schedule_status) : "–"} />
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
            <ConfirmButton label="Run now" disabled={!coll || !!refusals?.ingestion} title={`Run ${coll} now?`}
              message={`This starts one run of the Databricks job for ${coll}, on its own job cluster, with parallelism ${par}. The cluster and the source's load cost money while it runs.`}
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
                <ConfirmButton label="Cancel" danger confirmLabel="Cancel run" title={`Cancel run ${p.value}?`}
                  message="The run stops; tables it finished keep their loads, and the next run carries on from there." disabled={!!refusals?.ingestion}
                  onConfirm={() => post.mutateAsync({ path: `/jobs/runs/${p.value}/cancel` })} />
              ),
            }]} />
        ) : <p className="muted">No active runs.</p>}
      </Section>
      <Section title="Recent runs">
        <Grid rows={job.data?.recent ?? (job.error ? [] : undefined)} fillReserve={150} columns={autoColumns(job.data?.recent ?? [], stateCols)} />
      </Section>
      <Section title="Schema setup">
        {config && <p>State database: Lakebase instance <span className="mono">{config.lakebase_instance}</span>, database{" "}
          <span className="mono">{config.lakebase_database}</span>.</p>}
        <p className="muted small">Creates or migrates the Lakebase schema. Safe to re-run; upgrades also migrate on the next
          ingestion run.</p>
        <ConfirmButton label="Run setup job" plain disabled={!!refusals?.setup} title="Run the setup job?"
          message="It creates or migrates the state database's schema. It is safe to run again." confirmLabel="Run setup job" onConfirm={() => post.mutateAsync({ path: "/jobs/setup/run" })} />
        {refusals?.setup && <p className="muted small">Run setup job: {refusals.setup}.</p>}
      </Section>
    </>
  );
}
