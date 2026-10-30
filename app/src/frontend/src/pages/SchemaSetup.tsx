import { useEffect, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { api, type Row } from "../api";
import { ConfirmButton, ErrorBox, Notice, OutLink, Section } from "../components";
import { usePermissions } from "../permissions";
import { useSettings } from "../App";
import { setupRunBlocks, setupRunNotice, type SetupRunOutcome } from "../pending";

/** Schema setup: the state database's name and a button that runs the setup job, which creates or migrates its
 * schema, with the run's outcome. */
export default function SchemaSetup() {
  const [setupRunId, setSetupRunId] = useState<number | null>(null);
  // A setup run Databricks lists going (read from the run table) is this page's run too: a page reloaded during it keeps
  // the button off, and shows the run's outcome once it ends.
  const activeSetup = useQuery({
    queryKey: ["setup-run-active"],
    queryFn: () => api.get<{ run_id: number | null }>("/jobs/setup/runs/active"),
    refetchInterval: 15_000,
  });
  useEffect(() => { if (activeSetup.data?.run_id != null) setSetupRunId(Number(activeSetup.data.run_id)); }, [activeSetup.data]);
  const runSetup = useMutation({
    mutationFn: () => api.post<Row>("/jobs/setup/run"),
    onSuccess: (r) => setSetupRunId(Number(r.run_id)),
  });
  const setupRun = useQuery({
    queryKey: ["setup-run", setupRunId],
    queryFn: () => api.get<{ run_id: number; outcome: SetupRunOutcome }>(`/jobs/setup/runs/${setupRunId}`),
    enabled: setupRunId != null,
    refetchInterval: (q) => {
      const state = (q.state.data as { outcome?: SetupRunOutcome } | undefined)?.outcome?.state;
      return state === "pending" ? 5_000 : state === "timeout" ? 30_000 : false;
    },
  });
  const setupOutcome = setupRun.data?.outcome;
  const setupStarting = runSetup.isPending || (setupRunId != null && setupRun.isPending);
  const setupNote = setupRunNotice(setupOutcome, { starting: setupStarting, error: runSetup.error });
  const refusals = usePermissions().data?.run;
  const { config } = useSettings();
  return (
    <Section title="Schema setup">
      {config && <p>State database: Lakebase instance <span className="mono">{config.lakebase_instance}</span>, database{" "}
        <span className="mono">{config.lakebase_database}</span>.</p>}
      <p className="muted small">Creates or migrates the Lakebase schema. Safe to re-run; upgrades also migrate on the next
        ingestion run.</p>
      <ConfirmButton label="Run setup job" plain disabled={!!refusals?.setup || setupRunBlocks(setupOutcome, setupStarting)}
        title="Run the setup job?" message="It creates or migrates the state database's schema. It is safe to run again."
        confirmLabel="Run setup job" onConfirm={() => runSetup.mutateAsync()} />
      {refusals?.setup && <p className="muted small">Run setup job: {refusals.setup}.</p>}
      {setupNote && <Notice tone={setupNote.tone}>{setupNote.text}{setupOutcome?.url && <> <OutLink href={setupOutcome.url}>View run</OutLink></>}</Notice>}
      <ErrorBox error={setupRun.error} />
    </Section>
  );
}
