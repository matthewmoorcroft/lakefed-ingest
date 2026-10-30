import { Fragment, useEffect, useState } from "react";
import { keepPreviousData, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, qs, type Row } from "../api";
import { ErrorBox, Modal, Notice, OutLink, Pager, Tag, Tip, fmtInt, plural, Twisty } from "../components";
import { useSettings } from "../App";
import { DatabaseTables } from "./SourcePick";
import { accessOf, countsText, likeContaining, readable, selectionCounts, serverRowPick, underFilter, type Pick, type ServerCount } from "./pickState";
import { connectionNextBlock, discoveryWait, readsProgress, warehouseReadsState, warehouseRunState, waitingSince, type RunWait } from "../pending";

// Add tables, Discover on a source: as Lakeflow Connect's Connection and Source steps. Step 1 is the Unity Catalog
// connection, tested before anything is discovered; step 2 picks the whole server, whole databases or single tables in
// a tree, a row chosen in part showing a filled square. The tables found stay in Lakebase (discovered_table): a
// database's tables are shown a page at a time when it is opened, and what is picked is handed to New and Edit by
// reference (the discoveries, and the whole server, whole databases or tables chosen), never as the tables themselves.

export type { Pick };
const RUNNING = new Set(["PENDING", "QUEUED", "RUNNING", "TERMINATING", "BLOCKED", "WAITING_FOR_RETRY"]);
const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** A discovery run the dialog waits on: its state as last read, when the page started it, and what it does once going. */
type DialogWait = { wait: RunWait | null; asked: Date; reading: string };

/** Now, read again every second while `on`, so a wait's time so far keeps counting on the page. */
function useNow(on: boolean): Date {
  const [now, setNow] = useState(() => new Date());
  useEffect(() => {
    if (!on) return;
    setNow(new Date());
    const timer = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(timer);
  }, [on]);
  return now;
}

/** A discovery that has ended: its id, whether it ran through the SQL warehouse, and for a whole-server read there how
 *  many databases it left out because the login can't open them. */
type Discovered = { id: string; via: string; leftOut: number };

/** Start a discovery through the connection and wait for it to end; its id, or the reason it failed. onWait hears
 *  each change of its state with when the page first saw it, so the page can say what it waits for; onProgress hears
 *  each step of a read through the warehouse (its databases read so far). */
async function discover(connection: string, request: Row, onWait?: (wait: RunWait) => void,
                        onProgress?: (answer: Row) => void): Promise<Discovered> {
  const started = await api.post<Row>("/discoveries/source", { src_connection: connection, ...request });
  let waiting: RunWait | null = null;
  if (started.via === "warehouse") {
    // Through the SQL warehouse: a listing is one statement, asked about until it ends; a read is a step at a time
    // (each database one statement), asked for until none is left.
    const id = String(started.discovery_id);
    let answer = started;
    for (;;) {
      if (answer.state === "SUCCEEDED") return { id, via: "warehouse", leftOut: Number(started.left_out ?? 0) };
      if (answer.state === "FAILED" || answer.error) throw new Error(String(answer.error ?? "the listing failed").split("\n")[0]);
      const listing = answer.statement_id != null;
      const next = waitingSince(waiting, listing ? warehouseRunState(answer) : warehouseReadsState(answer), new Date());
      if (next !== waiting) { waiting = next; onWait?.(next); }
      if (!listing) onProgress?.(answer);
      await wait(1000);
      answer = listing
        ? await api.get<Row>(`/discoveries/${encodeURIComponent(id)}/warehouse/${encodeURIComponent(answer.statement_id)}${qs({ connection })}`)
        : await api.get<Row>(`/discoveries/${encodeURIComponent(id)}/reads${qs({ connection })}`);
    }
  }
  for (;;) {
    const run = await api.get<Row>(`/discoveries/runs/${started.run_id}`);
    const next = waitingSince(waiting, String(run.state ?? ""), new Date());
    if (next !== waiting) { waiting = next; onWait?.(next); }
    if (!RUNNING.has(run.state)) {
      if (run.result && run.result !== "SUCCESS") {
        const rows = await api.get<Row[]>(`/discoveries/${started.discovery_id}/databases`);
        const failed = rows.find((r) => r.status === "failed");
        throw new Error(failed?.error ? String(failed.error).split("\n")[0] : run.message || `the discovery run ended ${run.result}`);
      }
      return { id: String(started.discovery_id), via: "job", leftOut: 0 };
    }
    await wait(3000);
  }
}

/** Read the tables of some databases into Lakebase: the discovery's id, and each database that couldn't be read with
 *  the first line of why. A database the login can name but not open fails on its own; the others are still read. */
async function readTables(connection: string, request: Row, onWait?: (wait: RunWait) => void, onProgress?: (answer: Row) => void):
    Promise<{ id: string; failed: [string, string][]; leftOut: number }> {
  const { id, leftOut } = await discover(connection, request, onWait, onProgress);
  const rows = await api.get<Row[]>(`/discoveries/${id}/databases`);
  return { id, leftOut,
           failed: rows.filter((r) => r.status === "failed").map((r) => [r.src_database, String(r.error ?? "").split("\n")[0]]) };
}

/** The databases that couldn't be read, and why: often that the login can see the name but not open the database. */
function Unread({ failed, reading, onReadAgain }: { failed: [string, string][]; reading: boolean; onReadAgain: () => void }) {
  if (!failed.length) return null;
  const one = failed.length === 1;
  const named = failed.slice(0, 3).map(([d]) => d).join(", ");
  return <Notice tone="warn"><p>{fmtInt(failed.length)} {plural(failed.length, "database")} couldn't be read ({named}
    {failed.length > 3 ? `, and ${fmtInt(failed.length - 3)} more` : ""}), so none of {one ? "its" : "their"} tables are added.
    The login can see {one ? "its name" : "their names"} on the server but may not open {one ? "it" : "them"}: untick{" "}
    {one ? "it" : "them"}, or give the login access and read {one ? "it" : "them"} again.</p>
    <div className="toolbar"><button disabled={reading} onClick={onReadAgain}>{reading ? "Reading again…" : "Read them again"}</button></div>
    <details className="unread-all"><summary>Show all {fmtInt(failed.length)}, with why</summary>
      <ul>{failed.map(([d, why]) => <li key={d}><b>{d}</b>: {why}</li>)}</ul></details></Notice>;
}

/** A tick box with a third state: a filled square for a row chosen in part, a tick only when all of it is. */
export function TriBox({ pick, label, onClick }: { pick: Pick; label: string; onClick: () => void }) {
  return <button type="button" className={`tri-box ${pick}`} role="checkbox" aria-checked={pick === "all" ? true : pick === "part" ? "mixed" : false}
    aria-label={label} title={pick === "part" ? "Chosen in part" : undefined} onClick={onClick}>{pick === "all" ? "✓" : ""}</button>;
}

/** A test that failed because the login can't open master, where SQL Server lists its databases. */
const cantList = (error: unknown) => /FAILED_JDBC\.CONNECTION|Login failed|Cannot open database|HV000/i.test(String((error as Error)?.message ?? error));

/** The databases typed by hand, for a login that can't list the server's. */
function TypedDatabases({ onUse }: { onUse: (names: string[]) => void }) {
  const [text, setText] = useState("");
  const names = [...new Set(text.split(/[\s,;]+/).map((n) => n.trim()).filter(Boolean))];
  return (
    <div className="form-grid">
      <label className="wide">Its databases<textarea rows={4} value={text} placeholder={"lfc_eng_001\nlfc_eng_002"}
          onChange={(e) => setText(e.target.value)} />
        <span className="help">Type them one per line or separated by commas. Each is read on its own in the next step.</span></label>
      <div className="toolbar"><button disabled={!names.length} onClick={() => onUse(names)}>
        Use {names.length ? `these ${fmtInt(names.length)} ${plural(names.length, "database")}` : "these databases"}</button></div>
    </div>
  );
}

function Step1({ chosen, setChosen, tested, setTested, waiting, setWaiting }: { chosen: string; setChosen: (c: string) => void;
                 tested: Row | null; setTested: (t: Row | null) => void; waiting: string | null;
                 setWaiting: (wait: DialogWait | null) => void }) {
  const { config } = useSettings();
  const [how, setHow] = useState<"existing" | "create">("existing");
  const [search, setSearch] = useState("");
  const [testing, setTesting] = useState(false);
  // For an Azure SQL server: a service credential with Reader on it and its resource id, so the discovery job lists
  // the databases through Azure instead of master. Both or neither.
  const [azureCredential, setAzureCredential] = useState("");
  const [azureResource, setAzureResource] = useState("");
  const azure: Row = azureCredential.trim() && azureResource.trim()
    ? { azure_credential: azureCredential.trim(), azure_resource_id: azureResource.trim() } : {};
  const [error, setError] = useState<unknown>(null);
  const qc = useQueryClient();
  const list = useQuery({ queryKey: ["connections"], queryFn: () => api.get<Row>("/connections") });
  const [refreshing, setRefreshing] = useState(false);
  // Lists the connections again now, so one just made in Catalog Explorer shows up without leaving the form.
  const refresh = async () => {
    setRefreshing(true);
    try { qc.setQueryData(["connections"], await api.get<Row>("/connections?fresh=true")); }
    finally { setRefreshing(false); }
  };
  const found: Row[] = (list.data?.connections ?? []).filter((c: Row) => c.discoverable && c.name.toLowerCase().includes(search.toLowerCase()));
  const test = async () => {
    setTesting(true); setError(null); setTested(null);
    const asked = new Date();
    const reading = "Reading the server's databases";
    setWaiting({ wait: null, asked, reading });
    try {
      const { id, via } = await discover(chosen, { kind: "list", ...azure }, (wait) => setWaiting({ wait, asked, reading }));
      const rows = await api.get<Row[]>(`/discoveries/${id}/databases`);
      setTested({ discovery_id: id, via, databases: rows.filter((r) => r.status === "listed").map((r) => r.src_database), azure });
    } catch (e) { setError(e); } finally { setTesting(false); setWaiting(null); }
  };
  return (
    <>
      <p className="muted">The Unity Catalog connection the tables are read through. It is tested before anything is discovered, so a
        source that cannot be reached is found here, not in the first run.</p>
      <label className="radio-row"><input type="radio" checked={how === "existing"} onChange={() => setHow("existing")} /> Use an existing connection</label>
      {how === "existing" && (
        <div className="radio-body">
          <div className="toolbar"><input className="search" placeholder="Search connections" value={search} onChange={(e) => setSearch(e.target.value)} />
            <button className="ghost" disabled={refreshing} onClick={refresh}>{refreshing ? "Refreshing…" : "Refresh"}</button>
            <span className="muted">A connection made in Catalog Explorer shows here after Refresh, once this app has USE CONNECTION on it.</span></div>
          <ErrorBox error={list.error} />
          {list.data?.error && <Notice tone="warn">The app can't list connections: {list.data.error}</Notice>}
          <div className="table-wrap"><table className="data">
            <thead><tr><th></th><th>Name</th><th>Type</th><th>Host</th><th>Owner</th></tr></thead>
            <tbody>
              {found.map((c) => (
                <tr key={c.name} className="pick-row" onClick={() => { setChosen(c.name); setTested(null); }}>
                  <td><input type="radio" checked={chosen === c.name} readOnly aria-label={`Use ${c.name}`} /></td>
                  <td className="name"><b>{c.name}</b></td><td>{c.connection_type}</td><td className="mono">{c.host}</td><td>{c.owner}</td>
                </tr>
              ))}
              {list.data && !found.length && <tr><td colSpan={5} className="muted">No SQL Server connection {search ? "matches" : "the app can see"}.
                {!search && <> The app sees only connections its service principal{config?.app_principal ? <> (<span className="mono">{config.app_principal}</span>)</> : ""} has
                  USE CONNECTION on: the connection's owner grants it in Catalog Explorer, Connections, the connection, Permissions. Then Refresh.</>}</td></tr>}
            </tbody>
          </table></div>
          <div className="toolbar">
            <button disabled={!chosen || testing} onClick={test}>{testing ? "Testing…" : "Test connection"}</button>
            {!testing && !tested && <span className="muted">Lists every database on the server; you filter them on the next step.</span>}
            {tested && <span><Tag tone="ok">{tested.typed ? "Typed" : "Connected"}</Tag> <span className="muted">{tested.typed ? "uses" : "reads"}{" "}
              {fmtInt(tested.databases.length)} {plural(tested.databases.length, "database")} through {chosen}</span></span>}
          </div>
          {error != null && <Notice tone="bad">The connection failed its test: {String((error as Error).message ?? error)}
            {cantList(error) && <p>The server's databases are listed from its master database, and this connection's login can't open
              it, so they can't be listed here. The databases themselves may still be readable through the connection: type them below.
              Or give the login access to
              master (a user there with VIEW ANY DATABASE) and test again. On Azure SQL, Azure can list them instead: see
              Azure SQL below.</p>}</Notice>}
          <details className="advanced">
            <summary><b>Azure SQL</b> <span className="muted">list the databases through Azure instead of master
              {azure.azure_credential ? <>: on, with {azure.azure_credential}</> : ", for a login that can't open master"}</span></summary>
            <p className="muted">The discovery job asks Azure for the server's databases with a Unity Catalog service credential
              (Catalog Explorer, External data, Credentials) whose managed identity has Reader on the server, then reads each
              database through the connection. Whoever runs the discovery job needs ACCESS on the credential. Azure SQL Database
              and Managed Instance only.</p>
            <div className="toolbar">
              <label className="inline">Service credential<input className="search" placeholder="e.g. sql_reader" value={azureCredential}
                onChange={(e) => { setAzureCredential(e.target.value); setTested(null); }} /></label>
              <label className="inline">Server resource id<input className="search" size={60}
                placeholder="/subscriptions/…/resourceGroups/…/providers/Microsoft.Sql/servers/…" value={azureResource}
                onChange={(e) => { setAzureResource(e.target.value); setTested(null); }} /></label>
            </div>
            {Boolean(azureCredential.trim()) !== Boolean(azureResource.trim()) &&
              <span className="muted">Both are needed; with one, the databases are listed through master.</span>}
          </details>
          {error != null && cantList(error) && <TypedDatabases onUse={(names) => { setError(null); setTested({ discovery_id: null, databases: names, typed: true }); }} />}
        </div>
      )}
      <label className="radio-row"><input type="radio" checked={how === "create"} onChange={() => setHow("create")} /> Create a connection</label>
      {how === "create" && (
        <div className="radio-body">
          <p>A connection is made in Catalog Explorer, with its name, type, host, port, user and password, and needs CREATE
            CONNECTION on the metastore. Make it there, give this app USE CONNECTION on it, then come back and choose it above.</p>
          <div className="toolbar">
            {config?.workspace_url && <OutLink href={`${config.workspace_url}/explore/connections`}>Open Connections in Catalog Explorer</OutLink>}
            <button className="ghost" disabled={refreshing} onClick={() => { refresh(); setHow("existing"); }}>I made it: refresh the list</button>
          </div>
        </div>
      )}
    </>
  );
}

type Picks = Record<string, Pick | string[]>;
/** The whole server, as a flag: on, and the filter it was ticked under (it covers the databases whose names contain it). */
type WholeServer = { on: boolean; filter: string };
const DATABASES_PER_PAGE = 100;
/** Whether a whole-server tick covers a database: the same rule as SourcePick's, so the two pages never disagree. */
const coveredByServer = (server: WholeServer, database: string) => server.on && underFilter(database, server.filter);

/** What a choice takes, as Step 2 counts it: shown at its top, in its last line and in the job's Chosen summary. */
type Counts = ReturnType<typeof selectionCounts> & { listed: number };

function Step2({ connection, discoveryId, checksOn, typedNames, picks, setPicks, wholeServer, setWholeServer, read, setRead, failed,
                 setFailed, onCounts }: {
  connection: string; discoveryId: string | null; checksOn: boolean; typedNames: string[]; picks: Picks;
  setPicks: (update: (p: Picks) => Picks) => void; wholeServer: WholeServer; setWholeServer: (next: WholeServer) => void;
  read: Record<string, string>; setRead: (update: (t: Record<string, string>) => Record<string, string>) => void;
  failed: [string, string][]; setFailed: (update: (f: [string, string][]) => [string, string][]) => void;
  onCounts: (counts: Counts) => void }) {
  const [filter, setFilter] = useState("");
  const [offset, setOffset] = useState(0);
  const [open, setOpen] = useState<string | null>(null);
  const [reading, setReading] = useState<string | null>(null);
  const [error, setError] = useState<unknown>(null);
  // A discovery's databases come a page at a time from Lakebase, filtered there; typed names are a short list kept here.
  const listing = useQuery({
    queryKey: ["discovery-listing", discoveryId, filter, offset], placeholderData: keepPreviousData, enabled: discoveryId != null,
    queryFn: () => api.get<Row>(`/discoveries/${encodeURIComponent(String(discoveryId))}/listing${qs({ filter, offset, limit: DATABASES_PER_PAGE })}`),
  });
  const typedShown = typedNames.filter((d) => d.toLowerCase().includes(filter.toLowerCase()));
  const shown: string[] = discoveryId != null ? (listing.data?.databases ?? []).map((d: Row) => d.src_database) : typedShown;
  const total = discoveryId != null ? Number(listing.data?.total ?? 0) : typedNames.length;
  // Each listed database's check (can the login open it, and its tables), as the listing pages have shown them.
  const [seen, setSeen] = useState<Record<string, Row>>({});
  useEffect(() => {
    const rows: Row[] = listing.data?.databases ?? [];
    if (rows.length) setSeen((now) => ({ ...now, ...Object.fromEntries(rows.map((r) => [r.src_database, r])) }));
    // A database found closed after it was picked leaves the choice, so nothing reads it.
    const shut = rows.filter((r) => accessOf(r) === "closed").map((r) => String(r.src_database));
    if (shut.some((d) => picks[d] !== undefined)) {
      setPicks((now) => Object.fromEntries(Object.entries(now).filter(([d]) => !shut.includes(d))));
    }
  }, [listing.data]);
  const closed = (d: string) => accessOf(seen[d]) === "closed";
  // Through the SQL warehouse, every listed database is checked: the page shown first, then the rest while this step is
  // open. The answers stay on the listing in Lakebase, so a database is checked once.
  const qc = useQueryClient();
  const [checking, setChecking] = useState(false);
  const shownKey = shown.join("\n");
  useEffect(() => {
    if (!checksOn || discoveryId == null) return;
    let stopped = false;
    (async () => {
      setChecking(true);
      try {
        for (;;) {
          const step = await api.post<Row>(`/discoveries/${encodeURIComponent(discoveryId)}/access${qs({ connection })}`,
                                           { first: shownKey ? shownKey.split("\n") : [] });
          if (stopped) return;
          await qc.invalidateQueries({ queryKey: ["discovery-listing", discoveryId] });
          if (step.error) throw new Error(String(step.error));
          if (!step.pending) return;
          await wait(1500);
          if (stopped) return;
        }
      } catch (e) { if (!stopped) setError(e); } finally { if (!stopped) setChecking(false); }
    })();
    return () => { stopped = true; };
  }, [checksOn, discoveryId, connection, shownKey, qc]);
  // The whole server's part of the counts: the databases its filter keeps, less those the login can't open.
  const serverTotals = useQuery({
    queryKey: ["discovery-listing", discoveryId, wholeServer.filter, "totals"], placeholderData: keepPreviousData,
    enabled: discoveryId != null && wholeServer.on,
    queryFn: () => api.get<Row>(`/discoveries/${encodeURIComponent(String(discoveryId))}/listing${qs({ filter: wholeServer.filter, offset: 0, limit: 1 })}`),
  });
  const covered = (d: string) => coveredByServer(wholeServer, d) && !closed(d);
  const pickIn = (all: Picks, d: string): Pick => {
    const p = all[d];
    if (Array.isArray(p)) return p.length === 0 ? "none" : "part";
    return p ?? "none";
  };
  const pickOf = (d: string): Pick => (covered(d) ? "all" : pickIn(picks, d));
  const typedServer: Pick = typedNames.length && typedNames.every((d) => pickOf(d) === "all") ? "all"
    : typedNames.some((d) => pickOf(d) !== "none") ? "part" : "none";
  const anyChosen = Object.values(picks).some((p) => (Array.isArray(p) ? p.length > 0 : p === "all" || p === "part"));
  const server: Pick = discoveryId == null ? typedServer : serverRowPick(wholeServer, filter, anyChosen);
  // Each change starts from the choices as they are then, so quick clicks never undo each other. A database the login
  // can't open is never chosen.
  const setAll = (names: string[], pick: Pick) => setPicks((now) => ({
    ...now, ...Object.fromEntries(names.filter((d) => pick === "none" || !closed(d)).map((d) => [d, pick])) }));
  const access = serverTotals.data?.access;
  const serverCount: ServerCount | null = discoveryId != null && wholeServer.on
    ? { matched: Number(serverTotals.data?.matched ?? total), closed: Number(access?.matched_closed ?? 0),
        unknown: checksOn ? Number(access?.matched_unknown ?? 0) : 1, tables: Number(access?.matched_tables ?? 0) }
    : null;
  const wholeNames = Object.keys(picks).filter((d) => picks[d] === "all" && !covered(d));
  const tablesOf = (d: string): number | null => (accessOf(seen[d]) === "open" && seen[d]?.tables != null ? Number(seen[d].tables) : null);
  const counts: Counts = {
    ...selectionCounts(serverCount, wholeNames.map(tablesOf),
                       Object.entries(picks).filter(([d, p]) => Array.isArray(p) && p.length > 0 && !covered(d)).map(([, p]) => (p as string[]).length)),
    listed: total };
  const countsKey = JSON.stringify(counts);
  useEffect(() => { onCounts(JSON.parse(countsKey)); }, [countsKey]);
  // A whole-server tick takes over what it covers: the picks of the databases matching its filter go. A discovery keeps
  // just the flag; typed names are each ticked, as before.
  const tickServer = () => {
    if (discoveryId == null) return setAll(typedNames, server === "all" ? "none" : "all");
    const turningOff = server === "all";
    setWholeServer({ on: !turningOff, filter });
    if (!turningOff) setPicks((now) => Object.fromEntries(Object.entries(now).filter(([d]) => !underFilter(d, filter))));
  };
  const openDatabase = async (d: string) => {
    if (open === d) return setOpen(null);
    if (!readable([d], seen).length) return;
    setOpen(d);
    if (read[d]) return;
    setReading(d); setError(null);
    try {
      const got = await readTables(connection, { databases: [d] });
      if (got.failed.length) setFailed((now) => [...now.filter(([name]) => name !== d), ...got.failed]);
      else setRead((now) => ({ ...now, [d]: got.id }));
    } catch (e) { setError(e); } finally { setReading(null); }
  };
  // Read the databases that failed once more, by name: the ones that open now are kept, the rest stay listed.
  const [readingAgain, setReadingAgain] = useState(false);
  const readAgain = async () => {
    const names = failed.map(([d]) => d);
    setReadingAgain(true); setError(null);
    try {
      const got = await readTables(connection, { databases: names });
      const still = new Set(got.failed.map(([d]) => d));
      setRead((now) => ({ ...now, ...Object.fromEntries(names.filter((d) => !still.has(d)).map((d) => [d, got.id])) }));
      setFailed(() => got.failed);
    } catch (e) { setError(e); } finally { setReadingAgain(false); }
  };
  const toggleTable = (d: string, key: string) => setPicks((now) => {
    const chosen = Array.isArray(now[d]) ? (now[d] as string[]) : [];
    return { ...now, [d]: chosen.includes(key) ? chosen.filter((k) => k !== key) : [...chosen, key] };
  });
  return (
    <>
      <p className="muted">The connection passed its test. Tick the whole server, whole databases, or open a database for its tables.
        {checksOn ? " Each database is checked through the SQL warehouse: one the connection's login can't open is greyed out, and"
          + " hovering on it says why." : " Reading a database's catalog takes a few minutes when the discovery cluster starts cold."}</p>
      <p className="pick-counts"><b>{countsText(counts, total)}</b>
        {counts.leftOut > 0 && <span className="muted"> · the whole server leaves out {fmtInt(counts.leftOut)} {plural(counts.leftOut, "database")} the
          login can't open</span>}
        {checking && <span className="muted"> · checking which databases the login can open</span>}</p>
      <div className="toolbar"><input className="search" placeholder="Filter databases, e.g. eng_0" value={filter}
        onChange={(e) => { setFilter(e.target.value); setOffset(0); }} />
        <button className="ghost" onClick={() => setAll(shown.filter((d) => !covered(d) && !closed(d)), "all")}>Select all shown</button></div>
      <div className="table-wrap tree-pick"><table className="data tree">
        <thead><tr><th>Server › database › schema.table</th><th className="num">Tables</th><th>Load type</th></tr></thead>
        <tbody>
          <tr><td className="name"><TriBox pick={server} label={`The whole server ${connection}`} onClick={tickServer} />{" "}
            <b>{connection}</b>, {discoveryId != null && wholeServer.on && wholeServer.filter
              ? <>every database matching "{wholeServer.filter}"</> : "the whole server"}</td>
            <td className="num">{fmtInt(total)} {plural(total, "database")}</td>
            <td className="muted">{server === "part" ? "chosen in part" : ""}</td></tr>
          {shown.slice(0, 500).map((d) => (
            <Fragment key={d}>
              <tr className={`l2${closed(d) ? " closed" : ""}`}><td className="name" style={{ paddingLeft: 30 }}>
                <TriBox pick={pickOf(d)} label={`The whole database ${d}`} onClick={() => !covered(d) && setAll([d], pickOf(d) === "all" ? "none" : "all")} />{" "}
                <Twisty open={open === d} onToggle={() => openDatabase(d)}
                  closed={closed(d) ? `${d} can't be opened: ${String(seen[d]?.why ?? "the connection's login can't open this database")}` : undefined}>{d}</Twisty></td>
                <td className="num">{reading === d ? "reading…" : ""}</td>
                <td className="muted">{closed(d) ? <Tip text={String(seen[d]?.why ?? "the connection's login can't open this database")} focusable>
                  can't open: hover for why</Tip>
                  : pickOf(d) === "part" ? `${fmtInt((picks[d] as string[]).length)} chosen`
                  : tablesOf(d) != null ? `${fmtInt(tablesOf(d))} ${plural(tablesOf(d), "table")}`
                  : checksOn && accessOf(seen[d]) === "unknown" ? "checking…" : ""}</td></tr>
              {open === d && read[d] && <DatabaseTables kind="discovery" sourceId={read[d]} server={connection} database={d}
                whole={pickOf(d) === "all"} chosen={Array.isArray(picks[d]) ? (picks[d] as string[]) : []} onToggle={(key) => toggleTable(d, key)} />}
            </Fragment>
          ))}
          {discoveryId == null && shown.length > 500 && <tr className="l2"><td colSpan={3} className="muted" style={{ paddingLeft: 30 }}>
            and {fmtInt(shown.length - 500)} more: filter to find one, or tick the whole server</td></tr>}
          {discoveryId == null && typedNames.length > shown.length && <tr className="l2"><td colSpan={3} className="muted" style={{ paddingLeft: 30 }}>
            {fmtInt(typedNames.length - shown.length)} more {plural(typedNames.length - shown.length, "database")} hidden by the filter</td></tr>}
        </tbody>
      </table></div>
      {discoveryId != null && <Pager offset={offset} limit={DATABASES_PER_PAGE} total={Number(listing.data?.matched ?? 0)} noun="database"
        onPage={setOffset} />}
      <Unread failed={failed} reading={readingAgain} onReadAgain={readAgain} />
      <ErrorBox error={listing.error ?? error} />
    </>
  );
}

/** What Discover hands to New and Edit: the discoveries the tables are in, and what of them was chosen (main.Selection). */
export type DiscoveredPick = { source: Row; label: string };

/** Discover on a source: the two steps, then what was chosen by reference (onClose(pick)), or nothing on Cancel. */
export function DiscoverDialog({ onClose }: { onClose: (pick: DiscoveredPick | null) => void }) {
  const [step, setStep] = useState<1 | 2 | 3>(1);
  const [connection, setConnection] = useState("");
  const [tested, setTested] = useState<Row | null>(null);
  const [picks, setPicks] = useState<Picks>({});
  const [wholeServer, setWholeServer] = useState<WholeServer>({ on: false, filter: "" });
  // The whole server's discovery, read once (under its filter, through Azure if tested so); its databases aren't listed here.
  const [serverRead, setServerRead] = useState<string | null>(null);
  const [read, setRead] = useState<Record<string, string>>({});
  const [failed, setFailed] = useState<[string, string][]>([]);
  const [error, setError] = useState<unknown>(null);
  // What a discovery run the dialog waits on is doing, and since when: the connection test, or the reading on Add.
  const [waiting, setWaiting] = useState<DialogWait | null>(null);
  const now = useNow(waiting !== null);
  const waitText = waiting ? discoveryWait(waiting.wait, waiting.asked, waiting.reading, now) : null;
  const discoveryId: string | null = tested?.discovery_id ?? null;
  const throughWarehouse = tested?.via === "warehouse";
  const [counts, setCounts] = useState<Counts | null>(null);
  const countsLine = counts ? countsText(counts, counts.listed) : "";
  const typedNames: string[] = discoveryId == null ? (tested?.databases ?? []) : [];
  const whole = Object.keys(picks).filter((d) => picks[d] === "all" && !coveredByServer(wholeServer, d));
  const handPicked = Object.entries(picks).filter(([d, p]) => Array.isArray(p) && p.length > 0 && !coveredByServer(wholeServer, d)) as [string, string[]][];
  const chosen = (wholeServer.on ? 1 : 0) + whole.length + handPicked.length;
  const chosenWords = [wholeServer.on ? (wholeServer.filter ? `every database matching "${wholeServer.filter}"` : "the whole server") : "",
    whole.length ? `${fmtInt(whole.length)} whole ${plural(whole.length, "database")}` : "",
    handPicked.length ? `tables from ${fmtInt(handPicked.length)} more` : ""].filter(Boolean).join(", ");
  // Databases chosen whole whose tables aren't read yet are discovered on Add. The whole server is read once, only the
  // databases its filter keeps (likeContaining), and the filter also goes with the job's selection. The databases picked
  // one by one outside that filter are read by name.
  const finish = async () => {
    setStep(3); setError(null);
    const asked = new Date();
    const reading = "Reading the chosen databases' tables";
    const onWait = (wait: RunWait) => setWaiting((now) => ({ wait, asked, reading: now?.reading ?? reading }));
    const onProgress = (answer: Row) => setWaiting((now) => now && { ...now, reading: readsProgress(reading, answer) });
    setWaiting({ wait: null, asked, reading });
    try {
      let readServer = serverRead;
      if (wholeServer.on && !readServer) {
        // Through the warehouse, the whole server is the test's listing under the filter, less the databases the login
        // can't open; through the discovery job, it is listed again under the filter's LIKE pattern.
        const pattern = likeContaining(wholeServer.filter);
        const request = throughWarehouse ? { whole_server: true, listing_id: discoveryId, filter: wholeServer.filter }
          : { whole_server: true, ...(pattern ? { pattern } : {}), ...(tested?.azure ?? {}) };
        const got = await readTables(connection, request, onWait, onProgress);
        if (got.failed.length) { setFailed(() => got.failed); setStep(2); return; }
        readServer = got.id; setServerRead(got.id);
      }
      const unread = whole.filter((d) => !read[d]);
      const ids = new Set([...(readServer ? [readServer] : []), ...Object.values(read)]);
      if (unread.length) {
        // Databases typed by hand are read by name.
        const got = await readTables(connection, { databases: unread }, onWait, onProgress);
        ids.add(got.id);
        if (got.failed.length) {
          // Keep what was read, and say which databases weren't, before anything is added.
          const bad = new Set(got.failed.map(([d]) => d));
          setRead((now) => ({ ...now, ...Object.fromEntries(unread.filter((d) => !bad.has(d)).map((d) => [d, got.id])) }));
          setFailed(() => got.failed); setStep(2);
          return;
        }
      }
      const tables = handPicked.map(([d, keys]) => ({ server: connection, database: d, tables: keys }));
      const source = { discovery_ids: [...ids], whole_server: wholeServer.on, filter: wholeServer.on ? wholeServer.filter : "",
                       databases: whole.map((d) => ({ server: connection, database: d })), tables };
      const label = (wholeServer.on ? `${connection}, ${chosenWords}` : `${chosenWords} on ${connection}`)
        + (counts ? ` (${countsLine.replace(/^Chosen: /, "")})` : "");
      onClose({ source, label });
    } catch (e) { setError(e); setStep(2); } finally { setWaiting(null); }
  };
  const title = step === 1 ? "Add tables · Step 1 of 2: the connection" : `Add tables · Step 2 of 2: choose on ${connection}`;
  const blocked = step === 1 ? connectionNextBlock(Boolean(tested), waitText)
    : step === 3 ? waitText ?? "Reading the chosen databases' tables" : chosen ? undefined : "Choose the server, databases or tables first";
  return (
    <Modal title={title} action={step === 1 ? "Next: choose tables" : step === 3 ? "Reading…" : "Add the tables chosen"}
      actionDisabled={blocked} onClose={(ok) => {
        if (!ok) return onClose(null);
        if (step === 1) return setStep(2);
        finish();
      }}>
      <div className="discover">
        {step === 1 && <p className="muted">Discover is the first choice: it reads the tables through the connection. A CSV upload
          is the fallback, for a list Discover can't find.</p>}
        {step === 1 && <Step1 chosen={connection} setChosen={setConnection} tested={tested} setTested={setTested}
          waiting={waitText} setWaiting={setWaiting} />}
        {step >= 2 && <Step2 connection={connection} discoveryId={discoveryId} checksOn={throughWarehouse} typedNames={typedNames}
          picks={picks} setPicks={setPicks} wholeServer={wholeServer} setWholeServer={setWholeServer} read={read} setRead={setRead}
          failed={failed} setFailed={setFailed} onCounts={setCounts} />}
        {step === 3 && <p className="muted">The tables are added to the job's page when the reading ends.</p>}
        {step === 2 && chosen > 0 && <p className="muted">{chosenWords} chosen. {countsLine}.</p>}
        <ErrorBox error={error} />
      </div>
    </Modal>
  );
}
