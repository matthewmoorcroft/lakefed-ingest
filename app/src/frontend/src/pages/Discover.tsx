import { Fragment, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api, type Row } from "../api";
import { ErrorBox, Modal, Notice, OutLink, Tag, fmtInt, plural } from "../components";
import { useSettings } from "../App";

// Add tables, Discover on a source: as Lakeflow Connect's Connection and Source steps. Step 1 is the Unity Catalog
// connection, tested before anything is discovered; step 2 picks the whole server, whole databases or single tables in
// a tree, a row chosen in part showing a filled square. What is picked becomes the lines New and Edit take
// (src_connection, database, schema, table, columns, key and the load type discovery recommends).

type Pick = "all" | "part" | "none";
const RUNNING = new Set(["PENDING", "QUEUED", "RUNNING", "TERMINATING", "BLOCKED", "WAITING_FOR_RETRY"]);
const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Start a discovery through the connection and wait for its run to end; its id, or the reason it failed. */
async function discover(connection: string, request: Row): Promise<string> {
  const started = await api.post<Row>("/discoveries/source", { src_connection: connection, ...request });
  for (;;) {
    const run = await api.get<Row>(`/discoveries/runs/${started.run_id}`);
    if (!RUNNING.has(run.state)) {
      if (run.result && run.result !== "SUCCESS") {
        const rows = await api.get<Row[]>(`/discoveries/${started.discovery_id}/databases`);
        const failed = rows.find((r) => r.status === "failed");
        throw new Error(failed?.error ? String(failed.error).split("\n")[0] : run.message || `the discovery run ended ${run.result}`);
      }
      return started.discovery_id;
    }
    await wait(3000);
  }
}

const cell = (v: unknown) => { const s = v == null ? "" : String(v); return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s; };

/** The lines Add tables takes, one per discovered table. */
export function linesOf(connection: string, tables: Row[]): string {
  const head = ["src_connection", "src_database", "src_schema", "src_table", "select_list", "primary_key", "cursor_column", "load_type"];
  return [head.join(","), ...tables.map((t) => [connection, t.src_database, t.src_schema, t.src_table, t.select_list,
    t.primary_key, t.cursor_column, t.load_type].map(cell).join(","))].join("\n") + "\n";
}

/** A tick box with a third state: a filled square for a row chosen in part, a tick only when all of it is. */
function TriBox({ pick, label, onClick }: { pick: Pick; label: string; onClick: () => void }) {
  return <button type="button" className={`tri-box ${pick}`} role="checkbox" aria-checked={pick === "all" ? true : pick === "part" ? "mixed" : false}
    aria-label={label} title={pick === "part" ? "Chosen in part" : undefined} onClick={onClick}>{pick === "all" ? "✓" : ""}</button>;
}

/** A test that failed because the login can't open master, where SQL Server lists its databases. */
const cantList = (error: unknown) => /FAILED_JDBC\.CONNECTION|Login failed|Cannot open database|HV000/i.test(String((error as Error)?.message ?? error));

/** The databases typed by hand, one per line or separated by commas, for a login that can't list the server's. */
function TypedDatabases({ onUse }: { onUse: (names: string[]) => void }) {
  const [text, setText] = useState("");
  const names = [...new Set(text.split(/[\s,;]+/).map((n) => n.trim()).filter(Boolean))];
  return (
    <div className="form-grid">
      <label className="wide">Its databases<textarea rows={4} value={text} placeholder={"lfc_eng_001\nlfc_eng_002"}
        onChange={(e) => setText(e.target.value)} />
        <span className="help">One per line, or separated by commas. Each is read on its own in the next step.</span></label>
      <div className="toolbar"><button disabled={!names.length} onClick={() => onUse(names)}>
        Use {names.length ? `these ${fmtInt(names.length)} ${plural(names.length, "database")}` : "these databases"}</button></div>
    </div>
  );
}

function Step1({ chosen, setChosen, tested, setTested }: { chosen: string; setChosen: (c: string) => void;
                 tested: Row | null; setTested: (t: Row | null) => void }) {
  const { config } = useSettings();
  const [how, setHow] = useState<"existing" | "create">("existing");
  const [search, setSearch] = useState("");
  const [testing, setTesting] = useState(false);
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
    try {
      const id = await discover(chosen, { kind: "list" });
      const rows = await api.get<Row[]>(`/discoveries/${id}/databases`);
      setTested({ discovery_id: id, databases: rows.filter((r) => r.status === "listed").map((r) => r.src_database) });
    } catch (e) { setError(e); } finally { setTesting(false); }
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
            {testing && <span className="muted">Reading the server's databases: a few minutes when the discovery cluster starts cold.</span>}
            {tested && <span><Tag tone="ok">{tested.typed ? "Typed" : "Connected"}</Tag> <span className="muted">{tested.typed ? "uses" : "reads"}{" "}
              {fmtInt(tested.databases.length)} {plural(tested.databases.length, "database")} through {chosen}</span></span>}
          </div>
          {error != null && <Notice tone="bad">The connection failed its test: {String((error as Error).message ?? error)}
            {cantList(error) && <p>The server's databases are listed from its master database, and this connection's login can't open
              it. The databases themselves may still be readable through the connection: type them below. Or give the login access to
              master (a user there with VIEW ANY DATABASE) and test again.</p>}</Notice>}
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

function Step2({ connection, databases, picks, setPicks, tables, setTables }: { connection: string; databases: string[];
  picks: Picks; setPicks: (update: (p: Picks) => Picks) => void;
  tables: Record<string, Row[]>; setTables: (update: (t: Record<string, Row[]>) => Record<string, Row[]>) => void }) {
  const [filter, setFilter] = useState("");
  const [open, setOpen] = useState<string | null>(null);
  const [reading, setReading] = useState<string | null>(null);
  const [error, setError] = useState<unknown>(null);
  const shown = databases.filter((d) => d.toLowerCase().includes(filter.toLowerCase()));
  const pickIn = (all: Picks, d: string): Pick => {
    const p = all[d];
    if (Array.isArray(p)) return p.length === 0 ? "none" : tables[d] && p.length >= tables[d].length ? "all" : "part";
    return p ?? "none";
  };
  const pickOf = (d: string) => pickIn(picks, d);
  const server: Pick = databases.length && databases.every((d) => pickOf(d) === "all") ? "all"
    : databases.some((d) => pickOf(d) !== "none") ? "part" : "none";
  // Each change starts from the choices as they are then, so quick clicks never undo each other.
  const setAll = (names: string[], pick: Pick) => setPicks((now) => ({ ...now, ...Object.fromEntries(names.map((d) => [d, pick])) }));
  const openDatabase = async (d: string) => {
    if (open === d) return setOpen(null);
    setOpen(d);
    if (tables[d]) return;
    setReading(d); setError(null);
    try {
      const id = await discover(connection, { databases: [d] });
      const found = await api.get<Row[]>(`/discoveries/${id}/tables`);
      setTables((now) => ({ ...now, [d]: found }));
    } catch (e) { setError(e); } finally { setReading(null); }
  };
  const toggleTable = (d: string, key: string) => setPicks((now) => {
    const all = (tables[d] ?? []).map((t) => `${t.src_schema}.${t.src_table}`);
    const chosen = Array.isArray(now[d]) ? (now[d] as string[]) : pickIn(now, d) === "all" ? all : [];
    const next = chosen.includes(key) ? chosen.filter((k) => k !== key) : [...chosen, key];
    return { ...now, [d]: next.length === all.length ? "all" : next };
  });
  return (
    <>
      <p className="muted">The connection passed its test. Tick the whole server, whole databases, or open a database for its tables.
        Reading a database's catalog takes a few minutes when the discovery cluster starts cold.</p>
      <div className="toolbar"><input className="search" placeholder="Filter databases, e.g. eng_0" value={filter} onChange={(e) => setFilter(e.target.value)} />
        <button className="ghost" onClick={() => setAll(shown, "all")}>Select all shown</button></div>
      <div className="table-wrap tree-pick"><table className="data tree">
        <thead><tr><th>Server › database › schema.table</th><th className="num">Tables</th><th>Load type</th></tr></thead>
        <tbody>
          <tr><td className="name"><TriBox pick={server} label={`The whole server ${connection}`}
            onClick={() => setAll(databases, server === "all" ? "none" : "all")} /> <b>{connection}, the whole server</b></td>
            <td className="num">{fmtInt(databases.length)} {plural(databases.length, "database")}</td>
            <td className="muted">{server === "part" ? "chosen in part" : ""}</td></tr>
          {shown.map((d) => (
            <Fragment key={d}>
              <tr className="l2"><td className="name" style={{ paddingLeft: 30 }}>
                <TriBox pick={pickOf(d)} label={`The whole database ${d}`} onClick={() => setAll([d], pickOf(d) === "all" ? "none" : "all")} />{" "}
                <button className="twisty" aria-expanded={open === d} onClick={() => openDatabase(d)}>{open === d ? "▾" : "▸"}</button>{d}</td>
                <td className="num">{tables[d] ? fmtInt(tables[d].length) : reading === d ? "reading…" : ""}</td>
                <td className="muted">{pickOf(d) === "part" ? `${fmtInt((picks[d] as string[]).length)} chosen` : ""}</td></tr>
              {open === d && (tables[d] ?? []).map((t) => {
                const key = `${t.src_schema}.${t.src_table}`;
                const on = pickOf(d) === "all" || (Array.isArray(picks[d]) && (picks[d] as string[]).includes(key));
                return (
                  <tr key={key} className="l3"><td className="name" style={{ paddingLeft: 62 }}>
                    <TriBox pick={on ? "all" : "none"} label={key} onClick={() => toggleTable(d, key)} /> {key}
                    {t.notes && <span className="sub">{t.notes}</span>}</td>
                    <td className="num">{t.row_count != null ? `${fmtInt(t.row_count)} rows` : ""}</td>
                    <td>{t.load_type === "ct" ? "Change tracking" : t.load_type === "cursor" ? `Cursor column ${t.cursor_column}` : "Full reload"}</td></tr>
                );
              })}
            </Fragment>
          ))}
          {databases.length > shown.length && <tr className="l2"><td colSpan={3} className="muted" style={{ paddingLeft: 30 }}>
            {fmtInt(databases.length - shown.length)} more {plural(databases.length - shown.length, "database")} hidden by the filter</td></tr>}
        </tbody>
      </table></div>
      <ErrorBox error={error} />
    </>
  );
}

/** Discover on a source: the two steps, then the lines of the tables chosen (onClose(lines)), or nothing on Cancel. */
export function DiscoverDialog({ onClose }: { onClose: (lines: string | null) => void }) {
  const [step, setStep] = useState<1 | 2 | 3>(1);
  const [connection, setConnection] = useState("");
  const [tested, setTested] = useState<Row | null>(null);
  const [picks, setPicks] = useState<Picks>({});
  const [tables, setTables] = useState<Record<string, Row[]>>({});
  const [error, setError] = useState<unknown>(null);
  const databases: string[] = tested?.databases ?? [];
  const whole = databases.filter((d) => picks[d] === "all");
  const handPicked = Object.entries(picks).filter(([, p]) => Array.isArray(p) && p.length) as [string, string[]][];
  const chosen = whole.length + handPicked.length;
  // Databases chosen whole whose tables aren't read yet are discovered on Add; a whole server reads it all at once.
  const finish = async () => {
    setStep(3); setError(null);
    try {
      const unread = whole.filter((d) => !tables[d]);
      const read: Row[] = [];
      if (unread.length) {
        // A whole server is listed from master; databases typed by hand are read by name instead.
        const all = unread.length === databases.length && !tested?.typed;
        const id = await discover(connection, all ? { whole_server: true } : { databases: unread });
        read.push(...await api.get<Row[]>(`/discoveries/${id}/tables`));
      }
      const picked = [...whole.filter((d) => tables[d]).flatMap((d) => tables[d]), ...read,
        ...handPicked.flatMap(([d, keys]) => (tables[d] ?? []).filter((t) => keys.includes(`${t.src_schema}.${t.src_table}`)))];
      onClose(linesOf(connection, picked));
    } catch (e) { setError(e); setStep(2); }
  };
  const title = step === 1 ? "Add tables · Step 1 of 2: the connection" : `Add tables · Step 2 of 2: choose on ${connection}`;
  const blocked = step === 1 ? (tested ? undefined : "Test the connection first")
    : step === 3 ? "Reading the chosen databases' tables" : chosen ? undefined : "Choose the server, databases or tables first";
  return (
    <Modal title={title} action={step === 1 ? "Next: choose tables" : step === 3 ? "Reading…" : "Add the tables chosen"}
      actionDisabled={blocked} onClose={(ok) => {
        if (!ok) return onClose(null);
        if (step === 1) return setStep(2);
        finish();
      }}>
      <div className="discover">
        {step === 1 && <Step1 chosen={connection} setChosen={setConnection} tested={tested} setTested={setTested} />}
        {step >= 2 && <Step2 connection={connection} databases={databases} picks={picks} setPicks={setPicks} tables={tables} setTables={setTables} />}
        {step === 3 && <p className="muted">Reading the tables of {fmtInt(whole.filter((d) => !tables[d]).length)} {plural(whole.length, "database")}:
          a few minutes when the discovery cluster starts cold. The tables are added to the job's page when it ends.</p>}
        {step === 2 && chosen > 0 && <p className="muted">{fmtInt(whole.length)} whole {plural(whole.length, "database")}
          {handPicked.length ? `, and tables from ${fmtInt(handPicked.length)} more` : ""} chosen.</p>}
        <ErrorBox error={error} />
      </div>
    </Modal>
  );
}
