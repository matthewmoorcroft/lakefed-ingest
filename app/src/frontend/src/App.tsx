import { createContext, useContext } from "react";
import { NavLink, Navigate, Route, Routes, useLocation } from "react-router-dom";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api, type Config } from "./api";
import { Menu } from "./components";
import Overview from "./pages/Overview";
import Runs from "./pages/Runs";
import RunDetail from "./pages/RunDetail";
import Tables from "./pages/Tables";
import Jobs from "./pages/Jobs";
import AddTable from "./pages/AddTable";
import BulkImport from "./pages/BulkImport";
import Changes from "./pages/Changes";
import Schedules from "./pages/Schedules";
import Timeline from "./pages/Timeline";
import Collection from "./pages/Collection";
import IngestionJobs from "./pages/IngestionJobs";
import IngestionJob from "./pages/IngestionJob";
import NotInJob from "./pages/NotInJob";
import JobEdit from "./pages/JobEdit";
import JobRun from "./pages/JobRun";
import Sources from "./pages/Sources";
import Database from "./pages/Database";
import ImportCsv from "./pages/ImportCsv";

interface Settings { config?: Config }
const SettingsContext = createContext<Settings>({});
export const useSettings = () => useContext(SettingsContext);

const NAV = [
  ["Monitor", [["/overview", "Overview"], ["/ingestion-jobs", "Ingestion jobs"], ["/runs", "Runs"], ["/sources", "Sources"], ["/timeline", "Timeline"]]],
  ["Manage", [["/tables", "Tables"], ["/schedules", "Schedules"], ["/jobs", "Databricks jobs"], ["/add", "Add table"],
              ["/bulk", "Bulk import"], ["/import", "Import CSV"], ["/changes", "Changes"]]],
] as const;

// Pages under an ingestion job keep Ingestion jobs selected, as the workspace keeps a job's section selected on its pages.
// A group run (/runs/<id>) is under its job too, as its crumbs say; the Runs list itself stays under Runs.
const UNDER = { "/ingestion-jobs": ["/ingestion-jobs", "/collections", "/runs/"] } as Record<string, string[]>;

export default function App() {
  const config = useQuery({ queryKey: ["config"], queryFn: () => api.get<Config>("/config") });
  const qc = useQueryClient();
  const { pathname } = useLocation();
  return (
    <SettingsContext.Provider value={{ config: config.data }}>
      <div className="layout">
        <aside className="sidebar">
          <div className="brand"><div>Lakefed Ingest<small>Operations</small></div></div>
          <nav>
            {NAV.map(([group, links]) => (
              <div key={group}>
                <div className="nav-group">{group}</div>
                {links.map(([to, label]) => (
                  <NavLink key={to} to={to} end className={({ isActive }) =>
                    (isActive || (UNDER[to] ?? []).some((p) => pathname.startsWith(p)) ? "active" : "")}>{label}</NavLink>
                ))}
              </div>
            ))}
          </nav>
        </aside>
        <main className="content">
          {/* As the Databricks workspace's top bar: actions and the signed-in user at the top right. */}
          <div className="topbar">
            <button className="ghost" onClick={() => qc.invalidateQueries()}>Refresh data</button>
            <UserMenu config={config.data} />
          </div>
          <Routes>
            <Route path="/" element={<Navigate to="/overview" replace />} />
            <Route path="/overview" element={<Overview />} />
            <Route path="/ingestion-jobs" element={<IngestionJobs />} />
            <Route path="/ingestion-jobs/not-in-job" element={<NotInJob />} />
            <Route path="/ingestion-jobs/:id" element={<IngestionJob />} />
            <Route path="/ingestion-jobs/:id/edit" element={<JobEdit />} />
            <Route path="/ingestion-jobs/:id/runs/:slot" element={<JobRun />} />
            <Route path="/runs" element={<Runs />} />
            <Route path="/runs/:id" element={<RunDetail />} />
            <Route path="/tables" element={<Tables />} />
            <Route path="/jobs" element={<Jobs />} />
            <Route path="/schedules" element={<Schedules />} />
            <Route path="/timeline" element={<Timeline />} />
            <Route path="/collections/:name" element={<Collection />} />
            <Route path="/collections/:name/databases/:server/:database" element={<Database />} />
            <Route path="/sources" element={<Sources />} />
            <Route path="/add" element={<AddTable />} />
            <Route path="/bulk" element={<BulkImport />} />
            <Route path="/import" element={<ImportCsv />} />
            <Route path="/changes" element={<Changes />} />
            <Route path="*" element={<Navigate to="/overview" replace />} />
          </Routes>
        </main>
      </div>
    </SettingsContext.Provider>
  );
}


/** The signed-in user's menu, as the Databricks workspace shows it at the top right: their initials, opening to who
    they are and which Lakebase database this app reads. */
function UserMenu({ config }: { config?: Config }) {
  const user = config?.user ?? "";
  // Blank until the user is known, so the first paint never shows a placeholder as if it were someone.
  const initials = user.split("@")[0].split(/[._-]/).filter(Boolean).slice(0, 2).map((part) => part[0].toUpperCase()).join("");
  return (
    <Menu label={initials} buttonClass="avatar" title={user || "Your account"}>
      <div className="menu-note"><div className="muted small">Signed in as</div><div>{user || "unknown"}</div></div>
      {config && <div className="menu-note"><div className="muted small">Lakebase</div>
        <div className="mono">{config.lakebase_instance}</div><div className="mono">{config.lakebase_database}</div></div>}
    </Menu>
  );
}
