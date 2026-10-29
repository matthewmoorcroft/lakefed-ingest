"""A new ingestion job, made in one go: the tables it ingests (from a CSV of where each table is), how it writes them,
and its groups, each of the job's servers split into a few groups by a stable hash of the database name.

A CSV line says only where a table is: its UC connection or host, database, schema and table, and optionally its key,
columns and identity values. How to ingest and write it is the job's, as in the Edit view: load type, fan-in, write
modes, target names (templates over {server} {database} {schema} {table}), client_id and identity columns. Pure: no
database access, so main.py writes the plan in one transaction."""
import csv
import io
import json
from typing import Callable, Dict, List, Optional

import config_import as ci
import job_settings as js
import queries as q

LOAD_TYPES = ("ct", "cursor", "full")
DEFAULT_GROUPS_PER_SERVER = 4
MAX_GROUPS_PER_SERVER = 64
DEFAULTS = {"src_type": "sqlserver", "load_type": "ct", "fan_in": False, "write_mode_changes": "merge",
            "write_mode_full": "merge", "sink_catalog": "", "sink_schema": "", "sink_table": "{table}",
            "lowercase": True, "client_id": "{database}", "identity": {}, "freshness_sla_minutes": None}


def settings_of(given: dict) -> dict:
    """The job's settings as given, each blank or missing one at its default."""
    out = dict(DEFAULTS)
    out.update({k: v for k, v in (given or {}).items() if k in DEFAULTS and v is not None})
    out["identity"] = {str(k).strip(): str(v).strip() for k, v in (out.get("identity") or {}).items() if str(k).strip()}
    return out


def group_name(job_name: str, server: str, database: str, groups_per_server: int) -> str:
    """The group a database of a new job joins: its server's group by a stable hash of its name, as Add databases does."""
    return f"{q.identifier(job_name)}_{q.identifier(server)}_c{js._bucket(database, groups_per_server) + 1:02d}"


def setting_errors(settings: dict) -> List[str]:
    errors = []
    if settings["load_type"] not in LOAD_TYPES:
        errors.append(f"load type must be one of {', '.join(LOAD_TYPES)}")
    for key, label in js.TARGET_SETTINGS:
        if not str(settings[key] or "").strip():
            errors.append(f"set the {label}")
    return errors + js.check_templates(settings, bool(settings["fan_in"]))


def _write_mode(load_type: str, settings: dict) -> str:
    if load_type == "full":
        return settings["write_mode_full"]
    return settings["write_mode_changes"] if load_type in js.CHANGE_LOAD_TYPES else "merge"


def _line_load_types(text: str):
    """(the file without its load_type column, {file line number: load type}). A discovered table carries the load
    type discovery recommends for it; a plain CSV has none, and every line takes the job's."""
    reader = csv.reader(io.StringIO(text.lstrip("\ufeff")))
    rows = list(reader)
    if not rows or "load_type" not in [h.strip() for h in rows[0]]:
        return text, {}
    at = [h.strip() for h in rows[0]].index("load_type")
    types = {number: (row[at].strip() if at < len(row) else "") for number, row in enumerate(rows[1:], start=2)}
    out = io.StringIO()
    csv.writer(out, lineterminator="\n").writerows([row[:at] + row[at + 1:] for row in rows])
    return out.getvalue(), {n: t for n, t in types.items() if t}


def plan(job_name: str, text: str, given: dict, groups_per_server: int = DEFAULT_GROUPS_PER_SERVER,
         taken_groups: Optional[set] = None, group_of: Optional[Callable[[str, str], str]] = None,
         have: Optional[set] = None) -> dict:
    """What creating the job writes: {"errors": problems with the file or the settings (nothing is written while there
    are any), "lines": one per CSV line with its control row, status (valid, invalid, or in_job for a table the job
    already has) and errors, "groups": the groups its lines go to, "settings": the job's settings}. taken_groups are
    names other jobs or tables already use; group_of(server, database) places a database (by default a new job's
    groups); have holds the (target, client_id) pairs a job already writes."""
    settings = settings_of(given)
    group_of = group_of or (lambda server, database: group_name(job_name, server, database, groups_per_server))
    out = {"errors": setting_errors(settings), "lines": [], "groups": [], "settings": settings}
    if not str(job_name or "").strip():
        out["errors"].insert(0, "name the job")
    if not 1 <= int(groups_per_server or 0) <= MAX_GROUPS_PER_SERVER:
        out["errors"].append(f"groups per server must be from 1 to {MAX_GROUPS_PER_SERVER}")
    if out["errors"]:
        return out
    text, line_types = _line_load_types(text)
    # The file is read as Import CSV reads it; its sink names are made again below, with the job's templates.
    read = ci.plan_import(text, "_new", {"src_type": settings["src_type"], "load_type": settings["load_type"],
                                         "use_remote_query": False, "write_mode": "merge", "fan_in": False,
                                         "sink_catalog": "c", "sink_schema": "s", "sink_table": "{table}_{database}",
                                         "freshness_sla_minutes": settings["freshness_sla_minutes"]}, [])
    if read["errors"]:
        out["errors"] = read["errors"]
        return out
    seen: Dict[tuple, int] = {}
    groups = set()
    for line in read["lines"]:
        row = dict(line["row"])
        # The reader's row checks ran before the job's settings were applied: keep only its own (the file's) problems.
        before = set(q.validate_control_row(line["row"]))
        problems = [e for e in line["errors"] if e not in before and not e.startswith("client_id ")]
        row["use_remote_query"] = bool(row.get("src_connection")) and not row.get("src_host")
        if row.get("src_connection") and row.get("src_host"):
            problems.append("give the UC connection or the host, not both")
        if line["line"] in line_types:
            row["load_type"] = line_types[line["line"]]
            if row["load_type"] not in LOAD_TYPES:
                problems.append(f"load_type must be one of {', '.join(LOAD_TYPES)}")
        server = js.server_of(row)
        row["task_collection"] = group_of(server, row.get("src_database") or "")
        row["write_mode"] = _write_mode(row["load_type"], settings)
        for key, _ in js.TARGET_SETTINGS:
            row[key] = js.render(settings[key], row, target=True, lowercase=bool(settings["lowercase"]))
        if settings["fan_in"]:
            row["client_id"] = row.get("client_id") or js.render(settings["client_id"] or "{database}", row)
        elif row.get("client_id"):
            problems.append("client_id must be blank: this job has no fan-in")
        identity = {name: js.render(template, row) for name, template in settings["identity"].items()}
        identity.update(json.loads(row["client_attrs"]) if row.get("client_attrs") else {})   # the file's own values win
        row["client_attrs"] = json.dumps(identity) if identity else None
        problems += [e for e in q.validate_control_row(row) if e not in problems]
        if taken_groups and row["task_collection"] in taken_groups:
            problems.append(f"group {row['task_collection']} is already in use: give the job another name")
        sink = ".".join(str(row.get(k) or "").lower() for k, _ in js.TARGET_SETTINGS)
        key = (sink, str(row.get("client_id") or ""))
        if key in seen:
            problems.append(f"line {seen[key]} already writes {sink}" + (f" for {key[1]}" if key[1] else
                            ": turn fan-in on, or put {database} in the target names"))
        seen.setdefault(key, line["line"])
        in_job = bool(have) and key in have
        if not in_job:
            groups.add(row["task_collection"])
        out["lines"].append({"line": line["line"], "table": line["table"], "server": server, "sink": sink, "row": row,
                             "group": row["task_collection"],
                             "status": "invalid" if problems else "in_job" if in_job else "valid", "errors": problems})
    if not out["lines"]:
        out["errors"].append("the file has no tables")
    out["groups"] = sorted(groups)
    return out


# The job's settings a CSV's tables take, from what its tables share (job_settings.read), with the defaults where it has
# none (no full-reload tables yet, say).
_JOB_SETTINGS = ("fan_in", "write_mode_changes", "write_mode_full", "sink_catalog", "sink_schema", "sink_table", "lowercase",
                 "client_id", "identity", "freshness_sla_minutes")


def plan_for_job(job_name: str, text: str, load_type: str, job_rows: List[dict],
                 groups_per_server: int = DEFAULT_GROUPS_PER_SERVER, taken_groups: Optional[set] = None) -> dict:
    """What importing a CSV of tables into an existing job writes, as plan() does for a new one, plus "new_groups".
    Each table takes the job's settings. A database already in the job keeps its group; a new database on one of the
    job's servers joins one of that server's groups by a stable hash of its name, as Add databases does; a new server
    gets groups_per_server new groups. A table the job already writes is left as it is (in_job)."""
    read = js.read(job_rows)
    mixed = [name for name in _JOB_SETTINGS if "mixed" in read.get(name, {})]
    if mixed:
        return {"errors": [f"the job's {name.replace('_', ' ')} differs between its tables: set it in Edit first" for name in mixed],
                "lines": [], "groups": [], "new_groups": [], "settings": {}}
    given = {name: read[name]["value"] for name in _JOB_SETTINGS if read.get(name, {}).get("value") is not None}
    given["load_type"] = load_type
    database_groups: Dict[tuple, str] = {}
    server_groups: Dict[str, set] = {}
    for r in job_rows:
        server = js.server_of(r)
        database_groups.setdefault((server, str(r.get("src_database") or "").lower()), r["task_collection"])
        server_groups.setdefault(server, set()).add(r["task_collection"])

    def group_of(server: str, database: str) -> str:
        if (server, database.lower()) in database_groups:
            return database_groups[(server, database.lower())]
        if server in server_groups:
            names = sorted(server_groups[server])
            return names[js._bucket(database, len(names))]
        return group_name(job_name, server, database, groups_per_server)

    have = {(".".join(str(r.get(k) or "").lower() for k, _ in js.TARGET_SETTINGS), str(r.get("client_id") or ""))
            for r in job_rows}
    known = {g for gs in server_groups.values() for g in gs}
    out = plan(job_name, text, given, groups_per_server, (taken_groups or set()) - known, group_of, have)
    out["new_groups"] = sorted(set(out["groups"]) - known)
    return out


def summary(planned: dict) -> dict:
    """What the page shows before Create: tables, databases and groups per server, and the invalid lines."""
    servers: Dict[str, dict] = {}
    for line in (line for line in planned["lines"] if line["status"] != "in_job"):
        s = servers.setdefault(line["server"], {"server": line["server"], "databases": set(), "tables": 0, "groups": set(),
                                                "read_through": "UC connection" if line["row"].get("use_remote_query") else "JDBC"})
        s["databases"].add(line["row"].get("src_database"))
        s["tables"] += 1
        s["groups"].add(line["group"])
    return {"servers": [{**s, "databases": len(s["databases"]), "groups": sorted(s["groups"])}
                        for s in sorted(servers.values(), key=lambda s: s["server"])],
            "tables": sum(line["status"] != "in_job" for line in planned["lines"]), "groups": planned["groups"],
            "in_job": sum(line["status"] == "in_job" for line in planned["lines"]), "new_groups": planned.get("new_groups", []),
            "invalid": [{k: line[k] for k in ("line", "table", "errors")} for line in planned["lines"] if line["status"] == "invalid"]}
