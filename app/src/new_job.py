"""A new ingestion job, made in one go: the tables it ingests (from a CSV of where each table is), how it writes them,
and its groups, each of the job's servers split into a few groups by a stable hash of the database name.

A CSV line says only where a table is: its UC connection or host, database, schema and table, and optionally its key,
columns and identity values. How to ingest and write it is the job's, as in the Edit view: load type, fan-in, write
modes, target names (templates over {server} {database} {schema} {table}), client_id and identity columns. Pure: no
database access, so main.py writes the plan in one transaction."""
import json
from typing import Dict, List, Optional

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


def plan(job_name: str, text: str, given: dict, groups_per_server: int = DEFAULT_GROUPS_PER_SERVER,
         taken_groups: Optional[set] = None) -> dict:
    """What creating the job writes: {"errors": problems with the file or the settings (nothing is written while there
    are any), "lines": one per CSV line with its control row, status (valid or invalid) and errors, "groups": the
    groups the job gets, "settings": the job's settings}. taken_groups are names other jobs or tables already use."""
    settings = settings_of(given)
    out = {"errors": setting_errors(settings), "lines": [], "groups": [], "settings": settings}
    if not str(job_name or "").strip():
        out["errors"].insert(0, "name the job")
    if not 1 <= int(groups_per_server or 0) <= MAX_GROUPS_PER_SERVER:
        out["errors"].append(f"groups per server must be from 1 to {MAX_GROUPS_PER_SERVER}")
    if out["errors"]:
        return out
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
        server = js.server_of(row)
        row["task_collection"] = group_name(job_name, server, row.get("src_database") or "", groups_per_server)
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
        groups.add(row["task_collection"])
        out["lines"].append({"line": line["line"], "table": line["table"], "server": server, "sink": sink, "row": row,
                             "group": row["task_collection"], "status": "invalid" if problems else "valid",
                             "errors": problems})
    if not out["lines"]:
        out["errors"].append("the file has no tables")
    out["groups"] = sorted(groups)
    return out


def summary(planned: dict) -> dict:
    """What the page shows before Create: tables, databases and groups per server, and the invalid lines."""
    servers: Dict[str, dict] = {}
    for line in planned["lines"]:
        s = servers.setdefault(line["server"], {"server": line["server"], "databases": set(), "tables": 0, "groups": set(),
                                                "read_through": "UC connection" if line["row"].get("use_remote_query") else "JDBC"})
        s["databases"].add(line["row"].get("src_database"))
        s["tables"] += 1
        s["groups"].add(line["group"])
    return {"servers": [{**s, "databases": len(s["databases"]), "groups": sorted(s["groups"])}
                        for s in sorted(servers.values(), key=lambda s: s["server"])],
            "tables": len(planned["lines"]), "groups": planned["groups"],
            "invalid": [{k: line[k] for k in ("line", "table", "errors")} for line in planned["lines"] if line["status"] == "invalid"]}
