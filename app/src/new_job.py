"""A new ingestion job, made in one go: the tables it ingests (from a CSV of where each table is), how it writes them,
and its groups, each of the job's servers split into a few groups by a stable hash of the database name.

A CSV line says only where a table is: its UC connection or host, database, schema and table, and optionally its key,
columns and identity values. How to ingest and write it is the job's, as in the Edit view: load type, fan-in, write
modes, target names (templates over {server} {database} {schema} {table}), client_id and identity columns. Pure: no
database access, so main.py writes the plan in one transaction."""
import csv
import io
import json
from typing import Callable, Dict, List, Optional, Tuple

import config_import as ci
import job_settings as js
import queries as q

LOAD_TYPES = ("ct", "cursor", "full")
DEFAULT_GROUPS_PER_SERVER = 4
# A whole server at once: the 100 lab databases of 1,000 tables each, with room to spare. Import CSV keeps its own
# 5,000-line cap (config_import.MAX_LINES).
MAX_LINES = 120_000
MAX_GROUPS_PER_SERVER = 64
DEFAULTS = {"src_type": "sqlserver", "load_type": "ct", "fan_in": False, "write_mode_changes": "merge",
            "write_mode_full": "merge", "sink_catalog": "", "sink_schema": "", "sink_table": "{table}",
            "lowercase": True, "client_id": "{database}", "identity": {}, "freshness_sla_minutes": None}


BOOLEAN_SETTINGS = ("fan_in", "lowercase")


def given_errors(given: dict) -> List[str]:
    """What is wrong with the settings as given: a name the job has no setting for (a typo would otherwise be
    ignored), or a yes or no setting that is neither."""
    errors = [f"unknown setting {name}" for name in (given or {}) if name not in DEFAULTS]
    for name in BOOLEAN_SETTINGS:
        if (given or {}).get(name) is not None:
            try:
                q.parse_bool(given[name])
            except ValueError as e:
                errors.append(f"{name}: {e}")
    return errors


def settings_of(given: dict) -> dict:
    """The job's settings as given, each blank or missing one at its default."""
    out = dict(DEFAULTS)
    out.update({k: v for k, v in (given or {}).items() if k in DEFAULTS and v is not None})
    for name in BOOLEAN_SETTINGS:
        try:
            out[name] = q.parse_bool(out[name])
        except ValueError:
            out[name] = DEFAULTS[name]      # given_errors reports it, and nothing is written while it does
    out["identity"] = {str(k).strip(): str(v).strip() for k, v in (out.get("identity") or {}).items() if str(k).strip()}
    return out


def group_name(job_name: str, server: str, database: str, groups_per_server: int) -> str:
    """The group a database of a new job joins: its server's group by a stable hash of its name, as Add databases does."""
    return f"{q.identifier(job_name)}_{q.identifier(server)}_c{js._bucket(database, groups_per_server) + 1:02d}"


def group_names(job_name: str, server: str, groups_per_server: int) -> List[str]:
    """Every group of one server in a new job, in order: the names group_name gives its databases."""
    return [f"{q.identifier(job_name)}_{q.identifier(server)}_c{number:02d}" for number in range(1, groups_per_server + 1)]


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
    out = {"errors": given_errors(given) + setting_errors(settings), "lines": [], "groups": [], "settings": settings}
    if not str(job_name or "").strip():
        out["errors"].insert(0, "name the job")
    if group_of is None and not 1 <= int(groups_per_server or 0) <= MAX_GROUPS_PER_SERVER:
        out["errors"].append(f"groups per server must be from 1 to {MAX_GROUPS_PER_SERVER}")
    if out["errors"]:
        return out
    text, line_types = _line_load_types(text)
    # The file is read as Import CSV reads it; its sink names are made again below, with the job's templates.
    read = ci.plan_import(text, "_new", {"src_type": settings["src_type"], "load_type": settings["load_type"],
                                         "use_remote_query": False, "write_mode": "merge", "fan_in": False,
                                         "sink_catalog": "c", "sink_schema": "s", "sink_table": "{table}_{database}",
                                         "freshness_sla_minutes": settings["freshness_sla_minutes"]}, [],
                         max_lines=MAX_LINES)
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


def job_inputs(job_name: str, job_rows: List[dict], groups_per_server: int = DEFAULT_GROUPS_PER_SERVER) -> dict:
    """What a job's own rows say about the tables added to it: {"errors": the job's settings differ between its tables
    (nothing is planned while there are any), "given": the settings each table takes, "group_of": where a database
    goes (a database the job has keeps its group, a new one joins its server's groups, a new server gets new groups),
    "have": the (target, client_id) pairs the job already writes, "known": its groups}."""
    read = js.read(job_rows)
    mixed = [name for name in _JOB_SETTINGS if "mixed" in read.get(name, {})]
    if mixed:
        return {"errors": [f"the job's {name.replace('_', ' ')} differs between its tables: set it in Edit first"
                           for name in mixed], "given": {}, "group_of": None, "have": set(), "known": set()}
    given = {name: read[name]["value"] for name in _JOB_SETTINGS if read.get(name, {}).get("value") is not None}
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
    return {"errors": [], "given": given, "group_of": group_of, "have": have,
            "known": {g for gs in server_groups.values() for g in gs}}


def plan_for_job(job_name: str, text: str, load_type: str, job_rows: List[dict],
                 groups_per_server: int = DEFAULT_GROUPS_PER_SERVER, taken_groups: Optional[set] = None) -> dict:
    """What importing a CSV of tables into an existing job writes, as plan() does for a new one, plus "new_groups".
    Each table takes the job's settings. A database already in the job keeps its group; a new database on one of the
    job's servers joins one of that server's groups by a stable hash of its name, as Add databases does; a new server
    gets groups_per_server new groups. A table the job already writes is left as it is (in_job)."""
    inputs = job_inputs(job_name, job_rows, groups_per_server)
    if inputs["errors"]:
        return {"errors": inputs["errors"], "lines": [], "groups": [], "new_groups": [], "settings": {}}
    given = {**inputs["given"], "load_type": load_type}
    known = inputs["known"]
    out = plan(job_name, text, given, groups_per_server, (taken_groups or set()) - known, inputs["group_of"],
               inputs["have"])
    out["new_groups"] = sorted(set(out["groups"]) - known)
    return out


def summary(planned: dict) -> dict:
    """What the page shows before Create: tables, databases and groups per server, the invalid lines (a line of the
    file, or a database that could not be read, named with why), and what the file's database lines came to."""
    servers: Dict[str, dict] = {}
    for line in (line for line in planned["lines"] if line["status"] != "in_job"):
        s = servers.setdefault(line["server"], {"server": line["server"], "databases": set(), "tables": 0, "groups": set(),
                                                "read_through": "UC connection" if line["row"].get("use_remote_query") else "JDBC",
                                                "load_types": {}})
        s["load_types"][line["row"]["load_type"]] = s["load_types"].get(line["row"]["load_type"], 0) + 1
        s["databases"].add(line["row"].get("src_database"))
        s["tables"] += 1
        s["groups"].add(line["group"])
    return {"servers": [{**s, "databases": len(s["databases"]), "groups": sorted(s["groups"])}
                        for s in sorted(servers.values(), key=lambda s: s["server"])],
            "tables": sum(line["status"] != "in_job" for line in planned["lines"]), "groups": planned["groups"],
            "in_job": sum(line["status"] == "in_job" for line in planned["lines"]), "new_groups": planned.get("new_groups", []),
            "covered": planned.get("covered", 0), "discovered": planned.get("discovered", 0),
            "invalid": [{k: line[k] for k in ("line", "table", "errors")} for line in planned["lines"] if line["status"] == "invalid"]
                       + [{"line": u["line"], "table": u["database"], "errors": [u["error"]]} for u in planned.get("unreadable", [])]}


# ---- a CSV kept in Lakebase (an upload), and the CSV a plan reads from what a selection takes ---------------------

UPLOAD_COLUMNS = ci.CSV_COLUMNS + ("load_type",)
# A file reads through the connection chosen before it, and the job sets its groups: these columns are not accepted.
UPLOAD_REFUSED_COLUMNS = ("src_host", "src_port", "secret_key", "task_collection")
# An upload line's kind: a table (ok), a database alone, every table of which is discovered by name at Create
# (database), or one that can't be taken (invalid). Only ok lines are read as tables (queries.source_rows_sql).
TABLE_LINE, DATABASE_LINE, INVALID_LINE = "ok", "database", "invalid"


def read_upload(text: str, connection: str, max_lines: int = MAX_LINES) -> dict:
    """An uploaded CSV, read line by line for table_upload_line: {"errors": the file's own problems (nothing is kept
    while there are any), "columns": its header, "lines": [{line, server, src_database, src_schema, src_table,
    load_type, fields, status, errors}]}. The file is read through the connection chosen before it: a src_connection
    column is accepted only when every row names that connection, else the lines are named in the errors. A line with
    src_database alone is a database: every table of it, found by discovery at Create. A line that can't be taken
    (no table where a schema is given, a database named twice) is kept as invalid with why; the job's settings are
    checked later, when Create or Add tables plans what it takes."""
    connection = str(connection or "").strip()
    reader = csv.DictReader(io.StringIO(text.lstrip("\ufeff")))
    header = [h.strip() for h in (reader.fieldnames or [])]
    reader.fieldnames = header
    errors = ci._header_errors([h for h in header if h != "load_type"]) if header else ["the file is empty"]
    errors += [f"column {c} is not accepted: the file reads through the connection chosen before it, and the job sets "
               f"its groups" for c in header if c in UPLOAD_REFUSED_COLUMNS]
    if header and "src_database" not in header:
        errors.append("the file has no src_database column")
    if not connection:
        errors.append("choose the connection the file reads through")
    if errors:
        return {"errors": errors, "columns": header, "connection": connection, "lines": []}
    out, mismatched, named = [], [], {}
    for number, raw in enumerate((r for r in reader if any(str(v or "").strip() for v in r.values())), start=2):
        if len(out) >= max_lines:
            return {"errors": [f"more than {max_lines:,} lines: at most {max_lines:,} in one file"], "columns": header,
                    "connection": connection, "lines": []}
        problems = ["this line has more cells than the header has columns"] if None in raw else []
        fields = {k: str(v or "").strip() for k, v in raw.items() if k is not None}
        given = fields.get("src_connection", "")
        if given and given.lower() != connection.lower():
            mismatched.append(f"line {number} names {given}, not {connection}")
        fields["src_connection"] = connection
        database, schema, table = (fields.get(k) or "" for k in ("src_database", "src_schema", "src_table"))
        if not database:
            problems.append("src_database is required")
        if bool(schema) != bool(table):
            problems.append("src_table is required with src_schema" if schema else "src_schema is required with src_table")
        kind = TABLE_LINE if schema and table else DATABASE_LINE if database and not schema else INVALID_LINE
        if kind == DATABASE_LINE and database.lower() in named:
            problems.append(f"database {database} is already named on line {named[database.lower()]}: it is taken once")
            kind = INVALID_LINE
        if kind == DATABASE_LINE:
            named.setdefault(database.lower(), number)
        if problems:
            kind = INVALID_LINE
        out.append({"line": number, "server": js.server_of({"src_connection": connection, "use_remote_query": True}),
                    "src_database": database or None, "src_schema": schema or None, "src_table": table or None,
                    "load_type": fields.get("load_type") or None, "fields": fields,
                    "status": kind, "errors": problems or None})
    if mismatched:
        return {"errors": [f"{len(mismatched)} {plural(len(mismatched), 'line')} "
                           f"{'names' if len(mismatched) == 1 else 'name'} another connection: "
                           + "; ".join(mismatched[:20]) + (f"; and {len(mismatched) - 20} more" if len(mismatched) > 20 else "")],
                "columns": header, "connection": connection, "lines": []}
    if not out:
        errors.append("the file has no tables or databases")
    return {"errors": errors, "columns": header, "connection": connection, "lines": out if not errors else []}


def plural(count: int, word: str) -> str:
    return word if count == 1 else f"{word}s"


def discovered_tables(found: List[dict], explicit: set) -> Tuple[List[dict], int]:
    """The discovered tables a plan takes, and how many were left out: a table the file names in a row of its own wins
    over the one discovery recommends for it, so a discovered table with the same server, database, schema and table
    as an explicit one is left out (the explicit row's settings are used). ``found`` are discovered rows (fields: the
    CSV fields they take); ``explicit`` holds the (server, database, schema, table) keys of the file's table rows."""
    kept, covered = [], 0
    for row in found:
        if table_key(row) in explicit:
            covered += 1
        else:
            kept.append(row)
    return kept, covered


def table_key(row: dict) -> tuple:
    """A row's (server, database, schema, table) key, lower case, as discovered_tables compares them."""
    return tuple(str(part or "").lower() for part in (row["server"], row["src_database"], row["src_schema"], row["src_table"]))


def csv_of(columns: List[str], rows: List[dict]) -> str:
    """The CSV a plan reads: the columns, then each row's fields in their order."""
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(columns)
    writer.writerows([[("" if row.get(c) is None else row.get(c)) for c in columns] for row in rows])
    return out.getvalue()


DISCOVERY_COLUMNS = ("src_connection", "src_host", "src_port", "secret_key", "src_database", "src_schema", "src_table",
                     "select_list", "primary_key", "cursor_column", "load_type")
