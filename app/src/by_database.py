"""A big job written one database at a time: how many groups a server's tables take, the pages of databases a source is
read in, the units a selection takes, and the plan of one database. Pure: the endpoints and the background task read
Lakebase and write each database's own transaction (main.py)."""
import math
from typing import Callable, Dict, Iterator, List, Optional, Tuple

import new_job as nj

# A group of a big job takes about this many tables, so a server with 5.3 million tables gets about 425 groups.
TABLES_PER_GROUP = 12_500
# Databases are read this many at a time, each page starting after the key of the last one read.
DATABASE_PAGE = 500


def groups_for_tables(tables: int) -> int:
    """How many groups a server's tables take: one per TABLES_PER_GROUP tables, at least one."""
    return max(1, math.ceil(int(tables) / TABLES_PER_GROUP))


def groups_by_server(servers: List[dict]) -> Dict[str, int]:
    """{server: its groups} for a selection's servers (queries.selected_servers_sql rows)."""
    return {s["server"]: groups_for_tables(s["tables"]) for s in servers}


def database_pages(page: Callable[[Tuple[str, str, str]], List[dict]]) -> Iterator[dict]:
    """Every database of a source in key order, a page at a time. page(after) returns up to DATABASE_PAGE rows after the
    key (server, lower-case name, name) of the last one read; the first page is asked with ('', '', '')."""
    after = ("", "", "")
    while True:
        rows = page(after)
        yield from rows
        if len(rows) < DATABASE_PAGE:
            return
        last = rows[-1]
        after = (last["server"], str(last["src_database"]).lower(), last["src_database"])


class Unit:
    """One database a selection takes: all its tables (every is True) or only the named ones (schema.table, lower case)."""
    __slots__ = ("server", "database", "every", "tables")

    def __init__(self, server: str, database: str, every: bool, tables: Optional[frozenset] = None):
        self.server, self.database, self.every, self.tables = server, database, every, tables

    def keeps(self, row: dict) -> bool:
        """Whether a table row of this database is one the selection takes."""
        if self.every:
            return True
        return f"{row['src_schema']}.{row['src_table']}".lower() in self.tables


def selection_units(selection, server_database_keys: Iterator[Tuple[str, str]]) -> Iterator[Unit]:
    """The databases a selection takes, in order: each database the whole_server filter matches (given as
    server_database_keys, from database_pages), then the databases named and the tables named, skipping those the
    whole server already covers (a database whose name the filter keeps) and naming each database once."""
    seen = set()
    for server, database in server_database_keys:
        seen.add((server.lower(), database.lower()))
        yield Unit(server, database, True)
    filter_ = str(selection.filter or "").lower()
    covered = lambda database: bool(selection.whole_server) and filter_ in database.lower()
    for named in selection.databases:
        key = (named.server.lower(), named.database.lower())
        if key in seen or covered(named.database):
            continue
        seen.add(key)
        yield Unit(named.server, named.database, True)
    by_database: Dict[tuple, set] = {}
    for named in selection.tables:
        for name in named.tables:
            by_database.setdefault((named.server.lower(), named.database.lower(), named.database), set()).add(name.lower())
    for (server, database_lower, database), names in by_database.items():
        if (server, database_lower) in seen or covered(database):
            continue
        seen.add((server, database_lower))
        yield Unit(server, database, False, frozenset(names))


def plan_database(job_name: str, given: dict, rows: List[dict], columns: List[str], groups: Dict[str, int],
                  taken: set, own: set, group_of: Optional[Callable[[str, str], str]] = None,
                  have: Optional[set] = None) -> dict:
    """The plan of one database's rows (each {server, src_database, src_schema, src_table, line, fields}), made as
    new_job.plan makes a file's: its lines, groups and errors. groups is {server: its groups} (groups_by_server) and
    taken the groups other jobs use; the job's own groups (own) are not taken. A new job places its databases by
    groups; an existing job passes its own group_of and have (new_job.job_inputs). Each line's number is its line in
    the uploaded file, or None for a discovery."""
    text = nj.csv_of(columns, [row["fields"] for row in rows])
    if group_of is None:
        group_of = lambda server, database: nj.group_name(job_name, server, database, groups.get(server, nj.DEFAULT_GROUPS_PER_SERVER))
    plan = nj.plan(job_name, text, given, taken_groups=set(taken) - set(own), group_of=group_of, have=have)
    for line, row in zip(plan["lines"], rows):
        line["line"] = row.get("line") or None
    return plan


def summary_of(job_name: str, servers: List[dict]) -> dict:
    """What a big job's preview shows before Create, from the selection's servers (queries.selected_servers_sql): each
    server's databases, tables and group names, and the totals. Nothing is checked yet: checked is False, and by_database
    is True, so the page says the lines are read when the write runs."""
    groups_of = groups_by_server(servers)
    out_servers = []
    for s in sorted(servers, key=lambda s: s["server"]):
        names = nj.group_names(job_name, s["server"], groups_of[s["server"]])
        out_servers.append({"server": s["server"], "databases": s["databases"], "tables": s["tables"], "groups": names,
                            "read_through": "JDBC" if ":" in s["server"] else "UC connection"})
    all_groups = [g for s in out_servers for g in s["groups"]]
    return {"servers": out_servers, "tables": sum(s["tables"] for s in servers),
            "databases": sum(s["databases"] for s in servers), "groups": all_groups, "in_job": 0,
            "new_groups": all_groups, "invalid": [], "by_database": True, "checked": False}
