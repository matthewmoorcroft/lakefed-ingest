"""Sums of the per-database rows (queries.database_states_sql) into what the monitoring views show: per
ingestion group, per server and per UC connection. Pure, so the views can share one scan of the control
table and still be tested without a database.

A database is its server and its name: two servers' databases of the same name are two databases. A
database counts once it has an enabled table."""
from typing import Dict, Iterable, List, Optional

STATES = ("state_current", "state_late", "state_first_load", "state_failing")


def _count(row: dict, key: str) -> int:
    return int(row.get(key) or 0)


def _later(a, b):
    return b if a is None or (b is not None and b > a) else a


def _earlier(a, b):
    return b if a is None or (b is not None and b < a) else a


def groups(states: Iterable[dict]) -> Dict[str, dict]:
    """Per ingestion group: enabled tables, tables without a freshness target, tables by state, its
    databases and servers (with an enabled table), its first server by name, and its newest success.
    Every group with a control row is listed, even one whose tables are all disabled."""
    out: Dict[str, dict] = {}
    for s in states:
        name = s["task_collection"]
        g = out.setdefault(name, {"task_collection": name, "enabled_tables": 0, "no_sla_tables": 0,
                                  **{k: 0 for k in STATES}, "_databases": set(), "_servers": set(),
                                  "newest_success_at": None})
        g["enabled_tables"] += _count(s, "tables")
        g["no_sla_tables"] += _count(s, "no_sla_tables")
        for k in STATES:
            g[k] += _count(s, k)
        if _count(s, "tables"):
            g["_databases"].add((s["server"], s["src_database"]))
            g["_servers"].add(s["server"])
        g["newest_success_at"] = _later(g["newest_success_at"], s.get("newest_success_at"))
    for g in out.values():
        servers = sorted(g.pop("_servers"))
        g["databases"] = len(g.pop("_databases"))
        g["servers"] = len(servers)
        g["server"] = servers[0] if servers else None
    return out


def servers(states: Iterable[dict], running_groups: Optional[set] = None) -> List[dict]:
    """Per server, by name: its databases (with an enabled table), enabled tables, the ingestion groups
    with a row on it, tables by state, the oldest and newest success, and how many of its groups are
    running now."""
    running_groups = running_groups or set()
    out: Dict[str, dict] = {}
    for s in states:
        v = out.setdefault(s["server"], {"server": s["server"], "tables": 0, **{k: 0 for k in STATES},
                                         "_databases": set(), "_groups": set(),
                                         "oldest_success_at": None, "newest_success_at": None})
        v["tables"] += _count(s, "tables")
        for k in STATES:
            v[k] += _count(s, k)
        if _count(s, "tables"):
            v["_databases"].add(s["src_database"])
        v["_groups"].add(s["task_collection"])
        v["oldest_success_at"] = _earlier(v["oldest_success_at"], s.get("oldest_success_at"))
        v["newest_success_at"] = _later(v["newest_success_at"], s.get("newest_success_at"))
    rows = []
    for v in sorted(out.values(), key=lambda v: str(v["server"])):
        group_names = v.pop("_groups")
        v["databases"] = len(v.pop("_databases"))
        v["collections"] = len(group_names)
        v["running_collections"] = len(group_names & running_groups)
        rows.append(v)
    return rows


def _on_connection(s: dict, name: str, host: str) -> bool:
    return s.get("src_connection") == name or (bool(host) and (s.get("src_host") or "") == host.lower())


def connection_databases(states: Iterable[dict], name: str, host: Optional[str]) -> List[dict]:
    """The databases lakefed tracks on one UC connection, by name: control rows naming the connection or
    its host (case-insensitively). Each with its ingestion groups, enabled tables and newest success of any
    of its tables, enabled or not."""
    out: Dict[str, dict] = {}
    for s in states:
        if not _on_connection(s, name, host or ""):
            continue
        d = out.setdefault(s["src_database"], {"src_database": s["src_database"], "_groups": set(), "tables": 0,
                                               "newest_success_at": None})
        d["_groups"].add(s["task_collection"])
        d["tables"] += _count(s, "tables")
        d["newest_success_at"] = _later(d["newest_success_at"], s.get("newest_any_success_at"))
    rows = []
    for d in sorted(out.values(), key=lambda d: str(d["src_database"])):
        d["collections"] = ", ".join(sorted(d.pop("_groups")))
        rows.append(d)
    return rows


def tracked(states: Iterable[dict], name: str, host: Optional[str]) -> dict:
    """What lakefed tracks on one UC connection: databases, enabled tables and ingestion groups."""
    dbs = connection_databases(states, name, host)
    group_names = {g.strip() for d in dbs for g in d["collections"].split(",") if g.strip()}
    return {"databases": len(dbs), "tables": sum(d["tables"] for d in dbs), "collections": len(group_names)}
