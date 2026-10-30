"""Which group each database of a job goes to. A server gets the number of groups the job sets, or by default one for
about every TABLES_PER_GROUP of its tables, with no group of its own for a small remainder. Its databases are spread
so each group has about the same number of tables: the biggest first, each into the group with the fewest tables so
far. A database is never split: all its tables are in one group. Databases added to a job later each join their
server's group with the fewest tables at that time, and nothing already placed moves. Pure, so it is tested without a
database."""
from typing import Dict, Iterable, List, Optional, Tuple

TABLES_PER_GROUP = 12_500
# A remainder of up to this share of a group joins the other groups instead of making a group of its own: 100,200
# tables make 8 groups, not 9.
REMAINDER_SHARE = 0.1


def groups_for_tables(tables: int, per_group: int = TABLES_PER_GROUP) -> int:
    """The default number of groups for a server's tables: one per per_group tables, a remainder of more than a tenth
    of a group adding one more, and at least one."""
    whole, remainder = divmod(max(0, int(tables)), per_group)
    return max(1, whole + (1 if remainder > per_group * REMAINDER_SHARE else 0))


def even(databases: Iterable[Tuple[str, int]], groups: int) -> Dict[str, int]:
    """{database name, lower case: its group's index, from 0}, for a new job's databases of one server, given as
    (name, tables). The biggest database goes first, each into the group with the fewest tables so far, the lowest
    index on a tie, so the same databases always land the same way (a resumed Create places them again identically)."""
    sizes: Dict[str, int] = {}
    for name, tables in databases:
        key = str(name or "").lower()
        sizes[key] = sizes.get(key, 0) + int(tables or 0)
    loads = [0] * max(1, int(groups))
    out: Dict[str, int] = {}
    for name, tables in sorted(sizes.items(), key=lambda item: (-item[1], item[0])):
        index = min(range(len(loads)), key=lambda i: (loads[i], i))
        out[name] = index
        loads[index] += tables
    return out


def server_placements(rows: Iterable[Tuple[str, str, int]], groups: Dict[str, int]) -> Dict[str, Dict[str, int]]:
    """{server: even placement of its databases}, from (server, database, tables) rows and {server: its groups}."""
    by_server: Dict[str, List[Tuple[str, int]]] = {}
    for server, database, tables in rows:
        by_server.setdefault(server, []).append((database, tables))
    return {server: even(databases, groups.get(server, 1)) for server, databases in by_server.items()}


class Lightest:
    """Places the databases added to an existing job: each new one joins its server's group with the fewest tables now
    (the first by name on a tie), and every table of it counts there, so the next new database sees it. place() is
    called once per table, as a plan reads its lines; a database already placed keeps its group."""

    def __init__(self, tables_by_group: Dict[str, Dict[str, int]]):
        self._loads = {server: dict(groups) for server, groups in tables_by_group.items()}
        self._placed: Dict[Tuple[str, str], str] = {}

    def knows(self, server: str) -> bool:
        return bool(self._loads.get(server))

    def place(self, server: str, database: str, tables: int = 1) -> Optional[str]:
        """The group of a new database's tables (one table each call by default), or None when the server has no group
        in the job yet."""
        loads = self._loads.get(server)
        if not loads:
            return None
        key = (server, str(database or "").lower())
        group = self._placed.get(key)
        if group is None:
            group = min(sorted(loads), key=lambda name: loads[name])
            self._placed[key] = group
        loads[group] += tables
        return group
