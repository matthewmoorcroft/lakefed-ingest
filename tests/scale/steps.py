"""The scale steps. Each one runs the code the app or the discover job runs, against the fakes in simulation.py and
lakebase_fake.py, and reports what it sent (calls, rows, statements) and how long the Python took. Server times come from
the ledger (see simulation.py): they are simulated from the latency given, not measured.
"""
from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

import paths  # noqa: F401  (sys.path)
import server_catalog
import simulation as sim

# discover_tables.ipynb runs at most 8 databases at once (ThreadPoolExecutor(max_workers=min(8, databases))), each one's
# three catalog queries in turn. The design's cap is 32 queries at a time under the server's limit.
NOTEBOOK_WORKERS = 8
DESIGN_WORKERS = 32

FoundCallback = Callable[[str, List[dict]], None]


@dataclass
class DiscoveryRun:
    """One discover run over a server: what the listing and each database cost, and what discovery found."""
    listed: List[str]
    refused: List[str]
    retried: List[str]
    discovered: int
    tables: int
    ledger: Dict[str, sim.Ledger]
    calls_by_kind: Counter
    rows_by_kind: Counter
    retry_waits_s: float
    cpu_s: float
    azure: bool = False
    order: List[str] = field(default_factory=list)

    def calls(self) -> int:
        return sum(self.calls_by_kind.values())

    def wall_s(self, latency: sim.Latency, workers: int) -> float:
        """Simulated wall time: the listing (one call, or the Azure pages in order), then the databases on a pool."""
        listing_key = "azure" if self.azure else sim.MASTER
        listing = self.ledger[listing_key].seconds(latency) if listing_key in self.ledger else 0.0
        return listing + sim.pool_makespan([self.ledger[name].seconds(latency) for name in self.order], workers)


def discover(catalog: server_catalog.ServerCatalog, pattern: str = "", via_azure: bool = False,
             limit: Optional[int] = None, on_found: Optional[FoundCallback] = None) -> DiscoveryRun:
    """Run the discover job's reads over ``catalog``: list its databases (through master, or through Azure's list), then
    discover each one in listing order, as discover_tables.ipynb does. ``on_found(database, rows)`` gets each database's
    rows as they are read, so a whole server is never held in memory. ``limit`` takes the first databases of the
    listing (for the small runs)."""
    server = sim.SimulatedServer(catalog)
    spec = sim.connection_spec(pattern=pattern)
    started = time.perf_counter()
    refused: List[str] = []
    retried: List[str] = []
    discovered = tables_found = 0
    with sim.discovery_boundary(server):
        listed = sim.list_through_azure(server, spec) if via_azure else sim.list_through_master(spec)
        if limit is not None:
            listed = listed[:limit]
        for name in listed:
            waits_before = server.ledger[name].waits_s if name in server.ledger else 0.0
            try:
                found = sim.discover_one(server, spec, name)
            except RuntimeError:
                refused.append(name)
                continue
            if server.ledger[name].waits_s > waits_before:
                retried.append(name)
            discovered += 1
            tables_found += len(found)
            if on_found is not None:
                on_found(name, found)
    return DiscoveryRun(listed=listed, refused=refused, retried=retried, discovered=discovered, tables=tables_found,
                        ledger=server.ledger, calls_by_kind=server.calls_by_kind, rows_by_kind=server.rows_by_kind,
                        retry_waits_s=sum(server.waits_by_kind.values()), cpu_s=time.perf_counter() - started,
                        azure=via_azure, order=list(listed))
