// The tick states of the Chosen pickers' top rows (SourcePick and Discover). Kept free of React so a test can run it
// with node (tests/frontend_pick_test.py).

export type Pick = "all" | "part" | "none";

/** Whether a database's name contains a filter, in any case: the rule a whole-server choice covers databases by. */
export const underFilter = (database: unknown, filter: string) => String(database).toLowerCase().includes(filter.toLowerCase());

/** The SQL Server LIKE pattern for the database names containing a filter, as underFilter matches them, so reading a
 *  whole server under a filter reads only those databases. Its wildcards (_, % and [) are taken literally. Null when
 *  there is no filter, or when it has a character the server's listing doesn't take: the whole server is read then. */
export function likeContaining(filter: string): string | null {
  if (!filter) return null;
  const pattern = `%${filter.replace(/\[/g, "[[]").replace(/_/g, "[_]").replace(/%/g, "[%]")}%`;
  return /^[A-Za-z0-9_%\-\[\].$#@]{1,128}$/.test(pattern) ? pattern : null;
}

/**
 * The server row's tick. It is ticked when the whole server is chosen under a filter that covers every database the page
 * shows (an empty filter covers them all; a filter covers the databases whose names contain it). It is in part when
 * anything else is chosen, and empty when nothing is.
 */
export function serverRowPick(whole: { on: boolean; filter: string }, pageFilter: string, anythingElseChosen: boolean): Pick {
  const covers = whole.on && (whole.filter === "" || underFilter(pageFilter, whole.filter));
  if (covers) return "all";
  return whole.on || anythingElseChosen ? "part" : "none";
}
