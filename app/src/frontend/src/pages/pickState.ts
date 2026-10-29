// The tick states of the Chosen pickers' top rows (SourcePick and Discover). Kept free of React so a test can run it
// with node (tests/frontend_pick_test.py).

export type Pick = "all" | "part" | "none";

/** Whether a database's name contains a filter, in any case: the rule a whole-server choice covers databases by. */
export const underFilter = (database: unknown, filter: string) => String(database).toLowerCase().includes(filter.toLowerCase());

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
