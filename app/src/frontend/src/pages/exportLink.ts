/** Manage's export link: the ingestion jobs ticked, or every job when none is. */
export function exportHref(ids: number[]): string {
  return ids.length ? `/api/manage/export.csv?jobs=${ids.join(",")}` : "/api/manage/export.csv";
}
