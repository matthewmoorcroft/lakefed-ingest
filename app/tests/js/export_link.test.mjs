// Tests for Manage's export link (app/src/frontend/src/pages/exportLink.ts). Run with `node --test`.
import { test } from "node:test";
import assert from "node:assert/strict";
import { exportHref } from "../../src/frontend/src/pages/exportLink.ts";

test("no job ticked exports every ingestion job; ticked ones are named by id", () => {
  assert.equal(exportHref([]), "/api/manage/export.csv");
  assert.equal(exportHref([7]), "/api/manage/export.csv?jobs=7");
  assert.equal(exportHref([7, 12]), "/api/manage/export.csv?jobs=7,12");
});
