// Tests for the pickers' pure helpers (app/src/frontend/src/pages/pickState.ts). Run with `node --test`;
// frontend_pick_test.py runs this file from pytest.
import { test } from "node:test";
import assert from "node:assert/strict";
import { likeContaining } from "../../src/frontend/src/pages/pickState.ts";

test("a database filter becomes the SQL Server LIKE pattern for names containing it, its wildcards taken literally", () => {
  assert.equal(likeContaining(""), null);
  assert.equal(likeContaining("eng_0"), "%eng[_]0%");
  assert.equal(likeContaining("50%"), "%50[%]%");
  assert.equal(likeContaining("a[b]"), "%a[[]b]%");
  assert.equal(likeContaining("lfc.eng-1$#@"), "%lfc.eng-1$#@%");
});

test("a filter with a character the listing can't take reads the whole server instead", () => {
  assert.equal(likeContaining("eng 0"), null);
  assert.equal(likeContaining("eng'0"), null);
  assert.equal(likeContaining("x".repeat(127)), null, "longer than the 128 characters a pattern takes once wrapped");
});
