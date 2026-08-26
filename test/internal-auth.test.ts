import assert from "node:assert/strict";
import test from "node:test";
import { hasInternalKey } from "../src/internal-auth.ts";

test("accepts the primary and temporary overlap keys", () => {
  const env = { BWM_INTERNAL_KEY: "primary", BWM_INTERNAL_KEY_NEXT: "next" };
  assert.equal(hasInternalKey("primary", env), true);
  assert.equal(hasInternalKey("next", env), true);
  assert.equal(hasInternalKey("wrong", env), false);
});

test("never treats an absent or empty key as authorized", () => {
  assert.equal(hasInternalKey("", { BWM_INTERNAL_KEY: "primary" }), false);
  assert.equal(
    hasInternalKey("", { BWM_INTERNAL_KEY: "primary", BWM_INTERNAL_KEY_NEXT: "" }),
    false,
  );
});

test("requires the primary key even during overlap", () => {
  assert.equal(
    hasInternalKey("next", { BWM_INTERNAL_KEY: "", BWM_INTERNAL_KEY_NEXT: "next" }),
    false,
  );
});
