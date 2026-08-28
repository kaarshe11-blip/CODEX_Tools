import { createHash } from "node:crypto";
import { canonicalJson } from "./canonical-json.js";

export function payloadHash(payload) {
  return createHash("sha256").update(canonicalJson(payload), "utf8").digest("hex");
}
