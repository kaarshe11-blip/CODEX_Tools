import { createHash } from "node:crypto";
import { AUTH_PREFIX, GQB_PREFIX, REQUEST_ID_RE } from "./constants.js";

export class ValidationError extends Error {
  constructor(message, code = "INVALID_ARGUMENT") {
    super(message);
    this.name = "ValidationError";
    this.code = code;
  }
}

export function normalizeRequestId(requestId, options = {}) {
  const { allowReservedAuth = false } = options;
  if (typeof requestId !== "string" || requestId.trim() !== requestId || requestId.length === 0) {
    throw new ValidationError("request_id must be a non-empty string");
  }

  const upstreamRequestId = requestId.startsWith(GQB_PREFIX) ? requestId : `${GQB_PREFIX}${requestId}`;
  if (!allowReservedAuth && upstreamRequestId.startsWith(AUTH_PREFIX)) {
    throw new ValidationError("caller request_id must not use reserved gqb:auth namespace");
  }
  if (!REQUEST_ID_RE.test(upstreamRequestId)) {
    throw new ValidationError("request_id normalizes outside upstream requestId contract");
  }
  return upstreamRequestId;
}

export function authorizationRequestId(upstreamSubmitRequestId) {
  if (typeof upstreamSubmitRequestId !== "string" || !REQUEST_ID_RE.test(upstreamSubmitRequestId)) {
    throw new ValidationError("upstream submit request id is invalid");
  }
  const h = createHash("sha256").update(upstreamSubmitRequestId, "utf8").digest("hex").slice(0, 24);
  return `${AUTH_PREFIX}${h}`;
}

export function validateDecisionText(decisionText, { min = 1, max = 20000, field = "decision_text" } = {}) {
  if (typeof decisionText !== "string" || decisionText.length < min || decisionText.length > max) {
    throw new ValidationError(`${field} must be ${min}-${max} characters`);
  }
}

export function validateMaxAttempts(maxAttemptsPerTask, { required = false } = {}) {
  if (typeof maxAttemptsPerTask === "undefined" || maxAttemptsPerTask === null) {
    if (required) throw new ValidationError("max_attempts_per_task is required");
    return 4;
  }
  if (!Number.isInteger(maxAttemptsPerTask) || maxAttemptsPerTask < 1 || maxAttemptsPerTask > 10) {
    throw new ValidationError("max_attempts_per_task must be an integer from 1 through 10");
  }
  return maxAttemptsPerTask;
}

export function validateJiraKey(key) {
  if (typeof key !== "string" || key.length < 3 || key.length > 64 || !/^[A-Z][A-Z0-9]+-\d+$/.test(key)) {
    throw new ValidationError("ordered_jira_keys must contain valid Jira issue keys");
  }
}

export function toControllerTaskPlan(orderedJiraKeys) {
  if (!Array.isArray(orderedJiraKeys) || orderedJiraKeys.length < 1 || orderedJiraKeys.length > 50) {
    throw new ValidationError("ordered_jira_keys must contain 1-50 keys");
  }
  const seen = new Set();
  return orderedJiraKeys.map((jiraKey, index) => {
    validateJiraKey(jiraKey);
    if (seen.has(jiraKey)) {
      throw new ValidationError("ordered_jira_keys must not contain duplicates");
    }
    seen.add(jiraKey);
    return { jiraKey, position: index + 1 };
  });
}
