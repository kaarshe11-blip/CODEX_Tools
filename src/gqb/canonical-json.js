export function canonicalize(value) {
  if (value === null || typeof value !== "object") {
    if (typeof value === "undefined") return null;
    if (Number.isNaN(value)) throw new TypeError("NaN is not valid JSON");
    if (value === Infinity || value === -Infinity) throw new TypeError("Infinity is not valid JSON");
    return value;
  }

  if (Array.isArray(value)) {
    return value.map((item) => canonicalize(item));
  }

  const out = {};
  for (const key of Object.keys(value).sort()) {
    const child = value[key];
    out[key] = typeof child === "undefined" ? null : canonicalize(child);
  }
  return out;
}

export function canonicalJson(value) {
  return JSON.stringify(canonicalize(value));
}
