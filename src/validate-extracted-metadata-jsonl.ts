import fs from "node:fs";

const REQUIRED = new Set([
  "program_title",
  "episode_no",
  "subtitle",
  "air_date",
  "confidence",
  "needs_review",
  "model",
  "extraction_version",
  "normalized_program_key",
  "evidence",
]);

export type ValidateResult = {
  ok: boolean;
  rows: number;
  errors: string[];
};

// extracted metadata JSONL を検証する。
// - 必須キー
// - confidence の範囲
// - needs_review の型
// - 拡張キー(genre/broadcaster/channel)の型
export function validateExtractedMetadataJsonl(inputPath: string, maxErrors = 20): ValidateResult {
  const errors: string[] = [];
  let rows = 0;

  const text = fs.readFileSync(inputPath, "utf-8");
  const lines = text.split(/\r?\n/);

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i].trim();
    if (!line) continue;

    let obj: any;
    try {
      obj = JSON.parse(line);
    } catch {
      errors.push(`line ${i + 1}: invalid json`);
      if (errors.length >= maxErrors) break;
      continue;
    }

    if (obj && typeof obj === "object" && "_meta" in obj) continue;

    rows += 1;

    if (!(obj.path_id || obj.path)) {
      errors.push(`line ${i + 1}: missing path_id/path`);
    }

    for (const k of REQUIRED) {
      if (!(k in obj)) errors.push(`line ${i + 1}: missing key ${k}`);
    }

    const c = obj.confidence;
    if (!(typeof c === "number" && Number.isFinite(c) && c >= 0 && c <= 1)) {
      errors.push(`line ${i + 1}: confidence must be 0..1`);
    }

    if (typeof obj.needs_review !== "boolean") {
      errors.push(`line ${i + 1}: needs_review must be boolean`);
    }

    for (const k of ["genre", "broadcaster", "channel"]) {
      if (k in obj && obj[k] !== null && typeof obj[k] !== "string") {
        errors.push(`line ${i + 1}: ${k} must be string|null`);
      }
    }

    if (errors.length >= maxErrors) break;
  }

  return { ok: errors.length === 0, rows, errors };
}
