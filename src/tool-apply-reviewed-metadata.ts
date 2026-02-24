import fs from "node:fs";
import path from "node:path";
import { latestJsonlFile, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

function tsCompact(d = new Date()): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

function chooseSourceJsonl(llmDir: string, sourceJsonlPath: string | undefined): { ok: boolean; path?: string; error?: string } {
  const p = String(sourceJsonlPath || "").trim();
  if (p) {
    if (!fs.existsSync(p)) return { ok: false, error: `sourceJsonlPath does not exist: ${p}` };
    return { ok: true, path: p };
  }
  const latest = latestJsonlFile(llmDir, "llm_filename_extract_output_");
  if (!latest) return { ok: false, error: `no extraction output jsonl found in: ${llmDir}` };
  return { ok: true, path: latest };
}

function isRawExtractOutputJsonl(p: string): boolean {
  const base = path.basename(p);
  return /^llm_filename_extract_output_\d{4}_\d{4}\.jsonl$/i.test(base);
}

function inferBaselineExtractOutputPath(reviewedPath: string): string | null {
  const base = path.basename(reviewedPath);
  const m = base.match(/^(llm_filename_extract_output_\d{4}_\d{4})_reviewed_[0-9_]+\.jsonl$/i);
  if (!m) return null;
  return path.join(path.dirname(reviewedPath), `${m[1]}.jsonl`);
}

function stripReviewAuditFields(row: AnyObj): AnyObj {
  const out: AnyObj = { ...row };
  delete out.human_reviewed;
  delete out.human_reviewed_at;
  delete out.human_reviewed_by;
  return out;
}

function stableStringify(v: unknown): string {
  if (v === null || typeof v !== "object") return JSON.stringify(v);
  if (Array.isArray(v)) return `[${v.map((x) => stableStringify(x)).join(",")}]`;
  const obj = v as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(obj[k])}`).join(",")}}`;
}

function lowerCompact(s: string): string {
  return String(s || "")
    .normalize("NFKC")
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/[<>:"/\\|?*]+/g, "");
}

function byProgramGroupFromPath(p: string | undefined): string | undefined {
  const parts = String(p || "").split("\\");
  const idx = parts.findIndex((seg) => seg.toLowerCase() === "by_program");
  if (idx >= 0 && idx + 1 < parts.length) return parts[idx + 1];
  return undefined;
}

function looksSwallowedProgramTitleInRow(row: AnyObj): boolean {
  const programTitle = typeof row.program_title === "string" ? row.program_title : "";
  const folderTitle = byProgramGroupFromPath(typeof row.path === "string" ? row.path : undefined);
  if (!folderTitle) return false;
  const pNorm = lowerCompact(programTitle);
  const fNorm = lowerCompact(folderTitle);
  if (!pNorm || !fNorm || pNorm === fNorm) return false;
  if (!pNorm.startsWith(fNorm)) return false;
  return pNorm.length >= fNorm.length + 8;
}

function readComparableRows(sourcePath: string): { rows: AnyObj[]; parseErrors: number } {
  const out: AnyObj[] = [];
  let parseErrors = 0;
  const lines = fs.readFileSync(sourcePath, "utf-8").split(/\r?\n/);
  for (let i = 0; i < lines.length; i++) {
    const raw = i === 0 ? lines[i].replace(/^\uFEFF/, "") : lines[i];
    const s = raw.trim();
    if (!s) continue;
    try {
      const obj = JSON.parse(s);
      if (!obj || typeof obj !== "object" || Array.isArray(obj)) continue;
      if ("_meta" in obj) continue;
      out.push(stripReviewAuditFields(obj as AnyObj));
    } catch {
      parseErrors += 1;
    }
  }
  return { rows: out, parseErrors };
}

function summarizeReviewRisk(rows: AnyObj[]): {
  rows: number;
  needsReviewRows: number;
  suspiciousProgramTitleRows: number;
} {
  let needsReviewRows = 0;
  let suspiciousProgramTitleRows = 0;
  for (const row of rows) {
    if (row && row.needs_review === true) needsReviewRows += 1;
    if (looksSwallowedProgramTitleInRow(row)) suspiciousProgramTitleRows += 1;
  }
  return { rows: rows.length, needsReviewRows, suspiciousProgramTitleRows };
}

function compareReviewContent(editedPath: string, baselinePath: string): {
  comparable: boolean;
  changedRowsCount: number;
  changedFieldsCountEstimate: number;
  baselineRows: number;
  editedRows: number;
  parseErrors: number;
} {
  const base = readComparableRows(baselinePath);
  const edited = readComparableRows(editedPath);
  if (base.parseErrors || edited.parseErrors) {
    return {
      comparable: false,
      changedRowsCount: 0,
      changedFieldsCountEstimate: 0,
      baselineRows: base.rows.length,
      editedRows: edited.rows.length,
      parseErrors: base.parseErrors + edited.parseErrors,
    };
  }
  const n = Math.max(base.rows.length, edited.rows.length);
  let changedRowsCount = 0;
  let changedFieldsCountEstimate = 0;
  for (let i = 0; i < n; i++) {
    const a = i < base.rows.length ? stableStringify(base.rows[i]) : "__MISSING__";
    const b = i < edited.rows.length ? stableStringify(edited.rows[i]) : "__MISSING__";
    if (a !== b) {
      changedRowsCount += 1;
      changedFieldsCountEstimate += 1;
    }
  }
  return {
    comparable: true,
    changedRowsCount,
    changedFieldsCountEstimate,
    baselineRows: base.rows.length,
    editedRows: edited.rows.length,
    parseErrors: 0,
  };
}

function stampReviewedRows(
  sourcePath: string,
  markHumanReviewed: boolean,
  reviewedBy: string | undefined,
): { rows: AnyObj[]; parseErrors: number } {
  const out: AnyObj[] = [];
  let parseErrors = 0;
  const lines = fs.readFileSync(sourcePath, "utf-8").split(/\r?\n/);
  for (let i = 0; i < lines.length; i++) {
    const raw = i === 0 ? lines[i].replace(/^\uFEFF/, "") : lines[i];
    const s = raw.trim();
    if (!s) continue;
    try {
      const obj = JSON.parse(s);
      if (!obj || typeof obj !== "object" || Array.isArray(obj)) continue;
      if ("_meta" in obj) continue;
      const row: AnyObj = { ...obj };
      if (markHumanReviewed) {
        row.human_reviewed = true;
        row.human_reviewed_at = new Date().toISOString();
        if (reviewedBy) row.human_reviewed_by = reviewedBy;
      }
      out.push(row);
    } catch {
      parseErrors += 1;
    }
  }
  return { rows: out, parseErrors };
}

function writeJsonlRows(target: string, rows: AnyObj[]) {
  const body = rows.map((r) => JSON.stringify(r)).join("\n");
  fs.mkdirSync(path.dirname(target), { recursive: true });
  fs.writeFileSync(target, `${body}${rows.length > 0 ? "\n" : ""}`, "utf-8");
}

export function registerToolApplyReviewedMetadata(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_apply_reviewed_metadata",
      description: "Apply reviewed extracted metadata JSONL to DB and mark rows as human-reviewed.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          sourceJsonlPath: { type: "string" },
          outputStampedJsonlPath: { type: "string" },
          markHumanReviewed: { type: "boolean", default: true },
          allowNoContentChanges: { type: "boolean", default: false },
          reviewedBy: { type: "string" },
          source: { type: "string", default: "llm" },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const hostRoot = String(cfg.windowsOpsRoot || "/tmp").replace(/\/+$/, "");
        const llmDir = path.join(hostRoot, "llm");
        const resolved = resolvePythonScript("upsert_path_metadata_jsonl.py");
        const source = chooseSourceJsonl(llmDir, typeof params.sourceJsonlPath === "string" ? params.sourceJsonlPath : undefined);
        if (!source.ok || !source.path) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_apply_reviewed_metadata",
            error: source.error ?? "failed to resolve source jsonl",
            llmDir,
          });
        }

        const markHumanReviewed = params.markHumanReviewed !== false;
        const allowNoContentChanges = params.allowNoContentChanges === true;
        const reviewedBy = typeof params.reviewedBy === "string" && params.reviewedBy.trim() ? params.reviewedBy.trim() : undefined;

        const sourceBase = path.basename(source.path);
        const sourceLooksRawExtractionOutput = isRawExtractOutputJsonl(source.path);
        const reviewBaselinePath = inferBaselineExtractOutputPath(source.path);
        const reviewBaselineExists = reviewBaselinePath ? fs.existsSync(reviewBaselinePath) : false;
        const diff =
          reviewBaselinePath && reviewBaselineExists ? compareReviewContent(source.path, reviewBaselinePath) : null;
        const sourceComparable = readComparableRows(source.path);
        const reviewRiskSummary = summarizeReviewRisk(sourceComparable.rows);

        if (markHumanReviewed && !allowNoContentChanges) {
          if (sourceLooksRawExtractionOutput) {
            return toToolResult({
              ok: false,
              tool: "video_pipeline_apply_reviewed_metadata",
              error:
                "refusing to mark raw extraction output as human-reviewed without explicit override; edit a reviewed JSONL copy or set allowNoContentChanges=true if intentionally accepting machine output as-is",
              sourceJsonlPath: source.path,
              sourceBaseName: sourceBase,
              sourceLooksRawExtractionOutput: true,
              hint: "Use a *_reviewed_*.jsonl file for human edits, then apply it.",
            });
          }
          if (diff && diff.comparable && diff.changedRowsCount === 0) {
            return toToolResult({
              ok: false,
              tool: "video_pipeline_apply_reviewed_metadata",
              error:
                "no content edits detected between reviewed JSONL and its baseline extraction output; refusing to mark as human-reviewed without explicit override",
              sourceJsonlPath: source.path,
              reviewBaselinePath,
              reviewBaselineExists,
              sourceLooksRawExtractionOutput: false,
              reviewDiff: diff,
              hint: "Edit the reviewed JSONL content first, or set allowNoContentChanges=true if the human intentionally accepted all rows unchanged.",
            });
          }
        }
        if (
          markHumanReviewed &&
          allowNoContentChanges &&
          (reviewRiskSummary.suspiciousProgramTitleRows > 0 || reviewRiskSummary.needsReviewRows > 0)
        ) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_apply_reviewed_metadata",
            error:
              "allowNoContentChanges=true is not permitted while review-risk rows remain (suspicious program_title and/or needs_review=true present)",
            sourceJsonlPath: source.path,
            sourceBaseName: sourceBase,
            sourceLooksRawExtractionOutput,
            reviewBaselinePath,
            reviewBaselineExists,
            reviewDiff: diff,
            reviewRiskSummary,
            hint:
              "Edit the reviewed JSONL first (program_title / needs_review fields), then re-run without allowNoContentChanges. This override is only for true no-change approvals with no remaining review-risk rows.",
          });
        }

        const stamped = stampReviewedRows(source.path, markHumanReviewed, reviewedBy);
        if (stamped.rows.length === 0) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_apply_reviewed_metadata",
            error: "no valid metadata rows found in source jsonl",
            sourceJsonlPath: source.path,
            parseErrors: stamped.parseErrors,
          });
        }

        const outputStampedJsonlPath =
          typeof params.outputStampedJsonlPath === "string" && params.outputStampedJsonlPath.trim()
            ? params.outputStampedJsonlPath.trim()
            : path.join(llmDir, `reviewed_metadata_apply_${tsCompact()}.jsonl`);
        writeJsonlRows(outputStampedJsonlPath, stamped.rows);

        const upsertArgs = [
          "run",
          "python",
          resolved.scriptPath,
          "--db",
          String(cfg.db || ""),
          "--in",
          outputStampedJsonlPath,
          "--source",
          String(params.source || "llm"),
        ];
        const r = runCmd("uv", upsertArgs, resolved.cwd);
        return toToolResult({
          ok: r.ok,
          tool: "video_pipeline_apply_reviewed_metadata",
          scriptSource: resolved.source,
          scriptPath: resolved.scriptPath,
          exitCode: r.code,
          stdout: r.stdout,
          stderr: r.stderr,
          sourceJsonlPath: source.path,
          outputStampedJsonlPath,
          rows: stamped.rows.length,
          parseErrors: stamped.parseErrors,
          markHumanReviewed,
          allowNoContentChanges,
          reviewedBy: reviewedBy ?? null,
          sourceBaseName: sourceBase,
          sourceLooksRawExtractionOutput,
          reviewBaselinePath,
          reviewBaselineExists,
          reviewDiff: diff,
          reviewRiskSummary,
          sourceParseErrors: sourceComparable.parseErrors,
        });
      },
    },
    { optional: true },
  );
}
