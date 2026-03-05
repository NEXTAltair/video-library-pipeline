import fs from "node:fs";
import path from "node:path";
import { byProgramGroupFromPath, chooseSourceJsonl, getExtensionRootDir, latestJsonlFile, looksSwallowedProgramTitle, lowerCompact, resolvePythonScript, runCmd, toToolResult, tsCompact } from "./runtime";
import type { AnyObj } from "./types";

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

function looksSwallowedProgramTitleInRow(row: AnyObj): boolean {
  const programTitle = typeof row.program_title === "string" ? row.program_title : "";
  const folderTitle = byProgramGroupFromPath(typeof row.path === "string" ? row.path : undefined);
  if (!folderTitle) return false;
  return looksSwallowedProgramTitle(programTitle, folderTitle);
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
      description:
        "Apply reviewed extracted metadata JSONL to DB and mark rows as human-reviewed. " +
        "IMPORTANT: sourceJsonlPath must be a human-edited copy of the extraction output — NOT the original llm_filename_extract_output_*.jsonl (that filename is rejected). " +
        "If video_pipeline_apply_llm_extract_output returned needsReviewFlagRows=0, do NOT call this tool — records are already in DB; proceed to video_pipeline_relocate_existing_files instead.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          sourceJsonlPath: { type: "string", description: "Path to the human-edited reviewed JSONL (must NOT be a llm_filename_extract_output_*.jsonl raw file, and must NOT be a .yaml file)." },
          outputStampedJsonlPath: { type: "string" },
          markHumanReviewed: { type: "boolean", default: true },
          allowNoContentChanges: { type: "boolean", default: false },
          reviewedBy: { type: "string" },
          source: { type: "string", default: "rule_based" },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const hostRoot = String(cfg.windowsOpsRoot || "/tmp").replace(/\/+$/, "");
        const llmDir = path.join(hostRoot, "llm");
        const resolved = resolvePythonScript("upsert_path_metadata_jsonl.py");
        const franchiseRulesPath = path.join(getExtensionRootDir(), "rules", "franchise_rules.yaml");
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

        // Gate 1: raw 抽出ファイルは allowNoContentChanges でもバイパス不可
        if (markHumanReviewed && sourceLooksRawExtractionOutput) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_apply_reviewed_metadata",
            error: "refusing to mark raw extraction output as human-reviewed; this check cannot be bypassed",
            sourceJsonlPath: source.path,
            sourceBaseName: sourceBase,
            sourceLooksRawExtractionOutput: true,
            hint: "Copy the extraction output, review/edit it, then apply the reviewed copy.",
          });
        }

        // Gate 2: 変更なしチェック (allowNoContentChanges で合法的にバイパス可能)
        if (markHumanReviewed && !allowNoContentChanges) {
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

        // --- auto backup before DB write ---
        const backupResolved = resolvePythonScript("backup_mediaops_db.py");
        const backupResult = runCmd("uv", [
          "run", "python", backupResolved.scriptPath,
          "--db", String(cfg.db || ""),
          "--action", "backup",
          "--descriptor", "pre_apply_reviewed_metadata",
        ], backupResolved.cwd);
        if (backupResult.code !== 0) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_apply_reviewed_metadata",
            error: "pre-apply DB backup failed; aborting to protect data",
            backupStderr: backupResult.stderr,
          });
        }
        // auto rotate after successful backup
        runCmd("uv", [
          "run", "python", backupResolved.scriptPath,
          "--db", String(cfg.db || ""),
          "--action", "rotate",
          "--keep", "10",
        ], backupResolved.cwd);

        const upsertArgs = [
          "run",
          "python",
          resolved.scriptPath,
          "--db",
          String(cfg.db || ""),
          "--in",
          outputStampedJsonlPath,
          "--source",
          String(params.source || "rule_based"),
          "--franchise-rules",
          franchiseRulesPath,
        ];
        const r = runCmd("uv", upsertArgs, resolved.cwd);

        // Archive program_aliases_review_*.yaml files after successful DB write
        const archivedYamls: string[] = [];
        if (r.ok) {
          const archiveDir = path.join(llmDir, "archive");
          try {
            const yamlFiles = fs.readdirSync(llmDir)
              .filter((n) => n.startsWith("program_aliases_review_") && n.endsWith(".yaml"));
            if (yamlFiles.length > 0) {
              fs.mkdirSync(archiveDir, { recursive: true });
              for (const name of yamlFiles) {
                const src = path.join(llmDir, name);
                const dst = path.join(archiveDir, name);
                fs.renameSync(src, dst);
                archivedYamls.push(name);
              }
            }
          } catch {
            // archive failure is non-fatal
          }
        }

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
          archivedYamls,
        });
      },
    },
    { optional: true },
  );
}
