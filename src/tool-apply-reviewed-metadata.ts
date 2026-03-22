import fs from "node:fs";
import os from "node:os";
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

const SUBTITLE_SEPARATOR_RE = /[▽▼◇]/;

function summarizeReviewRisk(rows: AnyObj[]): {
  rows: number;
  needsReviewRows: number;
  suspiciousProgramTitleRows: number;
} {
  let needsReviewRows = 0;
  let suspiciousProgramTitleRows = 0;
  for (const row of rows) {
    if (row && row.needs_review === true) needsReviewRows += 1;
    if (looksSwallowedProgramTitleInRow(row)) {
      suspiciousProgramTitleRows += 1;
    } else {
      const pt = typeof row?.program_title === "string" ? row.program_title : "";
      if (SUBTITLE_SEPARATOR_RE.test(pt)) suspiciousProgramTitleRows += 1;
    }
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

function isYamlPath(p: string): boolean {
  return /\.(ya?ml)$/i.test(path.basename(p));
}

function parseJsonStringLiteral(s: string): string {
  try {
    const v = JSON.parse(s);
    return typeof v === "string" ? v : "";
  } catch {
    return "";
  }
}

function parseAliasMappingsFromYaml(yamlPath: string): {
  sourceJsonlPath?: string;
  aliasToCanonical: Map<string, string>;
} {
  const lines = fs.readFileSync(yamlPath, "utf-8").split(/\r?\n/);
  const aliasToCanonical = new Map<string, string>();
  let sourceJsonlPath: string | undefined;
  let currentCanonical = "";
  let inAliases = false;

  for (const line of lines) {
    const sourceMatch = line.match(/^source_jsonl:\s*(".*")\s*$/);
    if (sourceMatch) {
      const p = parseJsonStringLiteral(sourceMatch[1]);
      sourceJsonlPath = p || sourceJsonlPath;
      continue;
    }

    const canonicalMatch = line.match(/^\s*-\s+canonical_title:\s*(".*")\s*$/);
    if (canonicalMatch) {
      const canonical = parseJsonStringLiteral(canonicalMatch[1]).trim();
      currentCanonical = canonical;
      inAliases = false;
      if (canonical) aliasToCanonical.set(lowerCompact(canonical), canonical);
      continue;
    }

    if (/^\s+aliases:\s*$/.test(line)) {
      inAliases = true;
      continue;
    }

    const aliasMatch = line.match(/^\s+-\s+(".*")\s*$/);
    if (inAliases && currentCanonical && aliasMatch) {
      const alias = parseJsonStringLiteral(aliasMatch[1]).trim();
      if (alias) aliasToCanonical.set(lowerCompact(alias), currentCanonical);
      continue;
    }

    if (/^\S/.test(line)) {
      inAliases = false;
      currentCanonical = "";
    }
  }

  return { sourceJsonlPath, aliasToCanonical };
}

function applyYamlReviewToRows(rows: AnyObj[], aliasToCanonical: Map<string, string>): {
  editedRows: AnyObj[];
  changedRowsCount: number;
  retitledRowsCount: number;
  reviewClearedRowsCount: number;
} {
  // canonical titles の逆引きリストを構築 (prefix マッチ用)
  const canonicalSet = new Set(aliasToCanonical.values());
  const canonicalByNorm: Array<[string, string]> = [];
  for (const ct of canonicalSet) {
    canonicalByNorm.push([lowerCompact(ct), ct]);
  }
  // 長い順にソート → 最長一致を保証
  canonicalByNorm.sort((a, b) => b[0].length - a[0].length);

  const editedRows: AnyObj[] = [];
  let changedRowsCount = 0;
  let retitledRowsCount = 0;
  let reviewClearedRowsCount = 0;
  for (const row of rows) {
    const next: AnyObj = { ...row };
    let changed = false;
    const beforeTitle = typeof row.program_title === "string" ? row.program_title.trim() : "";
    let canonical = beforeTitle ? aliasToCanonical.get(lowerCompact(beforeTitle)) : undefined;

    // Prefix fallback: exact match がない場合、canonical_title の最長 prefix マッチを試行
    if (!canonical && beforeTitle) {
      const titleNorm = lowerCompact(beforeTitle);
      for (const [ctNorm, ct] of canonicalByNorm) {
        if (titleNorm.startsWith(ctNorm) && titleNorm.length >= ctNorm.length + 8) {
          canonical = ct;
          break; // 最長一致 (ソート済み)
        }
      }
    }

    if (canonical && canonical !== beforeTitle) {
      next.program_title = canonical;
      retitledRowsCount += 1;
      changed = true;
    }
    // needs_review をクリアするのはタイトルが実際に修正された場合のみ
    // かつ、タイトル以外の理由（missing_air_date 等）が残っていない場合のみ
    if (canonical && canonical !== beforeTitle && next.needs_review === true) {
      const reason = typeof next.needs_review_reason === "string" ? next.needs_review_reason.trim() : "";
      const titleRelatedReasons = [
        "needs_review_flagged",
        "program_title_may_include_description",
        // 抽出側 (relocate_ なし) と relocate 側 (relocate_ 付き) の両方を網羅
        "suspicious_program_title",
        "relocate_suspicious_program_title",
        "suspicious_program_title_shortened",
        "relocate_suspicious_program_title_shortened",
        "subtitle_separator_in_program_title",
        "relocate_subtitle_separator_in_program_title",
      ];
      const remainingReasons = reason.split(",").map((r: string) => r.trim()).filter((r: string) => r && !titleRelatedReasons.includes(r));
      if (remainingReasons.length === 0) {
        next.needs_review = false;
        next.needs_review_reason = "";
        reviewClearedRowsCount += 1;
        changed = true;
      }
    }
    if (changed) changedRowsCount += 1;
    editedRows.push(next);
  }
  return { editedRows, changedRowsCount, retitledRowsCount, reviewClearedRowsCount };
}

export function registerToolApplyReviewedMetadata(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_apply_reviewed_metadata",
      description:
        "Apply reviewed extracted metadata to DB and mark rows as human-reviewed. " +
        "Pass sourceYamlPath (preferred, from video_pipeline_export_program_yaml) or sourceJsonlPath (legacy). " +
        "If video_pipeline_apply_llm_extract_output returned needsReviewFlagRows=0, do NOT call this tool — records are already in DB; proceed to video_pipeline_relocate_existing_files instead. " +
        "When no content changes are detected and no review-risk rows exist (needsReviewRows=0, suspiciousProgramTitleRows=0), the tool auto-allows without requiring allowNoContentChanges=true.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          sourceJsonlPath: { type: "string", description: "Path to the reviewed extraction JSONL. If omitted, sourceYamlPath can be used instead." },
          sourceYamlPath: { type: "string", description: "Path to the reviewed YAML generated by video_pipeline_export_program_yaml." },
          markHumanReviewed: { type: "boolean", default: true },
          allowNoContentChanges: { type: "boolean", default: false },
          reviewedBy: { type: "string" },
          source: { type: "string", default: "human_reviewed" },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const hostRoot = String(cfg.windowsOpsRoot || "/tmp").replace(/\/+$/, "");
        const llmDir = path.join(hostRoot, "llm");
        const resolved = resolvePythonScript("upsert_path_metadata_jsonl.py");
        const franchiseRulesPath = path.join(getExtensionRootDir(), "rules", "franchise_rules.yaml");
        const explicitYamlPath = typeof params.sourceYamlPath === "string" ? params.sourceYamlPath.trim() : "";
        // YAML パスが指定された場合は chooseSourceJsonl を経由せず直接使う
        let source: { ok: boolean; path?: string; error?: string };
        if (explicitYamlPath) {
          if (!fs.existsSync(explicitYamlPath)) {
            source = { ok: false, error: `sourceYamlPath not found: ${explicitYamlPath}` };
          } else {
            source = { ok: true, path: explicitYamlPath };
          }
        } else {
          source = chooseSourceJsonl(llmDir, typeof params.sourceJsonlPath === "string" ? params.sourceJsonlPath : undefined);
        }
        if (!source.ok || !source.path) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_apply_reviewed_metadata",
            error: source.error ?? "failed to resolve source",
            llmDir,
          });
        }

        const markHumanReviewed = params.markHumanReviewed !== false;
        const allowNoContentChanges = params.allowNoContentChanges === true;
        const reviewedBy = typeof params.reviewedBy === "string" && params.reviewedBy.trim() ? params.reviewedBy.trim() : undefined;

        const sourceBase = path.basename(source.path);
        const sourceIsYaml = explicitYamlPath ? true : isYamlPath(source.path);

        let effectiveSourceJsonlPath = source.path;
        let yamlDerived: ReturnType<typeof applyYamlReviewToRows> | null = null;
        let yamlAliasCount = 0;
        if (sourceIsYaml) {
          const yamlParsed = parseAliasMappingsFromYaml(source.path);
          yamlAliasCount = yamlParsed.aliasToCanonical.size;
          if (!yamlParsed.sourceJsonlPath || !fs.existsSync(yamlParsed.sourceJsonlPath)) {
            return toToolResult({
              ok: false,
              tool: "video_pipeline_apply_reviewed_metadata",
              error: "reviewed YAML must include an existing source_jsonl path",
              sourceYamlPath: source.path,
              sourceJsonlPath: yamlParsed.sourceJsonlPath ?? null,
            });
          }
          effectiveSourceJsonlPath = yamlParsed.sourceJsonlPath;
          const baseRows = readComparableRows(effectiveSourceJsonlPath);
          yamlDerived = applyYamlReviewToRows(baseRows.rows, yamlParsed.aliasToCanonical);
        }

        const sourceLooksRawExtractionOutput = isRawExtractOutputJsonl(effectiveSourceJsonlPath);
        const reviewBaselinePath = inferBaselineExtractOutputPath(effectiveSourceJsonlPath);
        const reviewBaselineExists = reviewBaselinePath ? fs.existsSync(reviewBaselinePath) : false;
        const diff =
          reviewBaselinePath && reviewBaselineExists ? compareReviewContent(effectiveSourceJsonlPath, reviewBaselinePath) : null;
        const sourceComparable = sourceIsYaml && yamlDerived
          ? { rows: yamlDerived.editedRows, parseErrors: 0 }
          : readComparableRows(effectiveSourceJsonlPath);
        const reviewRiskSummary = summarizeReviewRisk(sourceComparable.rows);

        // Gate 1: raw 抽出ファイルは allowNoContentChanges でもバイパス不可
        // YAML フローでは source_jsonl が生抽出ファイルを指すのが正常なのでスキップ
        if (markHumanReviewed && sourceLooksRawExtractionOutput && !sourceIsYaml) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_apply_reviewed_metadata",
            error: "refusing to mark raw extraction output as human-reviewed; this check cannot be bypassed",
            sourceJsonlPath: effectiveSourceJsonlPath,
            sourceBaseName: sourceBase,
            sourceLooksRawExtractionOutput: true,
            hint: "Use sourceYamlPath parameter instead of sourceJsonlPath. Pass the YAML file path from video_pipeline_export_program_yaml. The tool will read the YAML's alias mappings and apply them to the source JSONL automatically. Do NOT copy or rename JSONL files.",
          });
        }

        // Gate 2: 変更なしチェック (allowNoContentChanges で合法的にバイパス可能)
        // YAML フローでは yamlDerived の変更数で判定
        const effectiveChangedRows = sourceIsYaml && yamlDerived ? yamlDerived.changedRowsCount : (diff?.comparable ? diff.changedRowsCount : null);
        // 変更 0 件かつリスク行 0 件なら「本当に修正不要」→ 自動パス
        const noRiskRows = reviewRiskSummary.needsReviewRows === 0 && reviewRiskSummary.suspiciousProgramTitleRows === 0;
        if (markHumanReviewed && !allowNoContentChanges && !noRiskRows) {
          if (effectiveChangedRows === 0) {
            return toToolResult({
              ok: false,
              tool: "video_pipeline_apply_reviewed_metadata",
              error:
                "no content edits detected between reviewed JSONL and its baseline extraction output; refusing to mark as human-reviewed without explicit override",
              sourceJsonlPath: effectiveSourceJsonlPath,
              reviewBaselinePath,
              reviewBaselineExists,
              sourceLooksRawExtractionOutput: false,
              reviewDiff: diff,
              reviewRiskSummary,
              hint: "Review-risk rows remain but no edits were made. Fix the flagged rows first, or set allowNoContentChanges=true if intentionally accepting all rows unchanged.",
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
            sourceJsonlPath: effectiveSourceJsonlPath,
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

        const stamped = sourceIsYaml && yamlDerived
          ? {
            rows: sourceComparable.rows.map((row) => {
              const next: AnyObj = { ...row };
              if (markHumanReviewed) {
                next.human_reviewed = true;
                next.human_reviewed_at = new Date().toISOString();
                if (reviewedBy) next.human_reviewed_by = reviewedBy;
              }
              return next;
            }),
            parseErrors: 0,
          }
          : stampReviewedRows(effectiveSourceJsonlPath, markHumanReviewed, reviewedBy);
        if (stamped.rows.length === 0) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_apply_reviewed_metadata",
            error: "no valid metadata rows found in source jsonl",
            sourceJsonlPath: effectiveSourceJsonlPath,
            parseErrors: stamped.parseErrors,
          });
        }

        const outputStampedJsonlPath = path.join(os.tmpdir(), `video_pipeline_apply_reviewed_${tsCompact()}_${process.pid}.jsonl`);
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
          String(params.source || (markHumanReviewed ? "human_reviewed" : "llm")),
          "--franchise-rules",
          franchiseRulesPath,
        ];
        const r = runCmd("uv", upsertArgs, resolved.cwd);
        try {
          fs.unlinkSync(outputStampedJsonlPath);
        } catch {
          // best effort cleanup for temp file
        }

        // --- auto archive after successful DB write ---
        const archivedFiles: string[] = [];
        if (r.ok) {
          const archiveDir = path.join(llmDir, "archive");
          const toArchive: string[] = [];
          // YAML ファイル
          if (sourceIsYaml && source.path) toArchive.push(source.path);
          // 抽出出力 JSONL
          if (effectiveSourceJsonlPath && fs.existsSync(effectiveSourceJsonlPath)) {
            toArchive.push(effectiveSourceJsonlPath);
          }
          // 対応する入力 JSONL (_output_ → _input_)
          const inputJsonlPath = effectiveSourceJsonlPath.replace(/_output_/g, "_input_");
          if (inputJsonlPath !== effectiveSourceJsonlPath && fs.existsSync(inputJsonlPath)) {
            toArchive.push(inputJsonlPath);
          }
          if (toArchive.length > 0) {
            try {
              fs.mkdirSync(archiveDir, { recursive: true });
              for (const src of toArchive) {
                if (!fs.existsSync(src)) continue;
                const dst = path.join(archiveDir, path.basename(src));
                fs.renameSync(src, dst);
                archivedFiles.push(path.basename(src));
              }
            } catch {
              // archive failure is non-fatal
            }
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
          sourceJsonlPath: effectiveSourceJsonlPath,
          sourceYamlPath: sourceIsYaml ? source.path : null,
          outputStampedJsonlPath: null,
          rows: stamped.rows.length,
          parseErrors: stamped.parseErrors,
          markHumanReviewed,
          allowNoContentChanges,
          reviewedBy: reviewedBy ?? null,
          sourceBaseName: sourceBase,
          sourceIsYaml,
          yamlAliasCount,
          yamlReviewApplied: yamlDerived,
          sourceLooksRawExtractionOutput,
          reviewBaselinePath,
          reviewBaselineExists,
          reviewDiff: diff,
          reviewRiskSummary,
          sourceParseErrors: sourceComparable.parseErrors,
          archivedFiles,
        });
      },
    }
  );
}
