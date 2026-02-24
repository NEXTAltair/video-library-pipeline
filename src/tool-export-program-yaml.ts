import fs from "node:fs";
import path from "node:path";
import { latestJsonlFile, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

type ProgramStat = {
  canonicalTitle: string;
  normalizedProgramKey: string;
  count: number;
  needsReviewCount: number;
  samplePaths: string[];
  sampleRawNames: string[];
};

type ReviewCandidate = {
  pathId?: string;
  path: string;
  columns: string[];
  reasons: string[];
  severity: "required" | "review";
  byProgramFolderTitle?: string;
  current: {
    program_title?: string;
    air_date?: string;
    subtitle?: string;
    needs_review?: boolean;
    needs_review_reason?: string;
    normalized_program_key?: string;
  };
  evidence?: {
    raw?: string;
  };
};

type ReviewSummary = {
  rowsNeedingReview: number;
  requiredFieldMissingRows: number;
  invalidAirDateRows: number;
  needsReviewFlagRows: number;
  suspiciousProgramTitleRows: number;
  fieldCounts: Record<string, number>;
  reasonCounts: Record<string, number>;
};

const MAX_REVIEW_CANDIDATES = 50;
const MAX_PREVIEW_CHARS = 220;

function tsCompact(d = new Date()): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

function jsonScalar(v: unknown): string {
  return JSON.stringify(v ?? "");
}

function normalizeKey(title: string): string {
  return String(title || "")
    .trim()
    .replace(/\s+/g, "_")
    .replace(/[<>:"/\\|?*]+/g, "")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}

function asStr(v: unknown): string {
  return typeof v === "string" ? v : "";
}

function previewText(v: unknown, max = MAX_PREVIEW_CHARS): string | undefined {
  const s = asStr(v).trim();
  if (!s) return undefined;
  return s.length <= max ? s : `${s.slice(0, max - 1)}…`;
}

function isIsoDate(s: string): boolean {
  return /^\d{4}-\d{2}-\d{2}$/.test(s);
}

function splitWinPathParts(winPath: string): string[] {
  return String(winPath || "")
    .split(/[\\/]+/)
    .filter(Boolean);
}

function byProgramFolderTitleFromPath(winPath: string): string | undefined {
  const parts = splitWinPathParts(winPath);
  const idx = parts.findIndex((p) => p.toLowerCase() === "by_program");
  if (idx >= 0 && idx + 1 < parts.length) return parts[idx + 1];
  return undefined;
}

function lowerCompact(s: string): string {
  return s
    .normalize("NFKC")
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/[<>:"/\\|?*]+/g, "");
}

function looksSwallowedProgramTitle(programTitle: string, folderTitle: string): boolean {
  const p = programTitle.trim();
  const f = folderTitle.trim();
  if (!p || !f) return false;
  if (p === f) return false;
  const pNorm = lowerCompact(p);
  const fNorm = lowerCompact(f);
  if (!pNorm || !fNorm) return false;
  if (!pNorm.startsWith(fNorm)) return false;
  return pNorm.length >= fNorm.length + 8;
}

function pushCount(map: Record<string, number>, key: string) {
  map[key] = (map[key] ?? 0) + 1;
}

function appendUnique(arr: string[], v: string) {
  if (!v) return;
  if (!arr.includes(v)) arr.push(v);
}

function buildReviewDiagnostics(rows: AnyObj[]): {
  summary: ReviewSummary;
  candidates: ReviewCandidate[];
  truncated: boolean;
} {
  const fieldCounts: Record<string, number> = {};
  const reasonCounts: Record<string, number> = {};
  const candidates: ReviewCandidate[] = [];
  let rowsNeedingReview = 0;
  let requiredFieldMissingRows = 0;
  let invalidAirDateRows = 0;
  let needsReviewFlagRows = 0;
  let suspiciousProgramTitleRows = 0;

  for (const r of rows) {
    const columns: string[] = [];
    const reasons: string[] = [];
    let severity: "required" | "review" = "review";
    const programTitle = asStr(r.program_title).trim();
    const airDate = asStr(r.air_date).trim();
    const needsReview = r.needs_review === true;
    const needsReviewReason = asStr(r.needs_review_reason).trim();
    const pathValue = asStr(r.path).trim();

    if (!programTitle) {
      appendUnique(columns, "program_title");
      appendUnique(reasons, "missing_program_title");
      severity = "required";
    }
    if (!airDate) {
      appendUnique(columns, "air_date");
      appendUnique(reasons, "missing_air_date");
      severity = "required";
    } else if (!isIsoDate(airDate)) {
      appendUnique(columns, "air_date");
      appendUnique(reasons, "invalid_air_date");
      severity = "required";
    }
    if (typeof r.needs_review !== "boolean") {
      appendUnique(columns, "needs_review");
      appendUnique(reasons, "missing_or_invalid_needs_review");
      severity = "required";
    }
    if (needsReview) {
      appendUnique(columns, "needs_review");
      if (needsReviewReason) appendUnique(columns, "needs_review_reason");
      appendUnique(reasons, needsReviewReason || "needs_review_flagged");
    }

    const byProgramFolderTitle = byProgramFolderTitleFromPath(pathValue);
    if (byProgramFolderTitle && programTitle && looksSwallowedProgramTitle(programTitle, byProgramFolderTitle)) {
      appendUnique(columns, "program_title");
      appendUnique(reasons, "program_title_may_include_description");
    }

    if (reasons.length === 0) continue;
    rowsNeedingReview += 1;
    if (reasons.some((x) => x.startsWith("missing_") || x === "invalid_air_date" || x === "missing_or_invalid_needs_review")) {
      requiredFieldMissingRows += 1;
    }
    if (reasons.includes("invalid_air_date")) invalidAirDateRows += 1;
    if (needsReview) needsReviewFlagRows += 1;
    if (reasons.includes("program_title_may_include_description")) suspiciousProgramTitleRows += 1;
    for (const c of columns) pushCount(fieldCounts, c);
    for (const reason of reasons) pushCount(reasonCounts, reason);

    if (candidates.length < MAX_REVIEW_CANDIDATES) {
      candidates.push({
        pathId: typeof r.path_id === "string" ? r.path_id : undefined,
        path: pathValue,
        columns,
        reasons,
        severity,
        byProgramFolderTitle,
        current: {
          program_title: previewText(r.program_title),
          air_date: previewText(r.air_date),
          subtitle: previewText(r.subtitle),
          needs_review: typeof r.needs_review === "boolean" ? r.needs_review : undefined,
          needs_review_reason: previewText(r.needs_review_reason),
          normalized_program_key: previewText(r.normalized_program_key),
        },
        evidence: {
          raw: previewText((r.evidence as AnyObj | undefined)?.raw),
        },
      });
    }
  }

  return {
    summary: {
      rowsNeedingReview,
      requiredFieldMissingRows,
      invalidAirDateRows,
      needsReviewFlagRows,
      suspiciousProgramTitleRows,
      fieldCounts,
      reasonCounts,
    },
    candidates,
    truncated: rowsNeedingReview > candidates.length,
  };
}

function readJsonlRows(sourceJsonlPath: string): AnyObj[] {
  const rows: AnyObj[] = [];
  const text = fs.readFileSync(sourceJsonlPath, "utf-8");
  for (const line of text.split(/\r?\n/)) {
    const s = line.trim();
    if (!s) continue;
    try {
      const obj = JSON.parse(s);
      if (obj && typeof obj === "object" && !Array.isArray(obj)) {
        if ("_meta" in obj) continue;
        rows.push(obj as AnyObj);
      }
    } catch {
      // Ignore malformed lines; this exporter is best-effort.
    }
  }
  return rows;
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

function buildYaml(
  sourceJsonlPath: string,
  rowsTotal: number,
  rowsUsed: number,
  includeNeedsReview: boolean,
  includeUnknown: boolean,
  stats: ProgramStat[],
): string {
  const lines: string[] = [];
  lines.push("# Auto-generated candidate YAML from extraction output.");
  lines.push("# Review manually before using as production hints.");
  lines.push(`generated_at: ${jsonScalar(new Date().toISOString())}`);
  lines.push(`source_jsonl: ${jsonScalar(sourceJsonlPath)}`);
  lines.push(`rows_total: ${rowsTotal}`);
  lines.push(`rows_used: ${rowsUsed}`);
  lines.push("filters:");
  lines.push(`  include_needs_review: ${includeNeedsReview ? "true" : "false"}`);
  lines.push(`  include_unknown: ${includeUnknown ? "true" : "false"}`);
  lines.push("hints:");
  for (const s of stats) {
    lines.push(`  - canonical_title: ${jsonScalar(s.canonicalTitle)}`);
    lines.push("    aliases:");
    lines.push(`      - ${jsonScalar(s.canonicalTitle)}`);
    lines.push("    stats:");
    lines.push(`      count: ${s.count}`);
    lines.push(`      needs_review_count: ${s.needsReviewCount}`);
    lines.push("    samples:");
    if (s.samplePaths.length === 0 && s.sampleRawNames.length === 0) {
      lines.push("      - {}");
    } else {
      const n = Math.max(s.samplePaths.length, s.sampleRawNames.length);
      for (let i = 0; i < n; i++) {
        const p = s.samplePaths[i] ?? "";
        const r = s.sampleRawNames[i] ?? "";
        lines.push(`      - path: ${jsonScalar(p)}`);
        lines.push(`        raw: ${jsonScalar(r)}`);
      }
    }
  }
  return `${lines.join("\n")}\n`;
}

export function registerToolExportProgramYaml(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_export_program_yaml",
      description: "Export reviewed candidate program info YAML from extracted metadata JSONL.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          sourceJsonlPath: { type: "string" },
          outputPath: { type: "string" },
          includeNeedsReview: { type: "boolean", default: true },
          includeUnknown: { type: "boolean", default: false },
          maxSamplesPerProgram: { type: "integer", minimum: 1, maximum: 20, default: 3 },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const hostRoot = String(cfg.windowsOpsRoot || "/tmp").replace(/\/+$/, "");
        const llmDir = path.join(hostRoot, "llm");
        const includeNeedsReview = params.includeNeedsReview !== false;
        const includeUnknown = params.includeUnknown === true;
        const maxSamplesPerProgram = Number(params.maxSamplesPerProgram ?? 3);

        const source = chooseSourceJsonl(llmDir, typeof params.sourceJsonlPath === "string" ? params.sourceJsonlPath : undefined);
        if (!source.ok || !source.path) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_export_program_yaml",
            error: source.error ?? "failed to resolve source jsonl",
            llmDir,
          });
        }

        const outPath = String(params.outputPath || path.join(llmDir, `program_aliases_review_${tsCompact()}.yaml`));
        const rows = readJsonlRows(source.path);
        const statsMap = new Map<string, ProgramStat>();
        let rowsUsed = 0;

        for (const r of rows) {
          const title = String(r.program_title || "").trim();
          if (!title) continue;
          if (!includeUnknown && title === "UNKNOWN") continue;
          const needsReview = r.needs_review === true;
          if (!includeNeedsReview && needsReview) continue;
          const normalizedProgramKey = String(r.normalized_program_key || normalizeKey(title));
          const key = `${title}::${normalizedProgramKey}`;
          const cur = statsMap.get(key) ?? {
            canonicalTitle: title,
            normalizedProgramKey,
            count: 0,
            needsReviewCount: 0,
            samplePaths: [],
            sampleRawNames: [],
          };
          cur.count += 1;
          if (needsReview) cur.needsReviewCount += 1;
          if (cur.samplePaths.length < maxSamplesPerProgram && typeof r.path === "string") {
            cur.samplePaths.push(String(r.path));
          }
          const rawName = (r.evidence as AnyObj)?.raw;
          if (cur.sampleRawNames.length < maxSamplesPerProgram && typeof rawName === "string") {
            cur.sampleRawNames.push(rawName);
          }
          statsMap.set(key, cur);
          rowsUsed += 1;
        }

        const stats = Array.from(statsMap.values()).sort((a, b) =>
          a.canonicalTitle.localeCompare(b.canonicalTitle, "ja"),
        );
        const review = buildReviewDiagnostics(rows);
        const yaml = buildYaml(source.path, rows.length, rowsUsed, includeNeedsReview, includeUnknown, stats);

        fs.mkdirSync(path.dirname(outPath), { recursive: true });
        fs.writeFileSync(outPath, yaml, "utf-8");

        return toToolResult({
          ok: true,
          tool: "video_pipeline_export_program_yaml",
          sourceJsonlPath: source.path,
          outputPath: outPath,
          rowsTotal: rows.length,
          rowsUsed,
          programs: stats.length,
          includeNeedsReview,
          includeUnknown,
          maxSamplesPerProgram,
          reviewSummary: review.summary,
          reviewCandidates: review.candidates,
          reviewCandidatesTruncated: review.truncated,
          reviewGuidance: {
            stage: "metadata_review",
            yamlRole: "human_review_artifact_only",
            agentShouldUseYamlForDecision: false,
            agentShouldUseStructuredFields: true,
            reviewColumnsInScope: [
              "program_title",
              "air_date",
              "needs_review",
              "needs_review_reason",
              "normalized_program_key",
              "aliases_in_yaml",
            ],
            reviewColumnsOutOfScope: [
              "destination_folder_path",
              "filesystem_move_plan",
              "category_assignment",
              "genre_classification_for_move",
            ],
            reportingRule:
              "Ask the human to review specific records and columns using reviewCandidates (path + columns + reasons). Do not request generic title consistency checks only.",
            handoffRule:
              "This stage reviews metadata only. Physical move destination decisions belong to relocate/move stages after metadata is confirmed.",
            yamlHandlingRule:
              "Do not derive automated corrections from YAML text. YAML is for human visual review; agent should rely on reviewCandidates/reviewSummary and user-confirmed column-level edits.",
          },
        });
      },
    },
    { optional: true },
  );
}
