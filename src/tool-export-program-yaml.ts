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
        });
      },
    },
    { optional: true },
  );
}
