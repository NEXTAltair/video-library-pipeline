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
        const reviewedBy = typeof params.reviewedBy === "string" && params.reviewedBy.trim() ? params.reviewedBy.trim() : undefined;
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
          reviewedBy: reviewedBy ?? null,
        });
      },
    },
    { optional: true },
  );
}
