import fs from "node:fs";
import path from "node:path";
import { lowerCompact, resolvePythonScript, runCmd, toToolResult, tsCompact } from "./runtime";
import { buildReviewDiagnostics, buildYaml, readJsonlRows } from "./tool-export-program-yaml";
import type { AnyObj } from "./types";

function generateReviewYaml(sourceJsonlPath: string, outDir: string, rows: AnyObj[]): { yamlPath: string | null; error?: string } {
  try {
    const statsMap = new Map<string, { canonicalTitle: string; normalizedProgramKey: string; count: number; needsReviewCount: number; samplePaths: string[]; sampleRawNames: string[] }>();

    for (const r of rows) {
      const title = String(r.program_title || "").trim();
      if (!title || title === "UNKNOWN") continue;
      const normalizedProgramKey = lowerCompact(title);
      const key = `${title}::${normalizedProgramKey}`;
      const cur = statsMap.get(key) ?? { canonicalTitle: title, normalizedProgramKey, count: 0, needsReviewCount: 0, samplePaths: [], sampleRawNames: [] };
      cur.count += 1;
      if (r.needs_review === true) cur.needsReviewCount += 1;
      if (cur.samplePaths.length < 3 && typeof r.path === "string") cur.samplePaths.push(String(r.path));
      const rawName = (r.evidence as AnyObj | undefined)?.raw;
      if (cur.sampleRawNames.length < 3 && typeof rawName === "string") cur.sampleRawNames.push(rawName);
      statsMap.set(key, cur);
    }

    const stats = Array.from(statsMap.values()).sort((a, b) => a.canonicalTitle.localeCompare(b.canonicalTitle, "ja"));
    const yaml = buildYaml(sourceJsonlPath, rows.length, stats.reduce((s, x) => s + x.count, 0), true, false, stats);
    const yamlPath = path.join(outDir, `program_aliases_review_${tsCompact()}.yaml`);
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(yamlPath, yaml, "utf-8");
    return { yamlPath };
  } catch (e: any) {
    return { yamlPath: null, error: String(e?.message || e) };
  }
}

export function registerToolLlmExtract(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_apply_llm_extract_output",
      description:
        "Validate and upsert LLM extraction output JSONL into the DB. " +
        "Call this after a sessions_spawn subagent has finished writing its extraction output. " +
        "Runs subtitle-separator / length checks, coerces types, and upserts to path_metadata with source='llm'. " +
        "Records with issues are marked needs_review=true instead of being rejected. " +
        "On success, if any records need review (needs_review=true), generates a human-review YAML and returns reviewYamlPath + reviewCandidates. " +
        "If no records need review (needsReviewFlagRows=0), no YAML is generated — proceed directly to video_pipeline_relocate_existing_files.",
      parameters: {
        type: "object",
        additionalProperties: false,
        required: ["outputJsonlPath"],
        properties: {
          outputJsonlPath: {
            type: "string",
            description: "Path to the JSONL file written by the LLM extraction flow (llm_filename_extract_output_*.jsonl).",
          },
          dryRun: {
            type: "boolean",
            default: false,
            description: "If true, validate and report without writing to DB.",
          },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const outputJsonlPath = typeof params.outputJsonlPath === "string" ? params.outputJsonlPath.trim() : "";

        if (!outputJsonlPath) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_apply_llm_extract_output",
            error: "outputJsonlPath is required",
          });
        }

        if (!fs.existsSync(outputJsonlPath)) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_apply_llm_extract_output",
            error: `outputJsonlPath does not exist: ${outputJsonlPath}`,
            hint: "Check that the subagent has finished writing its output before calling this tool.",
          });
        }

        const resolved = resolvePythonScript("apply_llm_extract_output.py");
        const args = [
          "run",
          "python",
          resolved.scriptPath,
          "--db",
          String(cfg.db || ""),
          "--in",
          outputJsonlPath,
          "--source",
          "llm",
        ];
        if (params.dryRun === true) args.push("--dry-run");

        const r = runCmd("uv", args, resolved.cwd);

        let parsed: AnyObj | null = null;
        if (r.ok) {
          try {
            const jsonLine = r.stdout.split("\n").find((l) => l.trim().startsWith("{"));
            if (jsonLine) parsed = JSON.parse(jsonLine);
          } catch {
            // ignore
          }
        }

        const out: AnyObj = {
          ok: r.ok,
          tool: "video_pipeline_apply_llm_extract_output",
          scriptSource: resolved.source,
          exitCode: r.code,
          stdout: r.stdout,
          stderr: r.stderr,
          outputJsonlPath,
        };
        if (parsed) {
          for (const [k, v] of Object.entries(parsed)) out[k] = v;
        }

        // 成功時: 診断を行い、レビューが必要な場合のみYAMLを生成する
        if (r.ok && !params.dryRun) {
          const hostRoot = String(cfg.windowsOpsRoot || "/tmp").replace(/\/+$/, "");
          const llmDir = path.join(hostRoot, "llm");
          const rows = readJsonlRows(outputJsonlPath);
          const review = buildReviewDiagnostics(rows);
          out.reviewSummary = review.summary;

          if (review.summary.needsReviewFlagRows > 0) {
            // レビューが必要: YAMLを生成してパスとcandidatesを返す
            const yamlResult = generateReviewYaml(outputJsonlPath, llmDir, rows);
            if (yamlResult.yamlPath) {
              out.reviewYamlPath = yamlResult.yamlPath;
            }
            out.reviewCandidates = review.candidates;
            out.reviewCandidatesTruncated = review.truncated;
            out.nextStep =
              `LLM extraction output applied to DB (source=llm). ` +
              `${review.summary.needsReviewFlagRows} records need human review — see reviewCandidates. ` +
              `To fix: (1) Ask the user to edit canonical_title / aliases in reviewYamlPath. ` +
              `(2) After user confirms YAML edits, call video_pipeline_apply_reviewed_metadata with sourceYamlPath set to reviewYamlPath. ` +
              `Legacy fallback: sourceJsonlPath with a reviewed copy is still supported.`;
          } else {
            // レビュー不要: YAMLファイルは生成しない。エージェントが混乱する原因になるため。
            // Records are already in DB with source=llm and needs_review=false.
            out.nextStep =
              "LLM extraction output applied to DB (source=llm). " +
              "No records need review (needsReviewFlagRows=0). " +
              "ACTION: Call video_pipeline_relocate_existing_files directly. " +
              "Do NOT call video_pipeline_apply_reviewed_metadata — records are already in DB and no review is needed.";
          }
        }

        return toToolResult(out);
      },
    }
  );
}
