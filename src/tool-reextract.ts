import fs from "node:fs";
import path from "node:path";
import { getExtensionRootDir, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

function ensureDefaultQueueFile(queuePath: string): { created: boolean; error?: string } {
  try {
    if (fs.existsSync(queuePath)) return { created: false };
    fs.mkdirSync(path.dirname(queuePath), { recursive: true });
    const meta = { _meta: { source: "video_pipeline_reextract", createdAt: new Date().toISOString() } };
    fs.writeFileSync(queuePath, `${JSON.stringify(meta)}\n`, "utf-8");
    return { created: true };
  } catch (e: any) {
    return { created: false, error: String(e?.message || e) };
  }
}

export function buildLlmExtractTask(inputJsonlPath: string, outputJsonlPath: string, hintsPath: string): string {
  return [
    "# video-library-pipeline: LLM title extraction task",
    "",
    "Extract program metadata from Japanese TV recording filenames.",
    "",
    `## Input`,
    `Read the JSONL file at: ${inputJsonlPath}`,
    `Each line is a JSON object with fields: path_id, name (filename), path (Windows path), mtime_utc, epg_hint(optional)`,
    "",
    `## Hints`,
    `Read program alias rules from: ${hintsPath}`,
    `Use the rules section to canonicalize program_title (e.g. strip subtitle after ▽/▼, apply regex replacements).`,
    "",
    `## Task`,
    `For each record in the input JSONL, extract:`,
    `- program_title: the TV program name only. Do NOT include episode subtitle, guest names, or description after ▽/▼/◇.`,
    `- air_date: broadcast date as YYYY-MM-DD, extracted from filename timestamp. null if not found.`,
    `- subtitle: episode subtitle or null`,
    `- episode_no: episode number string or null`,
    `- confidence: float 0.0–1.0 reflecting extraction certainty`,
    `- needs_review: true if uncertain (subtitle separator in program_title, unknown program, confidence < 0.7, etc.)`,
    `- needs_review_reason: comma-separated reason codes or null`,
    `- If epg_hint exists, prefer official_title candidates when filename is ambiguous; use air_date/start_time as supporting evidence.`,
    "",
    `## Output`,
    `Write a JSONL file to: ${outputJsonlPath}`,
    `One JSON object per line. Required fields per record:`,
    `  path_id, program_title, air_date, subtitle, episode_no, confidence, needs_review, needs_review_reason`,
    `Do not include records from the input that you skip — write one output line per input line, in order.`,
    "",
    `## Rules`,
    `- program_title must NOT contain ▽ ▼ ◇ separators — strip everything after the first one`,
    `- program_title must NOT be longer than 80 characters`,
    `- If you cannot determine program_title, set it to "UNKNOWN" and needs_review=true`,
    `- air_date format: YYYY-MM-DD (e.g. 2025-04-21). null if truly absent.`,
    `- confidence=1.0 means certain; 0.5 means very uncertain`,
    "",
    `When done, call video_pipeline_apply_llm_extract_output with outputJsonlPath="${outputJsonlPath}"`,
  ].join("\n");
}

export function registerToolReextract(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_reextract",
      description: "Run metadata re-extraction batch from queue JSONL. Set useLlmExtract=true to delegate extraction to a LLM subagent instead of the built-in rule-based engine.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          queuePath: { type: "string" },
          extractionVersion: { type: "string" },
          batchSize: { type: "integer", minimum: 1, maximum: 1000, default: 50 },
          maxBatches: { type: "integer", minimum: 1, maximum: 1000 },
          preserveHumanReviewed: { type: "boolean", default: true },
          useLlmExtract: {
            type: "boolean",
            default: false,
            description:
              "When true, prepare input JSONL batches only and return sessions_spawn payloads in followUpToolCalls. " +
              "The agent must then call sessions_spawn for each entry in followUpToolCalls where tool='sessions_spawn', " +
              "and after each subagent finishes, call video_pipeline_apply_llm_extract_output. " +
              "IMPORTANT: inputJsonlPaths in the result is informational only — it shows what was written to disk. " +
              "Do NOT pass inputJsonlPaths back to this tool or to any other tool as a parameter.",
          },
          llmModel: {
            type: "string",
            description: "Model to use for LLM extraction subagent (e.g. 'claude-opus-4-6'). Only used when useLlmExtract=true.",
          },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("run_metadata_batches_promptv1.py");
        const hostRoot = String(cfg.windowsOpsRoot || "/tmp").replace(/\/+$/, "");
        const outDir = `${hostRoot}/llm`;
        const hintsPath = path.join(getExtensionRootDir(), "rules", "program_aliases.yaml");
        const franchiseRulesPath = path.join(getExtensionRootDir(), "rules", "franchise_rules.yaml");
        const queueProvided = typeof params.queuePath === "string" && params.queuePath.trim().length > 0;
        const queue = String(params.queuePath || `${outDir}/queue_manual_reextract.jsonl`);
        let queueAutoCreated = false;

        if (!queueProvided) {
          const init = ensureDefaultQueueFile(queue);
          if (init.error) {
            return toToolResult({
              ok: false,
              tool: "video_pipeline_reextract",
              error: `failed to initialize default queue: ${init.error}`,
              queue,
            });
          }
          queueAutoCreated = init.created;
        } else if (!fs.existsSync(queue)) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_reextract",
            error: `queuePath does not exist: ${queue}`,
            queue,
          });
        }

        const args = [
          "run",
          "python",
          resolved.scriptPath,
          "--db",
          String(cfg.db || ""),
          "--queue",
          queue,
          "--outdir",
          outDir,
          "--hints",
          hintsPath,
          "--batch-size",
          String(params.batchSize ?? 50),
        ];
        if (params.maxBatches) args.push("--max-batches", String(params.maxBatches));
        args.push("--franchise-rules", franchiseRulesPath);
        if (params.extractionVersion) args.push("--extraction-version", String(params.extractionVersion));
        if (params.preserveHumanReviewed === false) args.push("--ignore-human-reviewed");
        if (params.useLlmExtract === true) args.push("--prepare-only");

        const r = runCmd("uv", args, resolved.cwd);

        // Parse JSON summary from the last JSON line of stdout
        let parsedResult: Record<string, unknown> | null = null;
        if (r.ok) {
          try {
            const lines = r.stdout.split("\n").filter((l) => l.trim().startsWith("{"));
            parsedResult = lines.length ? JSON.parse(lines[lines.length - 1]) : null;
          } catch {
            // ignore
          }
        }

        const followUpToolCalls: Array<Record<string, unknown>> = [];

        if (params.useLlmExtract === true && r.ok && parsedResult) {
          // LLM subagent path: return sessions_spawn payload for each input batch
          const inputJsonlPaths: string[] = Array.isArray(parsedResult.inputJsonlPaths)
            ? (parsedResult.inputJsonlPaths as string[])
            : parsedResult.latestInputJsonlPath
              ? [String(parsedResult.latestInputJsonlPath)]
              : [];

          const spawnPayloads: Array<Record<string, unknown>> = [];
          const outputJsonlPaths: string[] = [];

          for (const inputPath of inputJsonlPaths) {
            const outputPath = inputPath.replace(
              /llm_filename_extract_input_(\d+_\d+)\.jsonl$/,
              "llm_filename_extract_output_$1.jsonl",
            );
            outputJsonlPaths.push(outputPath);
            const task = buildLlmExtractTask(inputPath, outputPath, hintsPath);
            const model = typeof params.llmModel === "string" && params.llmModel.trim()
              ? params.llmModel.trim()
              : "claude-opus-4-6";
            const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
            const spawnPayload: Record<string, unknown> = {
              task,
              model,
              label: `video-llm-extract-${ts}`,
              runTimeoutSeconds: 600,
              cleanup: "keep",
            };
            spawnPayloads.push(spawnPayload);
            followUpToolCalls.push({
              tool: "sessions_spawn",
              reason: "llm_title_extraction",
              params: spawnPayload,
            });
          }
          // After all spawns complete, apply each output
          for (const outputPath of outputJsonlPaths) {
            followUpToolCalls.push({
              tool: "video_pipeline_apply_llm_extract_output",
              reason: "apply_llm_extract_output_after_subagent",
              params: { outputJsonlPath: outputPath },
            });
          }

          return toToolResult({
            ok: true,
            tool: "video_pipeline_reextract",
            useLlmExtract: true,
            queue,
            queueAutoCreated,
            inputJsonlPaths,   // informational only — do NOT pass as parameter
            outputJsonlPaths,  // informational only — do NOT pass as parameter
            spawnPayloads,
            followUpToolCalls,
            hasFollowUpToolCalls: followUpToolCalls.length > 0,
            followUpToolCallsInstructions:
              "Execute followUpToolCalls in order: first all sessions_spawn calls (one per batch), " +
              "then one video_pipeline_apply_llm_extract_output call per completed subagent. " +
              "Do not modify the params in followUpToolCalls — use them as-is.",
            nextStep:
              `Prepared ${inputJsonlPaths.length} input JSONL batch(es). ` +
              `ACTION REQUIRED: Follow followUpToolCalls in order. ` +
              `(1) For each entry where tool='sessions_spawn', call sessions_spawn using that entry's params. ` +
              `(2) After each subagent finishes, call video_pipeline_apply_llm_extract_output with the outputJsonlPath from the corresponding tool='video_pipeline_apply_llm_extract_output' entry. ` +
              `WARNING: inputJsonlPaths and outputJsonlPaths in this result are for reference only. ` +
              `Do NOT pass inputJsonlPaths as a parameter to video_pipeline_reextract or any other tool.`,
          });
        }

        // Rule-based extraction path (default)
        const outputJsonlPath =
          (typeof parsedResult?.latestOutputJsonlPath === "string" && parsedResult.latestOutputJsonlPath) ||
          (Array.isArray(parsedResult?.outputJsonlPaths) && (parsedResult.outputJsonlPaths as string[]).length
            ? String((parsedResult.outputJsonlPaths as string[])[(parsedResult.outputJsonlPaths as string[]).length - 1])
            : null);

        if (r.ok && outputJsonlPath) {
          followUpToolCalls.push({
            tool: "video_pipeline_export_program_yaml",
            reason: "export_human_review_yaml_from_reextract_output",
            params: { sourceJsonlPath: outputJsonlPath, outputPath: outputJsonlPath.replace(/\.jsonl$/i, "_review.yaml") },
          });
        }

        return toToolResult({
          ok: r.ok,
          tool: "video_pipeline_reextract",
          useLlmExtract: false,
          scriptSource: resolved.source,
          scriptPath: resolved.scriptPath,
          exitCode: r.code,
          stdout: r.stdout,
          stderr: r.stderr,
          queue,
          queueAutoCreated,
          preserveHumanReviewed: params.preserveHumanReviewed !== false,
          outputJsonlPath,
          followUpToolCalls,
          hasFollowUpToolCalls: followUpToolCalls.length > 0,
        });
      },
    }
  );
}
