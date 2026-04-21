import fs from "node:fs";
import path from "node:path";
import { getExtensionRootDir, toToolResult } from "./runtime";
import { buildLlmExtractTask } from "./tool-reextract";
import type { PluginApi, GetCfgFn } from "./types";

interface BatchInfo {
  inputPath: string;
  outputPath: string;
  status: "complete" | "pending" | "incomplete";
  inputRecords: number;
  outputRecords: number | null;
}

function countJsonlLines(filePath: string): number {
  const content = fs.readFileSync(filePath, "utf-8");
  return content
    .split("\n")
    .filter((line) => {
      const trimmed = line.trim();
      return trimmed.length > 0 && trimmed.startsWith("{");
    }).length;
}

export function registerToolLlmExtractStatus(api: PluginApi, getCfg: GetCfgFn) {
  api.registerTool(
    {
      name: "video_pipeline_llm_extract_status",
      description:
        "Check LLM subagent batch completion status by scanning for input/output JSONL pairs in the llm/ directory. " +
        "Returns batch status (complete/pending/incomplete) and followUpToolCalls for retrying pending batches. " +
        "Use this tool when a sessions_spawn call fails, times out, or a subagent doesn't produce output.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          llmModel: {
            type: "string",
            description:
              "Model to use for retry subagent spawns (default: 'claude-opus-4-6'). " +
              "Only affects the sessions_spawn entries in followUpToolCalls.",
          },
        },
      },
      async execute(_id: string, params: { llmModel?: string }) {
        const cfg = getCfg(api);
        const hostRoot = String(cfg.windowsOpsRoot || "/tmp").replace(/\/+$/, "");
        const llmDir = path.join(hostRoot, "llm");
        const hintsPath = path.join(getExtensionRootDir(), "rules", "program_aliases.yaml");
        const model =
          typeof params.llmModel === "string" && params.llmModel.trim()
            ? params.llmModel.trim()
            : "claude-opus-4-6";

        if (!fs.existsSync(llmDir)) {
          return toToolResult({
            ok: true,
            tool: "video_pipeline_llm_extract_status",
            batches: [],
            summary: { total: 0, complete: 0, pending: 0, incomplete: 0 },
            followUpToolCalls: [],
            hasFollowUpToolCalls: false,
            nextStep: "No llm/ directory found. Nothing to check.",
          });
        }

        // Scan for input JSONL files
        const inputPattern = /^llm_filename_extract_input_\d+_\d+\.jsonl$/;
        const inputFiles = fs
          .readdirSync(llmDir)
          .filter((name) => inputPattern.test(name))
          .sort();

        const batches: BatchInfo[] = [];

        for (const inputName of inputFiles) {
          const inputPath = path.join(llmDir, inputName);
          const outputName = inputName.replace("_input_", "_output_");
          const outputPath = path.join(llmDir, outputName);

          const inputRecords = countJsonlLines(inputPath);

          let status: BatchInfo["status"];
          let outputRecords: number | null = null;

          if (fs.existsSync(outputPath)) {
            outputRecords = countJsonlLines(outputPath);
            status = outputRecords > 0 ? "complete" : "incomplete";
          } else {
            status = "pending";
          }

          batches.push({ inputPath, outputPath, status, inputRecords, outputRecords });
        }

        const summary = {
          total: batches.length,
          complete: batches.filter((b) => b.status === "complete").length,
          pending: batches.filter((b) => b.status === "pending").length,
          incomplete: batches.filter((b) => b.status === "incomplete").length,
        };

        // Build followUpToolCalls
        const followUpToolCalls: Array<Record<string, unknown>> = [];
        const needsRetry = batches.filter((b) => b.status === "pending" || b.status === "incomplete");
        // Spawn subagents for pending/incomplete batches
        for (const batch of needsRetry) {
          const task = buildLlmExtractTask(batch.inputPath, batch.outputPath, hintsPath);
          const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
          followUpToolCalls.push({
            tool: "sessions_spawn",
            reason: "llm_title_extraction_retry",
            params: {
              task,
              model,
              label: `video-llm-extract-retry-${ts}`,
              runTimeoutSeconds: 600,
              cleanup: "keep",
            },
          });
        }

        // Apply outputs for pending/incomplete batches (after spawns complete)
        for (const batch of needsRetry) {
          followUpToolCalls.push({
            tool: "video_pipeline_apply_llm_extract_output",
            reason: "apply_llm_extract_output_after_retry",
            params: { outputJsonlPath: batch.outputPath },
          });
        }

        let nextStep: string;
        if (summary.total === 0) {
          nextStep = "No input JSONL batches found in llm/ directory. Nothing to do.";
        } else if (summary.pending === 0 && summary.incomplete === 0) {
          nextStep =
            `All ${summary.total} batch(es) complete. ` +
            `followUpToolCalls contains apply_llm_extract_output calls for each batch (idempotent). ` +
            `Execute them to ensure all results are in DB.`;
        } else {
          nextStep =
            `${needsRetry.length} of ${summary.total} batch(es) need retry (${summary.pending} pending, ${summary.incomplete} incomplete). ` +
            `Execute followUpToolCalls in order: first all sessions_spawn calls, then all apply_llm_extract_output calls.`;
        }

        return toToolResult({
          ok: true,
          tool: "video_pipeline_llm_extract_status",
          batches,
          summary,
          followUpToolCalls,
          hasFollowUpToolCalls: followUpToolCalls.length > 0,
          nextStep,
        });
      },
    }
  );
}
