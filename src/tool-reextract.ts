import path from "node:path";
import { resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

export function registerToolReextract(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_reextract",
      description: "Run metadata re-extraction batch from queue source conditions.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          source: { type: "string", enum: ["needsReview", "version", "inventory"] },
          extractionVersion: { type: "string" },
          limit: { type: "integer", minimum: 1, maximum: 5000, default: 200 },
          batchSize: { type: "integer", minimum: 1, maximum: 1000, default: 50 },
        },
        required: ["source"],
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("run_metadata_batches_promptv1.py");
        const outDir = path.join(cfg.hostDataRoot || "/tmp", "llm");

        // 現状の最小実装: inventory source のみ対応。
        if (params.source !== "inventory") {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_reextract",
            errorCode: "NOT_IMPLEMENTED_SOURCE",
            message: "source=needsReview/version not implemented yet in plugin; use inventory source for now.",
          });
        }

        const queue = path.join(outDir, "queue_manual_reextract.jsonl");
        const args = [
          "run",
          "python",
          resolved.scriptPath,
          "--queue",
          queue,
          "--outdir",
          outDir,
          "--batch-size",
          String(params.batchSize ?? 50),
        ];
        if (params.extractionVersion) args.push("--extraction-version", String(params.extractionVersion));
        const r = runCmd("uv", args, resolved.cwd);
        return toToolResult({
          ok: r.ok,
          tool: "video_pipeline_reextract",
          scriptSource: resolved.source,
          scriptPath: resolved.scriptPath,
          exitCode: r.code,
          stdout: r.stdout,
          stderr: r.stderr,
          queue,
        });
      },
    },
    { optional: true },
  );
}
