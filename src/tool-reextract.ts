import { resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

export function registerToolReextract(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_reextract",
      description: "Run metadata re-extraction batch from queue JSONL.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          queuePath: { type: "string" },
          extractionVersion: { type: "string" },
          batchSize: { type: "integer", minimum: 1, maximum: 1000, default: 50 },
          maxBatches: { type: "integer", minimum: 1, maximum: 1000 },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("run_metadata_batches_promptv1.py");
        const hostRoot = String(cfg.windowsOpsRoot || "/tmp").replace(/\/+$/, "");
        const outDir = `${hostRoot}/llm`;
        const hintsPath = `${hostRoot}/rules/program_aliases.yaml`;
        const queue = String(params.queuePath || `${outDir}/queue_manual_reextract.jsonl`);
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
