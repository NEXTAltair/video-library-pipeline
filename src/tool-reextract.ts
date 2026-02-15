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
        const hintsPath = path.join(getExtensionRootDir(), "rules", "program_aliases.yaml");
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
          queueAutoCreated,
        });
      },
    },
    { optional: true },
  );
}
