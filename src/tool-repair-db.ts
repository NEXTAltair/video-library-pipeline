import { resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

export function registerToolRepairDb(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_repair_db",
      description: "Run DB repair helpers for path/link consistency (safe/full modes).",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          mode: { type: "string", enum: ["safe", "full"], default: "safe" },
          dryRun: { type: "boolean", default: true },
          limit: { type: "integer", minimum: 1, maximum: 5000 },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("repair_paths_dir_name_from_path.py");
        const args = ["run", "python", resolved.scriptPath, "--db", cfg.db || ""];
        if (!params.dryRun) args.push("--apply");
        const r = runCmd("uv", args, resolved.cwd);
        return toToolResult({
          ok: r.ok,
          tool: "video_pipeline_repair_db",
          mode: params.mode ?? "safe",
          scriptSource: resolved.source,
          scriptPath: resolved.scriptPath,
          exitCode: r.code,
          stdout: r.stdout,
          stderr: r.stderr,
        });
      },
    },
    { optional: true },
  );
}
