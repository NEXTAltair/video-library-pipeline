import { resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj, PluginApi, GetCfgFn } from "./types";

export function registerToolRepairDb(api: PluginApi, getCfg: GetCfgFn) {
  api.registerTool(
    {
      name: "video_pipeline_repair_db",
      description:
        "Repair DB issues. Actions: " +
        "'repair_paths' (default) — fix path fields from canonical Windows path values. " +
        "'clear_review_flags' — clear needs_review for human_reviewed rows where the only remaining review reasons are title-related and program_title is already clean.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          action: { type: "string", enum: ["repair_paths", "clear_review_flags"], default: "repair_paths" },
          dryRun: { type: "boolean", default: true },
          limit: { type: "integer", minimum: 1, maximum: 5000 },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const action = String(params.action || "repair_paths");

        if (action === "clear_review_flags") {
          const resolved = resolvePythonScript("clear_stale_review_flags.py");
          const args = ["run", "python", resolved.scriptPath, "--db", cfg.db || ""];
          if (!params.dryRun) args.push("--apply");
          const r = runCmd("uv", args, resolved.cwd);
          return toToolResult({
            ok: r.ok,
            tool: "video_pipeline_repair_db",
            action,
            scriptSource: resolved.source,
            scriptPath: resolved.scriptPath,
            exitCode: r.code,
            stdout: r.stdout,
            stderr: r.stderr,
          });
        }

        // Default: repair_paths
        const resolved = resolvePythonScript("repair_paths_dir_name_from_path.py");
        const args = ["run", "python", resolved.scriptPath, "--db", cfg.db || ""];
        if (!params.dryRun) args.push("--apply");
        if (params.limit) args.push("--limit", String(params.limit));
        const r = runCmd("uv", args, resolved.cwd);
        return toToolResult({
          ok: r.ok,
          tool: "video_pipeline_repair_db",
          action,
          scriptSource: resolved.source,
          scriptPath: resolved.scriptPath,
          exitCode: r.code,
          stdout: r.stdout,
          stderr: r.stderr,
        });
      },
    }
  );
}
