import { resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";
import { ensureWindowsScripts } from "./windows-scripts-bootstrap";

export function registerToolRun(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_analyze_and_move_videos",
      description:
        "Analyze videos in source folder and move them to destination folder. Use apply=false for dry-run.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          apply: { type: "boolean", default: false },
          maxFilesPerRun: {
            type: "integer",
            minimum: 1,
            maximum: 5000,
            description: "Maximum files to process in one run for queue and plan stages.",
          },
          allowNeedsReview: { type: "boolean", default: false },
          profile: { type: "string" },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("unwatched_pipeline_runner.py");
        const scriptsProvision = ensureWindowsScripts(cfg);

        if (!scriptsProvision.ok) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_analyze_and_move_videos",
            error: "failed to provision required windows scripts",
            scriptsProvision: {
              created: scriptsProvision.created,
              existing: scriptsProvision.existing,
              failed: scriptsProvision.failed,
              missingTemplates: scriptsProvision.missingTemplates,
            },
          });
        }

        // uv run python で runner を起動。
        const args = [
          "run",
          "python",
          resolved.scriptPath,
          "--db",
          cfg.db,
          "--source-root",
          cfg.sourceRoot,
          "--dest-root",
          cfg.destRoot,
          "--windows-ops-root",
          cfg.windowsOpsRoot,
          "--max-files-per-run",
          String(params.maxFilesPerRun ?? cfg.defaultMaxFilesPerRun ?? 200),
        ];
        if (params.apply) args.push("--apply");
        if (params.allowNeedsReview) args.push("--allow-needs-review");

        const r = runCmd("uv", args, resolved.cwd);
        return toToolResult({
          ok: r.ok,
          tool: "video_pipeline_analyze_and_move_videos",
          scriptSource: resolved.source,
          scriptPath: resolved.scriptPath,
          exitCode: r.code,
          stdout: r.stdout,
          stderr: r.stderr,
          scriptsProvision: {
            created: scriptsProvision.created,
            existing: scriptsProvision.existing,
            failed: scriptsProvision.failed,
            missingTemplates: scriptsProvision.missingTemplates,
          },
        });
      },
    },
    { optional: true },
  );
}
