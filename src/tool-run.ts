import { injectCommonPathArgs, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

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
          pathsOverride: {
            type: "object",
            additionalProperties: false,
            properties: {
              db: { type: "string" },
              sourceRoot: { type: "string" },
              destRoot: { type: "string" },
              windowsOpsRoot: { type: "string" },
            },
          },
        },
      },
      async execute(_id: string, params: AnyObj) {
        // config 値に対して、呼び出し時の pathsOverride を上書き適用。
        const base = getCfg(api);
        const merged = { ...base, ...(params.pathsOverride ?? {}) };
        const resolved = resolvePythonScript("unwatched_pipeline_runner.py");

        // uv run python で runner を起動。
        let args = [
          "run",
          "python",
          resolved.scriptPath,
          "--max-files-per-run",
          String(params.maxFilesPerRun ?? merged.defaultMaxFilesPerRun ?? 200),
        ];
        if (params.apply) args.push("--apply");
        if (params.allowNeedsReview) args.push("--allow-needs-review");
        args = injectCommonPathArgs(args, merged as any);

        const r = runCmd("uv", args, resolved.cwd);
        return toToolResult({
          ok: r.ok,
          tool: "video_pipeline_analyze_and_move_videos",
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
