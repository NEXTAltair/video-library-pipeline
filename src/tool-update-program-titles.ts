import { resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

export function registerToolUpdateProgramTitles(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_update_program_titles",
      description:
        "Update program_title for specified records. Sets human_reviewed=1, needs_review=0. " +
        "Use path_pattern (SQL LIKE) or path_id to identify target records. Always dry-run first.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          updates: {
            type: "array",
            description:
              'Array of updates. Each item: {"path_pattern": "%LIKE pattern%", "new_title": "corrected title"} or {"path_id": "...", "new_title": "..."}',
            items: {
              type: "object",
              properties: {
                path_pattern: {
                  type: "string",
                  description: "SQL LIKE pattern matched against the Windows file path",
                },
                path_id: {
                  type: "string",
                  description: "Exact path_id to update",
                },
                new_title: {
                  type: "string",
                  description: "The corrected program title",
                },
              },
              required: ["new_title"],
            },
          },
          dryRun: {
            type: "boolean",
            default: true,
            description: "If true, show what would change without modifying DB",
          },
        },
        required: ["updates"],
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("update_program_titles.py");
        const updatesJson = JSON.stringify(params.updates || []);
        const args = [
          "run", "python", resolved.scriptPath,
          "--db", cfg.db || "",
          "--updates", updatesJson,
        ];
        if (params.dryRun !== false) args.push("--dry-run");
        const r = runCmd("uv", args, resolved.cwd);
        return toToolResult({
          ok: r.ok,
          tool: "video_pipeline_update_program_titles",
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
