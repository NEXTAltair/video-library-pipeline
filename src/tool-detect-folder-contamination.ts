import { parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

export function registerToolDetectFolderContamination(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_detect_folder_contamination",
      description:
        "Detect by_program folder names contaminated with subtitle/episode info. " +
        "Cross-references programs table for suggested corrections. " +
        "Supports full-scan or user-specified scope (representativePath / targetProgramTitle). Returns updateInstructions array compatible with video_pipeline_update_program_titles.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          minExtraChars: {
            type: "integer",
            minimum: 1,
            maximum: 20,
            default: 4,
            description: "Minimum extra characters beyond matched title to consider contaminated.",
          },
          representativePath: {
            type: "string",
            description: "Optional user-specified wrong path/folder. Limits detection scope to titles resolved from this path.",
          },
          targetProgramTitle: {
            type: "string",
            description: "Optional explicit current (wrong) program_title to correct. Can be combined with representativePath.",
          },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("detect_folder_contamination.py");

        const args = [
          "run",
          "python",
          resolved.scriptPath,
          "--db",
          String(cfg.db || ""),
          "--dry-run",
        ];

        if (typeof params.minExtraChars === "number" && Number.isFinite(params.minExtraChars)) {
          args.push("--min-extra-chars", String(Math.trunc(params.minExtraChars)));
        }
        if (typeof params.representativePath === "string" && params.representativePath.trim()) {
          args.push("--target-path", params.representativePath.trim());
        }
        if (typeof params.targetProgramTitle === "string" && params.targetProgramTitle.trim()) {
          args.push("--target-title", params.targetProgramTitle.trim());
        }

        const r = runCmd("uv", args, resolved.cwd);
        const parsed = parseJsonObject(r.stdout);
        const out: AnyObj = {
          ok: r.ok,
          tool: "video_pipeline_detect_folder_contamination",
          scriptSource: resolved.source,
          scriptPath: resolved.scriptPath,
          exitCode: r.code,
          stdout: r.stdout,
          stderr: r.stderr,
        };
        if (parsed) {
          for (const [k, v] of Object.entries(parsed)) out[k] = v;
        }

        // Suggest follow-up tool calls based on result
        if (parsed && Array.isArray(parsed.updateInstructions) && parsed.updateInstructions.length > 0) {
          out.followUpToolCalls = [
            {
              tool: "video_pipeline_update_program_titles",
              description: "Fix contaminated program titles (dry-run first, uses path_id matching)",
              params: {
                updates: JSON.stringify(parsed.updateInstructions),
                dryRun: true,
              },
            },
          ];
        }
        // Strip verbose pathIds from contaminatedTitles in tool output (keep in updateInstructions)
        if (parsed && Array.isArray(parsed.contaminatedTitles)) {
          out.contaminatedTitles = parsed.contaminatedTitles.map((e: any) => {
            const { pathIds, ...rest } = e;
            return { ...rest, pathIdCount: Array.isArray(pathIds) ? pathIds.length : 0 };
          });
        }
        return toToolResult(out);
      },
    }
  );
}
