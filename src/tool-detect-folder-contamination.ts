import { parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

export function registerToolDetectFolderContamination(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_detect_folder_contamination",
      description:
        "Detect by_program folder names contaminated with subtitle/episode info. " +
        "Cross-references programs table for suggested corrections. " +
        "Supports targeted mode (programTitle / representativePathLike / pathIds) for operator-directed cleanup. " +
        "Returns updateInstructions array compatible with video_pipeline_update_program_titles.",
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
          programTitle: {
            type: "string",
            description:
              "Optional explicit target. Analyze only rows whose current program_title exactly matches this value.",
          },
          representativePathLike: {
            type: "string",
            description:
              "Optional explicit target. SQL LIKE pattern used to resolve affected rows from a representative bad path.",
          },
          pathIds: {
            type: "array",
            description: "Optional explicit target. Restrict analysis to these path_id values.",
            items: { type: "string" },
          },
          includePathIds: {
            type: "boolean",
            default: true,
            description: "If false, hide pathIds in contaminatedTitles for compact output.",
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
        if (typeof params.programTitle === "string" && params.programTitle.trim()) {
          args.push("--program-title", params.programTitle.trim());
        }
        if (typeof params.representativePathLike === "string" && params.representativePathLike.trim()) {
          args.push("--path-like", params.representativePathLike.trim());
        }
        if (Array.isArray(params.pathIds)) {
          for (const id of params.pathIds) {
            if (typeof id === "string" && id.trim()) args.push("--path-id", id.trim());
          }
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
        // Conditionally strip verbose pathIds from contaminatedTitles (keep in updateInstructions)
        if (params.includePathIds === false && parsed && Array.isArray(parsed.contaminatedTitles)) {
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
