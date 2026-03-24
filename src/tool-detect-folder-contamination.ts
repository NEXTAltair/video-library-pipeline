import { parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

export function registerToolDetectFolderContamination(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_detect_folder_contamination",
      description:
        "Detect by_program folder names contaminated with subtitle/episode info. " +
        "Cross-references programs table for suggested corrections. " +
        "Returns updateInstructions array compatible with video_pipeline_update_program_titles. " +
        "Supports targeted mode (programTitle / representativePathLike) for operator-directed cleanup.",
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
            description: "Target a specific current program_title (exact match).",
          },
          representativePathLike: {
            type: "string",
            description: "Target records by paths.path LIKE pattern (for user-specified wrong folder).",
          },
          preferredTitle: {
            type: "string",
            description: "Operator-provided canonical title override to use when building suggestions.",
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
        if (typeof params.preferredTitle === "string" && params.preferredTitle.trim()) {
          args.push("--preferred-title", params.preferredTitle.trim());
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

        // Shared review contract with extract-review YAML: hints[].canonical_title + aliases[].
        if (parsed && Array.isArray(parsed.contaminatedTitles)) {
          out.reviewYamlContract = "program_aliases_v1";
          out.reviewYamlTemplate = {
            hints: parsed.contaminatedTitles.map((e: any) => ({
              canonical_title: String(e?.suggestedTitle || ""),
              aliases: [String(e?.programTitle || "")].filter(Boolean),
            })),
          };
          out.reviewInstructions = [
            "Keep entry as-is to accept suggested canonical title.",
            "Edit canonical_title to override suggestion.",
            "Remove entry to skip.",
            "Optionally append aliases[] synonyms if needed.",
          ];
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
        // Keep compact output while preserving targeted operator workflow details.
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
