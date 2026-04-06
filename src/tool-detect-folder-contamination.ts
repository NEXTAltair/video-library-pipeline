import { parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj, PluginApi, GetCfgFn } from "./types";

export function registerToolDetectFolderContamination(api: PluginApi, getCfg: GetCfgFn) {
  api.registerTool(
    {
      name: "video_pipeline_detect_folder_contamination",
      description:
        "Detect by_program folder names contaminated with subtitle/episode info. " +
        "Cross-references programs table for suggested corrections. " +
        "Supports targeted mode (programTitle / representativePathLike / pathIds) for operator-directed cleanup. " +
        "Returns updateInstructions array compatible with video_pipeline_update_program_titles. " +
        "In targeted mode, always returns resolvedTargets[] with current titles and path IDs even when no " +
        "auto-suggestion is generated, enabling operator-forced correction without detect blocking the flow.",
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
          programTitleContains: {
            type: "string",
            description:
              "Broad search: find all rows whose program_title contains this substring. " +
              "Returns all matching title groups in resolvedTargets for bulk YAML review, " +
              "even when auto-detection confidence is low. Use for series-family cleanup " +
              '(e.g., "NHKスペシャル" to list all NHK Special variants).',
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
          canonicalTitle: {
            type: "string",
            description:
              "Operator-supplied correct title. When provided in targeted mode, " +
              "builds updateInstructions directly from resolved records even if auto-detection fails. " +
              "Use when operator already knows the canonical title and wants to skip the YAML review round-trip.",
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
        if (typeof params.programTitleContains === "string" && params.programTitleContains.trim()) {
          args.push("--program-title-contains", params.programTitleContains.trim());
        }
        if (typeof params.representativePathLike === "string" && params.representativePathLike.trim()) {
          args.push("--path-like", params.representativePathLike.trim());
        }
        if (Array.isArray(params.pathIds)) {
          for (const id of params.pathIds) {
            if (typeof id === "string" && id.trim()) args.push("--path-id", id.trim());
          }
        }
        if (typeof params.canonicalTitle === "string" && params.canonicalTitle.trim()) {
          args.push("--canonical-title", params.canonicalTitle.trim());
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
        } else if (
          parsed &&
          parsed.mode === "targeted" &&
          Array.isArray(parsed.resolvedTargets) &&
          parsed.resolvedTargets.length > 0 &&
          parsed.totalContaminatedTitles === 0 &&
          !parsed.operatorForced
        ) {
          // No auto-suggestion and no operator-supplied title.
          // Signal that operator-forced correction path should be used via SKILL.
          out.operatorForcedPathAvailable = true;
          out.hint =
            "No auto-suggestion generated, but resolvedTargets are available. " +
            "Options: (1) Re-call with canonicalTitle param to bypass YAML round-trip, or " +
            "(2) Follow the operator-forced path in SKILL.md: write review YAML with empty canonical_title, " +
            "let operator fill in correct title, then build update instructions from resolvedTargets.";
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
