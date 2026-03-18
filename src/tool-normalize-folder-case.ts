import fs from "node:fs";
import { parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";
import { ensureWindowsScripts } from "./windows-scripts-bootstrap";

export function registerToolNormalizeFolderCase(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_normalize_folder_case",
      description:
        "Normalize case-only folder name differences using a relocate dry-run plan. " +
        "Use this when relocate marks entries as already_correct due to Windows case-insensitive paths.",
      parameters: {
        type: "object",
        additionalProperties: false,
        required: ["planPath"],
        properties: {
          planPath: {
            type: "string",
            description:
              "Path to relocate dry-run plan JSONL (planPath from video_pipeline_relocate_existing_files apply=false).",
          },
          apply: {
            type: "boolean",
            default: false,
            description: "false=dry-run candidate extraction only, true=perform actual case normalization and DB path update.",
          },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const scriptsProvision = ensureWindowsScripts(cfg);
        if (!scriptsProvision.ok) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_normalize_folder_case",
            error: "failed to provision required windows scripts",
            scriptsProvision,
          });
        }

        const planPath = typeof params.planPath === "string" ? params.planPath.trim() : "";
        if (!planPath) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_normalize_folder_case",
            error: "planPath is required",
          });
        }
        if (!fs.existsSync(planPath)) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_normalize_folder_case",
            error: `planPath does not exist: ${planPath}`,
          });
        }

        const resolved = resolvePythonScript("normalize_folder_case_from_plan.py");
        const args = [
          "run",
          "python",
          resolved.scriptPath,
          "--db",
          String(cfg.db || ""),
          "--windows-ops-root",
          String(cfg.windowsOpsRoot || ""),
          "--plan-path",
          planPath,
        ];
        if (params.apply === true) args.push("--apply");

        const r = runCmd("uv", args, resolved.cwd);
        const parsed = parseJsonObject(r.stdout);
        const out: AnyObj = {
          ok: r.ok,
          tool: "video_pipeline_normalize_folder_case",
          scriptSource: resolved.source,
          scriptPath: resolved.scriptPath,
          exitCode: r.code,
          stdout: r.stdout,
          stderr: r.stderr,
          scriptsProvision: {
            created: scriptsProvision.created,
            updated: scriptsProvision.updated,
            existing: scriptsProvision.existing,
            failed: scriptsProvision.failed,
            missingTemplates: scriptsProvision.missingTemplates,
          },
        };
        if (parsed) {
          for (const [k, v] of Object.entries(parsed)) out[k] = v;
        }
        if (r.ok && params.apply !== true && Number(out.caseCandidateDirs || 0) > 0) {
          out.nextAction =
            "Review normalize_case_plan and rerun with apply=true to perform directory case normalization and DB path updates.";
        }
        if (r.ok && params.apply !== true && Number(out.caseCandidateDirs || 0) === 0) {
          out.nextAction = "No case-only folder mismatch candidates found in this relocate plan.";
        }
        return toToolResult(out);
      },
    },
  );
}
