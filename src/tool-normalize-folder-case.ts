import fs from "node:fs";
import { parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";
import { ensureWindowsScripts } from "./windows-scripts-bootstrap";

export function registerToolNormalizeFolderCase(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_normalize_folder_case",
      description:
        "Normalize case-only folder name differences (e.g. 'vs' -> 'VS') based on DB metadata destination rules. " +
        "Use apply=false for dry-run first, then apply=true with planPath.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          apply: {
            type: "boolean",
            default: false,
            description: "false=dry-run (plan only), true=execute case-normalization renames.",
          },
          planPath: {
            type: "string",
            description: "Required when apply=true. Plan path returned by dry-run.",
          },
          roots: {
            type: "array",
            items: { type: "string" },
            description: "Optional Windows roots to limit target files. Default: [destRoot].",
          },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("normalize_folder_case.py");
        const scriptsProvision = ensureWindowsScripts(cfg);
        if (!scriptsProvision.ok) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_normalize_folder_case",
            error: "failed to provision required windows scripts",
            scriptsProvision,
          });
        }

        if (params.apply === true) {
          const planPath = typeof params.planPath === "string" ? params.planPath.trim() : "";
          if (!planPath) {
            return toToolResult({
              ok: false,
              tool: "video_pipeline_normalize_folder_case",
              error: "apply=true requires planPath.",
            });
          }
          if (!fs.existsSync(planPath)) {
            return toToolResult({
              ok: false,
              tool: "video_pipeline_normalize_folder_case",
              error: `planPath does not exist: ${planPath}`,
            });
          }
        }

        const args = [
          "run",
          "python",
          resolved.scriptPath,
          "--db",
          String(cfg.db || ""),
          "--windows-ops-root",
          String(cfg.windowsOpsRoot || ""),
          "--dest-root",
          String(cfg.destRoot || ""),
        ];

        if (params.apply === true) args.push("--apply");
        if (typeof params.planPath === "string" && params.planPath.trim()) {
          args.push("--plan-path", String(params.planPath));
        }
        if (Array.isArray(params.roots) && params.roots.length > 0) {
          args.push("--roots-json", JSON.stringify(params.roots));
        }
        if (typeof cfg.driveRoutesPath === "string" && cfg.driveRoutesPath.trim()) {
          args.push("--drive-routes", String(cfg.driveRoutesPath));
        }

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
          scriptsProvision,
        };
        if (parsed) {
          for (const [k, v] of Object.entries(parsed)) out[k] = v;
        }
        return toToolResult(out);
      },
    },
  );
}
