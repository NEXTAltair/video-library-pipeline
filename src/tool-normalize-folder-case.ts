import fs from "node:fs";
import path from "node:path";
import { getExtensionRootDir, parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";
import { ensureWindowsScripts } from "./windows-scripts-bootstrap";

export function registerToolNormalizeFolderCase(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_normalize_folder_case",
      description:
        "Normalize folder-name casing based on DB program_title and placement rules. Dry-run first, then apply with planPath.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          apply: { type: "boolean", default: false },
          planPath: { type: "string", description: "Required when apply=true. Use plan_path from dry-run." },
          roots: { type: "array", items: { type: "string" }, description: "Scan roots (Windows style paths)." },
          rootsFilePath: { type: "string", description: "Path to YAML/text file listing roots." },
          extensions: { type: "array", items: { type: "string" }, default: [".mp4"] },
          limit: { type: "integer", minimum: 1, maximum: 100000 },
          allowNeedsReview: { type: "boolean", default: false },
          allowUnreviewedMetadata: { type: "boolean", default: false },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("normalize_folder_case.py");
        const defaultRootsFile = path.join(getExtensionRootDir(), "rules", "relocate_roots.yaml");
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
              error: "apply=true requires planPath parameter. Run dry-run first and pass the returned plan_path.",
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

        const driveRoutesPath = cfg.driveRoutesPath || path.join(getExtensionRootDir(), "rules", "drive_routes.yaml");

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
          "--roots-file-path",
          String(params.rootsFilePath || defaultRootsFile),
          "--allow-needs-review",
          String(params.allowNeedsReview === true),
          "--allow-unreviewed-metadata",
          String(params.allowUnreviewedMetadata === true),
        ];

        if (driveRoutesPath && fs.existsSync(driveRoutesPath)) {
          args.push("--drive-routes", driveRoutesPath);
        }
        if (params.apply === true) {
          args.push("--apply");
          args.push("--roots-json", "[]");
          args.push("--extensions-json", "[]");
          if (typeof params.planPath === "string") {
            args.push("--plan-path", String(params.planPath));
          }
        } else {
          if (Array.isArray(params.roots) && params.roots.length > 0) {
            args.push("--roots-json", JSON.stringify(params.roots));
          }
          if (Array.isArray(params.extensions) && params.extensions.length > 0) {
            args.push("--extensions-json", JSON.stringify(params.extensions));
          }
          if (typeof params.limit === "number" && Number.isFinite(params.limit)) {
            args.push("--limit", String(Math.trunc(params.limit)));
          }
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
    }
  );
}
