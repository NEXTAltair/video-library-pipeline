import fs from "node:fs";
import path from "node:path";
import { getExtensionRootDir, parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj, PluginApi, GetCfgFn } from "./types";
import { ensureWindowsScripts } from "./windows-scripts-bootstrap";

export function registerToolNormalizeFolderCase(api: PluginApi, getCfg: GetCfgFn) {
  api.registerTool(
    {
      name: "video_pipeline_normalize_folder_case",
      description:
        "Fix case-only folder name differences between the filesystem and DB program_title. " +
        "Use after video_pipeline_relocate_existing_files dry-run shows already_correct entries caused by Windows case-insensitive paths (e.g. 'vs' vs 'VS', 'bs11' vs 'Bs11'). " +
        "Step 1: apply=false (dry-run) — queries DB to detect mismatches and writes a rename plan. " +
        "Step 2: review the plan, then apply=true + planPath — executes two-step renames and updates DB paths atomically.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          apply: { type: "boolean", default: false },
          planPath: { type: "string", description: "Required when apply=true. Use plan_path returned by dry-run." },
          roots: { type: "array", items: { type: "string" }, description: "Windows-style root paths to limit scope. Defaults to all is_current=1 DB entries." },
          rootsFilePath: { type: "string", description: "Path to YAML file with 'roots' list (e.g. relocate_roots.yaml). Used when roots is omitted." },
          limit: { type: "integer", minimum: 1, maximum: 100000, description: "Max files to process from DB (dry-run only)." },
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
          if (typeof params.planPath === "string") {
            args.push("--plan-path", String(params.planPath));
          }
        } else {
          if (Array.isArray(params.roots) && params.roots.length > 0) {
            args.push("--roots-json", JSON.stringify(params.roots));
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
