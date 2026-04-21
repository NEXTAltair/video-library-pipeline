import path from "node:path";
import { getExtensionRootDir, parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj, PluginApi, GetCfgFn } from "./types";
import { ensureWindowsScripts } from "./windows-scripts-bootstrap";

export function registerToolBackfill(api: PluginApi, getCfg: GetCfgFn) {
  api.registerTool(
    {
      name: "video_pipeline_backfill_moved_files",
      description: "DB sync: register existing library files into DB (paths/observations). Does NOT physically move files. Use for 'DBに登録', 'DB化', 'ライブラリを反映'. backfill_roots.yaml covers all drives — omit roots unless scanning a new location.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          apply: { type: "boolean", default: false, description: "false = dry-run (no DB changes). true = write to DB." },
          roots: { type: "array", items: { type: "string" }, description: "Windows paths to scan. Omit to use backfill_roots.yaml (covers all library drives). Only set if scanning a new/temporary location." },
          rootsFilePath: { type: "string", description: "Path to YAML/text file listing roots. Alternative to roots array." },
          extensions: { type: "array", items: { type: "string" }, default: [".mp4"] },
          limit: { type: "integer", minimum: 1, maximum: 100000 },
          includeObservations: { type: "boolean", default: true, description: "Also upsert file observations (size, mtime). Default true." },
          queueMissingMetadata: { type: "boolean", default: false, description: "Queue newly upserted rows that are missing metadata for reextract. Scoped to rows touched by this run only." },
          driveMap: { type: "object", additionalProperties: { type: "string" } },
          detectCorruption: { type: "boolean", default: true },
          corruptionReadBytes: { type: "integer", minimum: 1, maximum: 1048576, default: 4096 },
          scanErrorPolicy: { type: "string", enum: ["warn", "fail", "threshold"], default: "warn" },
          scanErrorThreshold: { type: "integer", minimum: 1, maximum: 100000 },
          scanRetryCount: { type: "integer", minimum: 0, maximum: 10, default: 1 },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("backfill_moved_files.py");
        const defaultRootsFile = path.join(getExtensionRootDir(), "rules", "backfill_roots.yaml");
        const scriptsProvision = ensureWindowsScripts(cfg);
        if (!scriptsProvision.ok) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_backfill_moved_files",
            error: "failed to provision required windows scripts",
            scriptsProvision: {
              created: scriptsProvision.created,
              updated: scriptsProvision.updated,
              existing: scriptsProvision.existing,
              failed: scriptsProvision.failed,
              missingTemplates: scriptsProvision.missingTemplates,
            },
          });
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
          "--roots-file-path",
          String(params.rootsFilePath || defaultRootsFile),
          "--include-observations",
          String(params.includeObservations !== false),
          "--queue-missing-metadata",
          String(params.queueMissingMetadata === true),
          "--detect-corruption",
          String(params.detectCorruption !== false),
          "--corruption-read-bytes",
          String(params.corruptionReadBytes ?? 4096),
          "--scan-error-policy",
          String(params.scanErrorPolicy || "warn"),
          "--scan-retry-count",
          String(params.scanRetryCount ?? 1),
        ];
        if (typeof params.scanErrorThreshold === "number" && Number.isFinite(params.scanErrorThreshold)) {
          args.push("--scan-error-threshold", String(Math.trunc(params.scanErrorThreshold)));
        }
        if (params.apply === true) args.push("--apply");
        if (Array.isArray(params.roots) && params.roots.length > 0) {
          args.push("--roots-json", JSON.stringify(params.roots));
        }
        if (Array.isArray(params.extensions) && params.extensions.length > 0) {
          args.push("--extensions-json", JSON.stringify(params.extensions));
        }
        if (typeof params.limit === "number" && Number.isFinite(params.limit)) {
          args.push("--limit", String(Math.trunc(params.limit)));
        }
        if (params.driveMap && typeof params.driveMap === "object" && !Array.isArray(params.driveMap)) {
          args.push("--drive-map-json", JSON.stringify(params.driveMap));
        }

        const r = runCmd("uv", args, resolved.cwd);
        const parsed = parseJsonObject(r.stdout);
        const out: AnyObj = {
          ok: r.ok,
          tool: "video_pipeline_backfill_moved_files",
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
        return toToolResult(out);
      },
    }
  );
}
