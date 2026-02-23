import path from "node:path";
import { getExtensionRootDir, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

function parseJsonObject(input: unknown): AnyObj | null {
  if (typeof input !== "string") return null;
  const s = input.trim();
  if (!s) return null;
  try {
    const parsed = JSON.parse(s);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) return parsed as AnyObj;
    return null;
  } catch {
    return null;
  }
}

export function registerToolBackfill(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_backfill_moved_files",
      description: "Backfill already-moved files into DB with dry-run/apply and optional metadata queue generation.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          apply: { type: "boolean", default: false },
          roots: { type: "array", items: { type: "string" } },
          rootsFilePath: { type: "string" },
          extensions: { type: "array", items: { type: "string" }, default: [".mp4"] },
          limit: { type: "integer", minimum: 1, maximum: 100000 },
          includeObservations: { type: "boolean", default: true },
          queueMissingMetadata: { type: "boolean", default: false },
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
        };
        if (parsed) {
          for (const [k, v] of Object.entries(parsed)) out[k] = v;
        }
        return toToolResult(out);
      },
    },
    { optional: true },
  );
}
