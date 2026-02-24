import path from "node:path";
import { getExtensionRootDir, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";
import { ensureWindowsScripts } from "./windows-scripts-bootstrap";

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

export function registerToolRelocate(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_relocate_existing_files",
      description: "Relocate existing files under specified roots to current placement rules using DB metadata (dry-run/apply).",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          apply: { type: "boolean", default: false },
          roots: { type: "array", items: { type: "string" } },
          rootsFilePath: { type: "string" },
          extensions: { type: "array", items: { type: "string" }, default: [".mp4"] },
          limit: { type: "integer", minimum: 1, maximum: 100000 },
          allowNeedsReview: { type: "boolean", default: false },
          queueMissingMetadata: { type: "boolean", default: false },
          writeMetadataQueueOnDryRun: { type: "boolean", default: false },
          scanErrorPolicy: { type: "string", enum: ["warn", "fail", "threshold"], default: "warn" },
          scanErrorThreshold: { type: "integer", minimum: 1, maximum: 100000 },
          scanRetryCount: { type: "integer", minimum: 0, maximum: 10, default: 1 },
          onDstExists: { type: "string", enum: ["error", "rename_suffix"], default: "error" },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("relocate_existing_files.py");
        const defaultRootsFile = path.join(getExtensionRootDir(), "rules", "relocate_roots.yaml");
        const scriptsProvision = ensureWindowsScripts(cfg);
        if (!scriptsProvision.ok) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_relocate_existing_files",
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
          "--allow-needs-review",
          String(params.allowNeedsReview === true),
          "--queue-missing-metadata",
          String(params.queueMissingMetadata === true),
          "--write-metadata-queue-on-dry-run",
          String(params.writeMetadataQueueOnDryRun === true),
          "--scan-error-policy",
          String(params.scanErrorPolicy || "warn"),
          "--scan-retry-count",
          String(params.scanRetryCount ?? 1),
          "--on-dst-exists",
          String(params.onDstExists || "error"),
        ];

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
        if (typeof params.scanErrorThreshold === "number" && Number.isFinite(params.scanErrorThreshold)) {
          args.push("--scan-error-threshold", String(Math.trunc(params.scanErrorThreshold)));
        }

        const r = runCmd("uv", args, resolved.cwd);
        const parsed = parseJsonObject(r.stdout);
        const out: AnyObj = {
          ok: r.ok,
          tool: "video_pipeline_relocate_existing_files",
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
        const followUpToolCalls: AnyObj[] = [];
        const roots = Array.isArray(params.roots) ? params.roots.filter((v) => typeof v === "string") : undefined;
        const rootsFilePath = typeof params.rootsFilePath === "string" ? params.rootsFilePath : undefined;
        const commonRelocateParams: AnyObj = {
          ...(roots && roots.length ? { roots } : {}),
          ...(rootsFilePath ? { rootsFilePath } : {}),
          ...(Array.isArray(params.extensions) ? { extensions: params.extensions } : {}),
          ...(typeof params.limit === "number" ? { limit: Math.trunc(params.limit) } : {}),
          ...(typeof params.scanErrorPolicy === "string" ? { scanErrorPolicy: params.scanErrorPolicy } : {}),
          ...(typeof params.scanRetryCount === "number" ? { scanRetryCount: Math.trunc(params.scanRetryCount) } : {}),
          ...(typeof params.scanErrorThreshold === "number"
            ? { scanErrorThreshold: Math.trunc(params.scanErrorThreshold) }
            : {}),
        };
        const metadataGap =
          Number(out.metadataQueuePlannedCount || 0) > 0 ||
          out.requiresMetadataPreparation === true ||
          Number(out.metadataMissingSkipped || 0) > 0;
        if (r.ok && params.apply !== true && metadataGap) {
          followUpToolCalls.push({
            tool: "video_pipeline_prepare_relocate_metadata",
            reason: "metadata_preparation_required_before_relocate_apply",
            params: {
              ...commonRelocateParams,
              runReextract: true,
            },
          });
        }
        if (r.ok && params.apply !== true && Number(out.plannedMoves || 0) > 0 && !metadataGap) {
          followUpToolCalls.push({
            tool: "video_pipeline_relocate_existing_files",
            reason: "relocate_plan_ready_for_apply_after_review",
            params: {
              ...commonRelocateParams,
              apply: true,
              allowNeedsReview: params.allowNeedsReview === true,
              onDstExists: typeof params.onDstExists === "string" ? params.onDstExists : "error",
            },
          });
        }
        if (followUpToolCalls.length > 0) {
          out.followUpToolCalls = followUpToolCalls;
          out.hasFollowUpToolCalls = true;
        } else {
          out.followUpToolCalls = [];
          out.hasFollowUpToolCalls = false;
        }
        return toToolResult(out);
      },
    },
    { optional: true },
  );
}
