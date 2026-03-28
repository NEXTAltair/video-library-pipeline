import path from "node:path";
import { getExtensionRootDir, parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";
import { ensureWindowsScripts } from "./windows-scripts-bootstrap";


export function registerToolDedupRebroadcasts(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_dedup_rebroadcasts",
      description:
        "Detect and quarantine same-episode recordings from different channels/dates (rebroadcast dedup). " +
        "Groups by normalized program key + episode number or subtitle. " +
        "Run after hash-exact dedup (dedup_recordings). " +
        "Dry-run by default; set apply=true to move drop candidates to rebroadcast_quarantine/.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          apply: { type: "boolean", default: false },
          maxGroups: { type: "integer", minimum: 1, maximum: 5000 },
          confidenceThreshold: { type: "number", minimum: 0, maximum: 1, default: 0.85 },
          allowNeedsReview: { type: "boolean", default: false },
          keepTerrestrialAndBscs: { type: "boolean", default: true },
          programTitleContains: { type: "string" },
          genreContains: { type: "string" },
          bucketRulesPath: { type: "string" },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("dedup_rebroadcasts.py");
        const defaultRulesPath = path.join(getExtensionRootDir(), "rules", "broadcast_buckets.yaml");
        const scriptsProvision = ensureWindowsScripts(cfg);
        if (!scriptsProvision.ok) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_dedup_rebroadcasts",
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
          "--confidence-threshold",
          String(params.confidenceThreshold ?? 0.85),
          "--allow-needs-review",
          String(params.allowNeedsReview === true),
          "--keep-terrestrial-and-bscs",
          String(params.keepTerrestrialAndBscs !== false),
          "--bucket-rules-path",
          String(params.bucketRulesPath || defaultRulesPath),
          "--program-title-contains",
          String(params.programTitleContains || ""),
          "--genre-contains",
          String(params.genreContains || ""),
        ];
        if (params.apply === true) args.push("--apply");
        if (typeof params.maxGroups === "number" && Number.isFinite(params.maxGroups)) {
          args.push("--max-groups", String(Math.trunc(params.maxGroups)));
        }

        const r = runCmd("uv", args, resolved.cwd);
        const parsed = parseJsonObject(r.stdout);
        const out: AnyObj = {
          ok: r.ok,
          tool: "video_pipeline_dedup_rebroadcasts",
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
