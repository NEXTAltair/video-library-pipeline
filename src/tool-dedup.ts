import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
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

export function registerToolDedup(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_dedup_recordings",
      description: "Detect duplicate recordings and isolate drop candidates to quarantine (dry-run/apply).",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          apply: { type: "boolean", default: false },
          maxGroups: { type: "integer", minimum: 1, maximum: 5000 },
          confidenceThreshold: { type: "number", minimum: 0, maximum: 1, default: 0.85 },
          allowNeedsReview: { type: "boolean", default: false },
          keepTerrestrialAndBscs: { type: "boolean", default: true },
          bucketRulesPath: { type: "string" },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("dedup_recordings.py");
        const defaultRulesPath = path.join(getExtensionRootDir(), "rules", "broadcast_buckets.yaml");
        const scriptsProvision = ensureWindowsScripts(cfg);
        if (!scriptsProvision.ok) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_dedup_recordings",
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

        // --- czkawka ハッシュスキャン前段 ---
        const czkawkaCfgRaw = api?.config?.plugins?.entries?.["czkawka-cli"]?.config ?? {};
        const czkawkaCliPath: string = String(czkawkaCfgRaw.czkawkaCliPath || "czkawka_cli");
        const tmpJsonPath = path.join(os.tmpdir(), `dedup_hash_${Date.now()}.json`);

        let hashScanOk = false;
        let hashScanExitCode: number | null = null;
        let hashScanError: string | undefined;

        try {
          const czkawkaArgs = [
            "dup",
            "--search-method", "HASH",
            "--hash-type", "BLAKE3",
            "--directories", cfg.sourceRoot, cfg.destRoot,
            "--thread-number", "4",
            "--compact-file-to-save", tmpJsonPath,
          ];
          const cp = spawnSync(czkawkaCliPath, czkawkaArgs, {
            env: process.env,
            encoding: "utf-8",
            timeout: 10 * 60 * 1000,
          });
          hashScanExitCode = cp.status ?? null;
          if (cp.error) {
            hashScanError = String(cp.error?.message || cp.error);
          } else if (cp.status !== 0) {
            hashScanError = `czkawka exited with code ${cp.status}. stderr: ${String(cp.stderr || "").slice(0, 500)}`;
          } else {
            hashScanOk = true;
          }
        } catch (e: any) {
          hashScanError = String(e?.message || e);
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
          "--hash-scan-path",
          hashScanOk ? tmpJsonPath : "",
        ];
        if (params.apply === true) args.push("--apply");
        if (typeof params.maxGroups === "number" && Number.isFinite(params.maxGroups)) {
          args.push("--max-groups", String(Math.trunc(params.maxGroups)));
        }

        const r = runCmd("uv", args, resolved.cwd);

        // 一時 JSON を削除
        try {
          if (fs.existsSync(tmpJsonPath)) fs.rmSync(tmpJsonPath);
        } catch {
          // ignore cleanup errors
        }

        const parsed = parseJsonObject(r.stdout);
        const out: AnyObj = {
          ok: r.ok,
          tool: "video_pipeline_dedup_recordings",
          scriptSource: resolved.source,
          scriptPath: resolved.scriptPath,
          exitCode: r.code,
          stdout: r.stdout,
          stderr: r.stderr,
          hashScan: {
            ok: hashScanOk,
            exitCode: hashScanExitCode,
            tmpJsonPath: hashScanOk ? tmpJsonPath : null,
            error: hashScanError ?? null,
          },
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
    },
    { optional: true },
  );
}
