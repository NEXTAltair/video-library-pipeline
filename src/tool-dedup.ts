import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { getExtensionRootDir, parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";
import { ensureWindowsScripts } from "./windows-scripts-bootstrap";


function findLatestRawJson(outputRoot: string, prefix: string): string {
  if (!outputRoot) return "";
  if (!fs.existsSync(outputRoot)) return "";
  const files = fs.readdirSync(outputRoot, { withFileTypes: true })
    .filter((d) => d.isFile() && d.name.startsWith(prefix) && d.name.endsWith(".json"))
    .map((d) => path.join(outputRoot, d.name));
  if (files.length === 0) return "";
  files.sort((a, b) => {
    const am = fs.statSync(a).mtimeMs;
    const bm = fs.statSync(b).mtimeMs;
    return bm - am;
  });
  return files[0] || "";
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
        const czkawkaOutputRoot: string = String(czkawkaCfgRaw.outputRoot || "");
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

        // czkawka-cli プラグインの最新 raw JSON を使用（ハッシュ値付き）
        // 直接実行は compact JSON のみ生成するため、raw JSON はプラグイン側の出力を参照
        const hashRawJsonPath = findLatestRawJson(czkawkaOutputRoot, "dup_hash");

        const args = [
          "run",
          "python",
          resolved.scriptPath,
          "--db",
          String(cfg.db || ""),
          "--windows-ops-root",
          String(cfg.windowsOpsRoot || ""),
          "--keep-terrestrial-and-bscs",
          String(params.keepTerrestrialAndBscs !== false),
          "--bucket-rules-path",
          String(params.bucketRulesPath || defaultRulesPath),
          "--hash-scan-path",
          hashScanOk ? tmpJsonPath : "",
          "--hash-raw-json",
          hashRawJsonPath,
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
            rawJsonPath: hashRawJsonPath || null,
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
    }
  );
}
