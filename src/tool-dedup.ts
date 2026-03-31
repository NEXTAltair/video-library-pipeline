import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { getExtensionRootDir, parseJsonObject, resolvePwsh, resolvePythonScript, runCmd, runCmdViaPwsh, toToolResult } from "./runtime";
import type { AnyObj } from "./types";
import { ensureWindowsScripts } from "./windows-scripts-bootstrap";


// /mnt/X/foo → X:\foo (drvfs WSL パスを Windows パスに変換)
function drvfsToWindowsPath(wslPath: string): string {
  const m = /^\/mnt\/([a-zA-Z])(\/(.*))?$/.exec(String(wslPath || ""));
  if (!m) return wslPath;
  const drive = m[1].toUpperCase();
  const rest = (m[3] || "").replace(/\//g, "\\");
  return rest ? `${drive}:\\${rest}` : `${drive}:\\`;
}

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

        // --- czkawka ハッシュスキャン前段 (Windows ネイティブバイナリ経由) ---
        // drvfs 経由だと長いファイル名を持つディレクトリを列挙できないため
        // windows_czkawka_cli を PowerShell 経由で呼び出す
        const czkawkaCfgRaw = api?.config?.plugins?.entries?.["czkawka-cli"]?.config ?? {};
        const czkawkaOutputRoot: string = String(czkawkaCfgRaw.outputRoot || "");
        const ts = Date.now();
        const winOpsRoot = drvfsToWindowsPath(String(cfg.windowsOpsRoot || ""));
        const winTmpJsonPath = `${winOpsRoot}\\dedup_hash_${ts}.json`;
        const wslTmpJsonPath = path.join(String(cfg.windowsOpsRoot || ""), `dedup_hash_${ts}.json`);

        // --- プリフライト: PowerShell 経由でパスアクセシビリティを確認 ---
        let preflightResult: { accessible: Record<string, boolean>; stdout?: string | null; stderr?: string | null; exitCode?: number; error?: string } | null = null;
        try {
          const checkPaths = [
            drvfsToWindowsPath(String(cfg.sourceRoot || "")),
            drvfsToWindowsPath(String(cfg.destRoot || "")),
            drvfsToWindowsPath(String(cfg.windowsOpsRoot || "")),
          ].filter(Boolean);
          const quoteLit = (p: string) => p.replace(/'/g, "''");
          const psCmd = `$r = @{}; ${checkPaths.map((p) => `$r['${quoteLit(p)}'] = (Test-Path -LiteralPath '${quoteLit(p)}' -ErrorAction SilentlyContinue)`).join("; ")}; $r | ConvertTo-Json -Compress`;
          const pfCp = spawnSync(resolvePwsh(), ["-NoProfile", "-Command", psCmd], { encoding: "utf-8", timeout: 15000, maxBuffer: 1024 * 1024 });
          const pfResult = { stdout: pfCp.stdout ?? "", stderr: pfCp.stderr ?? "", code: pfCp.status ?? -1 };
          const accessible: Record<string, boolean> = {};
          if (pfResult.stdout?.trim()) {
            try {
              const parsed = JSON.parse(pfResult.stdout.trim());
              for (const [k, v] of Object.entries(parsed)) accessible[k] = !!v;
            } catch {
              // ignore parse error
            }
          }
          preflightResult = { accessible, stdout: pfResult.stdout?.trim() || null, stderr: pfResult.stderr?.trim() || null, exitCode: pfResult.code };
        } catch (e: any) {
          preflightResult = { accessible: {}, error: String(e?.message || e) };
        }

        let hashScanOk = false;
        let hashScanExitCode: number | null = null;
        let hashScanError: string | undefined;

        try {
          const czkawkaArgs = [
            "dup",
            "--search-method", "HASH",
            "--hash-type", "BLAKE3",
            "-d", drvfsToWindowsPath(String(cfg.sourceRoot || "")),
            "-d", drvfsToWindowsPath(String(cfg.destRoot || "")),
            "--thread-number", "4",
            "--compact-file-to-save", winTmpJsonPath,
          ];
          const result = runCmdViaPwsh("windows_czkawka_cli", czkawkaArgs, { timeoutMs: 10 * 60 * 1000 });
          hashScanExitCode = result.code;
          // exit 1 = czkawka completed with warnings (e.g. some dirs unreadable)
          // JSON may still have been written — check file existence before giving up
          const jsonWritten = fs.existsSync(wslTmpJsonPath);
          if (result.ok || (result.code === 1 && jsonWritten)) {
            hashScanOk = true;
            if (result.code === 1) {
              hashScanError = `czkawka exited with code 1 (partial scan — some directories could not be read).`;
            }
          } else {
            hashScanError = [
              `czkawka exited with code ${result.code}.`,
              `jsonWritten: ${jsonWritten}`,
              `winSourceRoot: ${drvfsToWindowsPath(String(cfg.sourceRoot || ""))}`,
              `winDestRoot: ${drvfsToWindowsPath(String(cfg.destRoot || ""))}`,
              `winTmpJsonPath: ${winTmpJsonPath}`,
            ].join(" | ");
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
          hashScanOk ? wslTmpJsonPath : "",
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
          if (fs.existsSync(wslTmpJsonPath)) fs.rmSync(wslTmpJsonPath);
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
            wslTmpJsonPath: hashScanOk ? wslTmpJsonPath : null,
            rawJsonPath: hashRawJsonPath || null,
            error: hashScanError ?? null,
            preflight: preflightResult,
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

        // --- nextSteps guidance for the agent ---
        const nextSteps: string[] = [];
        const manualReview = Number(out.groupsManualReview ?? 0);
        const planPath = String(out.planPath ?? "");
        const filesDropped = Number(out.filesDropped ?? 0);

        if (manualReview > 0 && planPath) {
          // Gate 1 が未解決なら Gate 1 だけ案内。drop レビューは Gate 1 解消後の再実行で案内する
          nextSteps.push(
            `${manualReview} group(s) require broadcaster review (unknown_bucket_mixed). ` +
            `Generate the review YAML by calling video_pipeline_dedup_generate_broadcaster_yaml ` +
            `with planJsonlPath="${planPath}". ` +
            `After the operator edits the YAML, apply it with video_pipeline_dedup_apply_broadcaster_yaml, ` +
            `then re-run this tool to resolve the remaining groups.`
          );
        } else if (filesDropped > 0 && planPath) {
          nextSteps.push(
            `${filesDropped} file(s) marked as drop candidates (Phase 2 metadata dedup). ` +
            `Generate the drop-review YAML by calling video_pipeline_dedup_generate_drop_review_yaml ` +
            `with planJsonlPath="${planPath}". ` +
            `After the operator reviews keep/drop decisions, apply it with video_pipeline_dedup_apply_drop_review_yaml.`
          );
        }
        if (nextSteps.length > 0) {
          out.nextSteps = nextSteps;
        }

        return toToolResult(out);
      },
    }
  );
}
