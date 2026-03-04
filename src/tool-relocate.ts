import fs from "node:fs";
import path from "node:path";
import { getExtensionRootDir, parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";
import { ensureWindowsScripts } from "./windows-scripts-bootstrap";

export function registerToolRelocate(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_relocate_existing_files",
      description: "Relocate existing files under specified roots to correct folders using DB metadata. Use apply=false for dry-run first, then apply=true with planPath from dry-run result. Target: B:\\VideoLibrary or any library drive subtree. Do NOT use this for B:\\未視聴 (use analyze_and_move instead).",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          apply: { type: "boolean", default: false, description: "false = dry-run (safe, no files moved). true = execute physical move. Always run dry-run first." },
          planPath: { type: "string", description: "Required when apply=true. Path to plan file returned by dry-run (plan_path field). Do not guess — take the exact value from dry-run result." },
          roots: { type: "array", items: { type: "string" }, description: "Windows paths to scan, e.g. [\"B:\\\\VideoLibrary\"] or [\"D:\\\\Anime\"]. Must be Windows-style paths (backslash). Required unless rootsFilePath is given." },
          rootsFilePath: { type: "string", description: "Path to a YAML/text file listing roots. Alternative to roots array." },
          extensions: { type: "array", items: { type: "string" }, default: [".mp4"], description: "File extensions to include. Default: [\".mp4\"]." },
          limit: { type: "integer", minimum: 1, maximum: 100000, description: "Max files to process in one run." },
          allowNeedsReview: { type: "boolean", default: false, description: "Include files flagged needs_review in move plan. Default false (skip them)." },
          queueMissingMetadata: { type: "boolean", default: false, description: "Collect files with missing metadata into a queue for reextract. Set true to enable metadata preparation flow." },
          writeMetadataQueueOnDryRun: { type: "boolean", default: false, description: "Write the metadata queue file even during dry-run. Required to use the queue path in a subsequent reextract call." },
          scanErrorPolicy: { type: "string", enum: ["warn", "fail", "threshold"], default: "warn", description: "How to handle scan errors: warn=continue, fail=abort, threshold=abort after N errors." },
          scanErrorThreshold: { type: "integer", minimum: 1, maximum: 100000, description: "Error count threshold when scanErrorPolicy=threshold." },
          scanRetryCount: { type: "integer", minimum: 0, maximum: 10, default: 1, description: "Retry count per file on scan error." },
          onDstExists: { type: "string", enum: ["error", "rename_suffix"], default: "error", description: "Behavior when destination file already exists: error=abort that file, rename_suffix=add suffix." },
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

        if (params.apply === true) {
          // planPath 必須チェック
          const planPath = typeof params.planPath === "string" ? params.planPath.trim() : "";
          if (!planPath) {
            return toToolResult({
              ok: false,
              tool: "video_pipeline_relocate_existing_files",
              error: "apply=true requires planPath parameter. Run dry-run first, review the plan, then pass the plan file path.",
              hint: "Call video_pipeline_relocate_existing_files with apply=false first, then use the returned plan_path.",
            });
          }
          if (!fs.existsSync(planPath)) {
            return toToolResult({
              ok: false,
              tool: "video_pipeline_relocate_existing_files",
              error: `planPath does not exist: ${planPath}`,
              hint: "The dry-run plan file may have been rotated or deleted. Run a new dry-run.",
            });
          }
          const planStat = fs.statSync(planPath);
          const ageMs = Date.now() - planStat.mtimeMs;
          const MAX_PLAN_AGE_MS = 24 * 60 * 60 * 1000;
          if (ageMs > MAX_PLAN_AGE_MS) {
            return toToolResult({
              ok: false,
              tool: "video_pipeline_relocate_existing_files",
              error: `planPath is stale (${Math.round(ageMs / 3600000)}h old). Run a fresh dry-run.`,
            });
          }

          // --- auto backup before relocate apply ---
          const backupResolved = resolvePythonScript("backup_mediaops_db.py");
          const backupResult = runCmd("uv", [
            "run", "python", backupResolved.scriptPath,
            "--db", String(cfg.db || ""),
            "--action", "backup",
            "--descriptor", "pre_relocate_apply",
          ], backupResolved.cwd);
          if (backupResult.code !== 0) {
            return toToolResult({
              ok: false,
              tool: "video_pipeline_relocate_existing_files",
              error: "pre-relocate DB backup failed; aborting to protect data",
              backupStderr: backupResult.stderr,
            });
          }
          // auto rotate after successful backup
          runCmd("uv", [
            "run", "python", backupResolved.scriptPath,
            "--db", String(cfg.db || ""),
            "--action", "rotate",
            "--keep", "10",
          ], backupResolved.cwd);
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
        const plannedMoves = Number(out.plannedMoves || 0);
        const hasSuspiciousTitles = Number(out.suspiciousProgramTitleSkipped || 0) > 0;
        const hasMetadataGap =
          Number(out.metadataMissingSkipped || 0) > 0 ||
          Number(out.metadataQueuePlannedCount || 0) > 0;

        // ── Dry-run routing ──

        // Route A: plannedMoves > 0 → propose apply (partial moves OK)
        if (r.ok && params.apply !== true && plannedMoves > 0) {
          followUpToolCalls.push({
            tool: "video_pipeline_relocate_existing_files",
            reason: "relocate_plan_ready_for_apply_after_review",
            params: {
              ...commonRelocateParams,
              apply: true,
              planPath: out.planPath || out.plan_path,
              allowNeedsReview: params.allowNeedsReview === true,
              onDstExists: typeof params.onDstExists === "string" ? params.onDstExists : "error",
            },
          });
          if (hasSuspiciousTitles || hasMetadataGap) {
            const skippedParts: string[] = [];
            if (hasSuspiciousTitles)
              skippedParts.push(`${out.suspiciousProgramTitleSkipped} suspicious-title`);
            if (Number(out.metadataMissingSkipped || 0) > 0)
              skippedParts.push(`${out.metadataMissingSkipped} metadata-missing`);
            out.partialApplyNote =
              `${plannedMoves} files are ready to move. ` +
              `${skippedParts.join(" and ")} file(s) will be individually skipped. ` +
              "After apply, run video_pipeline_prepare_relocate_metadata for remaining files.";
          }
        }
        // Route B: plannedMoves === 0 + metadata issues → metadata preparation
        if (r.ok && params.apply !== true && plannedMoves === 0 && (hasMetadataGap || hasSuspiciousTitles)) {
          followUpToolCalls.push({
            tool: "video_pipeline_prepare_relocate_metadata",
            reason: "metadata_preparation_required_before_relocate_apply",
            params: {
              ...commonRelocateParams,
              runReextract: true,
            },
          });
          if (hasSuspiciousTitles) {
            out.diagnostics = {
              issue: "subtitle_contamination_detected",
              message:
                "program_title にサブタイトル区切り文字(▽/▼)が含まれるファイルがあります。" +
                "メタデータの再抽出が必要です。",
              suspiciousCount: out.suspiciousProgramTitleSkipped,
            };
            out.nextStep =
              `${out.suspiciousProgramTitleSkipped} files have subtitle separators (▽/▼) in program_title. ` +
              "This indicates incorrect metadata. Follow followUpToolCalls to fix.";
          }
        }
        // Route C: plannedMoves === 0 + no issues → all correct
        if (r.ok && params.apply !== true && plannedMoves === 0 && !hasMetadataGap && !hasSuspiciousTitles) {
          out.nextStep =
            "All scanned files are already in correct locations. plannedMoves=0 — no moves needed. " +
            "Task is complete. Stop here and report the result to the user. Do NOT call this tool again.";
        }

        // ── Apply routing ──

        if (r.ok && params.apply === true && Number(out.metadataMissingSkipped || 0) > 0) {
          // apply completed but some files were skipped due to missing metadata → extract-review next
          out.nextAction =
            `${out.metadataMissingSkipped} file(s) could not be moved due to missing metadata. ` +
            `Run video_pipeline_prepare_relocate_metadata with the same roots to extract metadata, ` +
            `then re-run relocate dry-run and apply for the remaining files. ` +
            `No user action is required to determine this — proceed with video_pipeline_prepare_relocate_metadata now.`;
          followUpToolCalls.push({
            tool: "video_pipeline_prepare_relocate_metadata",
            reason: "metadata_missing_files_remain_after_apply",
            params: {
              ...commonRelocateParams,
              runReextract: true,
            },
          });
        }
        if (r.ok && params.apply === true && hasSuspiciousTitles) {
          out.diagnostics = {
            issue: "suspicious_program_titles_blocked_apply",
            message:
              `${out.suspiciousProgramTitleSkipped} files have program_title containing subtitle/episode ` +
              "info (looks_like_swallowed or ▽/▼ separator). These files were skipped. " +
              "Fix program_title in DB (remove subtitle info), then run a fresh dry-run before applying.",
            suspiciousCount: out.suspiciousProgramTitleSkipped,
          };
        }
        if (r.ok && params.apply === true && followUpToolCalls.length === 0) {
          const movedFiles = Number(out.movedFiles || 0);
          if (movedFiles > 0) {
            const parts: string[] = [`Relocate apply completed. movedFiles=${movedFiles}.`];
            if (hasSuspiciousTitles) {
              parts.push(
                `${out.suspiciousProgramTitleSkipped} suspicious-title file(s) were individually skipped. ` +
                "Fix program_title in DB, then run a fresh dry-run for these remaining files."
              );
            }
            if (Number(out.metadataMissingSkipped || 0) > 0) {
              parts.push(
                `${out.metadataMissingSkipped} metadata-missing file(s) were skipped. ` +
                "Run video_pipeline_prepare_relocate_metadata for these files."
              );
            }
            out.nextStep = parts.join(" ") + " Report the result to the user.";
          } else if (out.outcomeType === "metadata_preparation_required") {
            out.nextStep =
              "Relocate apply was blocked — no files were moved. " +
              "Metadata issues must be resolved first. Check diagnostics, then run metadata preparation " +
              "and a fresh dry-run. Do NOT report this as a successful completion.";
          } else {
            out.nextStep =
              "Relocate apply completed. movedFiles=0. Report the result to the user and stop.";
          }
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
