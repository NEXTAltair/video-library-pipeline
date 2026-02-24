import path from "node:path";
import { getExtensionRootDir, latestJsonlFile, resolvePythonScript, runCmd, toToolResult } from "./runtime";
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

function parseLastJsonObjectLine(input: unknown): AnyObj | null {
  if (typeof input !== "string") return null;
  const lines = input.split(/\r?\n/);
  for (let i = lines.length - 1; i >= 0; i--) {
    const s = lines[i].trim();
    if (!s) continue;
    try {
      const parsed = JSON.parse(s);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) return parsed as AnyObj;
    } catch {
      // continue
    }
  }
  return null;
}

function stringArray(v: unknown): string[] {
  if (!Array.isArray(v)) return [];
  return v.filter((x): x is string => typeof x === "string" && x.length > 0);
}

function parseReextractStats(stdout: string): { processed?: number; batches?: number; preservedHumanReviewed?: number } {
  const out: { processed?: number; batches?: number; preservedHumanReviewed?: number } = {};
  const m1 = stdout.match(/OK preserved_human_reviewed=(\d+)/);
  if (m1) out.preservedHumanReviewed = Number(m1[1]);
  const m2 = stdout.match(/OK processed=(\d+)\s+batches=(\d+)/);
  if (m2) {
    out.processed = Number(m2[1]);
    out.batches = Number(m2[2]);
  }
  return out;
}

export function registerToolPrepareRelocateMetadata(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_prepare_relocate_metadata",
      description:
        "Prepare metadata for relocate flow: run relocate dry-run with queue generation, then run reextract on the generated queue.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          roots: { type: "array", items: { type: "string" } },
          rootsFilePath: { type: "string" },
          extensions: { type: "array", items: { type: "string" }, default: [".mp4"] },
          limit: { type: "integer", minimum: 1, maximum: 100000 },
          allowNeedsReview: { type: "boolean", default: false },
          scanErrorPolicy: { type: "string", enum: ["warn", "fail", "threshold"], default: "warn" },
          scanErrorThreshold: { type: "integer", minimum: 1, maximum: 100000 },
          scanRetryCount: { type: "integer", minimum: 0, maximum: 10, default: 1 },
          extractionVersion: { type: "string" },
          batchSize: { type: "integer", minimum: 1, maximum: 1000, default: 50 },
          maxBatches: { type: "integer", minimum: 1, maximum: 1000 },
          preserveHumanReviewed: { type: "boolean", default: true },
          runReextract: { type: "boolean", default: true },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const hostRoot = String(cfg.windowsOpsRoot || "/tmp").replace(/\/+$/, "");
        const llmDir = path.join(hostRoot, "llm");
        const defaultRootsFile = path.join(getExtensionRootDir(), "rules", "relocate_roots.yaml");
        const hintsPath = path.join(getExtensionRootDir(), "rules", "program_aliases.yaml");
        const scriptsProvision = ensureWindowsScripts(cfg);
        if (!scriptsProvision.ok) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_prepare_relocate_metadata",
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

        const relocateScript = resolvePythonScript("relocate_existing_files.py");
        const relocateArgs = [
          "run",
          "python",
          relocateScript.scriptPath,
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
          "true",
          "--write-metadata-queue-on-dry-run",
          "true",
          "--scan-error-policy",
          String(params.scanErrorPolicy || "warn"),
          "--scan-retry-count",
          String(params.scanRetryCount ?? 1),
          "--on-dst-exists",
          "error",
        ];
        if (Array.isArray(params.roots) && params.roots.length > 0) {
          relocateArgs.push("--roots-json", JSON.stringify(params.roots));
        }
        if (Array.isArray(params.extensions) && params.extensions.length > 0) {
          relocateArgs.push("--extensions-json", JSON.stringify(params.extensions));
        }
        if (typeof params.limit === "number" && Number.isFinite(params.limit)) {
          relocateArgs.push("--limit", String(Math.trunc(params.limit)));
        }
        if (typeof params.scanErrorThreshold === "number" && Number.isFinite(params.scanErrorThreshold)) {
          relocateArgs.push("--scan-error-threshold", String(Math.trunc(params.scanErrorThreshold)));
        }

        const rr = runCmd("uv", relocateArgs, relocateScript.cwd);
        const relocateParsed = parseLastJsonObjectLine(rr.stdout) ?? parseJsonObject(rr.stdout);
        const relocateResult: AnyObj = {
          ok: rr.ok,
          exitCode: rr.code,
          stdout: rr.stdout,
          stderr: rr.stderr,
        };
        if (relocateParsed) {
          for (const [k, v] of Object.entries(relocateParsed)) relocateResult[k] = v;
        }

        const queuePath = typeof relocateResult.metadataQueuePath === "string" ? relocateResult.metadataQueuePath : null;
        const queuePlanned = Number(relocateResult.metadataQueuePlannedCount || 0);
        const runReextract = params.runReextract !== false;
        const relocatePlannedMoves = Number(relocateResult.plannedMoves || 0);
        const relocateAlreadyCorrect = Number(relocateResult.alreadyCorrect || 0);
        const relocateMetadataMissingSkipped = Number(relocateResult.metadataMissingSkipped || 0);
        const relocateUnregisteredSkipped = Number(relocateResult.unregisteredSkipped || 0);
        const relocateOutcomeType =
          typeof relocateResult.outcomeType === "string" ? String(relocateResult.outcomeType) : null;
        const relocateRequiresMetadataPreparation = Boolean(relocateResult.requiresMetadataPreparation);
        const relocateNextActions = Array.isArray(relocateResult.nextActions)
          ? relocateResult.nextActions.filter((v) => typeof v === "string")
          : [];

        let interpretation = "relocate_dry_run_completed";
        let reportingHint =
          "Report explicit counts from relocate result. Do not infer already_correct from plannedMoves=0 alone.";
        if (relocateRequiresMetadataPreparation || queuePlanned > 0 || relocateMetadataMissingSkipped > 0) {
          interpretation = "metadata_preparation_required";
          reportingHint =
            "This is a metadata-preparation-required state, not a physical-move failure. Do not describe files as already_correct unless alreadyCorrect > 0.";
        } else if (relocatePlannedMoves > 0) {
          interpretation = "relocate_plan_ready_for_apply";
          reportingHint = "A relocate plan exists. Review the plan before apply.";
        } else if (relocateAlreadyCorrect > 0 && relocatePlannedMoves === 0) {
          interpretation = "already_correct_no_relocation_needed";
          reportingHint = "Only describe files as already_correct if relocate.alreadyCorrect > 0.";
        }

        const out: AnyObj = {
          ok: rr.ok,
          tool: "video_pipeline_prepare_relocate_metadata",
          relocate: relocateResult,
          relocateOutcomeType,
          relocateRequiresMetadataPreparation,
          relocateNextActions,
          relocatePlannedMoves,
          relocateAlreadyCorrect,
          relocateMetadataMissingSkipped,
          relocateUnregisteredSkipped,
          interpretation,
          reportingHint,
          queuePlanned,
          queuePath,
          reextractRan: false,
          reextractSkippedReason: null,
          scriptsProvision: {
            created: scriptsProvision.created,
            updated: scriptsProvision.updated,
            existing: scriptsProvision.existing,
            failed: scriptsProvision.failed,
            missingTemplates: scriptsProvision.missingTemplates,
          },
        };

        const roots = Array.isArray(params.roots) ? params.roots.filter((v) => typeof v === "string") : undefined;
        const rootsFilePath = typeof params.rootsFilePath === "string" ? params.rootsFilePath : undefined;
        const relocateCommonParams: AnyObj = {
          ...(roots && roots.length ? { roots } : {}),
          ...(rootsFilePath ? { rootsFilePath } : {}),
          ...(Array.isArray(params.extensions) ? { extensions: params.extensions } : {}),
          ...(typeof params.limit === "number" ? { limit: Math.trunc(params.limit) } : {}),
          ...(typeof params.allowNeedsReview === "boolean" ? { allowNeedsReview: params.allowNeedsReview } : {}),
          ...(typeof params.scanErrorPolicy === "string" ? { scanErrorPolicy: params.scanErrorPolicy } : {}),
          ...(typeof params.scanRetryCount === "number" ? { scanRetryCount: Math.trunc(params.scanRetryCount) } : {}),
          ...(typeof params.scanErrorThreshold === "number"
            ? { scanErrorThreshold: Math.trunc(params.scanErrorThreshold) }
            : {}),
        };
        const followUpToolCalls: AnyObj[] = [];

        if (!rr.ok) {
          out.followUpToolCalls = [];
          out.hasFollowUpToolCalls = false;
          return toToolResult(out);
        }
        if (!queuePlanned) {
          out.followUpToolCalls = [];
          out.hasFollowUpToolCalls = false;
          out.reextractSkippedReason = "no_queue_candidates";
          return toToolResult(out);
        }
        if (!queuePath) {
          out.ok = false;
          out.reextractSkippedReason = "queue_path_missing";
          out.error = "relocate reported queue candidates but metadataQueuePath was null";
          out.followUpToolCalls = [];
          out.hasFollowUpToolCalls = false;
          return toToolResult(out);
        }
        if (!runReextract) {
          out.reextractSkippedReason = "runReextract=false";
          followUpToolCalls.push({
            tool: "video_pipeline_reextract",
            reason: "metadata_queue_ready_manual_reextract_requested",
            params: { queuePath },
          });
          out.followUpToolCalls = followUpToolCalls;
          out.hasFollowUpToolCalls = true;
          return toToolResult(out);
        }

        const reextractScript = resolvePythonScript("run_metadata_batches_promptv1.py");
        const beforeInput = latestJsonlFile(llmDir, "llm_filename_extract_input_");
        const beforeOutput = latestJsonlFile(llmDir, "llm_filename_extract_output_");
        const reextractArgs = [
          "run",
          "python",
          reextractScript.scriptPath,
          "--db",
          String(cfg.db || ""),
          "--queue",
          queuePath,
          "--outdir",
          llmDir,
          "--hints",
          hintsPath,
          "--batch-size",
          String(params.batchSize ?? 50),
        ];
        if (typeof params.maxBatches === "number" && Number.isFinite(params.maxBatches)) {
          reextractArgs.push("--max-batches", String(Math.trunc(params.maxBatches)));
        }
        if (typeof params.extractionVersion === "string" && params.extractionVersion.trim()) {
          reextractArgs.push("--extraction-version", params.extractionVersion.trim());
        }
        if (params.preserveHumanReviewed === false) {
          reextractArgs.push("--ignore-human-reviewed");
        }

        const rx = runCmd("uv", reextractArgs, reextractScript.cwd);
        const reextractParsed = parseLastJsonObjectLine(rx.stdout);
        const reextractInputJsonlPaths = stringArray(reextractParsed?.inputJsonlPaths);
        const reextractOutputJsonlPaths = stringArray(reextractParsed?.outputJsonlPaths);
        const afterInput =
          (typeof reextractParsed?.latestInputJsonlPath === "string" && reextractParsed.latestInputJsonlPath) ||
          (reextractInputJsonlPaths.length ? reextractInputJsonlPaths[reextractInputJsonlPaths.length - 1] : null) ||
          latestJsonlFile(llmDir, "llm_filename_extract_input_");
        const afterOutput =
          (typeof reextractParsed?.latestOutputJsonlPath === "string" && reextractParsed.latestOutputJsonlPath) ||
          (reextractOutputJsonlPaths.length ? reextractOutputJsonlPaths[reextractOutputJsonlPaths.length - 1] : null) ||
          latestJsonlFile(llmDir, "llm_filename_extract_output_");
        out.reextractRan = true;
        out.reextract = {
          ok: rx.ok,
          exitCode: rx.code,
          stdout: rx.stdout,
          stderr: rx.stderr,
          queuePath,
          preserveHumanReviewed: params.preserveHumanReviewed !== false,
          batchSize: Number(params.batchSize ?? 50),
          maxBatches: typeof params.maxBatches === "number" ? Math.trunc(params.maxBatches) : null,
          extractionVersion:
            typeof params.extractionVersion === "string" && params.extractionVersion.trim()
              ? params.extractionVersion.trim()
              : null,
          summary: reextractParsed,
          inputJsonlPaths: reextractInputJsonlPaths,
          outputJsonlPaths: reextractOutputJsonlPaths,
          inputJsonlPath: afterInput,
          outputJsonlPath: afterOutput,
          inputJsonlChanged: afterInput !== beforeInput,
          outputJsonlChanged: afterOutput !== beforeOutput,
          stats: parseReextractStats(rx.stdout || ""),
          artifactLineage: {
            sourceQueuePath: queuePath,
            generatedInputJsonlPaths: reextractInputJsonlPaths,
            generatedOutputJsonlPaths: reextractOutputJsonlPaths,
          },
        };
        if (!rx.ok) out.ok = false;
        if (rx.ok && out.reextract) {
          const outputJsonlPaths =
            Array.isArray(out.reextract.outputJsonlPaths) && out.reextract.outputJsonlPaths.length > 0
              ? (out.reextract.outputJsonlPaths as unknown[]).filter(
                  (v): v is string => typeof v === "string" && v.length > 0,
                )
              : typeof out.reextract.outputJsonlPath === "string" && out.reextract.outputJsonlPath
                ? [String(out.reextract.outputJsonlPath)]
                : [];

          for (const outputJsonlPath of outputJsonlPaths) {
            followUpToolCalls.push({
              tool: "video_pipeline_export_program_yaml",
              reason: "export_human_review_yaml_from_reextract_output",
              params: { sourceJsonlPath: outputJsonlPath },
            });
            followUpToolCalls.push({
              tool: "video_pipeline_apply_reviewed_metadata",
              reason: "run_after_human_review_of_extracted_metadata",
              requiresHumanReview: true,
              params: { sourceJsonlPath: outputJsonlPath },
            });
          }
          if (outputJsonlPaths.length > 0) {
            followUpToolCalls.push({
              tool: "video_pipeline_relocate_existing_files",
              reason: "rerun_relocate_dry_run_after_metadata_apply",
              requiresHumanReview: true,
              params: {
                ...relocateCommonParams,
                apply: false,
                queueMissingMetadata: true,
                writeMetadataQueueOnDryRun: true,
              },
            });
          }
        }
        out.followUpToolCalls = followUpToolCalls;
        out.hasFollowUpToolCalls = followUpToolCalls.length > 0;
        out.stopAtHumanReview = true;
        out.workflowState = rx.ok ? "metadata_extracted_review_required" : "reextract_failed";
        return toToolResult(out);
      },
    },
    { optional: true },
  );
}
