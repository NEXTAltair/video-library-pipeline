import path from "node:path";
import { getExtensionRootDir, parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj, PluginApi, GetCfgFn } from "./types";
import { ensureWindowsScripts } from "./windows-scripts-bootstrap";

function stringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is string => typeof item === "string" && item.length > 0);
}

export function registerToolRun(api: PluginApi, getCfg: GetCfgFn) {
  api.registerTool(
    {
      name: "video_pipeline_analyze_and_move_videos",
      description:
        "Analyze videos in source folder and move them to destination folder. Use apply=false for dry-run.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          apply: { type: "boolean", default: false },
          maxFilesPerRun: {
            type: "integer",
            minimum: 1,
            maximum: 5000,
            description: "Maximum files to process in one run for queue and plan stages.",
          },
          allowNeedsReview: { type: "boolean", default: false },
          profile: { type: "string" },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("unwatched_pipeline_runner.py");
        const scriptsProvision = ensureWindowsScripts(cfg);

        if (!scriptsProvision.ok) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_analyze_and_move_videos",
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

        // uv run python で runner を起動。
        const args = [
          "run",
          "python",
          resolved.scriptPath,
          "--db",
          cfg.db,
          "--source-root",
          cfg.sourceRoot,
          "--dest-root",
          cfg.destRoot,
          "--windows-ops-root",
          cfg.windowsOpsRoot,
          "--max-files-per-run",
          String(params.maxFilesPerRun ?? cfg.defaultMaxFilesPerRun ?? 200),
        ];
        if (params.apply) args.push("--apply");
        if (params.allowNeedsReview) args.push("--allow-needs-review");

        // Pass drive routes for multi-destination routing
        const driveRoutesPath = cfg.driveRoutesPath
          || path.join(getExtensionRootDir(), "rules", "drive_routes.yaml");
        args.push("--drive-routes", driveRoutesPath);

        const r = runCmd("uv", args, resolved.cwd);
        const parsed = parseJsonObject(r.stdout);
        const out: AnyObj = {
          ok: r.ok,
          tool: "video_pipeline_analyze_and_move_videos",
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

        // nextStep / followUpToolCalls でエージェントにステージ遷移を案内
        if (r.ok && parsed) {
          const planStats = (parsed.plan_stats ?? {}) as Record<string, unknown>;
          const skippedNeedsReview = Number(planStats.skipped_needs_review || 0);
          const skippedSubtitleSeparator = Number(planStats.skipped_subtitle_separator || 0);
          const planned = Number(planStats.planned || 0);
          const apply = params.apply === true;
          const queuePath = String(parsed.queue || "");
          const reviewYamlPaths = stringArray(parsed.reviewYamlPaths)
            .concat(typeof parsed.reviewYamlPath === "string" && parsed.reviewYamlPath ? [parsed.reviewYamlPath] : [])
            .filter((value, index, arr) => arr.indexOf(value) === index);
          const reviewSummary = parsed.reviewSummary && typeof parsed.reviewSummary === "object"
            ? parsed.reviewSummary as Record<string, unknown>
            : null;
          const reviewRows = Number(reviewSummary?.rowsNeedingReview || 0);

          const skipParts: string[] = [];
          if (skippedNeedsReview > 0) skipParts.push(`${skippedNeedsReview} needs_review`);
          if (skippedSubtitleSeparator > 0) skipParts.push(`${skippedSubtitleSeparator} subtitle_separator`);

          if (reviewYamlPaths.length > 0) {
            out.followUpToolCalls = reviewYamlPaths.map((sourceYamlPath) => ({
              tool: "video_pipeline_apply_reviewed_metadata",
              reason: "apply_generated_review_yaml_after_human_review",
              requiresHumanReview: true,
              params: { sourceYamlPath },
            }));
            out.hasFollowUpToolCalls = out.followUpToolCalls.length > 0;
            if (typeof parsed.nextStep === "string" && parsed.nextStep.trim()) {
              out.nextStep = parsed.nextStep;
            } else if (!apply) {
              out.nextStep =
                `Dry-run complete. Generated ${reviewYamlPaths.length} review YAML artifact(s) for ${reviewRows} reviewable rows. ` +
                "Ask the user to review the YAML, then call video_pipeline_apply_reviewed_metadata with sourceYamlPath from followUpToolCalls. " +
                "Do NOT restart extraction for this cohort.";
            } else {
              out.nextStep =
                `Moves were applied for ready rows, but ${reviewRows} reviewable rows still require human review. ` +
                "Use followUpToolCalls after YAML review to apply metadata corrections before the next run.";
            }
          } else if (!apply && (skippedNeedsReview > 0 || skippedSubtitleSeparator > 0)) {
            out.nextStep =
              `Dry-run complete. ${planned} files planned for move, ${skipParts.join(" + ")} files skipped. ` +
              `Proceed to Stage 2: call video_pipeline_reextract with the queue path to extract metadata for review. ` +
              `Do NOT call this tool with apply=true until Stage 2 review is complete.`;
            out.followUpToolCalls = [
              {
                tool: "video_pipeline_reextract",
                reason: "extract_metadata_for_needs_review_files",
                params: { queuePath },
              },
            ];
          } else if (!apply && planned > 0) {
            out.nextStep =
              `Dry-run complete. ${planned} files planned for move, 0 files need review. ` +
              `Present the plan to the user. After user confirmation, call this tool with apply=true to execute moves.`;
          } else if (!apply && planned === 0) {
            out.nextStep =
              "Dry-run complete. No files planned for move. Check plan_stats for skip reasons.";
          } else if (apply) {
            out.nextStep =
              "Moves applied. Check moveApplyStats for results. " +
              "Call video_pipeline_logs to verify.";
          }
          if (!("hasFollowUpToolCalls" in out)) {
            out.hasFollowUpToolCalls = Array.isArray(out.followUpToolCalls) && out.followUpToolCalls.length > 0;
          }
        }

        return toToolResult(out);
      },
    }
  );
}
