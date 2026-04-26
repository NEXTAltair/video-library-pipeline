import { parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj, GetCfgFn, PluginApi } from "./types";
import { registerToolApplyReviewedMetadata } from "./tool-apply-reviewed-metadata";
import { translateWorkflowResult, type WorkflowResultPayload } from "../workflows/workflow-result-translator";

function stringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is string => typeof item === "string" && item.length > 0);
}

function finiteInteger(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  return Math.trunc(value);
}

function addFlag(args: string[], flag: string, enabled: unknown) {
  if (enabled === true) args.push(flag);
}

function runWorkflowCli(command: string, args: string[], okPayload: (parsed: AnyObj) => AnyObj) {
  const resolved = resolvePythonScript("workflow_cli.py");
  const result = runCmd("uv", ["run", "python", resolved.scriptPath, command, ...args], resolved.cwd);
  const parsed = parseJsonObject(result.stdout);
  if (parsed) {
    return toToolResult(okPayload(parsed));
  }
    return toToolResult({
      ok: false,
      tool: command === "inspect-artifact" ? "video_pipeline_inspect_artifact" : `video_pipeline_${command}`,
      error: "workflow_cli.py did not return a JSON object",
      exitCode: result.code,
    stdout: result.stdout,
    stderr: result.stderr,
  });
}

function isWorkflowResultPayload(value: AnyObj): value is WorkflowResultPayload {
  return typeof value.runId === "string"
    && typeof value.flow === "string"
    && typeof value.phase === "string"
    && typeof value.outcome === "string"
    && typeof value.ok === "boolean";
}

function workflowToolResult(parsed: AnyObj): AnyObj {
  if (isWorkflowResultPayload(parsed)) return translateWorkflowResult(parsed);
  return parsed;
}

function objectFromToolResult(value: unknown): AnyObj {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    const maybeResult = value as AnyObj;
    const text = maybeResult.content?.[0]?.text;
    if (typeof text === "string") {
      const parsed = parseJsonObject(text);
      if (parsed) return parsed;
    }
    return maybeResult;
  }
  return { ok: false, error: "tool returned a non-object result" };
}

function getInternalApplyReviewedMetadataTool(api: PluginApi, getCfg: GetCfgFn) {
  let tool: { execute: (id: string, params: AnyObj) => Promise<unknown> } | null = null;
  const internalApi: PluginApi = {
    ...api,
    registerTool(def) {
      tool = def;
    },
    on: api.on ?? (() => undefined),
  };
  registerToolApplyReviewedMetadata(internalApi, getCfg);
  if (!tool) throw new Error("failed to initialize internal reviewed metadata tool");
  return tool;
}

async function applyReviewedMetadataForResume(api: PluginApi, getCfg: GetCfgFn, params: AnyObj): Promise<AnyObj[]> {
  const reviewYamlPaths = stringArray(params.reviewYamlPaths);
  if (reviewYamlPaths.length === 0) {
    return [{
      ok: false,
      error: "resumeAction=apply_reviewed_metadata requires reviewYamlPaths from nextActions params",
      runId: params.runId,
    }];
  }

  const tool = getInternalApplyReviewedMetadataTool(api, getCfg);
  const results: AnyObj[] = [];
  for (const sourceYamlPath of reviewYamlPaths) {
    const result = objectFromToolResult(await tool.execute("internal-apply-reviewed-metadata", {
      sourceYamlPath,
      markHumanReviewed: true,
    }));
    results.push(result);
    if (result.ok !== true) break;
  }
  return results;
}

function registerStart(api: PluginApi, getCfg: GetCfgFn) {
  api.registerTool({
    name: "video_pipeline_start",
    description: "Start a V2 run-based workflow and return a structured WorkflowResult.",
    parameters: {
      type: "object",
      additionalProperties: false,
      required: ["flow"],
      properties: {
        flow: { type: "string", enum: ["source_root", "relocate"] },
        runId: { type: "string" },
        allowNeedsReview: { type: "boolean", default: false },
        driveRoutesPath: { type: "string" },
        maxFilesPerRun: { type: "integer", minimum: 1, maximum: 5000 },
        roots: { type: "array", items: { type: "string" } },
        rootsFilePath: { type: "string" },
        extensions: { type: "array", items: { type: "string" } },
        limit: { type: "integer", minimum: 0, maximum: 100000 },
        allowUnreviewedMetadata: { type: "boolean", default: false },
        queueMissingMetadata: { type: "boolean", default: true },
        writeMetadataQueueOnDryRun: { type: "boolean", default: true },
        scanErrorPolicy: { type: "string", enum: ["warn", "fail", "threshold"], default: "warn" },
        scanErrorThreshold: { type: "integer", minimum: 0, maximum: 100000 },
        scanRetryCount: { type: "integer", minimum: 0, maximum: 10, default: 1 },
        onDstExists: { type: "string", enum: ["error", "rename_suffix"], default: "error" },
        skipSuspiciousTitleCheck: { type: "boolean", default: false },
      },
    },
    async execute(_id: string, params: AnyObj) {
      const cfg = getCfg(api);
      const args = [
        "--flow", String(params.flow),
        "--windows-ops-root", cfg.windowsOpsRoot,
        "--source-root", cfg.sourceRoot,
        "--dest-root", cfg.destRoot,
        "--db", cfg.db,
        "--drive-routes-path", String(params.driveRoutesPath || cfg.driveRoutesPath || ""),
        "--max-files-per-run", String(finiteInteger(params.maxFilesPerRun) ?? cfg.defaultMaxFilesPerRun),
      ];
      if (typeof params.runId === "string" && params.runId.trim()) args.push("--run-id", params.runId.trim());
      addFlag(args, "--allow-needs-review", params.allowNeedsReview);
      if (Array.isArray(params.roots)) args.push("--roots-json", JSON.stringify(stringArray(params.roots)));
      if (typeof params.rootsFilePath === "string") args.push("--roots-file-path", params.rootsFilePath);
      if (Array.isArray(params.extensions)) args.push("--extensions-json", JSON.stringify(stringArray(params.extensions)));
      const limit = finiteInteger(params.limit);
      if (limit !== null) args.push("--limit", String(limit));
      addFlag(args, "--allow-unreviewed-metadata", params.allowUnreviewedMetadata);
      addFlag(args, "--queue-missing-metadata", params.queueMissingMetadata !== false);
      addFlag(args, "--write-metadata-queue-on-dry-run", params.writeMetadataQueueOnDryRun !== false);
      if (typeof params.scanErrorPolicy === "string") args.push("--scan-error-policy", params.scanErrorPolicy);
      const threshold = finiteInteger(params.scanErrorThreshold);
      if (threshold !== null) args.push("--scan-error-threshold", String(threshold));
      const retry = finiteInteger(params.scanRetryCount);
      if (retry !== null) args.push("--scan-retry-count", String(retry));
      if (typeof params.onDstExists === "string") args.push("--on-dst-exists", params.onDstExists);
      addFlag(args, "--skip-suspicious-title-check", params.skipSuspiciousTitleCheck);
      return runWorkflowCli("start", args, workflowToolResult);
    },
  });
}

function registerResume(api: PluginApi, getCfg: GetCfgFn) {
  api.registerTool({
    name: "video_pipeline_resume",
    description: "Resume an existing V2 workflow run using an action from nextActions.",
    parameters: {
      type: "object",
      additionalProperties: false,
      required: ["runId"],
      properties: {
        runId: { type: "string" },
        action: { type: "string" },
        resumeAction: { type: "string" },
        artifactId: { type: "string" },
        gateId: { type: "string" },
        artifactIds: { type: "array", items: { type: "string" } },
        reviewYamlPaths: { type: "array", items: { type: "string" } },
        onDstExists: { type: "string", enum: ["error", "rename_suffix"] },
      },
    },
    async execute(_id: string, params: AnyObj) {
      const cfg = getCfg(api);
      const action = typeof params.action === "string" && params.action.trim()
        ? params.action.trim()
        : typeof params.resumeAction === "string" && params.resumeAction.trim()
          ? params.resumeAction.trim()
          : "";
      const args = [
        "--windows-ops-root", cfg.windowsOpsRoot,
        "--db", cfg.db,
        "--run-id", String(params.runId || ""),
      ];
      if (action) args.push("--action", action);
      if (typeof params.artifactId === "string" && params.artifactId.trim()) args.push("--artifact-id", params.artifactId.trim());
      if (typeof params.onDstExists === "string" && params.onDstExists.trim()) args.push("--on-dst-exists", params.onDstExists.trim());
      if (action === "apply_reviewed_metadata") {
        const metadataResults = await applyReviewedMetadataForResume(api, getCfg, params);
        const failed = metadataResults.find((result) => result.ok !== true);
        if (failed) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_resume",
            runId: params.runId,
            action,
            outcome: "source_root_review_metadata_apply_failed",
            reviewedMetadataResults: metadataResults,
            error: failed.error ?? "failed to apply reviewed metadata",
          });
        }
        return runWorkflowCli("resume", args, (parsed) => ({
          ...workflowToolResult(parsed),
          reviewedMetadataResults: metadataResults,
        }));
      }
      return runWorkflowCli("resume", args, workflowToolResult);
    },
  });
}

function registerStatus(api: PluginApi, getCfg: GetCfgFn) {
  api.registerTool({
    name: "video_pipeline_status",
    description: "Read V2 workflow run status, recent runs, open gates, and latest artifacts.",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        runId: { type: "string" },
        limit: { type: "integer", minimum: 1, maximum: 100 },
        includeArtifacts: { type: "boolean", default: false },
      },
    },
    async execute(_id: string, params: AnyObj) {
      const cfg = getCfg(api);
      const args = ["--windows-ops-root", cfg.windowsOpsRoot];
      if (typeof params.runId === "string" && params.runId.trim()) args.push("--run-id", params.runId.trim());
      const limit = finiteInteger(params.limit);
      if (limit !== null) args.push("--limit", String(limit));
      addFlag(args, "--include-artifacts", params.includeArtifacts);
      return runWorkflowCli("status", args, (parsed) => parsed);
    },
  });
}

function registerInspectArtifact(api: PluginApi, getCfg: GetCfgFn) {
  api.registerTool({
    name: "video_pipeline_inspect_artifact",
    description: "Inspect a V2 workflow artifact by runId and artifactId.",
    parameters: {
      type: "object",
      additionalProperties: false,
      required: ["runId", "artifactId"],
      properties: {
        runId: { type: "string" },
        artifactId: { type: "string" },
        includeContentPreview: { type: "boolean", default: false },
        previewBytes: { type: "integer", minimum: 1, maximum: 65536 },
      },
    },
    async execute(_id: string, params: AnyObj) {
      const cfg = getCfg(api);
      const args = [
        "--windows-ops-root", cfg.windowsOpsRoot,
        "--run-id", String(params.runId || ""),
        "--artifact-id", String(params.artifactId || ""),
      ];
      addFlag(args, "--include-content-preview", params.includeContentPreview);
      const previewBytes = finiteInteger(params.previewBytes);
      if (previewBytes !== null) args.push("--preview-bytes", String(previewBytes));
      return runWorkflowCli("inspect-artifact", args, (parsed) => parsed);
    },
  });
}

export function registerWorkflowTools(api: PluginApi, getCfg: GetCfgFn) {
  registerStart(api, getCfg);
  registerResume(api, getCfg);
  registerStatus(api, getCfg);
  registerInspectArtifact(api, getCfg);
}
