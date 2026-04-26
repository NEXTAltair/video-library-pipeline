import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  applyReviewedExecute: vi.fn(),
}));

vi.mock("./runtime", async () => {
  const actual = await vi.importActual<typeof import("./runtime")>("./runtime");
  return {
    ...actual,
    resolvePythonScript: vi.fn(() => ({ scriptPath: "/ext/py/workflow_cli.py", cwd: "/ext/py", source: "extension" })),
    runCmd: vi.fn(),
    toToolResult: vi.fn((obj: Record<string, unknown>) => obj),
  };
});

vi.mock("./tool-apply-reviewed-metadata", () => ({
  registerToolApplyReviewedMetadata: (api: { registerTool: (def: unknown) => void }) => {
    api.registerTool({
      name: "video_pipeline_apply_reviewed_metadata",
      description: "mock",
      parameters: {},
      execute: mocks.applyReviewedExecute,
    });
  },
}));

import { registerWorkflowTools } from "./tool-workflows";
import { runCmd } from "./runtime";

function createMockApi() {
  return { registerTool: vi.fn() };
}

function cfg() {
  return {
    db: "/ops/db/mediaops.sqlite",
    sourceRoot: "/mnt/b/Unwatched",
    destRoot: "/mnt/b/VideoLibrary",
    windowsOpsRoot: "/ops",
    defaultMaxFilesPerRun: 200,
    tsRoot: "",
    driveRoutesPath: "/rules/drive_routes.yaml",
  };
}

function getRegisteredTool(api: ReturnType<typeof createMockApi>, name: string) {
  const toolDefs = api.registerTool.mock.calls.map(([def]) => def);
  const tool = toolDefs.find((def: { name: string }) => def.name === name);
  expect(tool).toBeTruthy();
  return tool;
}

function cmdResult(stdout: Record<string, unknown>, ok = true) {
  return {
    ok,
    code: ok ? 0 : 1,
    stdout: JSON.stringify(stdout),
    stderr: "",
    command: "uv",
    args: [],
    cwd: "/ext/py",
  };
}

describe("V2 workflow tools", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("starts sourceRoot through workflow_cli.py and translates WorkflowResult", async () => {
    vi.mocked(runCmd).mockReturnValue(cmdResult({
      ok: true,
      runId: "run_source",
      flow: "source_root",
      phase: "plan_ready",
      outcome: "source_root_dry_run_complete",
      artifacts: [],
      gates: [],
      nextActions: [
        {
          action: "review_plan",
          tool: "video_pipeline_resume",
          params: {
            runId: "run_source",
            artifactId: "source_root_move_plan",
            resumeAction: "apply_source_root_move_plan",
          },
          requiresHumanInput: true,
        },
      ],
      diagnostics: [],
    }));
    const api = createMockApi();
    registerWorkflowTools(api as never, () => cfg());

    const result = await getRegisteredTool(api, "video_pipeline_start").execute("call-1", {
      flow: "source_root",
      runId: "run_source",
      maxFilesPerRun: 25,
      allowNeedsReview: true,
    });

    expect(runCmd).toHaveBeenCalledWith("uv", expect.arrayContaining([
      "run",
      "python",
      "/ext/py/workflow_cli.py",
      "start",
      "--flow",
      "source_root",
      "--run-id",
      "run_source",
      "--max-files-per-run",
      "25",
      "--allow-needs-review",
    ]), "/ext/py");
    expect(result.followUpToolCalls).toEqual([
      {
        tool: "video_pipeline_resume",
        reason: "review_plan",
        params: {
          runId: "run_source",
          artifactId: "source_root_move_plan",
          resumeAction: "apply_source_root_move_plan",
        },
        requiresHumanReview: true,
      },
    ]);
  });

  it("starts relocate with relocate-specific options", async () => {
    vi.mocked(runCmd).mockReturnValue(cmdResult({
      ok: true,
      runId: "run_relocate",
      flow: "relocate",
      phase: "complete",
      outcome: "relocate_already_correct",
      artifacts: [],
      gates: [],
      nextActions: [],
      diagnostics: [],
    }));
    const api = createMockApi();
    registerWorkflowTools(api as never, () => cfg());

    await getRegisteredTool(api, "video_pipeline_start").execute("call-2", {
      flow: "relocate",
      roots: ["B:\\VideoLibrary"],
      extensions: [".mp4", ".mkv"],
      limit: 100,
      allowUnreviewedMetadata: true,
      queueMissingMetadata: true,
      writeMetadataQueueOnDryRun: true,
      scanErrorPolicy: "threshold",
      scanErrorThreshold: 5,
      scanRetryCount: 2,
      onDstExists: "rename_suffix",
      skipSuspiciousTitleCheck: true,
    });

    expect(runCmd).toHaveBeenCalledWith("uv", expect.arrayContaining([
      "--flow",
      "relocate",
      "--roots-json",
      JSON.stringify(["B:\\VideoLibrary"]),
      "--extensions-json",
      JSON.stringify([".mp4", ".mkv"]),
      "--limit",
      "100",
      "--allow-unreviewed-metadata",
      "--queue-missing-metadata",
      "--write-metadata-queue-on-dry-run",
      "--scan-error-policy",
      "threshold",
      "--scan-error-threshold",
      "5",
      "--scan-retry-count",
      "2",
      "--on-dst-exists",
      "rename_suffix",
      "--skip-suspicious-title-check",
    ]), "/ext/py");
  });

  it("resumes using resumeAction alias and translates WorkflowResult", async () => {
    vi.mocked(runCmd).mockReturnValue(cmdResult({
      ok: true,
      runId: "run_relocate",
      flow: "relocate",
      phase: "complete",
      outcome: "relocate_apply_complete",
      artifacts: [],
      gates: [],
      nextActions: [],
      diagnostics: [],
    }));
    const api = createMockApi();
    registerWorkflowTools(api as never, () => cfg());

    const result = await getRegisteredTool(api, "video_pipeline_resume").execute("call-3", {
      runId: "run_relocate",
      resumeAction: "apply_relocate_move_plan",
      artifactId: "relocate_plan",
      onDstExists: "rename_suffix",
    });

    expect(runCmd).toHaveBeenCalledWith("uv", expect.arrayContaining([
      "resume",
      "--run-id",
      "run_relocate",
      "--action",
      "apply_relocate_move_plan",
      "--artifact-id",
      "relocate_plan",
      "--on-dst-exists",
      "rename_suffix",
    ]), "/ext/py");
    expect(result).toMatchObject({
      ok: true,
      hasFollowUpToolCalls: false,
      followUpToolCalls: [],
    });
  });

  it("applies reviewed sourceRoot metadata before resuming the workflow", async () => {
    mocks.applyReviewedExecute.mockResolvedValue({ ok: true, rows: 1, sourceYamlPath: "/ops/review.yaml" });
    vi.mocked(runCmd).mockReturnValue(cmdResult({
      ok: true,
      runId: "run_source_review",
      flow: "source_root",
      phase: "plan_ready",
      outcome: "source_root_dry_run_complete",
      artifacts: [],
      gates: [{ id: "metadata_review", status: "approved" }],
      nextActions: [
        {
          action: "review_plan",
          tool: "video_pipeline_resume",
          params: {
            runId: "run_source_review",
            artifactId: "source_root_move_plan",
            resumeAction: "apply_source_root_move_plan",
          },
          requiresHumanInput: true,
        },
      ],
      diagnostics: [],
    }));
    const api = createMockApi();
    registerWorkflowTools(api as never, () => cfg());

    const result = await getRegisteredTool(api, "video_pipeline_resume").execute("call-review", {
      runId: "run_source_review",
      resumeAction: "apply_reviewed_metadata",
      reviewYamlPaths: ["/ops/review.yaml"],
    });

    expect(mocks.applyReviewedExecute).toHaveBeenCalledWith("internal-apply-reviewed-metadata", {
      sourceYamlPath: "/ops/review.yaml",
      markHumanReviewed: true,
    });
    expect(runCmd).toHaveBeenCalledWith("uv", expect.arrayContaining([
      "resume",
      "--run-id",
      "run_source_review",
      "--action",
      "apply_reviewed_metadata",
    ]), "/ext/py");
    expect(result).toMatchObject({
      ok: true,
      phase: "plan_ready",
      reviewedMetadataResults: [{ ok: true, rows: 1, sourceYamlPath: "/ops/review.yaml" }],
      followUpToolCalls: [
        {
          tool: "video_pipeline_resume",
          reason: "review_plan",
          params: {
            runId: "run_source_review",
            artifactId: "source_root_move_plan",
            resumeAction: "apply_source_root_move_plan",
          },
          requiresHumanReview: true,
        },
      ],
    });
  });

  it("does not resume when reviewed metadata application fails", async () => {
    mocks.applyReviewedExecute.mockResolvedValue({ ok: false, error: "bad yaml" });
    const api = createMockApi();
    registerWorkflowTools(api as never, () => cfg());

    const result = await getRegisteredTool(api, "video_pipeline_resume").execute("call-review-fail", {
      runId: "run_source_review",
      resumeAction: "apply_reviewed_metadata",
      reviewYamlPaths: ["/ops/review.yaml"],
    });

    expect(runCmd).not.toHaveBeenCalled();
    expect(result).toMatchObject({
      ok: false,
      outcome: "source_root_review_metadata_apply_failed",
      error: "bad yaml",
    });
  });

  it("returns status and inspect JSON without WorkflowResult translation", async () => {
    const api = createMockApi();
    registerWorkflowTools(api as never, () => cfg());
    vi.mocked(runCmd).mockReturnValueOnce(cmdResult({ ok: true, runs: [{ runId: "run_a" }] }));
    const status = await getRegisteredTool(api, "video_pipeline_status").execute("call-4", {
      limit: 5,
      includeArtifacts: true,
    });
    expect(status).toEqual({ ok: true, runs: [{ runId: "run_a" }] });
    expect(runCmd).toHaveBeenLastCalledWith("uv", expect.arrayContaining([
      "status",
      "--limit",
      "5",
      "--include-artifacts",
    ]), "/ext/py");

    vi.mocked(runCmd).mockReturnValueOnce(cmdResult({ ok: true, artifact: { id: "relocate_plan" } }));
    const inspected = await getRegisteredTool(api, "video_pipeline_inspect_artifact").execute("call-5", {
      runId: "run_a",
      artifactId: "relocate_plan",
      includeContentPreview: true,
      previewBytes: 128,
    });
    expect(inspected).toEqual({ ok: true, artifact: { id: "relocate_plan" } });
    expect(runCmd).toHaveBeenLastCalledWith("uv", expect.arrayContaining([
      "inspect-artifact",
      "--run-id",
      "run_a",
      "--artifact-id",
      "relocate_plan",
      "--include-content-preview",
      "--preview-bytes",
      "128",
    ]), "/ext/py");
  });

  it("returns structured failure when workflow_cli.py emits malformed JSON", async () => {
    vi.mocked(runCmd).mockReturnValue({
      ok: false,
      code: 1,
      stdout: "not json",
      stderr: "boom",
      command: "uv",
      args: [],
      cwd: "/ext/py",
    });
    const api = createMockApi();
    registerWorkflowTools(api as never, () => cfg());

    const result = await getRegisteredTool(api, "video_pipeline_start").execute("call-6", { flow: "source_root" });

    expect(result).toMatchObject({
      ok: false,
      error: "workflow_cli.py did not return a JSON object",
      exitCode: 1,
      stdout: "not json",
      stderr: "boom",
    });
  });
});
