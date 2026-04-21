import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("./runtime", async () => {
  const actual = await vi.importActual<typeof import("./runtime")>("./runtime");
  return {
    ...actual,
    getExtensionRootDir: vi.fn(() => "/ext"),
    resolvePythonScript: vi.fn(() => ({ scriptPath: "/ext/py/unwatched_pipeline_runner.py", cwd: "/ext/py", source: "extension" })),
    runCmd: vi.fn(),
    toToolResult: vi.fn((obj: Record<string, unknown>) => obj),
  };
});

vi.mock("./windows-scripts-bootstrap", () => ({
  ensureWindowsScripts: vi.fn(() => ({
    ok: true,
    created: [],
    updated: [],
    existing: [],
    failed: [],
    missingTemplates: [],
  })),
}));

import { registerToolRun } from "./tool-run";
import { runCmd } from "./runtime";

function createMockApi() {
  return {
    registerTool: vi.fn(),
  };
}

function getRegisteredTool(api: ReturnType<typeof createMockApi>) {
  const toolDefs = api.registerTool.mock.calls.map(([def]) => def);
  const tool = toolDefs.find((def: { name: string }) => def.name === "video_pipeline_analyze_and_move_videos");
  expect(tool).toBeTruthy();
  return tool;
}

describe("video_pipeline_analyze_and_move_videos", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("routes review-required runs to apply_reviewed_metadata when review YAML already exists", async () => {
    vi.mocked(runCmd).mockReturnValue({
      ok: true,
      code: 0,
      stdout: JSON.stringify({
        ok: true,
        queue: "/ops/llm/queue_unwatched_batch_20260407.jsonl",
        plan_stats: { planned: 0, skipped_needs_review: 13 },
        workflowState: "metadata_review_required",
        reviewYamlPath: "/ops/llm/program_aliases_review_0001_0013.yaml",
        reviewYamlPaths: ["/ops/llm/program_aliases_review_0001_0013.yaml"],
        reviewSummary: { rowsNeedingReview: 13 },
      }),
      stderr: "",
      command: "uv",
      args: [],
      cwd: "/ext/py",
    });

    const api = createMockApi();
    const cfg = {
      db: "/ops/db/mediaops.sqlite",
      sourceRoot: "B:\\Unwatched",
      destRoot: "B:\\VideoLibrary",
      windowsOpsRoot: "/ops",
      defaultMaxFilesPerRun: 200,
      driveRoutesPath: "",
    };

    registerToolRun(api as never, () => cfg);
    const tool = getRegisteredTool(api);
    const result = await tool.execute("tool-call-1", { apply: false });

    expect(result.reviewYamlPath).toBe("/ops/llm/program_aliases_review_0001_0013.yaml");
    expect(result.nextStep).toContain("review");
    expect(result.nextStep).not.toContain("video_pipeline_reextract");
    expect(result.followUpToolCalls).toEqual([
      {
        tool: "video_pipeline_apply_reviewed_metadata",
        reason: "apply_generated_review_yaml_after_human_review",
        requiresHumanReview: true,
        params: { sourceYamlPath: "/ops/llm/program_aliases_review_0001_0013.yaml" },
      },
    ]);
    expect(result.hasFollowUpToolCalls).toBe(true);
  });

  it("keeps legacy reextract guidance when review YAML was not generated yet", async () => {
    vi.mocked(runCmd).mockReturnValue({
      ok: true,
      code: 0,
      stdout: JSON.stringify({
        ok: true,
        queue: "/ops/llm/queue_unwatched_batch_20260407.jsonl",
        plan_stats: { planned: 0, skipped_needs_review: 5 },
      }),
      stderr: "",
      command: "uv",
      args: [],
      cwd: "/ext/py",
    });

    const api = createMockApi();
    const cfg = {
      db: "/ops/db/mediaops.sqlite",
      sourceRoot: "B:\\Unwatched",
      destRoot: "B:\\VideoLibrary",
      windowsOpsRoot: "/ops",
      defaultMaxFilesPerRun: 200,
      driveRoutesPath: "",
    };

    registerToolRun(api as never, () => cfg);
    const tool = getRegisteredTool(api);
    const result = await tool.execute("tool-call-2", { apply: false });

    expect(result.nextStep).toContain("video_pipeline_reextract");
    expect(result.followUpToolCalls).toEqual([
      {
        tool: "video_pipeline_reextract",
        reason: "extract_metadata_for_needs_review_files",
        params: { queuePath: "/ops/llm/queue_unwatched_batch_20260407.jsonl" },
      },
    ]);
  });
});
