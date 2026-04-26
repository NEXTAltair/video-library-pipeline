import { describe, expect, it } from "vitest";
import { translateWorkflowResult, type WorkflowResultPayload } from "./workflow-result-translator";

function basePayload(overrides: Partial<WorkflowResultPayload> = {}): WorkflowResultPayload {
  return {
    ok: true,
    runId: "run_fixture",
    flow: "source_root",
    phase: "complete",
    outcome: "complete",
    artifacts: [],
    gates: [],
    nextActions: [],
    diagnostics: [],
    ...overrides,
  };
}

describe("translateWorkflowResult", () => {
  it("maps sourceRoot plan-ready nextActions to followUpToolCalls", () => {
    const result = translateWorkflowResult(basePayload({
      phase: "plan_ready",
      outcome: "source_root_dry_run_complete",
      artifacts: [{ id: "source_root_move_plan", type: "source_root_move_plan" }],
      nextActions: [
        {
          action: "review_plan",
          label: "Review sourceRoot move plan",
          tool: "video_pipeline_resume",
          params: {
            runId: "run_fixture",
            artifactId: "source_root_move_plan",
            resumeAction: "apply_source_root_move_plan",
          },
          requiresHumanInput: true,
        },
      ],
    }));

    expect(result.followUpToolCalls).toEqual([
      {
        tool: "video_pipeline_resume",
        reason: "review_plan",
        params: {
          runId: "run_fixture",
          artifactId: "source_root_move_plan",
          resumeAction: "apply_source_root_move_plan",
        },
        requiresHumanReview: true,
      },
    ]);
    expect(result.hasFollowUpToolCalls).toBe(true);
    expect(result.nextStep).toContain("Review");
  });

  it("keeps human review explicit for sourceRoot metadata review handoff", () => {
    const result = translateWorkflowResult(basePayload({
      ok: false,
      phase: "review_required",
      outcome: "source_root_metadata_review_required",
      gates: [{ id: "metadata_review", requiresHumanReview: true }],
      nextActions: [
        {
          action: "review_metadata",
          label: "Review extracted metadata YAML",
          tool: "video_pipeline_resume",
          params: {
            runId: "run_fixture",
            gateId: "metadata_review",
            artifactIds: ["metadata_review_yaml_0001"],
            reviewYamlPaths: ["/tmp/runs/run_fixture/review/metadata_review_0001.yaml"],
            resumeAction: "apply_reviewed_metadata",
          },
          requiresHumanInput: true,
        },
      ],
    }));

    expect(result.followUpToolCalls[0]).toMatchObject({
      tool: "video_pipeline_resume",
      reason: "review_metadata",
      requiresHumanReview: true,
    });
    expect(result.followUpToolCalls[0].params).toEqual({
      runId: "run_fixture",
      gateId: "metadata_review",
      artifactIds: ["metadata_review_yaml_0001"],
      reviewYamlPaths: ["/tmp/runs/run_fixture/review/metadata_review_0001.yaml"],
      resumeAction: "apply_reviewed_metadata",
    });
  });

  it("maps relocate metadata preparation and explicitly marks non-human follow-up", () => {
    const result = translateWorkflowResult(basePayload({
      ok: false,
      flow: "relocate",
      phase: "review_required",
      outcome: "relocate_metadata_preparation_required",
      artifacts: [{ id: "relocate_metadata_queue", type: "relocate_metadata_queue" }],
      nextActions: [
        {
          action: "prepare_relocate_metadata",
          label: "Prepare missing or blocked relocate metadata",
          tool: "video_pipeline_resume",
          params: {
            runId: "run_relocate_gap",
            artifactIds: ["relocate_metadata_queue"],
          },
          requiresHumanInput: false,
        },
      ],
    }));

    expect(result.followUpToolCalls).toEqual([
      {
        tool: "video_pipeline_resume",
        reason: "prepare_relocate_metadata",
        params: {
          runId: "run_relocate_gap",
          artifactIds: ["relocate_metadata_queue"],
        },
        requiresHumanReview: false,
      },
    ]);
    expect(result.nextStep).toBe("Proceed with followUpToolCalls to continue the workflow.");
  });

  it("maps relocate plan-ready follow-up without rewriting params", () => {
    const result = translateWorkflowResult(basePayload({
      flow: "relocate",
      phase: "plan_ready",
      outcome: "relocate_plan_ready",
      nextActions: [
        {
          action: "review_plan",
          label: "Review relocate move plan",
          tool: "video_pipeline_resume",
          params: {
            runId: "run_relocate_plan",
            artifactId: "relocate_plan",
            resumeAction: "apply_relocate_move_plan",
          },
          requiresHumanInput: true,
        },
      ],
    }));

    expect(result.followUpToolCalls[0]).toEqual({
      tool: "video_pipeline_resume",
      reason: "review_plan",
      params: {
        runId: "run_relocate_plan",
        artifactId: "relocate_plan",
        resumeAction: "apply_relocate_move_plan",
      },
      requiresHumanReview: true,
    });
  });

  it.each([
    ["complete", true, "complete"],
    ["blocked", false, "source_root_blocked"],
    ["failed", false, "source_root_failed"],
  ])("returns no follow-up calls for terminal %s outcomes without nextActions", (phase, ok, outcome) => {
    const result = translateWorkflowResult(basePayload({ ok, phase, outcome }));

    expect(result.followUpToolCalls).toEqual([]);
    expect(result.hasFollowUpToolCalls).toBe(false);
    expect(result.nextStep).toBeUndefined();
  });

  it("normalizes missing optional arrays to empty arrays", () => {
    const result = translateWorkflowResult({
      ok: true,
      runId: "run_minimal",
      flow: "relocate",
      phase: "complete",
      outcome: "relocate_already_correct",
    });

    expect(result.artifacts).toEqual([]);
    expect(result.gates).toEqual([]);
    expect(result.nextActions).toEqual([]);
    expect(result.diagnostics).toEqual([]);
    expect(result.followUpToolCalls).toEqual([]);
    expect(result.hasFollowUpToolCalls).toBe(false);
  });

  it("preserves action data but skips follow-up calls when no tool is declared", () => {
    const result = translateWorkflowResult(basePayload({
      nextActions: [
        {
          action: "manual_note",
          label: "Read diagnostics",
          params: { runId: "run_fixture" },
          requiresHumanInput: true,
        },
      ],
    }));

    expect(result.nextActions).toHaveLength(1);
    expect(result.followUpToolCalls).toEqual([]);
    expect(result.hasFollowUpToolCalls).toBe(false);
  });

  it("filters malformed nextActions entries before reading fields", () => {
    const result = translateWorkflowResult({
      ...basePayload(),
      nextActions: [
        null,
        "review_plan",
        42,
        ["not", "an", "object"],
        { label: "Missing action", tool: "video_pipeline_resume" },
        {
          action: "review_plan",
          label: "Review sourceRoot move plan",
          tool: "video_pipeline_resume",
          params: { runId: "run_fixture" },
          requiresHumanInput: true,
        },
      ] as unknown as WorkflowResultPayload["nextActions"],
    });

    expect(result.nextActions).toEqual([
      {
        action: "review_plan",
        label: "Review sourceRoot move plan",
        tool: "video_pipeline_resume",
        params: { runId: "run_fixture" },
        requiresHumanInput: true,
      },
    ]);
    expect(result.followUpToolCalls).toEqual([
      {
        tool: "video_pipeline_resume",
        reason: "review_plan",
        params: { runId: "run_fixture" },
        requiresHumanReview: true,
      },
    ]);
  });
});
