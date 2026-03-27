import { describe, it, expect } from "vitest";
import { buildLlmExtractTask } from "./tool-reextract";

const INPUT_PATH = "/mnt/ops/llm/llm_filename_extract_input_20260327_100000.jsonl";
const OUTPUT_PATH = "/mnt/ops/llm/llm_filename_extract_output_20260327_100000.jsonl";
const HINTS_PATH = "/ext/rules/program_aliases.yaml";

describe("buildLlmExtractTask", () => {
  const task = buildLlmExtractTask(INPUT_PATH, OUTPUT_PATH, HINTS_PATH);

  it("returns a string", () => {
    expect(typeof task).toBe("string");
  });

  it("contains inputJsonlPath", () => {
    expect(task).toContain(INPUT_PATH);
  });

  it("contains outputJsonlPath", () => {
    expect(task).toContain(OUTPUT_PATH);
  });

  it("contains hintsPath", () => {
    expect(task).toContain(HINTS_PATH);
  });

  it("mentions program_title field", () => {
    expect(task).toContain("program_title");
  });

  it("mentions air_date field", () => {
    expect(task).toContain("air_date");
  });

  it("mentions UNKNOWN sentinel", () => {
    expect(task).toContain("UNKNOWN");
  });

  it("mentions video_pipeline_apply_llm_extract_output", () => {
    expect(task).toContain("video_pipeline_apply_llm_extract_output");
  });

  it("mentions subtitle field", () => {
    expect(task).toContain("subtitle");
  });

  it("mentions needs_review field", () => {
    expect(task).toContain("needs_review");
  });

  it("each call with same args produces same result (deterministic)", () => {
    const task2 = buildLlmExtractTask(INPUT_PATH, OUTPUT_PATH, HINTS_PATH);
    expect(task).toBe(task2);
  });
});
