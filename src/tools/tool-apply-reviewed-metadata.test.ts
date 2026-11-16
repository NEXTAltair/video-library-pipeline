import { describe, expect, it } from "vitest";
import { stampAcceptance } from "./tool-apply-reviewed-metadata";

describe("review provenance", () => {
  it("agent acceptance strips inherited human claims", () => {
    const row: Record<string, any> = { human_reviewed: true, human_reviewed_at: "old", human_reviewed_by: "old", human_confirmation: { evidence: "old" } };
    stampAcceptance(row, false);
    expect(row.human_reviewed).toBe(false);
    expect(row.review_acceptance).toBe("agent_accepted");
    expect(row.human_reviewed_at).toBeUndefined();
    expect(row.human_reviewed_by).toBeUndefined();
    expect(row.human_confirmation).toBeUndefined();
  });
  it("records supplied human confirmation separately", () => {
    const row: Record<string, any> = {};
    stampAcceptance(row, true, "owner", "original message reference");
    expect(row.review_acceptance).toBe("human_confirmed");
    expect(row.human_confirmation).toEqual({ confirmed_by: "owner", evidence: "original message reference" });
  });
});
