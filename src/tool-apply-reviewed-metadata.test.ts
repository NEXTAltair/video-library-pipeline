import { describe, expect, it } from "vitest";
import { applyYamlReviewToRows } from "./tool-apply-reviewed-metadata";

describe("applyYamlReviewToRows", () => {
  it("retitles rows and clears title-only review flags", () => {
    const rows = [{
      path: "B:\\VideoLibrary\\Show\\2026\\04\\episode.ts",
      program_title: "Show Episode guide",
      needs_review: true,
      needs_review_reason: "suspicious_program_title",
    }];
    const aliasToCanonical = new Map([
      ["showepisodeguide", "Show"],
      ["show", "Show"],
    ]);

    const result = applyYamlReviewToRows(rows, aliasToCanonical);

    expect(result.retitledRowsCount).toBe(1);
    expect(result.reviewClearedRowsCount).toBe(1);
    expect(result.staleReviewFlagsClearedWithoutRetitle).toBe(0);
    expect(result.editedRows[0]).toMatchObject({
      program_title: "Show",
      needs_review: false,
      needs_review_reason: "",
    });
  });

  it("clears stale title-only review flags even when the title is already canonical", () => {
    const rows = [{
      path: "B:\\VideoLibrary\\Show\\2026\\04\\episode.ts",
      program_title: "Show",
      needs_review: true,
      needs_review_reason: "suspicious_program_title",
    }];
    const aliasToCanonical = new Map([["show", "Show"]]);

    const result = applyYamlReviewToRows(rows, aliasToCanonical);

    expect(result.changedRowsCount).toBe(1);
    expect(result.retitledRowsCount).toBe(0);
    expect(result.reviewClearedRowsCount).toBe(1);
    expect(result.staleReviewFlagsClearedWithoutRetitle).toBe(1);
    expect(result.editedRows[0]).toMatchObject({
      program_title: "Show",
      needs_review: false,
      needs_review_reason: "",
    });
  });

  it("keeps needs_review when non-title reasons remain", () => {
    const rows = [{
      path: "B:\\VideoLibrary\\Show\\2026\\04\\episode.ts",
      program_title: "Show",
      needs_review: true,
      needs_review_reason: "suspicious_program_title,missing_air_date",
    }];
    const aliasToCanonical = new Map([["show", "Show"]]);

    const result = applyYamlReviewToRows(rows, aliasToCanonical);

    expect(result.changedRowsCount).toBe(0);
    expect(result.reviewClearedRowsCount).toBe(0);
    expect(result.editedRows[0]).toMatchObject({
      program_title: "Show",
      needs_review: true,
      needs_review_reason: "suspicious_program_title,missing_air_date",
    });
  });

  it("keeps needs_review when the title still looks shortened versus the folder title", () => {
    const rows = [{
      path: "B:\\VideoLibrary\\VeryLongProgram\\2026\\04\\episode.ts",
      program_title: "Very",
      needs_review: true,
      needs_review_reason: "suspicious_program_title_shortened",
    }];
    const aliasToCanonical = new Map([["very", "Very"]]);

    const result = applyYamlReviewToRows(rows, aliasToCanonical);

    expect(result.changedRowsCount).toBe(0);
    expect(result.reviewClearedRowsCount).toBe(0);
    expect(result.editedRows[0]).toMatchObject({
      program_title: "Very",
      needs_review: true,
      needs_review_reason: "suspicious_program_title_shortened",
    });
  });
});
