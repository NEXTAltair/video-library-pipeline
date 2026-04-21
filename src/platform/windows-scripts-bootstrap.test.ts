import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  resolveWindowsScriptTemplatePath,
  windowsScriptTemplateRoots,
} from "./windows-scripts-bootstrap";

const tempDirs: string[] = [];

afterEach(() => {
  for (const dir of tempDirs.splice(0)) {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

describe("windowsScriptTemplateRoots", () => {
  it("prefers templates before legacy assets", () => {
    const root = "/repo";
    expect(windowsScriptTemplateRoots(root)).toEqual([
      path.join(root, "templates", "windows-scripts"),
      path.join(root, "assets", "windows-scripts"),
    ]);
  });
});

describe("resolveWindowsScriptTemplatePath", () => {
  it("chooses the canonical templates copy when both locations exist", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "vlp-template-test-"));
    tempDirs.push(root);

    const templateDir = path.join(root, "templates", "windows-scripts");
    const assetDir = path.join(root, "assets", "windows-scripts");
    fs.mkdirSync(templateDir, { recursive: true });
    fs.mkdirSync(assetDir, { recursive: true });

    fs.writeFileSync(path.join(templateDir, "apply_move_plan.ps1"), "template");
    fs.writeFileSync(path.join(assetDir, "apply_move_plan.ps1"), "legacy");

    expect(resolveWindowsScriptTemplatePath("apply_move_plan.ps1", root)).toBe(
      path.join(templateDir, "apply_move_plan.ps1"),
    );
  });

  it("falls back to legacy assets during migration", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "vlp-template-test-"));
    tempDirs.push(root);

    const assetDir = path.join(root, "assets", "windows-scripts");
    fs.mkdirSync(assetDir, { recursive: true });
    fs.writeFileSync(path.join(assetDir, "unwatched_inventory.ps1"), "legacy");

    expect(resolveWindowsScriptTemplatePath("unwatched_inventory.ps1", root)).toBe(
      path.join(assetDir, "unwatched_inventory.ps1"),
    );
  });
});
