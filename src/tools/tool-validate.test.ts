import fs from "node:fs";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  extensionRoot: "",
  runCmd: vi.fn(),
}));

vi.mock("./runtime", async () => {
  const actual = await vi.importActual<typeof import("./runtime")>("./runtime");
  return {
    ...actual,
    getExtensionRootDir: vi.fn(() => mocks.extensionRoot),
    runCmd: mocks.runCmd,
    toToolResult: vi.fn((obj: Record<string, unknown>) => obj),
  };
});

vi.mock("./windows-scripts-bootstrap", () => ({
  REQUIRED_WINDOWS_SCRIPTS: ["unwatched_inventory.ps1", "apply_move_plan.ps1"],
  ensureWindowsScripts: vi.fn(() => ({
    created: [],
    updated: [],
    existing: [],
    failed: [],
    missingTemplates: [],
  })),
}));

import { registerToolValidate } from "./tool-validate";

function createMockApi() {
  return { registerTool: vi.fn() };
}

function getRegisteredTool(api: ReturnType<typeof createMockApi>, name: string) {
  const toolDefs = api.registerTool.mock.calls.map(([def]) => def);
  const tool = toolDefs.find((def: { name: string }) => def.name === name);
  expect(tool).toBeTruthy();
  return tool;
}

function cfg(root: string) {
  return {
    db: path.join(root, "db", "mediaops.sqlite"),
    sourceRoot: path.join(root, "source"),
    destRoot: path.join(root, "dest"),
    windowsOpsRoot: root,
    defaultMaxFilesPerRun: 200,
    tsRoot: "",
    driveRoutesPath: path.join(root, "rules", "drive_routes.yaml"),
  };
}

function cmd(ok = true, stderr = "") {
  return {
    ok,
    code: ok ? 0 : 1,
    stdout: "",
    stderr,
    command: "uv",
    args: [],
  };
}

describe("video_pipeline_validate hints readiness", () => {
  let tempRoot = "";

  beforeEach(() => {
    tempRoot = fs.mkdtempSync(path.join("/tmp", "vlp-validate-"));
    mocks.extensionRoot = tempRoot;
    for (const rel of ["db", "move", "llm", "scripts", "rules", "source", "dest"]) {
      fs.mkdirSync(path.join(tempRoot, rel), { recursive: true });
    }
    fs.writeFileSync(path.join(tempRoot, "db", "mediaops.sqlite"), "");
    vi.clearAllMocks();
  });

  afterEach(() => {
    fs.rmSync(tempRoot, { recursive: true, force: true });
  });

  it("marks existing broken program_aliases.yaml as not loadable", async () => {
    fs.writeFileSync(path.join(tempRoot, "rules", "program_aliases.yaml"), "hints: [\n", "utf-8");
    mocks.runCmd
      .mockReturnValueOnce(cmd(true))
      .mockReturnValueOnce(cmd(true))
      .mockReturnValueOnce(cmd(true))
      .mockReturnValueOnce(cmd(true))
      .mockReturnValueOnce(cmd(false, "yaml.parser.ParserError: expected ',' or ']'"));
    const api = createMockApi();
    registerToolValidate(api as never, () => cfg(tempRoot));

    const result = await getRegisteredTool(api, "video_pipeline_validate").execute("call-validate", {
      checkWindowsInterop: false,
    });

    expect(result).toMatchObject({
      ok: false,
      checks: {
        hintsFilePresent: true,
        hintsParserAvailable: true,
        hintsLoadable: false,
        hintsLoadError: "yaml.parser.ParserError: expected ',' or ']'",
      },
    });
    expect(mocks.runCmd).toHaveBeenCalledWith("uv", expect.arrayContaining([path.join(tempRoot, "rules", "program_aliases.yaml")]));
  });
});
