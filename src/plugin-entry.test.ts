import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { pluginId } from "./plugin-meta";

vi.mock("openclaw/plugin-sdk/plugin-entry", () => ({
  definePluginEntry: (entry: unknown) => entry,
}));

import pluginEntry from "../index";

const EXPECTED_TOOL_NAMES = [
  "video_pipeline_inspect_artifact",
  "video_pipeline_resume",
  "video_pipeline_start",
  "video_pipeline_status",
].sort();

function createMockApi() {
  return {
    registerTool: vi.fn(),
    registerGatewayMethod: vi.fn(),
    registerCli: vi.fn(),
    on: vi.fn(),
    config: { plugins: { entries: {} } },
    logger: { warn: vi.fn() },
  };
}

function getRegisteredTool(api: ReturnType<typeof createMockApi>, name: string) {
  const toolDefs = api.registerTool.mock.calls.map(([def]) => def);
  const tool = toolDefs.find((def: { name: string }) => def.name === name);
  expect(tool).toBeTruthy();
  return tool;
}

const TEMP_DIRS: string[] = [];

function makeTempDir() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "video-pipeline-test-"));
  TEMP_DIRS.push(dir);
  return dir;
}

describe("plugin entry", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    while (TEMP_DIRS.length > 0) {
      const dir = TEMP_DIRS.pop();
      if (dir) fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("exports the expected plugin metadata", () => {
    expect(pluginEntry).toMatchObject({
      id: pluginId,
      name: "Video Library Pipeline",
    });
    expect(typeof pluginEntry.description).toBe("string");
    expect(typeof pluginEntry.register).toBe("function");
  });

  it("registers the expected tools, hooks, gateway method, and CLI descriptors", () => {
    const api = createMockApi();

    pluginEntry.register(api);

    const toolDefs = api.registerTool.mock.calls.map(([def]) => def);
    const toolNames = toolDefs.map((def: { name: string }) => def.name).sort();

    expect(toolNames).toEqual(EXPECTED_TOOL_NAMES);
    expect(new Set(toolNames).size).toBe(EXPECTED_TOOL_NAMES.length);
    for (const def of toolDefs) {
      expect(typeof def.description).toBe("string");
      expect(def.description.length).toBeGreaterThan(0);
      expect(typeof def.execute).toBe("function");
      expect(def.parameters).toBeTruthy();
      expect(typeof def.parameters).toBe("object");
    }

    expect(api.registerGatewayMethod).toHaveBeenCalledTimes(1);
    expect(api.registerGatewayMethod).toHaveBeenCalledWith(`${pluginId}.status`, expect.any(Function));

    const hookEvents = api.on.mock.calls.map(([event]) => event).sort();
    expect(hookEvents).toEqual([
      "after_tool_call",
      "before_prompt_build",
      "before_tool_call",
      "tool_result_persist",
    ]);

    expect(api.registerCli).toHaveBeenCalledTimes(1);
    const [, cliOptions] = api.registerCli.mock.calls[0];
    expect(cliOptions).toMatchObject({
      descriptors: [
        {
          name: "video-pipeline-status",
          hasSubcommands: false,
        },
        {
          name: "video-pipeline-ingest-epg",
          hasSubcommands: false,
        },
      ],
    });
  });

  it("returns normalized config through the gateway status method when configuration is valid", () => {
    const sourceRoot = makeTempDir();
    const destRoot = makeTempDir();
    const windowsOpsRoot = makeTempDir();
    const api = createMockApi();
    api.config.plugins.entries[pluginId] = {
      config: {
        sourceRoot,
        destRoot,
        windowsOpsRoot,
      },
    };

    pluginEntry.register(api);

    const [, handler] = api.registerGatewayMethod.mock.calls[0];
    const respond = vi.fn();

    handler({ respond });

    expect(respond).toHaveBeenCalledTimes(1);
    expect(respond).toHaveBeenCalledWith(true, {
      ok: true,
      pluginId,
      configured: {
        db: path.join(windowsOpsRoot, "db", "mediaops.sqlite"),
        sourceRoot,
        destRoot,
        windowsOpsRoot,
        defaultMaxFilesPerRun: 200,
        tsRoot: "",
        driveRoutesPath: "",
      },
    });
  });

  it("returns a gateway error response when configuration is invalid", () => {
    const api = createMockApi();

    pluginEntry.register(api);

    const [, handler] = api.registerGatewayMethod.mock.calls[0];
    const respond = vi.fn();

    handler({ respond });

    expect(respond).toHaveBeenCalledTimes(1);
    const [ok, payload] = respond.mock.calls[0];
    expect(ok).toBe(false);
    expect(payload).toMatchObject({
      ok: false,
      pluginId,
    });
    expect(payload.error).toContain("sourceRoot is required");
    expect(payload.error).toContain("destRoot is required");
    expect(payload.error).toContain("windowsOpsRoot is required");
  });

  it("keeps high-signal V2 tool parameter schemas stable", () => {
    const api = createMockApi();

    pluginEntry.register(api);

    const start = getRegisteredTool(api, "video_pipeline_start");
    expect(start.parameters).toMatchObject({
      type: "object",
      additionalProperties: false,
      required: ["flow"],
      properties: {
        flow: { type: "string", enum: ["source_root", "relocate"] },
        runId: { type: "string" },
        maxFilesPerRun: { type: "integer", minimum: 1, maximum: 5000 },
        roots: { type: "array", items: { type: "string" } },
        onDstExists: { type: "string", enum: ["error", "rename_suffix"], default: "error" },
      },
    });

    const resume = getRegisteredTool(api, "video_pipeline_resume");
    expect(resume.parameters).toMatchObject({
      type: "object",
      additionalProperties: false,
      required: ["runId"],
      properties: {
        runId: { type: "string" },
        action: { type: "string" },
        resumeAction: { type: "string" },
        artifactId: { type: "string" },
      },
    });

    const status = getRegisteredTool(api, "video_pipeline_status");
    expect(status.parameters).toMatchObject({
      type: "object",
      additionalProperties: false,
      properties: {
        runId: { type: "string" },
        limit: { type: "integer", minimum: 1, maximum: 100 },
        includeArtifacts: { type: "boolean", default: false },
      },
    });

    const inspect = getRegisteredTool(api, "video_pipeline_inspect_artifact");
    expect(inspect.parameters).toMatchObject({
      type: "object",
      additionalProperties: false,
      required: ["runId", "artifactId"],
      properties: {
        runId: { type: "string" },
        artifactId: { type: "string" },
        includeContentPreview: { type: "boolean", default: false },
      },
    });
  });
});
