import { afterEach, describe, expect, it, vi } from "vitest";
import { spawnSync } from "node:child_process";
import { runCmd } from "./runtime";

vi.mock("node:child_process", () => ({
  spawnSync: vi.fn(() => ({ status: 0, stdout: "", stderr: "" })),
}));

afterEach(() => { vi.unstubAllEnvs(); vi.clearAllMocks(); });

describe("Python environment isolation at the subprocess boundary", () => {
  it("uses runtime state instead of creating .venv in a linked plugin checkout", () => {
    vi.stubEnv("OPENCLAW_STATE_DIR", "/tmp/test-openclaw-state");
    vi.stubEnv("OPENCLAW_VIDEO_PIPELINE_VENV", "");
    vi.stubEnv("UV_PROJECT_ENVIRONMENT", "/tmp/unrelated-python-project");
    runCmd("uv", ["run", "python", "workflow_cli.py", "status"], "/tmp/plugin/py");
    expect(vi.mocked(spawnSync).mock.calls[0][2]?.env?.UV_PROJECT_ENVIRONMENT).toBe(
      "/tmp/test-openclaw-state/plugin-runtime/video-library-pipeline/.venv",
    );
    expect(process.env.UV_PROJECT_ENVIRONMENT).toBe("/tmp/unrelated-python-project");
  });

  it("honors an explicit plugin-specific environment without changing other commands", () => {
    vi.stubEnv("OPENCLAW_VIDEO_PIPELINE_VENV", "/tmp/custom-pipeline-env");
    runCmd("uv", ["run", "python", "workflow_cli.py"]);
    expect(vi.mocked(spawnSync).mock.calls[0][2]?.env?.UV_PROJECT_ENVIRONMENT).toBe("/tmp/custom-pipeline-env");
    runCmd("ffprobe", ["-version"]);
    expect(vi.mocked(spawnSync).mock.calls[1][2]?.env).toBe(process.env);
  });
});
