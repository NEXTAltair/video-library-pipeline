import { afterEach, describe, expect, it, vi } from "vitest";
import { registerCli } from "./cli";
import { runCmd } from "./runtime";

vi.mock("./runtime", () => ({
  resolvePythonScript: vi.fn(() => ({ scriptPath: "/plugin/py/artifact_cli.py", cwd: "/plugin" })),
  runCmd: vi.fn(() => ({ ok: true, code: 0, stdout: '{"ok":true}', stderr: "" })),
  latestJsonlFile: vi.fn(),
}));

afterEach(() => {
  vi.restoreAllMocks();
  vi.clearAllMocks();
  process.exitCode = 0;
});

function artifactCommand(getCfg: any, name = "video-pipeline-artifacts") {
  const actions = new Map<string, (opts: any) => void>();
  const program = {
    command(name: string) {
      const builder: any = {
        description: () => builder,
        option: () => builder,
        requiredOption: () => builder,
        action: (callback: (opts: any) => void) => { actions.set(name, callback); return builder; },
      };
      return builder;
    },
  };
  const api: any = { registerCli: (register: any) => register({ program }) };
  registerCli(api, "video-library-pipeline", getCfg);
  vi.spyOn(console, "log").mockImplementation(() => {});
  return actions.get(name)!;
}

describe("quarantine catalogue CLI", () => {
  it("forwards exact handoff/output paths without loading unrelated configuration", () => {
    const getCfg = vi.fn(() => { throw new Error("not configured"); });
    artifactCommand(getCfg, "video-pipeline-quarantine-catalog")({
      handoff: "/audit/handoff.json", out: "B:\\_AI_WORK\\duplicates\\view.html", opsRoot: "/mnt/b/_AI_WORK",
    });
    expect(getCfg).not.toHaveBeenCalled();
    const args = vi.mocked(runCmd).mock.calls[0][1];
    expect(args).toEqual(expect.arrayContaining(["--handoff", "/audit/handoff.json", "--out",
      "B:\\_AI_WORK\\duplicates\\view.html", "--ops-root", "/mnt/b/_AI_WORK"]));
    expect(args).not.toContain("--apply");
  });
});

describe("artifact maintenance CLI", () => {
  it("supports explicit audit roots without validating unrelated library configuration", () => {
    const getCfg = vi.fn(() => { throw new Error("not configured"); });
    artifactCommand(getCfg)({ root: "/project/artifacts", cacheDays: 7 });
    expect(getCfg).not.toHaveBeenCalled();
    expect(runCmd).toHaveBeenCalledWith("uv", ["run", "python", "/plugin/py/artifact_cli.py",
      "--root", "/project/artifacts", "--cache-days", "7"], "/plugin");
  });

  it("resolves the configured runtime artifact root and forwards exact cleanup identity", () => {
    artifactCommand(vi.fn(() => ({ windowsOpsRoot: "/ops" })))({ applyPlan: "cleanup_exact", cacheDays: 7 });
    expect(runCmd).toHaveBeenCalledWith("uv", expect.arrayContaining([
      "--root", "/ops/artifacts", "--apply-plan", "cleanup_exact",
    ]), "/plugin");
  });

  it("restoration stays a preview unless explicitly applied and propagates failures", () => {
    const command = artifactCommand(vi.fn());
    command({ root: "/ops/artifacts", restore: "cleanup_exact", cacheDays: 7 });
    expect(vi.mocked(runCmd).mock.calls[0][1]).not.toContain("--apply");
    vi.mocked(runCmd).mockReturnValueOnce({ ok: false, code: 3, stdout: "", stderr: "" });
    command({ root: "/ops/artifacts", restore: "cleanup_exact", cacheDays: 7, apply: true });
    expect(vi.mocked(runCmd).mock.calls[1][1]).toContain("--apply");
    expect(process.exitCode).toBe(3);
  });

  it("forwards a legacy archive receipt without implicitly applying restoration", () => {
    artifactCommand(vi.fn())({ root: "/project/artifacts", restoreArchive: "/state/receipt.json", cacheDays: 7 });
    const args = vi.mocked(runCmd).mock.calls[0][1];
    expect(args).toContain("--restore-archive");
    expect(args).toContain("/state/receipt.json");
    expect(args).not.toContain("--apply");
  });
});
