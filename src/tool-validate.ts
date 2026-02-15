import fs from "node:fs";
import path from "node:path";
import { getExtensionRootDir, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

export function registerToolValidate(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_validate",
      description: "Validate config, binaries, and key path accessibility without side effects.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          checkWindowsInterop: { type: "boolean", default: true },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const dbDir = !!cfg.db ? path.dirname(cfg.db) : "";
        const moveDir = !!cfg.windowsOpsRoot ? path.join(cfg.windowsOpsRoot, "move") : "";
        const llmDir = !!cfg.windowsOpsRoot ? path.join(cfg.windowsOpsRoot, "llm") : "";
        const rulesDir = path.join(getExtensionRootDir(), "rules");
        const scriptsDir = !!cfg.windowsOpsRoot ? path.join(cfg.windowsOpsRoot, "scripts") : "";
        const hintsPath = rulesDir ? path.join(rulesDir, "program_aliases.yaml") : "";
        const checks: AnyObj = {
          dbPathConfigured: !!cfg.db,
          dbParentDirExists: !!dbDir && fs.existsSync(dbDir),
          windowsOpsRootExists: !!cfg.windowsOpsRoot && fs.existsSync(cfg.windowsOpsRoot),
          moveDirExists: !!moveDir && fs.existsSync(moveDir),
          llmDirExists: !!llmDir && fs.existsSync(llmDir),
          rulesDirExists: !!rulesDir && fs.existsSync(rulesDir),
          scriptsDirExists: !!scriptsDir && fs.existsSync(scriptsDir),
          hintsYamlPresent: !!hintsPath && fs.existsSync(hintsPath),
        };
        const uv = runCmd("uv", ["--version"]);
        const py = runCmd("uv", ["run", "python", "--version"]);
        const sqliteOpen = runCmd("uv", [
          "run",
          "python",
          "-c",
          "import sqlite3,sys; sqlite3.connect('file:' + sys.argv[1] + '?mode=ro', uri=True).close()",
          cfg.db,
        ]);
        checks.uv = uv.ok;
        checks.pythonViaUv = py.ok;
        checks.sqliteDbOpen = sqliteOpen.ok;

        if (params.checkWindowsInterop) {
          let pw = runCmd("pwsh", ["-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"]);
          if (!pw.ok) {
            pw = runCmd("pwsh.exe", ["-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"]);
          }
          checks.pwsh7 = pw.ok;
          checks.pwshVersion = pw.stdout.trim();

          // Active pipeline depends on these Windows-side scripts.
          const requiredScripts = [
            "normalize_filenames.ps1",
            "unwatched_inventory.ps1",
            "apply_move_plan.ps1",
            "list_remaining_unwatched.ps1",
          ];
          checks.requiredWindowsScripts = requiredScripts.map((name) => ({
            name,
            exists: !!scriptsDir && fs.existsSync(path.join(scriptsDir, name)),
            path: scriptsDir ? path.join(scriptsDir, name) : "",
          }));
        }

        const scriptChecks = Array.isArray(checks.requiredWindowsScripts)
          ? checks.requiredWindowsScripts.every((s: AnyObj) => s.exists === true)
          : true;
        const ok = Object.entries(checks).every(([k, v]) => {
          if (
            k === "requiredWindowsScripts" ||
            k === "hintsYamlPresent" ||
            k === "moveDirExists" ||
            k === "llmDirExists"
          ) {
            return true;
          }
          return v === true || typeof v === "string";
        }) && scriptChecks;
        return toToolResult({ ok, tool: "video_pipeline_validate", checks });
      },
    },
    { optional: true },
  );
}
