import fs from "node:fs";
import path from "node:path";
import { getExtensionRootDir, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";
import { REQUIRED_WINDOWS_SCRIPTS, ensureWindowsScripts } from "./windows-scripts-bootstrap";

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
        const scriptsProvision = ensureWindowsScripts(cfg);
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
          scriptsProvision: {
            created: scriptsProvision.created,
            existing: scriptsProvision.existing,
            failed: scriptsProvision.failed,
            missingTemplates: scriptsProvision.missingTemplates,
          },
          requiredWindowsScripts: REQUIRED_WINDOWS_SCRIPTS.map((name) => ({
            name,
            exists: !!scriptsDir && fs.existsSync(path.join(scriptsDir, name)),
            path: scriptsDir ? path.join(scriptsDir, name) : "",
          })),
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
          const pwshCandidates = ["/mnt/c/Program Files/PowerShell/7/pwsh.exe", "pwsh.exe"];
          let pw = runCmd(pwshCandidates[0], ["-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"]);
          if (!pw.ok && pwshCandidates.length > 1) {
            pw = runCmd(pwshCandidates[1], ["-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"]);
          }
          checks.pwsh7 = pw.ok;
          checks.pwshCommand = pw.command;
          checks.pwshVersion = pw.stdout.trim();
          checks.pwshStderr = pw.stderr.trim();
        }

        const scriptChecks = Array.isArray(checks.requiredWindowsScripts)
          ? checks.requiredWindowsScripts.every((s: AnyObj) => s.exists === true)
          : true;
        const scriptsProvisionOk =
          Array.isArray(checks.scriptsProvision?.failed) &&
          checks.scriptsProvision.failed.length === 0 &&
          Array.isArray(checks.scriptsProvision?.missingTemplates) &&
          checks.scriptsProvision.missingTemplates.length === 0;
        const ok = Object.entries(checks).every(([k, v]) => {
          if (
            k === "requiredWindowsScripts" ||
            k === "scriptsProvision" ||
            k === "hintsYamlPresent" ||
            k === "moveDirExists" ||
            k === "llmDirExists"
          ) {
            return true;
          }
          return v === true || typeof v === "string";
        }) && scriptChecks && scriptsProvisionOk;
        return toToolResult({ ok, tool: "video_pipeline_validate", checks });
      },
    },
    { optional: true },
  );
}
