import fs from "node:fs";
import path from "node:path";
import { getExtensionRootDir, runCmd, toToolResult } from "./runtime";
import type { AnyObj, PluginApi, GetCfgFn } from "./types";
import { REQUIRED_WINDOWS_SCRIPTS, ensureWindowsScripts } from "./windows-scripts-bootstrap";

const NEXT_STEP_HINTS: Record<string, string> = {
  relocate:
    "Validation passed. Immediately call video_pipeline_relocate_existing_files with {apply: false, roots: [<user-specified roots>], queueMissingMetadata: true, writeMetadataQueueOnDryRun: true, scanErrorPolicy: \"warn\", scanRetryCount: 2}. Do not ask the user for additional details — proceed with the dry-run now.",
  inventory:
    "Validation passed. Immediately call video_pipeline_analyze_and_move_videos with {apply: false, allowNeedsReview: false} to start the inventory dry-run.",
  backfill:
    "Validation passed. Immediately call video_pipeline_backfill_moved_files with {apply: false} to start the DB sync dry-run.",
  extract:
    "Validation passed. Immediately call video_pipeline_reextract using the queuePath from the previous inventory stage.",
};

export function registerToolValidate(api: PluginApi, getCfg: GetCfgFn) {
  api.registerTool(
    {
      name: "video_pipeline_validate",
      description: "Validate config, binaries, and key path accessibility without side effects.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          checkWindowsInterop: { type: "boolean", default: true, description: "Also check PowerShell 7 and Windows LongPathsEnabled. Required before any apply operation." },
          intent: {
            type: "string",
            enum: ["relocate", "inventory", "backfill", "extract"],
            description: "Declared intent for this pipeline run. When provided, nextStep is included in the result to guide the next tool call.",
          },
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
        const hintsFilePresent = !!hintsPath && fs.existsSync(hintsPath);
        const checks: AnyObj = {
          dbPathConfigured: !!cfg.db,
          dbParentDirExists: !!dbDir && fs.existsSync(dbDir),
          windowsOpsRootExists: !!cfg.windowsOpsRoot && fs.existsSync(cfg.windowsOpsRoot),
          moveDirExists: !!moveDir && fs.existsSync(moveDir),
          llmDirExists: !!llmDir && fs.existsSync(llmDir),
          rulesDirExists: !!rulesDir && fs.existsSync(rulesDir),
          scriptsDirExists: !!scriptsDir && fs.existsSync(scriptsDir),
          hintsPath,
          hintsFilePresent,
          scriptsProvision: {
            created: scriptsProvision.created,
            updated: scriptsProvision.updated,
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
        const hintsParser = hintsFilePresent
          ? runCmd("uv", ["run", "python", "-c", "import yaml"])
          : { ok: true, stderr: "", stdout: "" };
        const hintsLoad = hintsFilePresent && hintsParser.ok
          ? runCmd("uv", [
            "run",
            "python",
            "-c",
            "import sys,yaml; yaml.safe_load(open(sys.argv[1], encoding='utf-8-sig'))",
            hintsPath,
          ])
          : hintsParser;
        checks.hintsParserAvailable = hintsParser.ok;
        checks.hintsLoadable = !hintsFilePresent || (hintsParser.ok && hintsLoad.ok);
        checks.hintsLoadError = hintsFilePresent && !checks.hintsLoadable
          ? String((!hintsParser.ok ? hintsParser.stderr || hintsParser.stdout : hintsLoad.stderr || hintsLoad.stdout) || "failed to load program_aliases.yaml").trim()
          : "";

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

          const regCandidates = ["/mnt/c/Windows/System32/reg.exe", "reg.exe"];
          let reg = runCmd(regCandidates[0], [
            "query",
            "HKLM\\SYSTEM\\CurrentControlSet\\Control\\FileSystem",
            "/v",
            "LongPathsEnabled",
          ]);
          if (!reg.ok && regCandidates.length > 1) {
            reg = runCmd(regCandidates[1], [
              "query",
              "HKLM\\SYSTEM\\CurrentControlSet\\Control\\FileSystem",
              "/v",
              "LongPathsEnabled",
            ]);
          }
          checks.longPathsRegistryQuery = reg.command;
          checks.longPathsRegistryStdout = reg.stdout.trim();
          checks.longPathsRegistryStderr = reg.stderr.trim();
          let longPathsEnabled = false;
          if (reg.ok) {
            const m = reg.stdout.match(/LongPathsEnabled\s+REG_DWORD\s+0x([0-9a-fA-F]+)/);
            if (m) {
              longPathsEnabled = parseInt(m[1], 16) === 1;
            }
          }
          checks.longPathsEnabled = longPathsEnabled;
        } else {
          checks.longPathsEnabled = true;
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
            k === "hintsPath" ||
            k === "hintsFilePresent" ||
            k === "hintsLoadError" ||
            k === "moveDirExists" ||
            k === "llmDirExists"
          ) {
            return true;
          }
          return v === true || typeof v === "string";
        }) && scriptChecks && scriptsProvisionOk;
        const result: AnyObj = { ok, tool: "video_pipeline_validate", checks };
        if (ok && params.intent) {
          result.nextStep = NEXT_STEP_HINTS[params.intent as string] ?? null;
        }
        return toToolResult(result);
      },
    }
  );
}
