import fs from "node:fs";
import path from "node:path";
import { runCmd, toToolResult } from "./runtime";
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
          strict: { type: "boolean", default: true },
          checkWindowsInterop: { type: "boolean", default: true },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const checks: AnyObj = {
          dbExists: !!cfg.db && fs.existsSync(cfg.db),
          hostDataRootExists: !!cfg.hostDataRoot && fs.existsSync(cfg.hostDataRoot),
        };
        const uv = runCmd("uv", ["--version"]);
        const py = runCmd("python3", ["--version"]);
        checks.uv = uv.ok;
        checks.python3 = py.ok;

        if (params.checkWindowsInterop) {
          const pw = runCmd("/mnt/c/Program Files/PowerShell/7/pwsh.exe", ["-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"]);
          checks.pwsh7 = pw.ok;
          checks.pwshVersion = pw.stdout.trim();

          // Active pipeline depends on these Windows-side scripts.
          const scriptsDir = "B:\\_AI_WORK\\scripts";
          const requiredScripts = [
            "unwatched_inventory.ps1",
            "apply_move_plan.ps1",
            "fix_prefix_timestamp_names.ps1",
            "normalize_unwatched_names.ps1",
            "list_remaining_unwatched.ps1",
          ];
          checks.requiredWindowsScripts = requiredScripts.map((name) => ({
            name,
            exists: fs.existsSync(path.join("/mnt/b/_AI_WORK/scripts", name)),
            windowsPath: `${scriptsDir}\\${name}`,
          }));
        }

        const scriptChecks = Array.isArray(checks.requiredWindowsScripts)
          ? checks.requiredWindowsScripts.every((s: AnyObj) => s.exists === true)
          : true;
        const ok = Object.entries(checks).every(([k, v]) => {
          if (k === "requiredWindowsScripts") return true;
          return v === true || typeof v === "string";
        }) && scriptChecks;
        return toToolResult({ ok, tool: "video_pipeline_validate", checks });
      },
    },
    { optional: true },
  );
}
