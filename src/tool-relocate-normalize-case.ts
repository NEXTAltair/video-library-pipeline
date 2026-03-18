import { parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

export function registerToolRelocateNormalizeCase(api: any, _getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_relocate_normalize_case",
      description:
        "Normalize case-only folder-name differences using relocate dry-run plan. Use after video_pipeline_relocate_existing_files dry-run reports already_correct due to case-only mismatch.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          planPath: {
            type: "string",
            description: "Path to relocate_plan_*.jsonl (required).",
          },
          apply: {
            type: "boolean",
            default: false,
            description: "false=dry-run, true=execute two-step renames.",
          },
          limit: {
            type: "integer",
            minimum: 1,
            maximum: 100000,
            description: "Optional max operations.",
          },
        },
        required: ["planPath"],
      },
      async execute(_id: string, params: AnyObj) {
        const planPath = String(params.planPath || "").trim();
        if (!planPath) {
          return toToolResult({ ok: false, tool: "video_pipeline_relocate_normalize_case", error: "planPath is required" });
        }

        const resolved = resolvePythonScript("normalize_relocate_case.py");
        const args = ["run", "python", resolved.scriptPath, "--plan", planPath];
        if (params.apply === true) args.push("--apply");
        if (typeof params.limit === "number" && Number.isFinite(params.limit)) {
          args.push("--limit", String(Math.trunc(params.limit)));
        }

        const r = runCmd("uv", args, resolved.cwd);
        const parsed = parseJsonObject(r.stdout);
        const out: AnyObj = {
          ok: r.ok,
          tool: "video_pipeline_relocate_normalize_case",
          scriptSource: resolved.source,
          scriptPath: resolved.scriptPath,
          exitCode: r.code,
          stdout: r.stdout,
          stderr: r.stderr,
        };
        if (parsed) {
          for (const [k, v] of Object.entries(parsed)) out[k] = v;
        }
        return toToolResult(out);
      },
    },
    { displayName: "Relocate Normalize Case" },
  );
}
