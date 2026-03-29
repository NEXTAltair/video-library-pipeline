import { parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";
import { ensureWindowsScripts } from "./windows-scripts-bootstrap";

export function registerToolDedupDropReview(api: any, getCfg: (api: any) => any) {
  // --- Generate YAML ---
  api.registerTool(
    {
      name: "video_pipeline_dedup_generate_drop_review_yaml",
      description: "Generate drop-review YAML from dedup plan JSONL for human review of keep/drop decisions.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          planJsonlPath: { type: "string", description: "Path to dedup_plan_*.jsonl" },
          outputPath: { type: "string", description: "Output YAML path (optional, auto-generated if empty)" },
        },
        required: ["planJsonlPath"],
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("dedup_generate_drop_review_yaml.py");
        const args = [
          "run", "python", resolved.scriptPath,
          "--plan-jsonl", String(params.planJsonlPath),
          "--db", String(cfg.db || ""),
        ];
        if (params.outputPath) args.push("--output", String(params.outputPath));

        const r = runCmd("uv", args, resolved.cwd);
        const parsed = parseJsonObject(r.stdout);
        const out: AnyObj = {
          ok: r.ok,
          tool: "video_pipeline_dedup_generate_drop_review_yaml",
          exitCode: r.code,
          stdout: r.stdout,
          stderr: r.stderr,
        };
        if (parsed) {
          for (const [k, v] of Object.entries(parsed)) out[k] = v;
        }
        const outputPath = String(out.outputPath ?? "");
        if (outputPath && Number(out.groupCount ?? 0) > 0) {
          out.nextSteps = [
            `Drop-review YAML generated at: ${outputPath}`,
            `The operator should review each group's "decision" field (keep/drop/skip).`,
            `After editing, apply with: video_pipeline_dedup_apply_drop_review_yaml reviewYamlPath="${outputPath}"`,
          ];
        }
        return toToolResult(out);
      },
    }
  );

  // --- Apply YAML ---
  api.registerTool(
    {
      name: "video_pipeline_dedup_apply_drop_review_yaml",
      description: "Apply human-edited drop-review YAML to quarantine drop candidates.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          reviewYamlPath: { type: "string", description: "Path to edited drop-review YAML" },
        },
        required: ["reviewYamlPath"],
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("dedup_apply_drop_review_yaml.py");
        const scriptsProvision = ensureWindowsScripts(cfg);
        if (!scriptsProvision.ok) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_dedup_apply_drop_review_yaml",
            error: "failed to provision required windows scripts",
          });
        }

        const args = [
          "run", "python", resolved.scriptPath,
          "--yaml", String(params.reviewYamlPath),
          "--db", String(cfg.db || ""),
          "--windows-ops-root", String(cfg.windowsOpsRoot || ""),
        ];

        const r = runCmd("uv", args, resolved.cwd);
        const parsed = parseJsonObject(r.stdout);
        const out: AnyObj = {
          ok: r.ok,
          tool: "video_pipeline_dedup_apply_drop_review_yaml",
          exitCode: r.code,
          stdout: r.stdout,
          stderr: r.stderr,
        };
        if (parsed) {
          for (const [k, v] of Object.entries(parsed)) out[k] = v;
        }
        return toToolResult(out);
      },
    }
  );
}
