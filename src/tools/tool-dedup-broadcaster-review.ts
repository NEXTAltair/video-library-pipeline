import path from "node:path";
import { getExtensionRootDir, parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj, PluginApi, GetCfgFn } from "./types";

export function registerToolDedupBroadcasterReview(api: PluginApi, getCfg: GetCfgFn) {
  // --- Generate YAML ---
  api.registerTool(
    {
      name: "video_pipeline_dedup_generate_broadcaster_yaml",
      description: "Generate broadcaster-assign review YAML from dedup plan JSONL (for unknown_bucket_mixed items).",
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
        const resolved = resolvePythonScript("dedup_generate_broadcaster_yaml.py");
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
          tool: "video_pipeline_dedup_generate_broadcaster_yaml",
          exitCode: r.code,
          stdout: r.stdout,
          stderr: r.stderr,
        };
        if (parsed) {
          for (const [k, v] of Object.entries(parsed)) out[k] = v;
        }
        const outputPath = String(out.outputPath ?? "");
        if (outputPath && Number(out.itemCount ?? 0) > 0) {
          out.nextSteps = [
            `Review YAML generated at: ${outputPath}`,
            `The operator should edit the "broadcaster" field for each item (and optionally "bucket" for unknown broadcasters).`,
            `After editing, apply with: video_pipeline_dedup_apply_broadcaster_yaml reviewYamlPath="${outputPath}"`,
          ];
        }
        return toToolResult(out);
      },
    }
  );

  // --- Apply YAML ---
  api.registerTool(
    {
      name: "video_pipeline_dedup_apply_broadcaster_yaml",
      description: "Apply human-edited broadcaster-assign YAML to update DB and broadcast_buckets.yaml.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          reviewYamlPath: { type: "string", description: "Path to edited broadcaster-assign YAML" },
          bucketRulesPath: { type: "string", description: "Path to broadcast_buckets.yaml (optional)" },
        },
        required: ["reviewYamlPath"],
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("dedup_apply_broadcaster_yaml.py");
        const defaultRulesPath = path.join(getExtensionRootDir(), "rules", "broadcast_buckets.yaml");
        const args = [
          "run", "python", resolved.scriptPath,
          "--yaml", String(params.reviewYamlPath),
          "--db", String(cfg.db || ""),
          "--bucket-rules-path", String(params.bucketRulesPath || defaultRulesPath),
        ];

        const r = runCmd("uv", args, resolved.cwd);
        const parsed = parseJsonObject(r.stdout);
        const out: AnyObj = {
          ok: r.ok,
          tool: "video_pipeline_dedup_apply_broadcaster_yaml",
          exitCode: r.code,
          stdout: r.stdout,
          stderr: r.stderr,
        };
        if (parsed) {
          for (const [k, v] of Object.entries(parsed)) out[k] = v;
        }
        if (out.ok && Number(out.updated ?? 0) > 0) {
          out.nextSteps = [
            `${out.updated} broadcaster(s) updated in DB.`,
            `Re-run video_pipeline_dedup_recordings to resolve the previously unknown_bucket_mixed groups.`,
          ];
        }
        return toToolResult(out);
      },
    }
  );
}
