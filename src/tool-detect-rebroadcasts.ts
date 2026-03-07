import { parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

export function registerToolDetectRebroadcasts(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_detect_rebroadcasts",
      description:
        "Detect rebroadcast groups and classify members using EPG is_rebroadcast_flag (broadcasts.data_json). " +
        "Classification is original/rebroadcast/unknown (unknown when EPG flag is unavailable). Use apply=false for dry-run.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          apply: {
            type: "boolean",
            default: false,
            description: "If true, write broadcast_groups to DB. If false, dry-run only.",
          },
          maxGroups: {
            type: "integer",
            minimum: 1,
            maximum: 5000,
            description: "Max number of rebroadcast groups to process.",
          },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("detect_rebroadcasts.py");

        const args = [
          "run",
          "python",
          resolved.scriptPath,
          "--db",
          String(cfg.db || ""),
        ];

        if (params.apply === true) args.push("--apply");
        if (typeof params.maxGroups === "number" && Number.isFinite(params.maxGroups)) {
          args.push("--max-groups", String(Math.trunc(params.maxGroups)));
        }

        const r = runCmd("uv", args, resolved.cwd);
        const parsed = parseJsonObject(r.stdout);
        const out: AnyObj = {
          ok: r.ok,
          tool: "video_pipeline_detect_rebroadcasts",
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
    { optional: true },
  );
}
