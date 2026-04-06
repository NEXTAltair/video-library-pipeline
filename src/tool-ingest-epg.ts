import { toToolResult } from "./runtime";
import { runIngestEpg } from "./core-ingest-epg";
import type { AnyObj, PluginApi, GetCfgFn } from "./types";

export function registerToolIngestEpg(api: PluginApi, getCfg: GetCfgFn) {
  api.registerTool(
    {
      name: "video_pipeline_ingest_epg",
      description:
        "Scan a TS recording directory for EDCB .program.txt files and ingest EPG metadata into programs/broadcasts tables. " +
        "Run this before deleting program.txt files so that encoded files can be matched to broadcast history.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          tsRoot: {
            type: "string",
            description: "WSL path to TS recording directory (e.g. /mnt/j/TVFile). Defaults to plugin config tsRoot.",
          },
          apply: {
            type: "boolean",
            default: false,
            description: "If true, write to DB. If false, dry-run only.",
          },
          limit: {
            type: "integer",
            minimum: 1,
            maximum: 10000,
            description: "Max number of .program.txt files to process.",
          },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        return toToolResult(runIngestEpg(cfg, params));
      },
    }
  );
}
