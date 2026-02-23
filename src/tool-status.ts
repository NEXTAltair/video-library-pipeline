import fs from "node:fs";
import path from "node:path";
import { getExtensionRootDir, latestJsonlFile, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

export function registerToolStatus(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_status",
      description: "Read latest pipeline status summary from windowsOpsRoot/move.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          includeRawPaths: { type: "boolean", default: false },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const rawCfg = api?.config?.plugins?.entries?.["video-library-pipeline"]?.config ?? {};
        const missingConfigKeys = ["windowsOpsRoot", "sourceRoot", "destRoot"].filter((k) => {
          const v = rawCfg?.[k];
          return !(typeof v === "string" && v.trim().length > 0);
        });
        let cfg: AnyObj;
        try {
          cfg = getCfg(api);
        } catch (e: any) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_status",
            missingConfigKeys,
            error: String(e?.message || e),
            rulesState: "unknown",
          });
        }
        const root = String(cfg.windowsOpsRoot || "").replace(/\/+$/, "");
        const moveDir = root ? `${root}/move` : "";
        const llmDir = root ? `${root}/llm` : "";
        const latestApply = latestJsonlFile(moveDir, "move_apply_");
        const latestPlan = latestJsonlFile(moveDir, "move_plan_from_inventory_");
        const latestBackfillPlan = latestJsonlFile(moveDir, "backfill_plan_");
        const latestBackfillApply = latestJsonlFile(moveDir, "backfill_apply_");
        const latestBackfillQueue = latestJsonlFile(llmDir, "backfill_metadata_queue_");
        const latestDedupPlan = latestJsonlFile(moveDir, "dedup_plan_");
        const latestDedupApply = latestJsonlFile(moveDir, "dedup_apply_");
        const hintsYaml = path.join(getExtensionRootDir(), "rules", "program_aliases.yaml");
        const hintsPresent = fs.existsSync(hintsYaml);
        const out: AnyObj = {
          ok: true,
          tool: "video_pipeline_status",
          missingConfigKeys,
          rulesState: hintsPresent ? "configured_optional" : "missing_ai_only_mode",
          hintsPath: hintsYaml,
          hintsPresent,
        };
        if (params.includeRawPaths) {
          out.moveDir = moveDir;
          out.latestApply = latestApply;
          out.latestPlan = latestPlan;
          out.latestBackfillPlan = latestBackfillPlan;
          out.latestBackfillApply = latestBackfillApply;
          out.latestBackfillQueue = latestBackfillQueue;
          out.latestDedupPlan = latestDedupPlan;
          out.latestDedupApply = latestDedupApply;
        }
        return toToolResult(out);
      },
    },
    { optional: true },
  );
}
