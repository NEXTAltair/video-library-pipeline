import { latestJsonlFile, toToolResult } from "./runtime";
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
        const cfg = getCfg(api);
        const root = String(cfg.windowsOpsRoot || "").replace(/\/+$/, "");
        const moveDir = root ? `${root}/move` : "";
        const latestSummary = moveDir ? `${moveDir}/LATEST_SUMMARY.md` : "";
        const latestApply = latestJsonlFile(moveDir, "move_apply_");
        const latestPlan = latestJsonlFile(moveDir, "move_plan_from_inventory_");
        const out: AnyObj = {
          ok: true,
          tool: "video_pipeline_status",
          latestSummary,
        };
        if (params.includeRawPaths) {
          out.moveDir = moveDir;
          out.latestApply = latestApply;
          out.latestPlan = latestPlan;
        }
        return toToolResult(out);
      },
    },
    { optional: true },
  );
}
