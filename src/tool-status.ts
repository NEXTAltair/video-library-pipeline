import path from "node:path";
import { latestJsonlFile, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

export function registerToolStatus(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_status",
      description: "Read latest pipeline status from recent logs under hostDataRoot/move.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          windowHours: { type: "integer", minimum: 1, maximum: 168, default: 24 },
          includeLogs: { type: "boolean", default: true },
        },
      },
      async execute(_id: string, _params: AnyObj) {
        const cfg = getCfg(api);
        const moveDir = path.join(cfg.hostDataRoot || "", "move");
        const latestApply = latestJsonlFile(moveDir, "move_apply_");
        const latestPlan = latestJsonlFile(moveDir, "move_plan_from_inventory_");
        return toToolResult({ ok: true, tool: "video_pipeline_status", moveDir, latestApply, latestPlan });
      },
    },
    { optional: true },
  );
}
