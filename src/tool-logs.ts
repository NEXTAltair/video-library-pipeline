import fs from "node:fs";
import path from "node:path";
import { latestJsonlFile, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

export function registerToolLogs(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_logs",
      description: "Get latest log file pointers and optional tail text for video pipeline logs.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          kind: {
            type: "string",
            enum: [
              "apply",
              "plan",
              "inventory",
              "remaining",
              "backfill-plan",
              "backfill-apply",
              "backfill-queue",
              "dedup-plan",
              "dedup-apply",
              "relocate-plan",
              "relocate-apply",
              "relocate-queue",
              "all",
            ],
            default: "all",
          },
          tail: { type: "integer", minimum: 1, maximum: 500, default: 50 },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const moveDir = path.join(cfg.windowsOpsRoot || "", "move");
        const out: AnyObj = { ok: true, tool: "video_pipeline_logs", moveDir };
        const kind = params.kind ?? "all";

        if (kind === "all" || kind === "apply") out.apply = latestJsonlFile(moveDir, "move_apply_");
        if (kind === "all" || kind === "plan") out.plan = latestJsonlFile(moveDir, "move_plan_from_inventory_");
        if (kind === "all" || kind === "inventory") out.inventory = latestJsonlFile(moveDir, "inventory_unwatched_");
        if (kind === "all" || kind === "backfill-plan") out.backfillPlan = latestJsonlFile(moveDir, "backfill_plan_");
        if (kind === "all" || kind === "backfill-apply") out.backfillApply = latestJsonlFile(moveDir, "backfill_apply_");
        if (kind === "all" || kind === "backfill-queue") {
          out.backfillQueue = latestJsonlFile(path.join(cfg.windowsOpsRoot || "", "llm"), "backfill_metadata_queue_");
        }
        if (kind === "all" || kind === "dedup-plan") out.dedupPlan = latestJsonlFile(moveDir, "dedup_plan_");
        if (kind === "all" || kind === "dedup-apply") out.dedupApply = latestJsonlFile(moveDir, "dedup_apply_");
        if (kind === "all" || kind === "relocate-plan") out.relocatePlan = latestJsonlFile(moveDir, "relocate_plan_");
        if (kind === "all" || kind === "relocate-apply") out.relocateApply = latestJsonlFile(moveDir, "relocate_apply_");
        if (kind === "all" || kind === "relocate-queue") {
          out.relocateQueue = latestJsonlFile(path.join(cfg.windowsOpsRoot || "", "llm"), "relocate_metadata_queue_");
        }

        if (kind === "all" || kind === "remaining") {
          const rem = fs.existsSync(moveDir)
            ? fs
                .readdirSync(moveDir)
                .filter((n) => n.startsWith("remaining_unwatched_") && n.endsWith(".txt"))
                .map((n) => path.join(moveDir, n))
                .sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs)[0]
            : null;
          out.remaining = rem ?? null;
          if (rem && fs.existsSync(rem)) {
            const lines = fs.readFileSync(rem, "utf-8").split(/\r?\n/).filter(Boolean);
            out.remainingTail = lines.slice(-Number(params.tail ?? 50));
          }
        }

        return toToolResult(out);
      },
    },
    { optional: true },
  );
}
