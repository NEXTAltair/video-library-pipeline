import path from "node:path";
import { latestJsonlFile } from "./runtime";

export function registerCli(api: any, pluginId: string, getCfg: (api: any) => any) {
  // CLI補助コマンド: 現在の plugin config を確認する。
  api.registerCli?.(
    ({ program }: any) => {
      program
        .command("video-pipeline-status")
        .description("Show configured video-library-pipeline plugin values")
        .action(() => {
          const cfg = getCfg(api);
          const moveDir = path.join(cfg.windowsOpsRoot || "", "move");
          const llmDir = path.join(cfg.windowsOpsRoot || "", "llm");
          console.log(
            JSON.stringify(
              {
                pluginId,
                config: cfg,
                latestBackfillPlan: latestJsonlFile(moveDir, "backfill_plan_"),
                latestBackfillApply: latestJsonlFile(moveDir, "backfill_apply_"),
                latestBackfillQueue: latestJsonlFile(llmDir, "backfill_metadata_queue_"),
                latestDedupPlan: latestJsonlFile(moveDir, "dedup_plan_"),
                latestDedupApply: latestJsonlFile(moveDir, "dedup_apply_"),
              },
              null,
              2,
            ),
          );
        });
    },
    { commands: ["video-pipeline-status"] },
  );
}
