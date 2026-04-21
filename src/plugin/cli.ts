import path from "node:path";
import { latestJsonlFile } from "./runtime";
import { runIngestEpg } from "./core-ingest-epg";
import type { PluginApi, GetCfgFn } from "./types";

export function registerCli(api: PluginApi, pluginId: string, getCfg: GetCfgFn) {
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

      program
        .command("video-pipeline-ingest-epg")
        .description("Ingest EDCB .program.txt EPG data into DB (config auto-resolved)")
        .option("--apply", "Write to DB (default: dry-run)", false)
        .option("--ts-root <path>", "Override tsRoot from plugin config")
        .option("--limit <n>", "Max files to process", parseInt)
        .action((opts: { apply?: boolean; tsRoot?: string; limit?: number }) => {
          const cfg = getCfg(api);
          const result = runIngestEpg(cfg, {
            tsRoot: opts.tsRoot,
            apply: opts.apply,
            limit: opts.limit,
          });
          console.log(JSON.stringify(result, null, 2));
          process.exitCode = result.ok ? 0 : 1;
        });
    },
    {
      descriptors: [
        { name: "video-pipeline-status", description: "Show configured video-library-pipeline plugin values", hasSubcommands: false },
        { name: "video-pipeline-ingest-epg", description: "Ingest EDCB .program.txt EPG data into DB", hasSubcommands: false },
      ],
    },
  );
}
