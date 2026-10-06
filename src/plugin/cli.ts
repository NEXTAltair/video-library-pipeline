import path from "node:path";
import { ensureWindowsScripts } from "../platform/windows-scripts-bootstrap";
import { latestJsonlFile, resolvePythonScript, runCmd } from "./runtime";
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
        .command("video-pipeline-organize")
        .description("Deterministic daily controller; preview uses a DB copy, --apply authorizes safe moves")
        .option("--apply", "Apply fresh safe moves; never approve metadata review or overwrite", false)
        .option("--scheduled", "Japanese alerts on changes/errors; silent success", false)
        .option("--limit <n>", "Maximum files per run", Number, 500)
        .action((opts: { apply?: boolean; scheduled?: boolean; limit: number }) => {
          const cfg = getCfg(api);
          const scripts = ensureWindowsScripts(cfg);
          if (!scripts.ok) throw new Error(`Windows scripts provisioning failed: ${JSON.stringify(scripts.failed)}`);
          const resolved = resolvePythonScript("daily_cli.py");
          const args = ["run", "python", resolved.scriptPath, "--db", cfg.db,
            "--windows-ops-root", cfg.windowsOpsRoot, "--source-root", cfg.sourceRoot,
            "--dest-root", cfg.destRoot, "--ts-root", cfg.tsRoot,
            "--drive-routes", cfg.driveRoutesPath, "--limit", String(opts.limit)];
          if (opts.apply) args.push("--apply");
          if (opts.scheduled) args.push("--scheduled");
          const result = runCmd("uv", args, resolved.cwd);
          if (result.stdout) console.log(result.stdout.trim());
          if (result.stderr) console.error(result.stderr.trim());
          process.exitCode = result.code || (result.ok ? 0 : 1);
        });

      program
        .command("video-pipeline-repair-history")
        .description("Repair proven audit records; default preview DB, never move/delete video files")
        .requiredOption("--pairs-json <path>", "Audit missing-destination pairs JSON")
        .requiredOption("--titles-json <path>", "Audit demonstrated title regression JSON")
        .requiredOption("--outdir <path>", "Preview/backup and exact repair receipt directory")
        .option("--apply", "Apply the audited repair after creating an online DB backup", false)
        .option("--archive-orphans", "Preserve exact orphan history rows and clear broken references", false)
        .action((opts: { pairsJson: string; titlesJson: string; outdir: string; apply?: boolean; archiveOrphans?: boolean }) => {
          const cfg = getCfg(api);
          const resolved = resolvePythonScript("repair_legacy_records.py");
          const args = ["run", "python", resolved.scriptPath, "--db",cfg.db,
            "--pairs-json",path.resolve(opts.pairsJson),"--titles-json",path.resolve(opts.titlesJson),"--outdir",path.resolve(opts.outdir)];
          if (opts.apply) args.push("--apply");
          if (opts.archiveOrphans) args.push("--archive-orphans");
          const result = runCmd("uv",args,resolved.cwd);
          if (result.stdout) console.log(result.stdout.trim());
          if (result.stderr) console.error(result.stderr.trim());
          process.exitCode = result.code || (result.ok ? 0 : 1);
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
        { name: "video-pipeline-organize", description: "Deterministic daily organization with non-destructive preview", hasSubcommands: false },
        { name: "video-pipeline-repair-history", description: "Repair audit-proven legacy records with a DB backup", hasSubcommands: false },
        { name: "video-pipeline-ingest-epg", description: "Ingest EDCB .program.txt EPG data into DB", hasSubcommands: false },
      ],
    },
  );
}
