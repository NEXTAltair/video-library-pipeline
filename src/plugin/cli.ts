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
        .command("video-pipeline-dedup")
        .description("Deduplicate episodes while separately preserving original/repeat broadcasts; preview by default")
        .option("--apply", "Quarantine confirmed duplicates with restore receipts; never permanently delete", false)
        .option("--scheduled", "Silent no-op success; report changed results and failures", false)
        .option("--max-groups <n>", "Groups per run; scheduled runs rotate through all groups", Number, 50)
        .option("--min-age-seconds <n>", "Exclude recently modified recordings", Number, 3600)
        .option("--restore-run <id>", "Preview restoration of an exact dedup run; --apply executes")
        .option("--date-evidence <directory>", "Explicit physical discovery and sourced date evidence; never a scheduled-policy change")
        .option("--apply-run <id>", "Exact prepared date-first run; never replay a completed run")
        .option("--native-preview", "Validate date-first source and every coverage keeper on Windows without moving", false)
        .action((opts: { apply?: boolean; scheduled?: boolean; maxGroups: number; minAgeSeconds: number; restoreRun?: string; dateEvidence?: string; applyRun?: string; nativePreview?: boolean }) => {
          const cfg = getCfg(api);
          const resolved = resolvePythonScript("dedup_cli.py");
          const args = ["run", "python", resolved.scriptPath, "--db", cfg.db,
            "--windows-ops-root", cfg.windowsOpsRoot, "--source-root", cfg.sourceRoot,
            "--dest-root", cfg.destRoot, "--max-groups", String(opts.maxGroups),
            "--min-age-seconds", String(opts.minAgeSeconds)];
          if (cfg.driveRoutesPath) args.push("--drive-routes", cfg.driveRoutesPath);
          if (opts.apply) args.push("--apply");
          if (opts.scheduled) args.push("--scheduled");
          if (opts.restoreRun) args.push("--restore-run", opts.restoreRun);
          if (opts.dateEvidence) args.push("--date-evidence", path.resolve(opts.dateEvidence));
          if (opts.applyRun) args.push("--apply-run", opts.applyRun);
          if (opts.nativePreview) args.push("--native-preview");
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
        .command("video-pipeline-quarantine-catalog")
        .description("One searchable view of a verified quarantine handoff; never move media or update the DB")
        .requiredOption("--handoff <path>", "Existing verified deletion-handoff.json")
        .requiredOption("--out <path>", "New HTML file outside the deletable quarantine directories")
        .option("--ops-root <directory>", "Original run records root; default: windowsOpsRoot")
        .action((opts: { handoff: string; out: string; opsRoot?: string }) => {
          const resolved = resolvePythonScript("quarantine_catalog_cli.py");
          const args = ["run", "python", resolved.scriptPath,
            "--handoff", opts.handoff, "--out", opts.out,
            "--ops-root", opts.opsRoot || getCfg(api).windowsOpsRoot];
          const result = runCmd("uv", args, resolved.cwd);
          if (result.stdout) console.log(result.stdout.trim());
          if (result.stderr) console.error(result.stderr.trim());
          process.exitCode = result.code || (result.ok ? 0 : 1);
        });

      program
        .command("video-pipeline-artifacts")
        .description("Artifact storage report and recoverable cleanup of explicitly managed caches; never delete evidence")
        .option("--root <directory>", "Artifact root; default: windowsOpsRoot/artifacts (legacy roots are report-only)")
        .option("--create <id>", "Create a managed job with separate cache/ and evidence/ directories")
        .option("--complete <id>", "Mark an explicitly finished job complete; open reviews still prevent cleanup")
        .option("--cache-days <n>", "Retain completed caches for this many days", Number, 7)
        .option("--plan", "Save a checksum-bound cleanup preview", false)
        .option("--apply-plan <id>", "Move the exact preview's caches to recoverable .trash; never permanently delete")
        .option("--restore <id>", "Preview restoration of the exact cleanup; --apply executes")
        .option("--archive-to <directory>", "Plan byte-preserving archival of an existing artifacts/ root outside the plugin source")
        .option("--apply-archive <plan-path>", "Execute the exact legacy archival plan; no permanent deletion")
        .option("--restore-archive <receipt-path>", "Preview restoring the original artifacts/ path; --apply executes")
        .option("--apply", "Execute --restore/--restore-archive only", false)
        .action((opts: { root?: string; create?: string; complete?: string; cacheDays: number; plan?: boolean; applyPlan?: string; restore?: string; archiveTo?: string; applyArchive?: string; restoreArchive?: string; apply?: boolean }) => {
          const resolved = resolvePythonScript("artifact_cli.py");
          const root = opts.root || path.join(getCfg(api).windowsOpsRoot, "artifacts");
          const args = ["run", "python", resolved.scriptPath, "--root", root, "--cache-days", String(opts.cacheDays)];
          if (opts.create) args.push("--create", opts.create);
          if (opts.complete) args.push("--complete", opts.complete);
          if (opts.plan) args.push("--plan");
          if (opts.applyPlan) args.push("--apply-plan", opts.applyPlan);
          if (opts.restore) args.push("--restore", opts.restore);
          if (opts.archiveTo) args.push("--archive-to", opts.archiveTo);
          if (opts.applyArchive) args.push("--apply-archive", opts.applyArchive);
          if (opts.restoreArchive) args.push("--restore-archive", opts.restoreArchive);
          if (opts.apply) args.push("--apply");
          const result = runCmd("uv", args, resolved.cwd);
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
        { name: "video-pipeline-dedup", description: "Duplicate quarantine preserving original and repeat broadcasts separately", hasSubcommands: false },
        { name: "video-pipeline-repair-history", description: "Repair audit-proven legacy records with a DB backup", hasSubcommands: false },
        { name: "video-pipeline-quarantine-catalog", description: "Flat searchable view of an existing verified recording handoff", hasSubcommands: false },
        { name: "video-pipeline-artifacts", description: "Storage inventory and recoverable managed-cache cleanup", hasSubcommands: false },
        { name: "video-pipeline-ingest-epg", description: "Ingest EDCB .program.txt EPG data into DB", hasSubcommands: false },
      ],
    },
  );
}
