import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

type AnyObj = Record<string, any>;

const pluginId = "video-library-pipeline";

function toToolResult(obj: AnyObj) {
  return { content: [{ type: "text", text: JSON.stringify(obj, null, 2) }] };
}

function getCfg(api: any) {
  const cfg = api?.config?.plugins?.entries?.[pluginId]?.config ?? {};
  return {
    mediaopsDir: cfg.mediaopsDir,
    db: cfg.db,
    sourceRoot: cfg.sourceRoot,
    destRoot: cfg.destRoot,
    hostDataRoot: cfg.hostDataRoot,
    defaultLimit: Number.isFinite(cfg.defaultLimit) ? cfg.defaultLimit : 200,
  };
}

function runCmd(command: string, args: string[], cwd?: string) {
  const cp = spawnSync(command, args, {
    cwd,
    env: process.env,
    encoding: "utf-8",
  });
  return {
    ok: cp.status === 0,
    code: cp.status ?? 1,
    stdout: cp.stdout ?? "",
    stderr: cp.stderr ?? "",
    command,
    args,
    cwd,
  };
}

function latestFile(dir: string, prefix: string) {
  if (!fs.existsSync(dir)) return null;
  const files = fs
    .readdirSync(dir)
    .filter((n) => n.startsWith(prefix) && n.endsWith(".jsonl"))
    .map((n) => path.join(dir, n))
    .map((p) => ({ p, m: fs.statSync(p).mtimeMs }))
    .sort((a, b) => b.m - a.m);
  return files[0]?.p ?? null;
}

function injectCommonPathArgs(args: string[], cfg: ReturnType<typeof getCfg>) {
  const out = [...args];
  if (cfg.db && !out.includes("--db")) out.push("--db", cfg.db);
  if (cfg.sourceRoot && !out.includes("--source-root")) out.push("--source-root", cfg.sourceRoot);
  if (cfg.destRoot && !out.includes("--dest-root")) out.push("--dest-root", cfg.destRoot);
  return out;
}

export default function register(api: any) {
  api.registerGatewayMethod(`${pluginId}.status`, ({ respond }: any) => {
    const cfg = getCfg(api);
    respond(true, { ok: true, pluginId, configured: cfg });
  });

  // 1) run
  api.registerTool(
    {
      name: "video_pipeline_run",
      description:
        "Run end-to-end video pipeline (inventory, metadata, plan, apply, db update). Use apply=false for dry-run.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          apply: { type: "boolean", default: false },
          limit: { type: "integer", minimum: 1, maximum: 5000 },
          allowNeedsReview: { type: "boolean", default: false },
          profile: { type: "string" },
          pathsOverride: {
            type: "object",
            additionalProperties: false,
            properties: {
              mediaopsDir: { type: "string" },
              db: { type: "string" },
              sourceRoot: { type: "string" },
              destRoot: { type: "string" },
              hostDataRoot: { type: "string" },
            },
          },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const base = getCfg(api);
        const merged = { ...base, ...(params.pathsOverride ?? {}) };
        const mediaopsDir = merged.mediaopsDir || process.cwd();
        let args = [
          "run",
          "python",
          path.join(mediaopsDir, "unwatched_pipeline_runner.py"),
          "--limit",
          String(params.limit ?? merged.defaultLimit ?? 200),
        ];
        if (params.apply) args.push("--apply");
        if (params.allowNeedsReview) args.push("--allow-needs-review");
        args = injectCommonPathArgs(args, merged as any);

        const r = runCmd("uv", args, mediaopsDir);
        return toToolResult({
          ok: r.ok,
          tool: "video_pipeline_run",
          exitCode: r.code,
          stdout: r.stdout,
          stderr: r.stderr,
        });
      },
    },
    { optional: true },
  );

  // 2) status
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
        const latestApply = latestFile(moveDir, "move_apply_");
        const latestPlan = latestFile(moveDir, "move_plan_from_inventory_");
        return toToolResult({ ok: true, tool: "video_pipeline_status", moveDir, latestApply, latestPlan });
      },
    },
    { optional: true },
  );

  // 3) validate
  api.registerTool(
    {
      name: "video_pipeline_validate",
      description: "Validate config, binaries, and key path accessibility without side effects.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          strict: { type: "boolean", default: true },
          checkWindowsInterop: { type: "boolean", default: true },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const checks: AnyObj = {
          mediaopsDirExists: !!cfg.mediaopsDir && fs.existsSync(cfg.mediaopsDir),
          dbExists: !!cfg.db && fs.existsSync(cfg.db),
          hostDataRootExists: !!cfg.hostDataRoot && fs.existsSync(cfg.hostDataRoot),
        };
        const uv = runCmd("uv", ["--version"]);
        const py = runCmd("python3", ["--version"]);
        checks.uv = uv.ok;
        checks.python3 = py.ok;

        if (params.checkWindowsInterop) {
          const pw = runCmd("/mnt/c/Program Files/PowerShell/7/pwsh.exe", ["-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"]);
          checks.pwsh7 = pw.ok;
          checks.pwshVersion = pw.stdout.trim();
        }

        const ok = Object.values(checks).every((v) => v === true || typeof v === "string");
        return toToolResult({ ok, tool: "video_pipeline_validate", checks });
      },
    },
    { optional: true },
  );

  // 4) repair_db
  api.registerTool(
    {
      name: "video_pipeline_repair_db",
      description: "Run DB repair helpers for path/link consistency (safe/full modes).",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          mode: { type: "string", enum: ["safe", "full"], default: "safe" },
          dryRun: { type: "boolean", default: true },
          limit: { type: "integer", minimum: 1, maximum: 5000 },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const mediaopsDir = cfg.mediaopsDir || process.cwd();
        // current safe baseline script
        const script = path.join(mediaopsDir, "repair_paths_dir_name_from_path.py");
        const args = ["run", "python", script, "--db", cfg.db || ""];
        if (!params.dryRun) args.push("--apply");
        const r = runCmd("uv", args, mediaopsDir);
        return toToolResult({ ok: r.ok, tool: "video_pipeline_repair_db", mode: params.mode ?? "safe", exitCode: r.code, stdout: r.stdout, stderr: r.stderr });
      },
    },
    { optional: true },
  );

  // 5) reextract
  api.registerTool(
    {
      name: "video_pipeline_reextract",
      description: "Run metadata re-extraction batch from queue source conditions.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          source: { type: "string", enum: ["needsReview", "version", "inventory"] },
          extractionVersion: { type: "string" },
          limit: { type: "integer", minimum: 1, maximum: 5000, default: 200 },
          batchSize: { type: "integer", minimum: 1, maximum: 1000, default: 50 },
        },
        required: ["source"],
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const mediaopsDir = cfg.mediaopsDir || process.cwd();
        const outDir = path.join(cfg.hostDataRoot || "/tmp", "llm");

        // Minimal scaffold: currently supports inventory source by reusing existing queue if present.
        if (params.source !== "inventory") {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_reextract",
            errorCode: "NOT_IMPLEMENTED_SOURCE",
            message: "source=needsReview/version not implemented yet in plugin; use inventory source for now.",
          });
        }

        const queue = path.join(outDir, "queue_manual_reextract.jsonl");
        const args = [
          "run",
          "python",
          path.join(mediaopsDir, "run_metadata_batches_promptv1.py"),
          "--queue",
          queue,
          "--outdir",
          outDir,
          "--batch-size",
          String(params.batchSize ?? 50),
        ];
        if (params.extractionVersion) args.push("--extraction-version", String(params.extractionVersion));
        const r = runCmd("uv", args, mediaopsDir);
        return toToolResult({ ok: r.ok, tool: "video_pipeline_reextract", exitCode: r.code, stdout: r.stdout, stderr: r.stderr, queue });
      },
    },
    { optional: true },
  );

  // 6) logs
  api.registerTool(
    {
      name: "video_pipeline_logs",
      description: "Get latest log file pointers and optional tail text for video pipeline logs.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          kind: { type: "string", enum: ["apply", "plan", "inventory", "remaining", "all"], default: "all" },
          tail: { type: "integer", minimum: 1, maximum: 500, default: 50 },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const moveDir = path.join(cfg.hostDataRoot || "", "move");
        const out: AnyObj = { ok: true, tool: "video_pipeline_logs", moveDir };
        const kind = params.kind ?? "all";

        if (kind === "all" || kind === "apply") out.apply = latestFile(moveDir, "move_apply_");
        if (kind === "all" || kind === "plan") out.plan = latestFile(moveDir, "move_plan_from_inventory_");
        if (kind === "all" || kind === "inventory") out.inventory = latestFile(moveDir, "inventory_unwatched_");

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

  api.registerCli?.(
    ({ program }: any) => {
      program
        .command("video-pipeline-status")
        .description("Show configured video-library-pipeline plugin values")
        .action(() => {
          const cfg = getCfg(api);
          console.log(JSON.stringify({ pluginId, config: cfg }, null, 2));
        });
    },
    { commands: ["video-pipeline-status"] },
  );
}
