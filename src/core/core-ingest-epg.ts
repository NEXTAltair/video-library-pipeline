import { parseJsonObject, resolvePythonScript, runCmd } from "./runtime";
import type { AnyObj } from "./types";

export type IngestEpgParams = {
  tsRoot?: string;
  apply?: boolean;
  limit?: number;
};

export type IngestEpgResult = AnyObj & {
  ok: boolean;
  tool: string;
  exitCode: number;
};

// EPG ingest のコアロジック。tool / CLI 両方から呼ばれる。
export function runIngestEpg(cfg: AnyObj, params: IngestEpgParams): IngestEpgResult {
  const resolved = resolvePythonScript("ingest_program_txt.py");

  const tsRoot = String(params.tsRoot || cfg.tsRoot || "");
  if (!tsRoot) {
    return {
      ok: false,
      tool: "video_pipeline_ingest_epg",
      exitCode: 1,
      error: "tsRoot is required. Pass it as a parameter or configure plugins.entries.video-library-pipeline.config.tsRoot.",
    };
  }

  const args = [
    "run",
    "python",
    resolved.scriptPath,
    "--db",
    String(cfg.db || ""),
    "--ts-root",
    tsRoot,
  ];

  if (params.apply === true) args.push("--apply");
  if (typeof params.limit === "number" && Number.isFinite(params.limit)) {
    args.push("--limit", String(Math.trunc(params.limit)));
  }

  const r = runCmd("uv", args, resolved.cwd);
  const parsed = parseJsonObject(r.stdout);
  const out: IngestEpgResult = {
    ok: r.ok,
    tool: "video_pipeline_ingest_epg",
    scriptSource: resolved.source,
    scriptPath: resolved.scriptPath,
    exitCode: r.code,
    stdout: r.stdout,
    stderr: r.stderr,
  };
  if (parsed) {
    for (const [k, v] of Object.entries(parsed)) out[k] = v;
  }
  return out;
}
