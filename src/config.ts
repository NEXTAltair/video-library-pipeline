import fs from "node:fs";
import path from "node:path";

// このファイルは plugin 設定値の「型」と「正規化」を担当する。
// - 生の設定値(raw): plugins.entries.video-library-pipeline.config
// - 解決済み設定値(resolved): 実行時に使う安全な値

export type VideoPipelinePluginConfig = {
  db?: string;
  sourceRoot?: string;
  destRoot?: string;
  windowsOpsRoot?: string;
  defaultMaxFilesPerRun?: number;
};

export type VideoPipelineResolvedConfig = {
  db: string;
  sourceRoot: string;
  destRoot: string;
  windowsOpsRoot: string;
  defaultMaxFilesPerRun: number;
};

// 入力が不正・未指定のときに使う既定値。
const DEFAULTS = {
  defaultMaxFilesPerRun: 200,
} as const;

// 文字列入力を trim して返す。文字列でなければ空文字。
function asNonEmptyString(v: unknown): string {
  return typeof v === "string" ? v.trim() : "";
}

// Windows drive path (e.g. B:\foo or B:/foo) を WSL path (/mnt/b/foo) に変換する。
function normalizeWindowsDrivePathToWsl(input: string): string {
  const m = input.match(/^([A-Za-z]):(?:[\\/](.*))?$/);
  if (!m) return input;
  const drive = m[1].toLowerCase();
  const rest = (m[2] ?? "").replace(/\\/g, "/");
  return rest ? `/mnt/${drive}/${rest}` : `/mnt/${drive}`;
}

function normalizePathInput(v: unknown): string {
  const s = asNonEmptyString(v);
  if (!s) return "";
  return normalizeWindowsDrivePathToWsl(s);
}

// limit の範囲を 1..5000 に丸める。
function asLimit(v: unknown): number {
  if (typeof v !== "number" || !Number.isFinite(v)) return DEFAULTS.defaultMaxFilesPerRun;
  const n = Math.trunc(v);
  if (n < 1) return 1;
  if (n > 5000) return 5000;
  return n;
}

// 生設定(raw)を実行可能な形へ正規化する。
export function resolveConfig(raw: VideoPipelinePluginConfig | null | undefined): VideoPipelineResolvedConfig {
  const cfg = raw ?? {};
  const sourceRoot = normalizePathInput(cfg.sourceRoot);
  const destRoot = normalizePathInput(cfg.destRoot);
  const windowsOpsRoot = normalizePathInput(cfg.windowsOpsRoot);
  const dbPath = normalizePathInput(cfg.db);
  const errors: string[] = [];

  if (!sourceRoot) errors.push("sourceRoot is required. Configure plugins.entries.video-library-pipeline.config.sourceRoot.");
  if (!destRoot) errors.push("destRoot is required. Configure plugins.entries.video-library-pipeline.config.destRoot.");
  if (!windowsOpsRoot) {
    errors.push("windowsOpsRoot is required. Configure plugins.entries.video-library-pipeline.config.windowsOpsRoot.");
  }

  const resolvedOpsRoot = windowsOpsRoot ? path.resolve(windowsOpsRoot) : "";
  const resolvedSourceRoot = sourceRoot ? path.resolve(sourceRoot) : "";
  const resolvedDestRoot = destRoot ? path.resolve(destRoot) : "";

  if (resolvedSourceRoot && !fs.existsSync(resolvedSourceRoot)) {
    errors.push(`sourceRoot does not exist: ${resolvedSourceRoot}`);
  }
  if (resolvedDestRoot && !fs.existsSync(resolvedDestRoot)) {
    errors.push(`destRoot does not exist: ${resolvedDestRoot}`);
  }
  if (resolvedOpsRoot && !fs.existsSync(resolvedOpsRoot)) {
    errors.push(`windowsOpsRoot does not exist: ${resolvedOpsRoot}`);
  }
  const scriptsDir = resolvedOpsRoot ? path.join(resolvedOpsRoot, "scripts") : "";
  if (scriptsDir && !fs.existsSync(scriptsDir)) {
    errors.push(`windowsOpsRoot contract violation: missing scripts directory: ${scriptsDir}`);
  }

  if (errors.length > 0) {
    throw new Error(`video-library-pipeline config error:\n- ${errors.join("\n- ")}`);
  }

  const resolvedDb = dbPath ? path.resolve(dbPath) : path.join(resolvedOpsRoot, "db", "mediaops.sqlite");
  return {
    db: resolvedDb,
    sourceRoot: resolvedSourceRoot,
    destRoot: resolvedDestRoot,
    windowsOpsRoot: resolvedOpsRoot,
    defaultMaxFilesPerRun: asLimit(cfg.defaultMaxFilesPerRun),
  };
}
