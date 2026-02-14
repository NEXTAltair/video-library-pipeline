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
const DEFAULTS: VideoPipelineResolvedConfig = {
  db: "",
  sourceRoot: "",
  destRoot: "",
  windowsOpsRoot: "",
  defaultMaxFilesPerRun: 200,
};

// 文字列入力を trim して返す。文字列でなければ空文字。
function asNonEmptyString(v: unknown): string {
  return typeof v === "string" ? v.trim() : "";
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
  return {
    db: asNonEmptyString(cfg.db),
    sourceRoot: asNonEmptyString(cfg.sourceRoot),
    destRoot: asNonEmptyString(cfg.destRoot),
    windowsOpsRoot: asNonEmptyString(cfg.windowsOpsRoot),
    defaultMaxFilesPerRun: asLimit(cfg.defaultMaxFilesPerRun),
  };
}
